var Socket = require('net').Socket,
    EventEmitter = require('events').EventEmitter,
    util = require('util'),
    crypto = require('crypto');

var P = require('p-promise'),
    _ = require('lodash');

var Parser = require('./parser'),
    Command = require('./command'),
    helpers = require('./helpers');

var Client = module.exports = function Client(options)
{
    EventEmitter.call(this);
    options = Object(options);

    if ('port' in options)
        this.port = options.port;

    if ('host' in options)
        this.host = options.host;

    this.socket = new Socket();
    this.noDelay = options.noDelay !== false;

    var maxAttempts = options.maxAttempts;
    if (isFinite(maxAttempts) && maxAttempts > 0)
        this.maxAttempts = maxAttempts;

    this.commandQueue = [];
    this.offlineQueue = [];
    this.transactionQueue = [];
    this.authQueue = [];

    this.enableOffline = options.enableOffline !== false;

    var connectTimeout = options.connectTimeout;
    if (isFinite(connectTimeout) && connectTimeout > 0)
        this.connectTimeout = connectTimeout;

    var retryMaxDelay = options.retryMaxDelay;
    if (isFinite(retryMaxDelay) && retryMaxDelay > 0)
        this.retryMaxDelay = retryMaxDelay;

    this.setRetryProperties();

    this.subscriptionToIndex = {};
    this.patternSubscriptionToIndex = {};
    this.subscriptions = [];

    this.serverInfo = {};

    this.returnBuffers = options.returnBuffers === true;
    this.detectBuffers = options.detectBuffers === true;
};

util.inherits(Client, EventEmitter);

Client.prototype.host = '127.0.0.1';
Client.prototype.port = 6379;
Client.prototype.maxAttempts = 0;
Client.prototype.connections = 0;
Client.prototype.commandsSent = 0;
Client.prototype.commandQueueLowWater = 0;
Client.prototype.commandQueueHighWater = 1000;
Client.prototype.connectTimeout = 0;

Client.prototype.retryMaxDelay = null;
Client.prototype.password = null;
Client.prototype.state = null;

Client.prototype.noDelay = true;
Client.prototype.enableOffline = true;
Client.prototype.returnBuffers = false;
Client.prototype.detectBuffers = false;

Client.prototype.socket = null;
Client.prototype.database = null;
Client.prototype.serverInfo = null;

Client.prototype.subscriptionToIndex = null;
Client.prototype.patternSubscriptionToIndex = null;
Client.prototype.subscriptions = null;

Client.prototype.commandQueue = null;
Client.prototype.offlineQueue = null;
Client.prototype.transactionQueue = null;
Client.prototype.authQueue = null;

Client.prototype.isConnected = false;
Client.prototype.isReady = false;
Client.prototype.isBuffering = false;
Client.prototype.isSubscriber = false;
Client.prototype.isMonitoring = false;
Client.prototype.isClosing = false;
Client.prototype.isQueueing = false;

Client.prototype.onDrain = function onDrain()
{
    this.isBuffering = false;
    this.emit('drain');
};

Client.prototype.setRetryProperties = function setRetryProperties()
{
    this.retryTimer = null;
    this.retryTotalTime = 0;
    this.retryDelay = 150;
    this.retryBackoff = 1.7;
    this.attempts = 1;
};

Client.prototype.rejectAllQueues = function rejectAllQueues(message)
{
    _.forEach([this.offlineQueue, this.commandQueue, this.transactionQueue], function(queue)
    {
        helpers.rejectQueueWith(queue, message);
    });
};

Client.prototype.onError = function onError(message)
{
    if (this.isClosing)
        return;

    this.rejectAllQueues(message);
    this.isConnected = false;
    this.isReady = false;

    var socketError = new Error('Unexpected socket error.');
    _.assign(socketError, { 'message': message });
    this.emit('error', socketError);

    this.onClose('error');
};

Client.prototype.authenticateWith = function authenticateWith(password)
{
    var self = this;

    if (password == null)
        return P('OK');

    this.password = password;

    return this.send('auth', password).then(function(reply)
    {
        if (reply != 'OK')
        {
            var error = new Error('`authenticateWith`: Unexpected Redis reply.');
            error.reply = reply;
            throw error;
        }
        return reply;
    }, function(error)
    {
        // If Redis is still loading, authentication will not succeed and all
        // subsequent commands will be rejected. TODO The two-second delay is
        // entirely arbitrary.
        if (/Loading/i.test(error))
        {
            return helpers.waitFor(2000).then(function()
            {
                return self.authenticateWith(password);
            });
        }
        // Unexpected error; reject the transformed promise.
        if (!/(OK|No password is set)/i.test(error))
            throw error;
        return 'OK';
    });
};

Client.prototype.onConnect = function onConnect()
{
    this.isConnected = true;
    this.isReady = false;
    this.attempts = 0;
    this.connections++;
    this.setRetryProperties();

    if (this.noDelay)
        this.socket.setNoDelay();
    this.socket.setTimeout(0);

    this.initializeParser();
    this.sendAuthQueue();
};

Client.prototype.handleAuthenticate = function handleAuthenticate()
{
    this.emit('connect');
    return this.readyCheck();
};

Client.prototype.initializeParser = function initializeParser()
{
    var parser = this.parser = new Parser({ 'returnBuffers': this.returnBuffers || this.detectBuffers });
    parser.on('reply', _.bindKey(this, 'onReply'));
    parser.on('error', _.bindKey(this, 'emit', 'error'));
};

Client.prototype.onReply = function onReply(reply)
{
    if (reply instanceof Error)
        this.handleError(reply);
    else
        this.handleReply(reply);
};

Client.prototype.onReady = function onReady()
{
    var self = this;
    this.isReady = true;

    if (this.state != null)
    {
        _.assign(this, this.state);
        this.state = null;
    }

    if (this.database != null)
    {
        var isSubscriber = this.isSubscriber;
        this.isSubscriber = false;
        this.send('select', this.database);
        this.isSubscriber = isSubscriber;
    }

    if (this.isSubscriber === true)
    {
        var subscriptions = _.reduce(this.subscriptions, function(subscriptions, subscription)
        {
            if (subscription == null)
                return subscriptions;
            subscriptions.push(self.send(subscription.type, subscription.channel));
            return subscriptions;
        }, []);
        // Use `allSettled` to avoid blocking the `ready` event in case one or
        // more subscriptions could not be re-established.
        return P.allSettled(subscriptions).then(function()
        {
            self.sendOfflineQueue();
            self.emit('ready');
        });
    }
    if (this.isMonitoring === true)
        this.send('monitor');
    else
        this.sendOfflineQueue();
    this.emit('ready');
};

Client.prototype.parseInfo = function parseInfo(reply)
{
    var self = this;

    var serverInfo = this.serverInfo = {};
    String(reply).replace(/^\s*([^:\W]+):(.*?)\s*$/gm, function(match, key, value)
    {
        serverInfo[key] = value;
    });

    if (!serverInfo.loading || serverInfo.loading == 0)
        return this.onReady();

    var retryTime = Math.max(1000, serverInfo.loading_eta_seconds * 1000);
    return helpers.waitFor(retryTime).then(function()
    {
        return self.readyCheck();
    });
};

Client.prototype.readyCheck = function readyCheck()
{
    var self = this;
    return this.send('info').then(function(reply)
    {
        return self.parseInfo(reply);
    });
};

Client.prototype.sendQueue = function sendQueue(offlineQueue)
{
    var bufferedWrites = 0,
        promises = [];

    while (offlineQueue.length)
    {
        var command = offlineQueue.shift(),
            bufferedWrite = this.writeCommand(command);

        if (bufferedWrite instanceof Error)
        {
            this.emit('error', bufferedWrite);
            continue;
        }

        bufferedWrites += bufferedWrite;
        promises.push(command.promise);
    }

    if (!bufferedWrites)
        this.onDrain();

    return promises;
};

Client.prototype.sendAuthQueue = function sendAuthQueue()
{
    var authQueue = this.authQueue,
        promise = authQueue.length ? P.allSettled(this.sendQueue(authQueue)) : this.authenticateWith(this.password);

    return promise.then(_.bindKey(this, 'handleAuthenticate'));
};

Client.prototype.sendOfflineQueue = function sendOfflineQueue()
{
    return this.sendQueue(this.offlineQueue);
};

Client.prototype.onClose = function onClose(type)
{
    var self = this;

    if (this.retryTimer)
        return;

    this.isConnected = false;
    this.isReady = false;

    if (this.state == null)
    {
        this.state =
        {
            'isMonitoring': this.isMonitoring,
            'isSubscriber': this.isSubscriber,
            'database': this.database
        };
        this.isMonitoring = false;
        this.isSubscriber = false;
        this.database = null;
    }

    if (type == 'end')
        this.emit('end');

    if (this.retryMaxDelay != null && this.retryDelay > this.retryMaxDelay)
        this.retryDelay = this.retryMaxDelay;
    else
        this.retryDelay = Math.floor(this.retryDelay * this.retryBackoff);

    var rejectionError = new Error('The Redis connection has been terminated.');
    _.assign(rejectionError, { 'event': type, 'retryDelay': this.retryDelay, 'retryMaxDelay': this.retryMaxDelay });
    this.rejectAllQueues(rejectionError);

    if (this.isClosing || this.maxAttempts && this.attempts >= this.maxAttempts)
    {
        clearTimeout(this.retryTimer);
        this.retryTimer = null;
        return;
    }

    this.attempts++;
    this.emit('reconnecting', { 'delay': this.retryDelay, 'attempt': this.attempts });
    this.retryTimer = setTimeout(function()
    {
        self.retryTotalTime += self.retryDelay;
        if (self.connectTimeout && self.retryTotalTime >= self.connectTimeout)
        {
            clearTimeout(self.retryTimer);
            self.retryTimer = null;
            return;
        }
        self.socket.connect(self.port, self.host);
        self.retryTimer = null;
    }, this.retryDelay);
};

Client.prototype.onData = function onData(data)
{
    this.parser.parse(data);
};

Client.prototype.handleError = function handleError(error)
{
    var command = this.commandQueue.shift(),
        queueLength = this.commandQueue.length;

    if (this.isSubscriber === false && !queueLength)
        this.emit('idle');

    if (this.isBuffering && queueLength <= this.commandQueueLowWater)
        this.onDrain();

    if (!command)
        return this.emit('error', error);

    command.rejectWith(error);

    var transactionQueue = this.transactionQueue;
    if (command.isQueued)
    {
        // If the command is part of a transaction, remove it from the
        // transaction queue.
        var commandIndex = _.indexOf(transactionQueue, command);
        if (commandIndex > -1)
            transactionQueue.splice(commandIndex, 1);
        command.isQueued = false;
    }

    // Flush the transaction queue for rejected `exec` and `discard`
    // operations.
    var isEnding = command.endsQueue;
    if (isEnding || command.discardsQueue)
    {
        while (transactionQueue.length)
        {
            var queuedCommand = transactionQueue.shift();
            queuedCommand.isQueued = false;
            queuedCommand[isEnding ? 'fulfillWith' : 'rejectWith'](null);
        }
        command.endsQueue = command.discardsQueue = false;
    }
};

Client.prototype.handleReply = function handleReply(reply)
{
    var isArrayReply = Array.isArray(reply),
        command = null,
        type = isArrayReply && reply[0] != null ? String(reply[0]) : null;

    // Avoid shifting the command queue if the reply is a message broadcast
    // on a channel that the client is subscribed to.
    if (!/^p?message$/i.test(type))
        command = this.commandQueue.shift();

    var queueLength = this.commandQueue.length;

    if (this.isSubscriber === false && !queueLength)
        this.emit('idle');

    if (this.isBuffering && queueLength <= this.commandQueueLowWater)
        this.onDrain();

    if (command && !command.isSubscription)
        return this.fulfillCommand(command, reply);

    if (this.isSubscriber || command && command.isSubscription)
        return this.fulfillSubscription(command, reply);

    if (this.isMonitoring)
        return this.fulfillMonitor(reply);

    var unexpectedResponseError = new Error('Received unexpected Redis reply.');
    _.assign(unexpectedResponseError, { 'type': type, 'reply': reply, 'commandQueue': this.commandQueue.slice(0) });
    this.emit('error', unexpectedResponseError);
};

Client.prototype.fulfillCommand = function fulfillCommand(command, reply)
{
    // If the `detectBuffers` option is enabled, match the command's
    // requested return type.
    if (this.detectBuffers && !command.hasBuffers)
        reply = helpers.decodeBuffers(reply);

    if (!command.endsQueue && !command.discardsQueue)
    {
        // Keep queued commands in the pending state until the queue is
        // flushed by an `exec` or `discard` operation.
        if (command.isQueued)
            return;

        // Fulfill other commands.
        return command.fulfillWith(reply);
    }

    // Handle `exec` and `discard` operations.
    var isEnding = command.endsQueue,
        isArrayReply = Array.isArray(reply),
        transactionQueue = this.transactionQueue;

    while (transactionQueue.length)
    {
        // Flush the transaction queue.
        var queuedCommand = transactionQueue.shift();
        queuedCommand.isQueued = false;
        if (isEnding)
            queuedCommand.fulfillWith(isArrayReply ? reply.shift() : null);
        else
            queuedCommand.rejectWith(null);
    }

    // Fulfill the `exec` or `discard` command.
    command.endsQueue = command.discardsQueue = false;
    return command.fulfillWith(reply);
};

Client.prototype.fulfillSubscription = function fulfillSubscription(command, reply)
{
    if (!Array.isArray(reply) && !this.isClosing)
    {
        var invalidError = new Error('Expected an array response for a subscription event.');
        _.assign(invalidError, { 'command': command, 'reply': reply });
        return this.emit('error', invalidError);
    }

    var type = reply.shift();

    if (/^message$/i.test(type))
        return this.emit('message', String(reply[0]), reply[1]);

    if (/^pmessage$/i.test(type))
        return this.emit('pmessage', String(reply[0]), String(reply[1]), reply[2]);

    if (/^p?(?:subscribe|unsubscribe)$/i.test(type))
    {
        var channelId = reply[0] === null ? reply[0] : String(reply[0]),
            subscribedChannels = reply[1];

        // Exit subscriber mode once all subscriptions have been removed.
        this.isSubscriber = subscribedChannels !== 0;

        if (command)
            command.fulfillWith(channelId);

        return this.emit(type, channelId, subscribedChannels);
    }

    var unrecognizedError = new Error('Unexpected subscription event type.');
    _.assign(unrecognizedError, { 'command': command, 'reply': reply, 'type': type });
    this.emit('error', unrecognizedError);
};

Client.prototype.fulfillMonitor = function fulfillMonitor(reply)
{
    var length = reply.indexOf(' '),
        timestamp = reply.slice(0, length),
        argumentIndex = reply.indexOf('"'),
        parameters = _.invoke(reply.slice(argumentIndex + 1, -1).split('" "'), 'replace', (/\\"/g), '"');

    this.emit('monitor', timestamp, parameters);
};

Client.prototype.send = function send(name)
{
    var deferred = P.defer(),
        parameters = _.rest(arguments);

    if (parameters.length == 1 && Array.isArray(parameters[0]))
        parameters.push.apply(parameters, parameters.pop());

    if (parameters.length > 1)
    {
        var lastParameter = parameters.pop();
        if (/^s(?:add|rem)$/i.test(name) && Array.isArray(lastParameter))
            parameters.push.apply(parameters, lastParameter);
        else
        {
            parameters.push(lastParameter);
            if (/^set(ex)?$/i.test(name) && lastParameter == null)
            {
                var argumentError = new Error('This command requires an argument value.');
                _.assign(argumentError, { 'name': name, 'parameters': parameters });
                deferred.reject(argumentError);
                return deferred.promise;
            }
        }
    }

    if (/^hmset$/i.test(name) && parameters.length == 2)
    {
        var lastParameter = parameters.pop();
        if (Array.isArray(lastParameter))
            parameters.push.apply(parameters, lastParameter);
        else if (_.isObject(lastParameter))
        {
            _.forOwn(lastParameter, function(value, key)
            {
                parameters.push(key, value);
            });
        }
        else
            parameters.push(lastParameter);
    }

    var command = new Command(name, parameters),
        bufferedWrite = this.writeCommand(command);

    if (bufferedWrite instanceof Error)
    {
        deferred.reject(bufferedWrite);
        return deferred.promise;
    }

    return command.promise;
};

Client.prototype.writeCommand = function writeCommand(command)
{
    var name = command.name;

    var isAuthentication = /^auth$/i.test(name),
        isReady = this.isReady || isAuthentication || /^info$/i.test(name),
        isWritable = this.socket.writable,
        isOffline = !isReady || !isWritable;

    if (isOffline && !this.enableOffline)
    {
        var connectionError = new Error('Offline queueing has been disabled for this connection.');
        _.assign(connectionError, { 'command': command, 'isReady': isReady, 'isOffline': isOffline });
        return connectionError;
    }

    if (isAuthentication && !command.isAuthentication && !isWritable)
    {
        this.authQueue.push(command);
        command.isAuthentication = true;
        return command.promise;
    }

    if (isOffline)
    {
        this.offlineQueue.push(command);
        this.isBuffering = true;
        return false;
    }

    if (/^p?(un)?subscribe$/i.test(name))
        this.updateSubscribers(command);
    else if (/^monitor$/i.test(name))
        this.isMonitoring = true;
    else if (/^quit$/i.test(name))
        this.isClosing = true;
    else if (this.isSubscriber)
    {
        var modeError = new Error('A client in subscriber mode cannot issue commands.');
        _.assign(modeError, { 'command': command });
        return modeError;
    }

    this.commandQueue.push(command);

    var name = command.name;
    if (/^exec$/i.test(name))
    {
        this.isQueueing = false;
        command.endsQueue = true;
    }
    else if (/^discard$/i.test(name))
    {
        this.isQueueing = false;
        command.discardsQueue = true;
    }
    else if (this.isQueueing)
    {
        this.transactionQueue.push(command);
        command.isQueued = true;
    }
    else if (/^multi$/i.test(name))
    {
        this.isQueueing = true;
        command.beginsQueue = true;
    }

    this.commandsSent++;

    var bufferedWrites = command.writeTo(this.socket);
    if (bufferedWrites || this.commandQueue.length >= this.commandQueueHighWater)
        this.isBuffering = true;

    return !this.isBuffering;
};

Client.prototype.updateSubscribers = function updateSubscribers(command)
{
    var subscriptions = this.subscriptions,
        name = command.name,
        isSubscribe = /^p?subscribe$/i.test(name),
        isPatternCommand = /^p/i.test(name),
        channelToIndex = this[isPatternCommand ? 'patternSubscriptionToIndex' : 'subscriptionToIndex'];

    this.isSubscriber = true;
    command.isSubscription = true;

    _.forEach(command.parameters, function(channel)
    {
        var subscriptionIndex = channelToIndex[channel];

        if (subscriptionIndex != null)
        {
            // Exit early if a `subscribe` or `psubscribe` command is already
            // queued for a channel. If an `unsubscribe` or `punsubscribe`
            // command was issued; dequeue the subscription command.
            if (!isSubscribe)
                channelToIndex[channel] = subscriptions[subscriptionIndex] = null;
            return;
        }

        // Queue the subscription command, storing its index in the
        // appropriate channel-to-index map.
        if (isSubscribe)
            channelToIndex[channel] = subscriptions.push({ 'type': name, 'channel': channel }) - 1;
    });
};

Client.prototype.end = function end()
{
    this.socket.removeAllListeners();
    this.isConnected = false;
    this.isReady = false;
    this.isClosing = true;
    return this.socket.destroySoon();
};

Client.prototype.connect = function connect()
{
    var deferred = P.defer(),
        socket = this.socket,
        self = this;

    socket.on('connect', _.bindKey(this, 'onConnect'));
    socket.once('connect', function()
    {
        deferred.resolve(self);
    });

    socket.on('error', _.bindKey(this, 'onError'));
    socket.once('error', deferred.reject);

    socket.on('data', _.bindKey(this, 'onData'));
    socket.on('drain', _.bindKey(this, 'onDrain'));

    socket.on('close', _.bindKey(this, 'onClose', 'close'));
    socket.on('end', _.bindKey(this, 'onClose', 'end'));

    socket.connect({ 'host': this.host, 'port': this.port });

    return deferred.promise;
};

Client.prototype.select = function select(database)
{
    var self = this;
    return this.send('select', database).then(function(reply)
    {
        self.database = database;
        return reply;
    });
};

Client.prototype.transact = function transact(commands)
{
    var self = this;
    return P.all(
    [
        this.send('multi'),
        P.allSettled(_.map(commands, function(command)
        {
            return self.send.apply(self, command);
        })),
        this.send('exec')
    ]).spread(function(beginTransaction, queuedWrites, endTransaction)
    {
        return queuedWrites;
    });
};

Client.prototype.eval = function evaluate(source)
{
    var self = this,
        parameters = _.rest(arguments);

    parameters.unshift(crypto.createHash('sha1').update(source).digest('hex'));

    return this.send.apply(this, ['evalsha'].concat(parameters)).fail(function(error)
    {
        if (/noscript/i.test(error))
        {
            parameters.shift();
            parameters.unshift('eval', source);
            return self.send.apply(self, parameters);
        }
        throw error;
    });
};
