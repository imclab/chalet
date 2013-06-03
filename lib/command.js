var P = require('p-promise'),
    _ = require('lodash');

var helpers = require('./helpers');

var Command = module.exports = function Command(name, parameters)
{
    this.name = name;
    this.parameters = parameters;
    this.hasBuffers = false;
    this.deferred = P.defer();
};

Command.prototype.name = '';
Command.prototype.deferred = null;
Command.prototype.parameters = null;

Command.prototype.isSubscription = false;
Command.prototype.isAuthentication = false;

Command.prototype.isQueued = false;
Command.prototype.beginsQueue = false;
Command.prototype.endsQueue = false;
Command.prototype.discardsQueue = false;

Object.defineProperty(Command.prototype, 'promise',
{
    'get': function get()
    {
        return this.deferred.promise;
    },
    'enumerable': true,
    'configurable': true
});

Command.prototype.fulfillWith = function fulfillWith(argument)
{
    var deferred = this.deferred;
    if (!/^hgetall$/i.test(this.name) || !Array.isArray(argument))
    {
        deferred.resolve(argument);
        return deferred.promise;
    }
    var length = argument.length;
    if (!length)
    {
        deferred.resolve(null);
        return deferred.promise;
    }
    // Expand `hgetall` results.
    var results = {};
    for (var index = 0; index < length; index += 2)
    {
        results[argument[index]] = argument[index + 1];
    }
    deferred.resolve(results);
    return deferred.promise;
};

Command.prototype.rejectWith = function rejectWith(message)
{
    var error = message instanceof Error ? message : new Error(message),
        deferred = this.deferred;
    error.command = this;
    deferred.reject(error);
    return deferred.promise;
};

Command.prototype.writeTo = function writeTo(socket)
{
    var parameters = [this.name].concat(this.parameters);

    var commandHeader = '*' + parameters.length + '\r\n',
        hasBuffers = this.hasBuffers = _.some(parameters, Buffer.isBuffer);

    // Fast path: no buffers; serialize the arguments and make one write to the
    // socket.
    if (!hasBuffers)
        return socket.write(commandHeader + _.map(parameters, helpers.serializeArgument).join(''));

    // Slow path: a buffer has been specified as an argument. Account for
    // three cases: an empty buffer, a string, and a full buffer. The full
    // buffer is slowest because the data must be padded with the byte
    // length and terminal CRLF, which requires three socket writes.
    return _.reduce(parameters, function(bufferedWrites, parameter)
    {
        var isBuffer = Buffer.isBuffer(parameter),
            argument = isBuffer ? parameter : String(parameter);

        if (!argument.length)
        {
            var bodyString = isBuffer ? '$0\r\n\r\n' : helpers.serializeArgument(argument);
            return bufferedWrites + socket.write(bodyString);
        }

        return bufferedWrites + socket.write('$' + argument.length + '\r\n') + socket.write(argument) + socket.write('\r\n');
    }, socket.write(commandHeader));
};
