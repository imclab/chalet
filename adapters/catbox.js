var _ = require('lodash'),
    P = require('p-promise');

var chalet = require('../');

var Connection = module.exports = function Connection(options)
{
    options = _.pick(Object(options), 'port', 'host', 'password', 'partition');

    var port = this.port = ((isFinite(options.port) && options.port > -1) ? options : this).port,
        host = this.host = (_.isString(options.host) ? options : this).host;

    this.password = options.password;
    this.partition = options.partition;
    this.client = chalet.createClient(port, host);
};

Connection.prototype.port = 6379;
Connection.prototype.host = '127.0.0.1';

Connection.prototype.password = null;
Connection.prototype.partition = null;
Connection.prototype.client = null;

Connection.prototype.start = function start(callback)
{
    var self = this;

    if (this.isReady())
        return callback();

    var deferred = P.defer(),
        promise = deferred.promise;

    if (this.password)
        promise = promise.then(function() { return self.client.authenticateWith(self.password); });

    this.client.once('ready', deferred.resolve).
                once('error', deferred.reject);

    self.client.connect().then(function() { return promise; }).then(function() { callback(null); }, callback).done();
};

Connection.prototype.stop = function stop()
{
    this.client.send('quit');
};

Connection.prototype.isReady = function isReady()
{
    return this.client && this.client.isReady;
};

Connection.prototype.validateSegmentName = function validateSegmentName(name)
{
    return (/^[^\0]+$/).test(name) ? null : new Error('Missing or invalid segment name.');
};

Connection.prototype.get = function get(key, callback)
{
    if (!this.isReady())
        return callback(new Error('The connection is unavailable.'));

    return this.client.send('get', this.generateKey(key)).then(function(result)
    {
        return _.isString(result) ? JSON.parse(result) : result;
    }).then(function(envelope)
    {
        if (!envelope)
            return null;

        if (!envelope.item || !envelope.stored)
        {
            var envelopeError = new Error('Invalid envelope structure.');
            _.assign(envelopeError, { 'envelope': envelope });
            throw envelopeError;
        }

        return envelope;
    }).then(function(envelope) { callback(null, envelope); }, callback).done();
};

Connection.prototype.set = function set(key, value, ttl, callback)
{
    var self = this;

    if (!this.isReady())
        return callback(new Error('The connection is unavailable.'));

    var envelope =
    {
        'item': value,
        'stored': Date.now(),
        'ttl': ttl
    };

    var cacheKey = this.generateKey(key);

    try
    {
        var stringifiedEnvelope = JSON.stringify(envelope);
    }
    catch (exception)
    {
        return callback(exception);
    }

    this.client.send('set', cacheKey, stringifiedEnvelope).then(function()
    {
        var ttlSec = Math.max(1, Math.floor(ttl / 1000));
        return self.client.send('expire', ttlSec);
    }).then(function() { callback(null); }, callback).done();
};

Connection.prototype.drop = function drop(key, callback)
{
    if (!this.isReady())
        return callback(new Error('The connection is unavailable.'));

    this.client.send('del', this.generateKey(key)).then(function() { callback(null); }, callback).done();
};

Connection.prototype.generateKey = function generateKey(key)
{
    return _.map([this.partition, key.segment, key.id], encodeURIComponent).join(':');
};
