var P = require('p-promise'),
    _ = require('lodash');

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
    var error = new Error(message),
        deferred = this.deferred;
    error.command = this;
    deferred.reject(error);
    return deferred.promise;
};

Command.prototype.writeTo = function writeTo(socket)
{
    var parameters = [this.name].concat(this.parameters),
        bufferedWrites = 0;

    var command = '*' + parameters.length + '\r\n',
        hasBuffers = this.hasBuffers = _.some(this.parameters, Buffer.isBuffer);

    if (!hasBuffers)
    {
        // Fast path: no buffers; serialize the arguments and make one write
        // to the socket.
        command += _.map(parameters, function(parameter)
        {
            var argument = String(parameter);
            return '$' + Buffer.byteLength(argument) + '\r\n' + argument;
        }).join('\r\n');

        bufferedWrites += socket.write(command + '\r\n');
        return bufferedWrites;
    }

    // Slow path: a buffer has been specified as an argument. Account for
    // three cases: an empty buffer, a string, and a full buffer. The full
    // buffer is slowest because the data must be padded with the byte
    // length and terminal CRLF, which requires three socket writes.
    bufferedWrites += socket.write(command);

    _.forEach(parameters, function(parameter)
    {
        var isBuffer = Buffer.isBuffer(parameter),
            argument = isBuffer ? parameter : String(parameter);

        if (!isBuffer || !argument.length)
        {
            var bodyString = isBuffer ? '$0\r\n\r\n' : ('$' + Buffer.byteLength(argument) + '\r\n' + argument + '\r\n');
            bufferedWrites += socket.write(bodyString);
            return;
        }

        bufferedWrites += socket.write('$' + argument.length + '\r\n');
        bufferedWrites += socket.write(argument);
        bufferedWrites += socket.write('\r\n');
    });

    return bufferedWrites;
};
