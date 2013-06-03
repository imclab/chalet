var P = require('p-promise'),
    _ = require('lodash');

exports.waitFor = function waitFor(milliseconds)
{
    var deferred = P.defer();
    setTimeout(deferred.resolve, milliseconds);
    return deferred.promise;
};

exports.serializeArgument = function serializeArgument(argument)
{
    var value = String(argument),
        length = Buffer.byteLength(value);

    return '$' + length + '\r\n' + value + '\r\n';
};

exports.decodeBuffers = function decodeBuffers(reply)
{
    if (Buffer.isBuffer(reply))
        return String(reply);

    if (Array.isArray(reply))
        return _.map(reply, decodeBuffers);

    return reply;
};

exports.rejectQueueWith = function rejectQueueWith(queue, message)
{
    while (queue.length)
    {
        var command = queue.shift();
        command.isQueued = command.beginsQueue = command.endsQueue = command.discardsQueue = false;
        command.rejectWith(message);
    }
};
