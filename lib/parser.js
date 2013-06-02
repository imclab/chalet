var EventEmitter = require('events').EventEmitter,
    util = require('util');

var Reader = require('hiredis').Reader;

var Parser = module.exports = function Parser(options)
{
    EventEmitter.call(this);
    options = Object(options);
    this.returnBuffers = options.returnBuffers;
    this.reset();
};

util.inherits(Parser, EventEmitter);

Parser.prototype.returnBuffers = false;
Parser.prototype.reader = null;

Parser.prototype.reset = function reset()
{
    this.reader = new Reader({ 'return_buffers': this.returnBuffers });
};

Parser.prototype.parse = function parse(data)
{
    var reader = this.reader;
    reader.feed(data);
    for (;;)
    {
        try
        {
            var reply = reader.get();
        }
        catch (exception)
        {
            this.emit('error', exception);
            break;
        }

        if (typeof reply == 'undefined')
            break;

        this.emit('reply', reply);
    }
};
