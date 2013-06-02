var _ = require('lodash');

var Client = require('./lib/client'),
    Command = require('./lib/command'),
    Parser = require('./lib/parser');

var createClient = module.exports = function createClient(port, host)
{
    var isOptions = _.isObject(port),
        options = isOptions ? port : {};

    if (!isOptions)
    {
        if (_.isString(port) && (!isFinite(port) || port < 0))
            options.host = port;
        else if (_.isNumber(port))
        {
            options.port = port;
            if (_.isString(host))
                options.host = host;
        }
    }

    return new Client(options);
};

var createConnection = createClient.createConnection = function createConnection(port, host, options)
{
    var client = createClient(port, host, options);
    return client.connect();
};

_.assign(createClient,
{
    'createClient': createClient,

    // Aliased for compatibility with Node's `net` API.
    'connect': createConnection,

    'Client': Client,
    'Command': Command,
    'Parser': Parser
});
