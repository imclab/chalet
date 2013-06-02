# Chalet #

**Chalet** is a promise-based Redis client for Node. It transparently supports all Redis commands, including authentication, scripting, messaging, and transactions. Like [`node_redis`](https://github.com/mranney/node_redis), Chalet supports queuing offline commands, and uses pipelining to issue commands.

## Usage ##

```js
var chalet = require('./'),
    client = chalet.createClient();

client.send('SET', 'The Raven', 'Once upon a midnight dreary...');
client.send('HSET', 'Poets', 'T.S. Eliot', 'Rhapsody on a Windy Night');
client.send('HSET', 'Poets', 'Ezra Pound', 'Ripostes');

client.connect().then(function()
{
    return client.send('HKEYS', 'Poets');
}).then(function(reply)
{
    console.log(reply);
    // => ["T.S. Eliot", "Ezra Pound"]
    return client.send('QUIT');
}).fail(function(error)
{
    console.error('An unexpected error occurred.', error);
});
```

## Contribute ##

Check out a working copy of the Chalet source code with [Git](http://git-scm.com/):

    $ git clone git://github.com/kitcambridge/chalet.git
    $ cd chalet
    $ npm install

If you'd like to contribute a feature or bug fix, you can [fork](http://help.github.com/fork-a-repo/) Chalet, commit your changes, and [send a pull request](http://help.github.com/send-pull-requests/). Please make sure to update the unit tests in the `test` directory as well.

Alternatively, you can use the [GitHub issue tracker](https://github.com/kitcambridge/chalet/issues) to submit bug reports, feature requests, and questions, or send tweets to [@kitcambridge](http://twitter.com/kitcambridge).

## License ##

* Copyright &copy; 2010-2013 [Matthew Ranney](http://ranney.com/).
* Copyright &copy; 2013 [Kit Cambridge](http://kitcambridge.be/).

Chalet is released under the MIT License.
