/*global describe: true, it: true, before: true, after: true */

var util = require('util'),
    crypto = require('crypto');

var chai = require('chai'),
    chaiAsPromised = require('chai-as-promised'),
    P = require('p-promise'),
    _ = require('lodash'),
    semver = require('semver');

var chalet = require('../'),
    helpers = require('../lib/helpers');

var expect = chai.expect,
    should = chai.should();

require('mocha-as-promised')();
chai.use(chaiAsPromised);

describe('Chalet', function()
{
    this.timeout(0);

    var port = 6379,
        host = '127.0.0.1';

    var client, client2, client3, bclient, testDBNum = 15;

    before(function()
    {
        client = chalet.createClient(port, host);
        client2 = chalet.createClient(port, host);
        client3 = chalet.createClient(port, host);
        bclient = chalet.createClient({ 'port': port, 'host': host, 'returnBuffers': true });
        return P.all(_.invoke([client, client2, client3, bclient], 'connect')).should.be.fulfilled;
    });

    it('should support `FLUSHDB`', function()
    {
        return P.all(_.map([client, client2, client3], function(client)
        {
            var promise = client.select(testDBNum);
            return P.all([promise.should.be.fulfilled, promise.should.become('OK')]).should.be.fulfilled;
        })).then(function()
        {
            var promise = client.send('mset', ['flush keys 1', 'flush val 1', 'flush keys 2', 'flush val 2']);
            return P.all([promise.should.be.fulfilled, promise.should.become('OK')]).should.be.fulfilled;
        }).then(function()
        {
            var promise = client.send('flushdb');
            return P.all([promise.should.be.fulfilled, promise.should.become('OK')]).should.be.fulfilled;
        }).then(function()
        {
            var promise = client.send('dbsize');
            return promise.should.be.fulfilled;
        }).should.become(0);
    });

    it('should reject transactions with invalid arguments', function()
    {
        var multi1 = client.transact(
        [
            ['mset', ['multifoo', '10', 'multibar', '20']],
            ['set', ['foo2']],
            ['incr', ['multifoo']],
            ['incr', ['multibar']]
        ]);

        var expectedBar = 1,
            expectedFoo = 1;

        if (semver.lt(client.serverInfo.redis_version, '2.6.5'))
        {
            expectedBar = 22;
            expectedFoo = 12;
        }

        return P.all(
        [
            multi1.should.be.rejected,
            multi1.fail(function(results)
            {
                var promise = client.transact(
                [
                    ['incr', ['multibar']],
                    ['incr', ['multifoo']]
                ]);
                return promise.should.be.fulfilled;
            }).should.become(
            [
                { 'state': 'fulfilled', 'value': expectedBar },
                { 'state': 'fulfilled', 'value': expectedFoo }
            ])
        ]).should.be.fulfilled;
    });

    it('should not persist updates for aborted transactions', function()
    {
        var promise = client.transact(
        [
            ['mget', ['multifoo', 'multibar']],
            ['set', ['foo2']],
            ['incr', ['multifoo']],
            ['incr', ['multibar']]
        ]);

        if (semver.lt(client.serverInfo.redis_version, '2.6.5'))
            return P();

        return P.all(
        [
            promise.should.be.rejected,
            // The values should remain unchanged.
            promise.fail(function()
            {
                var promise = client.send('mget', ['multifoo', 'multibar']);
                return promise.should.be.fulfilled;
            }).should.become(['1', '1'])
        ]).should.be.fulfilled;
    });

    it('should be supported in conjunction with pipelining', function()
    {
        var additions = P.all(
        [
            client.send('sadd', ['some set', 'mem 1']),
            client.send('sadd', ['some set', 'mem 2']),
            client.send('sadd', ['some set', 'mem 3']),
            client.send('sadd', ['some set', 'mem 4'])
        ]);

        return P.all(
        [
            additions.should.be.fulfilled,
            additions.should.become([1, 1, 1, 1]),
            additions.then(function()
            {
                var deletion = client.send('del', ['some missing set']);
                return deletion.should.be.fulfilled;
            }).then(function()
            {
                var members = client.send('smembers', ['some missing set']);
                return P.all([members.should.be.fulfilled, members.should.become([])]);
            }).then(function()
            {
                var transaction = client.transact([
                    ['smembers', ['some set']],
                    ['del', ['some set']],
                    ['smembers', ['some set']],
                    ['scard', ['some set']]
                ]);
                return transaction.should.be.fulfilled;
            })
        ]).should.be.fulfilled;
    });

    it('should fulfill transaction promises with their respective values', function()
    {
        var transaction = client.transact(
        [
            ['mset', ['some', '10', 'keys', '20']],
            ['incr', ['some']],
            ['incr', ['keys']],
            ['mget', ['some', 'keys']]
        ]);

        return P.all(
        [
            transaction.should.be.fulfilled,
            transaction.should.become([
                { 'state': 'fulfilled', 'value': 'OK' },
                { 'state': 'fulfilled', 'value': 11 },
                { 'state': 'fulfilled', 'value': 21 },
                { 'state': 'fulfilled', 'value': ['11', '21'] }
            ])
        ]).should.be.fulfilled;
    });

    it('should support aggregate values', function()
    {
        var transaction = client.transact(
        [
            ['mget', ['multifoo', 'some', 'random', 'keys']],
            ['incr', ['multifoo']]
        ]);

        return P.all([
            transaction.should.be.fulfilled,
            transaction.should.eventually.have.property('length', 2),
            transaction.spread(function(mget, incr)
            {
                return mget.value.length;
            }).should.become(4)
        ]).should.be.fulfilled;
    });

    it('should support expanding `HGETALL` results', function()
    {
        var transaction = client.transact(
        [
            ['hmset', ['multihash', 'a', 'foo', 'b', 1]],
            ['hmset', ['multihash', { 'extra': 'fancy', 'things': 'here' }]],
            ['hgetall', ['multihash']]
        ]);

        return P.all(
        [
            transaction.should.be.fulfilled,
            transaction.should.become([
                { 'state': 'fulfilled', 'value': 'OK' },
                { 'state': 'fulfilled', 'value': 'OK' },
                { 'state': 'fulfilled', 'value': { 'a': 'foo', 'b': '1', 'extra': 'fancy', 'things': 'here' } }
            ])
        ]).should.be.fulfilled;
    });

    it('should reject invalid transactions with the correct error', function()
    {
        if (semver.lt(client.serverInfo.redis_version, '2.6.5'))
            return P();
        var transaction = client.transact([['set', ['foo']]]);
        return transaction.should.be.rejected.with(/execabort/i);
    });

    it('should forward subscription messages', function()
    {
        var name = 'errorForwarding',
            deferredMessage = P.defer();

        client3.on('message', function(channel, data)
        {
            if (channel == name)
                deferredMessage.resolve('Some message');
        });
        client3.send('subscribe', name);

        client.send('publish', name, 'Some message');

        return deferredMessage.promise.should.become('Some message');
    });

    it('should support `EVAL`', function()
    {
        if (semver.lt(client.serverInfo.redis_version, '2.5.0'))
            return P();

        var source = "return redis.call('get', 'sha test')",
            commandSha = crypto.createHash('sha1').update(source).digest('hex');

        return P.all(
        [
            // test {EVAL - Lua integer -> Redis protocol type conversion}
            client.eval('return 100.5', 0).should.become(100),

            // test {EVAL - Lua string -> Redis protocol type conversion}
            client.eval("return 'hello world'", 0).should.become('hello world'),

            // test {EVAL - Lua true boolean -> Redis protocol type conversion}
            client.eval('return true', 0).should.become(1),

            // test {EVAL - Lua false boolean -> Redis protocol type conversion}
            client.eval('return false', 0).should.become(null),

            // test {EVAL - Lua status code reply -> Redis protocol type conversion}
            client.eval("return {ok='fine'}", 0).should.become('fine'),

            // test {EVAL - Lua error reply -> Redis protocol type conversion}
            client.eval("return {err='this is an error'}", 0).should.be.rejected,

            // test {EVAL - Lua table -> Redis protocol type conversion}
            client.eval("return {1,2,3,'ciao',{1,2}}", 0).should.become([1, 2, 3, 'ciao', [1, 2]]),

            // test {EVAL - Are the KEYS and ARGS arrays populated correctly?}
            client.eval('return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}', 2, 'a', 'b', 'c', 'd').should.become(['a', 'b', 'c', 'd']),

            client.send('set', 'sha test', 'eval get sha test').then(function()
            {
                // test {EVAL - is Lua able to call Redis API?}
                var promise = client.eval(source, 0);
                return P.all(
                [
                    promise.should.become('eval get sha test'),
                    // test {EVALSHA - Can we call a SHA1 if already defined?}
                    client.send('evalsha', commandSha, 0).should.become('eval get sha test'),
                    // test {EVALSHA - Do we get an error on non defined SHA1?}
                    client.send('evalsha', 'ffffffffffffffffffffffffffffffffffffffff', 0).should.be.rejected
                ]).should.be.fulfilled;
            }),

            client.send('set', 'incr key', 0).then(function()
            {
                // test {EVAL - Redis integer -> Lua type conversion}
                var promise = client.eval("local foo = redis.call('incr','incr key')\n" + "return {type(foo),foo}", 0);
                return promise.should.become(['number', 1]);
            }).should.be.fulfilled,

            client.send('set', 'bulk reply key', 'bulk reply value').then(function()
            {
                // test {EVAL - Redis bulk -> Lua type conversion}
                var promise = client.eval("local foo = redis.call('get','bulk reply key'); return {type(foo),foo}", 0);
                return promise.should.become(['string', 'bulk reply value']);
            }).should.be.fulfilled,

            // test {EVAL - Redis multi bulk -> Lua type conversion}
            client.transact([
                ['del', ['mylist']],
                ['rpush', ['mylist', 'a']],
                ['rpush', ['mylist', 'b']],
                ['rpush', ['mylist', 'c']]
            ]).then(function()
            {
                var promise = client.eval("local foo = redis.call('lrange','mylist',0,-1); return {type(foo),foo[1],foo[2],foo[3],# foo}", 0);
                return promise.should.be.fulfilled;
            }).should.be.fulfilled,

            // test {EVAL - Redis status reply -> Lua type conversion}
            client.eval("local foo = redis.call('set','mykey','myval'); return {type(foo),foo['ok']}", 0).should.become(['table', 'OK']),

            // test {EVAL - Redis error reply -> Lua type conversion}
            client.send('set', 'error reply key', 'error reply value').then(function()
            {
                var promise = client.eval("local foo = redis.pcall('incr','error reply key'); return {type(foo),foo['err']}", 0);
                return promise.should.become(['table', 'ERR value is not an integer or out of range']);
            }).should.be.fulfilled,

            // test {EVAL - Redis nil bulk reply -> Lua type conversion}
            client.send('del', 'nil reply key').then(function()
            {
                var promise = client.eval("local foo = redis.call('get','nil reply key'); return {type(foo),foo == false}", 0);
                return promise.should.become(['boolean', 1]);
            }).should.be.fulfilled
        ]).should.be.fulfilled;
    });

    it('should support `SCRIPT LOAD`', function()
    {
        var command = 'return 1',
            commandSha = crypto.createHash('sha1').update(command).digest('hex');

        if (semver.lt(client.serverInfo.redis_version, '2.6.0'))
            return P();

        var promise = bclient.send('script', 'load', command).should.become(new Buffer(commandSha));

        return promise.then(function()
        {
            var transaction = client.transact([['script', ['load', command]]]);
            return P.all(
            [
                transaction.should.be.fulfilled,
                transaction.spread(function(promise)
                {
                    return promise;
                }).should.become({ 'state': 'fulfilled', 'value': commandSha })
            ]);
        }).should.be.fulfilled;
    });

    it('should support `CLIENT LIST`', function()
    {
        if (semver.lt(client.serverInfo.redis_version, '2.4.0'))
            return P();

        var promise = bclient.send('client', 'list').then(function()
        {
            return client.transact([['client', 'list']]);
        }).spread(function(reply)
        {
            return String(reply.value).split('\n').slice(0, -1);
        });

        return P.all(
        [
            promise.should.be.fulfilled,
            promise.then(function(lines)
            {
                return lines.length;
            }).should.eventually.be.at.least(4),
            promise.then(function(lines)
            {
                return lines.every(function(line)
                {
                    return (/^addr=/).test(line);
                });
            }).should.be.ok
        ]).should.be.fulfilled;
    });

    it('should support `WATCH` when used with transactions and pipelines', function()
    {
        var key = 'watchMulti';

        if (semver.lt(client.serverInfo.redis_version, '2.2.0'))
            return P();

        client.send('watch', key);
        client.send('incr', key);
        var transaction = client.transact([['incr', ['watch multi']]]);
        return P.all(
        [
            transaction.should.be.fulfilled,
            transaction.should.become([{ 'state': 'fulfilled', 'value': null }])
        ]).should.be.fulfilled;
    });

    it('should support `WATCH` when used with a transaction', function()
    {
        if (semver.lt(client.serverInfo.redis_version, '2.1.0'))
            return P();

        var key = 'watchTransaction';

        client.send('set', 'unwatched', 200);
        client.send('set', key, 0);
        client.send('watch', key);
        client.send('incr', key);

        var transaction = client.transact([['incr', [key]]]);

        return P.all([
            transaction.should.be.fulfilled,
            transaction.should.become([{ 'state': 'fulfilled', 'value': null }]),
            transaction.then(function()
            {
                var promise = client.send('get', 'unwatched');
                return promise.should.be.fulfilled;
            }).should.become('200'),
            transaction.then(function()
            {
                return client.send('set', 'unrelated', 100);
            }).should.become('OK')
        ]).should.be.fulfilled;
    });

    it('should detect buffers if the `detectBuffers` option is set', function()
    {
        var detectClient = chalet.createClient({ 'port': port, 'host': host, 'detectBuffers': true }),
            deferred = P.defer();

        detectClient.on('ready', deferred.resolve);
        detectClient.connect();

        return deferred.promise.then(function()
        {
            return P.all(
            [
                detectClient.send('set', 'string key 1', 'string value'),
                detectClient.send('get', 'string key 1').should.become('string value'),
                detectClient.send('get', new Buffer('string key 1')).should.become(new Buffer('string value')),

                detectClient.send('hmset', 'hash key 2', 'key 1', 'val 1', 'key 2', 'val 2'),
                detectClient.send('hmget', new Buffer('hash key 2'), 'key 1', 'key 2').should.become([new Buffer('val 1'), new Buffer('val 2')]),

                // mranney/node_redis#344.
                detectClient.send('hmget', 'hash key 2', 'key 3', 'key 4').should.become([null, null]),

                detectClient.send('hgetall', 'hash key 2').should.become({ 'key 1': 'val 1', 'key 2': 'val 2' }),
                detectClient.send('hgetall', new Buffer('hash key 2')).should.become({ 'key 1': new Buffer('val 1'), 'key 2': new Buffer('val 2')}),

                detectClient.send('quit').should.become('OK')
            ]).should.be.fulfilled;
        });
    });

    it('should support the `noDelay` option', function()
    {
        var noDelayClient = chalet.createClient({ 'port': port, 'host': host, 'noDelay': true }),
            delayClient = chalet.createClient({ 'port': port, 'host': host, 'noDelay': false }),
            defaultClient = chalet.createClient(port, host);

        var noDelayReady = P.defer(),
            delayReady = P.defer(),
            defaultReady = P.defer();

        noDelayClient.on('ready', noDelayReady.resolve).connect();
        delayClient.on('ready', delayReady.resolve).connect();
        defaultClient.on('ready', defaultReady.resolve).connect();

        return P.all([noDelayReady.promise, delayReady.promise, defaultReady.promise]).then(function()
        {
            return P.all(
            [
                P(noDelayClient.noDelay).should.become(true),
                P(delayClient.noDelay).should.become(false),
                P(defaultClient.noDelay).should.become(true),

                noDelayClient.send('set', ['set key 1', 'set val']).should.become('OK'),
                noDelayClient.send('set', ['set key 2', 'set val']).should.become('OK'),
                noDelayClient.send('get', 'set key 1').should.become('set val'),
                noDelayClient.send('get', 'set key 2').should.become('set val'),

                delayClient.send('set', ['set key 3', 'set val']).should.become('OK'),
                delayClient.send('set', ['set key 4', 'set val']).should.become('OK'),
                delayClient.send('get', 'set key 3').should.become('set val'),
                delayClient.send('get', 'set key 4').should.become('set val'),

                defaultClient.send('set', ['set key 5', 'set val']).should.become('OK'),
                defaultClient.send('set', ['set key 6', 'set val']).should.become('OK'),
                defaultClient.send('get', 'set key 5').should.become('set val'),
                defaultClient.send('get', 'set key 6').should.become('set val'),

                noDelayClient.send('quit').should.become('OK'),
                delayClient.send('quit').should.become('OK'),
                defaultClient.send('quit').should.become('OK')
            ]);
        }).should.be.fulfilled;
    });

    it('should reconnect after a socket hang-up', function()
    {
        var deferred = P.defer();

        client.send('set', 'recon 1', 'one');
        client.send('set', 'recon 2', 'two').then(function()
        {
            client.socket.destroy();
        });

        client.once('reconnecting', function()
        {
            client.once('connect', function()
            {
                P.all(
                [
                    client.send('select', testDBNum).should.become('OK'),
                    client.send('get', 'recon 1').should.become('one'),
                    client.send('get', 'recon 1').should.become('one'),
                    client.send('get', 'recon 2').should.become('two'),
                    client.send('get', 'recon 2').should.become('two')
                ]).then(deferred.resolve, deferred.reject);
            });
        });

        return deferred.promise;
    });

    it('should select the correct database and restore subscriptions when reconnecting', function(done)
    {
        var deferred = P.defer(),
            key = 'subscriptionTest';

        client.select(testDBNum);
        client.send('set', key, 'one');
        client.send('subscribe', 'ChannelV').then(function()
        {
            client.socket.destroy();
        });

        client.once('reconnecting', function()
        {
            client.once('connect', function()
            {
                var promise = client.send('unsubscribe', 'ChannelV').then(function()
                {
                    return client.send('get', key);
                });

                P.all(
                [
                    promise.should.be.fulfilled,
                    promise.should.become('one')
                ]).then(deferred.resolve, deferred.reject);
            });
        });

        return deferred.promise;
    });

    it('should support the `idle` event', function(done)
    {
        client.once('idle', done);
        client.send('set', 'idle', 'test');
    });

    it('should support `HSET`', function()
    {
        var key = 'test hash',
            field1 = new Buffer('0123456789'),
            value1 = new Buffer('abcdefghij'),
            field2 = new Buffer(0),
            value2 = new Buffer(0);

        return P.all(
        [
            client.send('hset', key, field1, value1).should.become(1),
            client.send('hget', key, field1).should.become(String(value1)),

            client.send('hset', key, field1, value2).should.become(0),
            client.send('hget', [key, field1]).should.become(''),

            client.send('hset', [key, field2, value1]).should.become(1),
            client.send('hset', key, field2, value2).should.become(0)
        ]).should.be.fulfilled;
    });

    it('should support `HLEN`', function()
    {
        var key = 'test hash',
            field1 = new Buffer('0123456789'),
            value1 = new Buffer('abcdefghij'),
            field2 = new Buffer(0),
            value2 = new Buffer(0);

        var promise = client.send('hset', key, field1, value1).then(function()
        {
            return client.send('hlen', key);
        });

        return P.all(
        [
            promise.should.be.fulfilled,
            promise.should.become(2)
        ]).should.be.fulfilled;
    });

    it('should support `HMSET` with buffer and array arguments', function()
    {
        var key = 'test hash',
            field1 = 'buffer',
            value1 = new Buffer('abcdefghij'),
            field2 = 'array',
            value2 = ['array contents'];

        var promise = client.send('hmset', key, field1, value1, field2, value2);
        return P.all(
        [
            promise.should.be.fulfilled,
            promise.should.become('OK')
        ]).should.be.fulfilled;
    });

    it('should support `HMGET`', function()
    {
        var key1 = 'test hash 1', key2 = 'test hash 2';

        return P.all(
        [
            client.send('hmset', key1, '0123456789', 'abcdefghij', 'some manner of key', 'a type of value').should.become('OK'),
            client.send('hmset', key2, { '0123456789': 'abcdefghij', 'some manner of key': 'a type of value' }).should.become('OK'),

            client.send('hmget', key1, '0123456789', 'some manner of key').should.become(['abcdefghij', 'a type of value']),
            client.send('hmget', key2, '0123456789', 'some manner of key').should.become(['abcdefghij', 'a type of value']),

            client.send('hmget', [key1, '0123456789']).should.become(['abcdefghij']),
            client.send('hmget', [key1, '0123456789', 'some manner of key']).should.become(['abcdefghij', 'a type of value']),

            client.send('hmget', key1, 'missing thing', 'another missing thing').should.become([null, null])
        ]).should.be.fulfilled;
    });

    it('should support `HINCRBY`', function()
    {
        return P.all(
        [
            client.send('hset', 'hash incr', 'value', 10).should.become(1),
            client.send('hincrby', 'hash incr', 'value', 1).should.become(11),
            client.send('hincrby', 'hash incr', 'value 2', 1).should.become(1)
        ]).should.be.fulfilled;
    });

    it('should pipeline `SUBSCRIBE` and `UNSUBSCRIBE` commands', function()
    {
        var client1 = client, messageCount = 0;

        var subscribeDeferred = P.defer(),
            unsubscribeDeferred = P.defer(),
            messageDeferred = P.defer();

        client1.on('subscribe', function(channel, count)
        {
            if (channel == 'chan1')
            {
                P.all(
                [
                    client2.send('publish', 'chan1', 'message 1').should.become(1),
                    client2.send('publish', 'chan2', 'message 2').should.become(1),
                    client2.send('publish', 'chan1', 'message 3').should.become(1)
                ]).then(subscribeDeferred.resolve, subscribeDeferred.reject);
            }
        });

        client1.on('unsubscribe', function(channel, count)
        {
            if (count === 0)
                client.send('incr', 'did a thing').should.become(2).then(unsubscribeDeferred.resolve, unsubscribeDeferred.reject);
        });

        client1.on('message', function(channel, message)
        {
            messageCount++;
            expect('message ' + messageCount).to.equal(message);
            if (messageCount == 3)
                client1.send('unsubscribe', 'chan1', 'chan2').then(messageDeferred.resolve, messageDeferred.reject);
        });

        return P.all(
        [
            client1.send('set', 'did a thing', 1).should.become('OK'),
            client1.send('subscribe', 'chan1', 'chan2').should.become('chan1'),
            subscribeDeferred.promise,
            unsubscribeDeferred.promise,
            messageDeferred.promise
        ]).should.be.fulfilled;
    });

    it('should support `UNSUBSCRIBE` without arguments', function()
    {
        if (semver.lt(client.serverInfo.redis_version, '2.6.11'))
            return P();

        return P.all(
        [
            client3.send('unsubscribe').should.be.fulfilled,
            client3.send('unsubscribe').should.be.fulfilled
        ]).should.be.fulfilled;
    });

    it('should support `PUNSUBSCRIBE` without arguments', function()
    {
        if (semver.lt(client.serverInfo.redis_version, '2.6.11'))
            return P();

        return P.all(
        [
            client3.send('punsubscribe').should.be.fulfilled,
            client3.send('punsubscribe').should.be.fulfilled
        ]).should.be.fulfilled;
    });

    it('should support interleaved subscriptions and publications', function()
    {
        var deferred = P.defer();

        if (semver.lt(client.serverInfo.redis_version, '2.6.11'))
            return P();

        client3.send('subscribe', 'chan3');
        client3.send('unsubscribe', 'chan3');
        client3.send('subscribe', 'chan3').then(function()
        {
            client2.send('publish', 'chan3', 'foo');
        });

        client3.on('message', function(channel, message)
        {
            deferred.resolve([channel, message]);
        });

        return deferred.promise.should.become(['chan3', 'foo']);
    });

    it('should support interleaved subscriptions with `SUBSCRIBE`', function()
    {
        if (semver.lt(client.serverInfo.redis_version, '2.6.11'))
            return P();

        client3.send('subscribe', 'chan8');
        client3.send('subscribe', 'chan9');
        client3.send('unsubscribe', 'chan9');
        client2.send('publish', 'chan8', 'something');

        var promise = client3.send('subscribe', 'chan9');
        return promise.should.be.fulfilled;
    });

    it('should support interleaved subscriptions with `PSUBSCRIBE`', function()
    {
        if (semver.lt(client.serverInfo.redis_version, '2.6.11'))
            return P();

        client3.send('psubscribe', 'abc*');
        client3.send('subscribe', 'xyz');
        client3.send('unsubscribe', 'xyz');
        client2.send('publish', 'abcd', 'something');

        var promise = client3.send('subscribe', 'xyz');
        return promise.should.be.fulfilled;
    });

    it('should allow `QUIT` to be called after `SUBSCRIBE`', function()
    {
        var deferred = P.defer();
        client3.on('end', deferred.resolve);
        client3.on('subscribe', function()
        {
            this.send('quit');
        });
        client3.send('subscribe', 'chan3');
        return deferred.promise;
    });

    it('should re-establish subscriptions after a socket hang-up', function()
    {
        var subscriber = chalet.createClient(port, host),
            publisher = chalet.createClient(port, host),
            exchangeCount = 0,
            deferred = P.defer();

        subscriber.on('message', function(channel, message)
        {
            if (channel == 'chan1')
            {
                expect(message).to.equal('hi on channel 1');
                subscriber.socket.end();
            }
            else if (channel == 'chan2')
            {
                expect(message).to.equal('hi on channel 2');
                subscriber.socket.end();
            }
            else
            {
                subscriber.send('quit');
                publisher.send('quit');
                throw new Error('Test failed.');
            }
        });

        subscriber.send('subscribe', 'chan1', 'chan2');

        publisher.once('ready', function()
        {
            subscriber.on('ready', function()
            {
                exchangeCount++;
                if (exchangeCount == 1)
                {
                    publisher.send('publish', 'chan1', 'hi on channel 1');
                    return;
                }
                if (exchangeCount == 2)
                {
                    publisher.send('publish', 'chan2', 'hi on channel 2');
                    return;
                }
                P.all([subscriber.send('quit'), publisher.send('quit')]).then(deferred.resolve, deferred.reject);
            });

            publisher.send('publish', 'chan1', 'hi on channel 1');
        });

        return P.all(
        [
            subscriber.connect(),
            publisher.connect(),
            deferred.promise
        ]).should.be.fulfilled;
    });

    it('should support `EXISTS`', function()
    {
        return P.all(
        [
            client.send('del', 'foo', 'foo2').should.eventually.be.a('number'),
            client.send('set', 'foo', 'bar').should.become('OK'),
            client.send('exists', 'foo').should.become(1),
            client.send('exists', 'foo2').should.become(0)
        ]).should.be.fulfilled;
    });

    it('should support `DEL`', function()
    {
        return P.all(
        [
            client.send('del', 'delkey').should.eventually.be.a('number'),
            client.send('set', 'delkey', 'delvalue').should.become('OK'),
            client.send('del', 'delkey').should.become(1),
            client.send('exists', 'delkey').should.become(0),
            client.send('del', 'delkey').should.become(0),
            client.send('mset', 'delkey', 'delvalue', 'delkey2', 'delvalue2').should.become('OK'),
            client.send('del', 'delkey', 'delkey2').should.become(2)
        ]).should.be.fulfilled;
    });

    it('should support `TYPE`', function()
    {
        return P.all(
        [
            client.send('set', ['string key', 'should be a string']).should.become('OK'),
            client.send('rpush', ['list key', 'should be a list']).should.eventually.be.above(0),
            client.send('sadd', ['set key', 'should be a set']).should.eventually.be.a('number'),
            client.send('zadd', ['zset key', '10.0', 'should be a zset']).should.eventually.be.a('number'),
            client.send('hset', ['hash key', 'hashtest', 'should be a hash']).should.eventually.be.a('number'),

            client.send('type', 'string key').should.become('string'),
            client.send('type', 'list key').should.become('list'),
            client.send('type', 'set key').should.become('set'),
            client.send('type', 'zset key').should.become('zset'),
            client.send('type', 'not here yet').should.become('none'),
            client.send('type', 'hash key').should.become('hash')
        ]).should.be.fulfilled;
    });

    it('should support `KEYS`', function()
    {
        var promise = client.send('mset', 'test keys 1', 'test val 1', 'test keys 2', 'test val 2');
        var keys = client.send('keys', 'test keys*');

        return P.all(
        [
            promise.should.be.fulfilled,
            promise.should.become('OK'),

            keys.should.be.fulfilled,
            keys.should.eventually.include('test keys 1'),
            keys.should.eventually.include('test keys 2')
        ]).should.be.fulfilled;
    });

    it('should support multibulk requests', function()
    {
        var keyValues = [];
        _.times(200, function(index)
        {
            var keyValue =
            [
                'multibulk:' + crypto.randomBytes(256).toString('hex'),
                'test val ' + index
            ];
            keyValues.push(keyValue);
        });

        return P.all(
        [
            client.send('mset', _.flatten(keyValues, true)).should.become('OK'),
            client.send('keys', 'multibulk:*').then(function(reply)
            {
                return reply.sort();
            }).should.become(_.pluck(keyValues, 0).sort())
        ]).should.be.fulfilled;
    });

    it('should support zero-length multibulk requests', function()
    {
        var promise = client.send('keys', 'users:*');
        return P.all(
        [
            promise.should.be.fulfilled,
            promise.should.become([])
        ]).should.be.fulfilled;
    });

    it('should support `RANDOMKEY`', function()
    {
        var mset = client.send('mset', ['test keys 1', 'test val 1', 'test keys 2', 'test val 2']);
        var randomKey = client.send('randomkey');

        return P.all(
        [
            mset.should.be.fulfilled,
            mset.should.become('OK'),
            randomKey.should.be.fulfilled,
            randomKey.should.eventually.match(/\w+/)
        ]).should.be.fulfilled;
    });

    it('should support `RENAME`', function()
    {
        return P.all(
        [
            client.send('set', ['foo', 'bar']).should.become('OK'),
            client.send('rename', ['foo', 'new foo']).should.become('OK'),
            client.send('exists', 'foo').should.become(0),
            client.send('exists', 'new foo').should.become(1)
        ]).should.be.fulfilled;
    });

    it('should support `RENAMENX`', function()
    {
        return P.all(
        [
            client.send('set', ['foo', 'bar']).should.become('OK'),
            client.send('set', ['foo2', 'bar2']).should.become('OK'),
            client.send('renamenx', ['foo', 'foo2']).should.become(0),
            client.send('exists', 'foo').should.become(1),
            client.send('exists', 'foo2').should.become(1),
            client.send('del', 'foo2').should.become(1),
            client.send('renamenx', 'foo', 'foo2').should.become(1),
            client.send('exists', 'foo').should.become(0),
            client.send('exists', 'foo2').should.become(1)
        ]).should.be.fulfilled;
    });

    it('should support `DBSIZE`', function()
    {
        return P.all(
        [
            client.send('set', 'foo', 'bar').should.become('OK'),
            client.send('dbsize').should.eventually.be.above(0)
        ]).should.be.fulfilled;
    });

    it('should support `GET` with existing keys', function()
    {
        return P.all(
        [
            client.send('set', ['get key', 'get val']).should.become('OK'),
            client.send('get', 'get key').should.become('get val')
        ]).should.be.fulfilled;
    });

    it('should support `GET` for nonexistent keys', function()
    {
        var promise = client.send('get', 'this_key_shouldnt_exist');

        return P.all(
        [
            promise.should.be.fulfilled,
            promise.should.become(null)
        ]).should.be.fulfilled;
    });

    it('should support `SET`', function()
    {
        var missingValue = client.send('set', 'set key', null);

        return P.all(
        [
            missingValue.should.be.rejected.with(/requires an argument value/),
            client.send('set', 'set key', 'set val').should.become('OK'),
            client.send('get', 'set key').should.become('set val'),
        ]).should.be.fulfilled;
    });

    it('should support `GETSET`', function()
    {
        return P.all(
        [
            client.send('set', 'getset key', 'getset val').should.become('OK'),
            client.send('getset', 'getset key', 'new getset val').should.become('getset val'),
            client.send('get', 'getset key').should.become('new getset val')
        ]).should.be.fulfilled;
    });

    it('should support `MGET`', function()
    {
        return P.all(
        [
            client.send('mset', 'mget keys 1', 'mget val 1', 'mget keys 2', 'mget val 2', 'mget keys 3', 'mget val 3').should.become('OK'),
            client.send('mget', 'mget keys 1', 'mget keys 2', 'mget keys 3').should.become(['mget val 1', 'mget val 2', 'mget val 3']),
            client.send('mget', 'mget keys 1', 'mget keys 3').should.become(['mget val 1', 'mget val 3']),
            client.send('mget', 'mget keys 1', 'some nonexistent key', 'mget keys 2').should.become(['mget val 1', null, 'mget val 2'])
        ]).should.be.fulfilled;
    });

    it('should support `SETNX`', function()
    {
        return P.all(
        [
            client.send('set', ['setnx key', 'setnx value']).should.become('OK'),
            client.send('setnx', ['setnx key', 'new setnx value']).should.become(0),
            client.send('del', 'setnx key').should.become(1),
            client.send('exists', 'setnx key').should.become(0),
            client.send('setnx', 'setnx key', 'new setnx value').should.become(1),
            client.send('exists', 'setnx key').should.become(1),
        ]).should.be.fulfilled;
    });

    it('should support `SETEX`', function()
    {
        var missingValue = client.send('setex', 'setex key', '100', null);

        return P.all(
        [
            missingValue.should.be.rejected.with(/requires an argument value/),
            client.send('setex', 'setex key', '100', 'setex val').should.become('OK'),
            client.send('exists', 'setex key').should.become(1),
            client.send('ttl', 'setex key').should.eventually.be.above(0)
        ]).should.be.fulfilled;
    });

    it('should support `MSETNX`', function()
    {
        return P.all(
        [
            client.send('mset', 'mset 1', 'val 1', 'mset 2', 'val 2', 'mset 3', 'val 3').should.become('OK'),
            client.send('msetnx', 'mset 3', 'val 3', 'mset 4', 'val 4').should.become(0),
            client.send('del', 'mset 3').should.become(1),
            client.send('msetnx', 'mset 3', 'val 3', 'mset 4', 'val 4').should.become(1),
            client.send('exists', 'mset 3').should.become(1),
            client.send('exists', 'mset 4').should.become(1)
        ]).should.be.fulfilled;
    });

    it('should support `HGETALL`', function()
    {
        return P.all(
        [
            client.send('hmset', ['hosts', 'ceejbot', '1', 'another', '23', 'home', '1234']),
            client.send('hgetall', 'hosts').should.become({ 'ceejbot': '1', 'another': '23', 'home': '1234' })
        ]).should.be.fulfilled;
    });

    it('should support `HGETALL` for nonexistent keys', function()
    {
        var promise = client.send('hgetall', 'missing');
        return P.all(
        [
            promise.should.be.fulfilled,
            promise.should.become(null)
        ]).should.be.fulfilled;
    });

    it('should support Unicode values', function()
    {
        var utf8String = 'ಠ_ಠ',
            set = client.send('set', 'utf8test', utf8String),
            get = client.send('get', 'utf8test');
        return P.all(
        [
            set.should.be.fulfilled,
            set.should.become('OK'),
            get.should.be.fulfilled,
            get.should.become(utf8String)
        ]).should.be.fulfilled;
    });

    // Set tests were adapted from Brian Hammond's `redis-node-client.js`, which
    // has a comprehensive test suite.

    it('should support `SADD`', function()
    {
        return P.all(
        [
            client.send('del', 'set0').should.be.fulfilled,
            client.send('sadd', 'set0', 'member0').should.become(1),
            client.send('sadd', 'set0', 'member0').should.become(0)
        ]).should.be.fulfilled;
    });

    it('should support `SADD` with multiple arguments', function()
    {
        return P.all(
        [
            client.send('del', 'set0').should.be.fulfilled,
            client.send('sadd', 'set0', ['member0', 'member1', 'member2']).should.become(3),
            client.send('smembers', 'set0').then(function(reply)
            {
                return reply.sort();
            }).should.become(['member0', 'member1', 'member2']),

            client.send('sadd', 'set1', ['member0', 'member1', 'member2']).should.become(3),
            client.send('smembers', 'set1').then(function(reply)
            {
                return reply.sort();
            }).should.become(['member0', 'member1', 'member2'])
        ]).should.be.fulfilled;
    });

    it('should support `SISMEMBER`', function()
    {
        return P.all(
        [
            client.send('del', 'set0').should.be.fulfilled,
            client.send('sadd', 'set0', 'member0').should.become(1),
            client.send('sismember', 'set0', 'member0').should.become(1),
            client.send('sismember', 'set0', 'member1').should.become(0)
        ]).should.be.fulfilled;
    });

    it('should support `SCARD`', function()
    {
        return P.all(
        [
            client.send('del', 'set0').should.be.fulfilled,
            client.send('sadd', 'set0', 'member0').should.become(1),
            client.send('scard', 'set0').should.become(1),
            client.send('sadd', 'set0', 'member1').should.become(1),
            client.send('scard', 'set0').should.become(2)
        ]).should.be.fulfilled;
    });

    it('should support `SREM`', function()
    {
        return P.all(
        [
            client.send('del', 'set0').should.be.fulfilled,
            client.send('sadd', 'set0', 'member0').should.become(1),
            client.send('srem', 'set0', 'foobar').should.become(0),
            client.send('srem', 'set0', 'member0').should.become(1),
            client.send('scard', 'set0').should.become(0)
        ]).should.be.fulfilled;
    });

    it('should support `SREM` with multiple arguments', function()
    {
        return P.all(
        [
            client.send('del', 'set0').should.be.fulfilled,
            client.send('sadd', 'set0', ['member0', 'member1', 'member2']).should.become(3),
            client.send('srem', 'set0', ['member1', 'member2']).should.become(2),
            client.send('smembers', 'set0').should.become(['member0']),
            client.send('sadd', 'set0', ['member3', 'member4', 'member5']).should.become(3),
            client.send('srem', 'set0', ['member0', 'member6']).should.become(1),
            client.send('smembers', 'set0').then(function(reply)
            {
                return reply.sort();
            }).should.become(['member3', 'member4', 'member5'])
        ]).should.be.fulfilled;
    });

    it('should support `SPOP`', function()
    {
        return P.all(
        [
            client.send('del', 'zzz').should.be.fulfilled,
            client.send('sadd', 'zzz', 'member0').should.become(1),
            client.send('scard', 'zzz').should.become(1),
            client.send('spop', 'zzz').should.become('member0'),
            client.send('scard', 'zzz').should.become(0)
        ]).should.be.fulfilled;
    });

    it('should support `SDIFF`', function()
    {
        return P.all(
        [
            client.send('del', 'foo').should.be.fulfilled,
            client.send('sadd', 'foo', 'x').should.become(1),
            client.send('sadd', 'foo', 'a').should.become(1),
            client.send('sadd', 'foo', 'b').should.become(1),
            client.send('sadd', 'foo', 'c').should.become(1),

            client.send('sadd', 'bar', 'c').should.become(1),

            client.send('sadd', 'baz', 'a').should.become(1),
            client.send('sadd', 'baz', 'd').should.become(1),

            client.send('sdiff', 'foo', 'bar', 'baz').then(function(reply)
            {
                return reply.sort();
            }).should.become(['b', 'x'])
        ]).should.be.fulfilled;
    });

    it('should support `SDIFFSTORE`', function()
    {
        return P.all(
        [
            client.send('del', 'foo').should.be.fulfilled,
            client.send('del', 'bar').should.be.fulfilled,
            client.send('del', 'baz').should.be.fulfilled,
            client.send('del', 'quux').should.be.fulfilled,

            client.send('sadd', 'foo', 'x').should.become(1),
            client.send('sadd', 'foo', 'a').should.become(1),
            client.send('sadd', 'foo', 'b').should.become(1),
            client.send('sadd', 'foo', 'c').should.become(1),

            client.send('sadd', 'bar', 'c').should.become(1),

            client.send('sadd', 'baz', 'a').should.become(1),
            client.send('sadd', 'baz', 'd').should.become(1),

            client.send('sdiffstore', 'quux', 'foo', 'bar', 'baz').should.become(2),

            client.send('smembers', 'quux').then(function(reply)
            {
                return reply.sort();
            }).should.become(['b', 'x'])
        ]).should.be.fulfilled;
    });

    it('should support `SMEMBERS`', function()
    {
        return P.all(
        [
            client.send('del', 'foo').should.be.fulfilled,

            client.send('sadd', 'foo', 'x').should.become(1),
            client.send('smembers', 'foo').should.become(['x']),

            client.send('sadd', 'foo', 'y').should.become(1),
            client.send('smembers', 'foo').then(function(reply)
            {
                return reply.sort();
            }).should.become(['x', 'y'])
        ]).should.be.fulfilled;
    });

    it('should support `SMOVE`', function()
    {
        return P.all(
        [
            client.send('del', 'foo').should.be.fulfilled,
            client.send('del', 'bar').should.be.fulfilled,

            client.send('sadd', 'foo', 'x').should.become(1),
            client.send('smove', 'foo', 'bar', 'x').should.become(1),
            client.send('sismember', 'foo', 'x').should.become(0),
            client.send('sismember', 'bar', 'x').should.become(1),
            client.send('smove', 'foo', 'bar', 'x').should.become(0)
        ]).should.be.fulfilled;
    });

    it('should support `SINTER`', function()
    {
        return P.all(
        [
            client.send('del', 'sa').should.be.fulfilled,
            client.send('del', 'sb').should.be.fulfilled,
            client.send('del', 'sc').should.be.fulfilled,

            client.send('sadd', 'sa', 'a').should.become(1),
            client.send('sadd', 'sa', 'b').should.become(1),
            client.send('sadd', 'sa', 'c').should.become(1),

            client.send('sadd', 'sb', 'b').should.become(1),
            client.send('sadd', 'sb', 'c').should.become(1),
            client.send('sadd', 'sb', 'd').should.become(1),

            client.send('sadd', 'sc', 'c').should.become(1),
            client.send('sadd', 'sc', 'd').should.become(1),
            client.send('sadd', 'sc', 'e').should.become(1),

            client.send('sinter', 'sa', 'sb').then(function(reply)
            {
                return reply.sort();
            }).should.become(['b', 'c']),

            client.send('sinter', 'sb', 'sc').then(function(reply)
            {
                return reply.sort();
            }).should.become(['c', 'd']),

            client.send('sinter', 'sa', 'sc').should.become(['c']),
            client.send('sinter', 'sa', 'sb', 'sc').should.become(['c'])
        ]).should.be.fulfilled;
    });

    it('should support `SINTERSTORE`', function()
    {
        return P.all(
        [
            client.send('del', 'sa').should.be.fulfilled,
            client.send('del', 'sb').should.be.fulfilled,
            client.send('del', 'sc').should.be.fulfilled,
            client.send('del', 'foo').should.be.fulfilled,

            client.send('sadd', 'sa', 'a').should.become(1),
            client.send('sadd', 'sa', 'b').should.become(1),
            client.send('sadd', 'sa', 'c').should.become(1),

            client.send('sadd', 'sb', 'b').should.become(1),
            client.send('sadd', 'sb', 'c').should.become(1),
            client.send('sadd', 'sb', 'd').should.become(1),

            client.send('sadd', 'sc', 'c').should.become(1),
            client.send('sadd', 'sc', 'd').should.become(1),
            client.send('sadd', 'sc', 'e').should.become(1),

            client.send('sinterstore', 'foo', 'sa', 'sb', 'sc').should.become(1),
            client.send('smembers', 'foo').should.become(['c'])
        ]).should.be.fulfilled;
    });

    it('should support `SUNION`', function()
    {
        return P.all(
        [
            client.send('del', 'sa').should.be.fulfilled,
            client.send('del', 'sb').should.be.fulfilled,
            client.send('del', 'sc').should.be.fulfilled,

            client.send('sadd', 'sa', 'a').should.become(1),
            client.send('sadd', 'sa', 'b').should.become(1),
            client.send('sadd', 'sa', 'c').should.become(1),

            client.send('sadd', 'sb', 'b').should.become(1),
            client.send('sadd', 'sb', 'c').should.become(1),
            client.send('sadd', 'sb', 'd').should.become(1),

            client.send('sadd', 'sc', 'c').should.become(1),
            client.send('sadd', 'sc', 'd').should.become(1),
            client.send('sadd', 'sc', 'e').should.become(1),

            client.send('sunion', 'sa', 'sb', 'sc').then(function(reply)
            {
                return reply.sort();
            }).should.become(['a', 'b', 'c', 'd', 'e'])
        ]).should.be.fulfilled;
    });

    it('should support `SUNIONSTORE`', function()
    {
        return P.all(
        [
            client.send('del', 'sa').should.be.fulfilled,
            client.send('del', 'sb').should.be.fulfilled,
            client.send('del', 'sc').should.be.fulfilled,
            client.send('del', 'foo').should.be.fulfilled,

            client.send('sadd', 'sa', 'a').should.become(1),
            client.send('sadd', 'sa', 'b').should.become(1),
            client.send('sadd', 'sa', 'c').should.become(1),

            client.send('sadd', 'sb', 'b').should.become(1),
            client.send('sadd', 'sb', 'c').should.become(1),
            client.send('sadd', 'sb', 'd').should.become(1),

            client.send('sadd', 'sc', 'c').should.become(1),
            client.send('sadd', 'sc', 'd').should.become(1),
            client.send('sadd', 'sc', 'e').should.become(1),

            client.send('sunionstore', 'foo', 'sa', 'sb', 'sc').should.become(5),
            client.send('smembers', 'foo').then(function(reply)
            {
                return reply.sort();
            }).should.become(['a', 'b', 'c', 'd', 'e'])
        ]).should.be.fulfilled;
    });

    it('should support `SORT`', function()
    {
        return P.all(
        [
            client.send('del', 'y').should.be.fulfilled,
            client.send('del', 'x').should.be.fulfilled,

            client.send('rpush', 'y', 'd').should.become(1),
            client.send('rpush', 'y', 'b').should.become(2),
            client.send('rpush', 'y', 'a').should.become(3),
            client.send('rpush', 'y', 'c').should.become(4),

            client.send('rpush', 'x', '3').should.become(1),
            client.send('rpush', 'x', '9').should.become(2),
            client.send('rpush', 'x', '2').should.become(3),
            client.send('rpush', 'x', '4').should.become(4),

            client.send('set', 'w3', '4').should.become('OK'),
            client.send('set', 'w9', '5').should.become('OK'),
            client.send('set', 'w2', '12').should.become('OK'),
            client.send('set', 'w4', '6').should.become('OK'),

            client.send('set', 'o2', 'buz').should.become('OK'),
            client.send('set', 'o3', 'foo').should.become('OK'),
            client.send('set', 'o4', 'baz').should.become('OK'),
            client.send('set', 'o9', 'bar').should.become('OK'),

            client.send('set', 'p2', 'qux').should.become('OK'),
            client.send('set', 'p3', 'bux').should.become('OK'),
            client.send('set', 'p4', 'lux').should.become('OK'),
            client.send('set', 'p9', 'tux').should.become('OK'),

            client.send('sort', 'y', 'asc', 'alpha').should.become(['a', 'b', 'c', 'd']),
            client.send('sort', 'y', 'desc', 'alpha').should.become(['d', 'c', 'b', 'a']),
            client.send('sort', 'x', 'asc').should.become(['2', '3', '4', '9']),
            client.send('sort', 'x', 'desc').should.become(['9', '4', '3', '2']),
            client.send('sort', 'x', 'by', 'w*', 'asc').should.become(['3', '9', '4', '2']),
            client.send('sort', 'x', 'by', 'w*', 'asc', 'get', 'o*').should.become(['foo', 'bar', 'baz', 'buz']),
            client.send('sort', 'x', 'by', 'w*', 'asc', 'get', 'o*', 'get', 'p*').should.become(['foo', 'bux', 'bar', 'tux', 'baz', 'lux', 'buz', 'qux']),

            client.send('sort', 'x', 'by', 'w*', 'asc', 'get', 'o*', 'get', 'p*', 'store', 'bacon').should.be.fulfilled,
            client.send('lrange', 'bacon', 0, -1).should.become(['foo', 'bux', 'bar', 'tux', 'baz', 'lux', 'buz', 'qux'])

            // TODO Sort by hash value.
        ]).should.be.fulfilled;
    });

    it('should support `MONITOR`', function()
    {
        if (semver.lt(client.serverInfo.redis_version, '2.6.0'))
            return P();

        var responses = [],
            deferred = P.defer();

        var monitorClient = chalet.createClient(port, host);

        monitorClient.on('monitor', function(timestamp, parameters)
        {
            // Skip the `monitor` command for Redis <= 2.4.16.
            if (parameters[0] == 'monitor')
                return;

            responses.push(parameters);
            if (responses.length == 2)
            {
                P.all(
                [
                    P(responses).should.become([['mget', 'some', 'keys', 'foo', 'bar'], ['set', 'json', '{"foo":"123","bar":"yet another value","another":false}']]),
                    monitorClient.send('quit').should.become('OK')
                ]).then(deferred.resolve, deferred.reject);
            }
        });

        var promise = monitorClient.send('monitor').then(function()
        {
            return P.all(
            [
                client.send('mget', 'some', 'keys', 'foo', 'bar'),
                client.send('set', 'json', JSON.stringify({ 'foo': '123', 'bar': 'yet another value', 'another': false}))
            ]);
        });

        return P.all(
        [
            monitorClient.connect().should.be.fulfilled,
            promise.should.be.fulfilled,
            deferred.promise.should.be.fulfilled
        ]).should.be.fulfilled;
    });

    it('should support `BLPOP`', function()
    {
        var promise = client.send('rpush', 'blocking list', 'initial value');
        return promise.then(function()
        {
            var blockingPop = client2.send('blpop', 'blocking list', 0);
            return P.all(
            [
                blockingPop.should.become(['blocking list', 'initial value']),
                blockingPop.then(function()
                {
                    return client.send('rpush', 'blocking list', 'wait for this value');
                }).should.become(1),
                client2.send('blpop', 'blocking list', 0).should.become(['blocking list', 'wait for this value'])
            ]);
        }).should.be.fulfilled;
    });

    it('should automatically time out `BLPOP` commands', function()
    {
        var promise = client2.send('blpop', 'blocking list', 1);
        return P.all(
        [
            promise.should.be.fulfilled,
            promise.should.become(null)
        ]).should.be.fulfilled;
    });

    it('should support `EXPIRE`', function()
    {
        return P.all(
        [
            client.send('set', ['expiry key', 'bar']).should.become('OK'),
            client.send('expire', ['expiry key', '1']).should.eventually.be.above(0),
            helpers.waitFor(2000).then(function()
            {
                return client.send('exists', 'expiry key');
            }).should.become(0)
        ]).should.be.fulfilled;
    });

    it('should support `TTL`', function()
    {
        return P.all(
        [
            client.send('set', 'ttl key', 'ttl val').should.become('OK'),
            client.send('expire', 'ttl key', '100').should.eventually.be.above(0),
            helpers.waitFor(500).then(function()
            {
                return client.send('ttl', 'ttl key');
            }).should.eventually.be.above(0)
        ]).should.be.fulfilled;
    });

    it('should automatically pipeline requests', function()
    {
        return P.all(
        [
            client.send('del', 'op_cb1').should.be.fulfilled,
            client.send('set', 'op_cb1', 'x').should.be.fulfilled,
            client.send('get', 'op_cb1').should.become('x')
        ]).should.be.fulfilled;
    });

    it('can reject pipelined commands with invalid arguments', function()
    {
        return P.all(
        [
            client.send('del', 'op_cb2').should.be.fulfilled,
            client.send('set', 'op_cb2', 'y').should.be.fulfilled,
            client.send('get', 'op_cb2').should.become('y'),
            client.send('set', 'op_cb_undefined', null).should.be.rejected
        ]).should.be.fulfilled;
    });

    it('should queue offline commands if the `enableOfflineQueue` option is set', function()
    {
        var offlineClient = chalet.createClient({ 'port': 9999, 'host': null, 'maxAttempts': 1 }),
            deferred = P.defer();

        offlineClient.on('error', deferred.reject);

        var connection = offlineClient.connect();

        return P.all(
        [
            connection.should.be.rejected,
            deferred.promise.should.be.rejected,
            // The promise will be kept in the pending state until a
            // connection can be established.
            offlineClient.send('set', 'offline queue', 'queued set operation').timeout(25).should.be.rejected,
            P(offlineClient.offlineQueue.length).should.become(1)
        ]).should.be.fulfilled;
    });

    it('should reject offline commands if offline queueing is disabled', function()
    {
        var offlineClient = chalet.createClient({ 'port': 9999, 'host': null, 'maxAttempts': 1, 'enableOfflineQueue': false }),
            deferred = P.defer();

        offlineClient.on('error', deferred.reject);

        var connection = offlineClient.connect();

        return P.all(
        [
            connection.should.be.rejected,
            deferred.promise.should.be.rejected,
            offlineClient.send('set', 'no offline queue', 'rejected set operation').should.be.rejected
        ]).should.be.fulfilled;
    });

    it('should support `SLOWLOG`', function()
    {
        return P.all(
        [
            client.send('config', 'set', 'slowlog-log-slower-than', 0).should.become('OK'),
            client.send('slowlog', 'reset').should.become('OK'),
            client.send('set', 'foo', 'bar').should.become('OK'),
            client.send('get', 'foo').should.become('bar'),
            client.send('slowlog', 'get').then(function(reply)
            {
                return P.all(
                [
                    P(reply.length).should.become(3),
                    P(reply[0][3]).should.become(['get', 'foo']),
                    P(reply[1][3]).should.become(['set', 'foo', 'bar']),
                    P(reply[2][3]).should.become(['slowlog', 'reset'])
                ]).then(function()
                {
                    return client.send('config', 'set', 'slowlog-log-slower-than', 10000);
                });
            }).should.become('OK')
        ]).should.be.fulfilled;
    });

    it('should support authentication with `AUTH`', function()
    {
        var authClient = chalet.createClient({ 'port': 9006, 'host': 'filefish.redistogo.com' }),
            readyCount = 0,
            deferred = P.defer();

        var credentialsSent = authClient.authenticateWith('664b1b6aaf134e1ec281945a8de702a9');

        authClient.on('ready', function()
        {
            readyCount++;
            // Authenticate, then terminate the connection to trigger automatic
            // re-authentication.
            if (readyCount == 1)
                authClient.socket.destroy();
            else
                authClient.send('quit').then(deferred.resolve, deferred.reject);
        });

        return P.all(
        [
            authClient.connect().should.be.fulfilled,

            credentialsSent.should.be.fulfilled,
            credentialsSent.should.become('OK'),

            deferred.promise.should.be.fulfilled,
            deferred.promise.should.become('OK')
        ]).should.be.fulfilled;
    });

    it('should support the `retryMaxDelay` option', function()
    {
        var currentTime = Date.now(),
            isReconnecting = false,
            delayClient = chalet.createClient({ 'port': port, 'host': host, 'retryMaxDelay': 1 }),
            deferred = P.defer();

        delayClient.on('ready', function()
        {
            if (!isReconnecting)
            {
                isReconnecting = true;
                this.retryDelay = 1000;
                this.retryBackoff = 1;
                this.socket.end();
            }
            else
            {
                this.send('quit').then(function()
                {
                    deferred.resolve(Date.now() - currentTime);
                }, deferred.reject);
            }
        });

        return P.all(
        [
            delayClient.connect().should.be.fulfilled,
            deferred.promise.should.be.fulfilled,
            deferred.promise.should.eventually.be.below(1000)
        ]).should.be.fulfilled;
    });
});
