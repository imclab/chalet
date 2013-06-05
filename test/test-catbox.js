/*global describe: true, it: true, before: true, after: true */

var chai = require('chai'),
    P = require('p-promise');

var chalet = require('../'),
    Adapter = require('../adapters/catbox');

var expect = chai.expect,
    should = chai.should();

require('mocha-as-promised')();

describe('Catbox Adapter', function()
{
    var port = 6379,
        host = '127.0.0.1';

    describe('#start', function()
    {
        it('can establish a connection', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host });
            adapter.start(function(error, result)
            {
                should.not.exist(error);
                should.not.exist(result);

                should.exist(adapter.client);
                adapter.isReady().should.be.true;

                done();
            });
        });

        it('reuses the client when a connection has already been established', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host });
            adapter.start(function(error)
            {
                should.not.exist(error);
                adapter.isReady().should.be.true;

                var client = adapter.client;
                adapter.start(function()
                {
                    client.should.equal(adapter.client);
                    adapter.isReady().should.be.true;

                    done();
                });
            });
        });

        it('returns an error if the connection cannot be established', function(done)
        {
            var adapter = new Adapter({ 'port': 6969, 'host': 'SHAZAM!' });
            adapter.client.maxAttempts = 1;

            adapter.start(function(error, result)
            {
                should.exist(error);
                error.should.be.an.instanceof(Error);
                adapter.isReady().should.be.false;

                done();
            });
        });

        it('sends credentials when specified', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host, 'password': 'test' });
            adapter.start(function(error, result)
            {
                should.not.exist(error);
                should.not.exist(result);
                adapter.isReady().should.be.true;

                done();
            });
        });
    });

    describe('#validateSegmentName', function()
    {
        it('returns an error when the segment name is empty', function()
        {
            var adapter = new Adapter({ 'port': port, 'host': host }),
                result = adapter.validateSegmentName('');

            expect(result).to.be.an.instanceof(Error);
            expect(result.message).to.equal('Missing or invalid segment name.');
        });

        it('returns an error when the segment name contains a null character', function()
        {
            var adapter = new Adapter({ 'port': port, 'host': host }),
                result = adapter.validateSegmentName('\0test');

            expect(result).to.be.an.instanceof(Error);
            expect(result.message).to.equal('Missing or invalid segment name.');
        });

        it('returns `null` if the segment is valid', function()
        {
            var adapter = new Adapter({ 'port': port, 'host': host }),
                result = adapter.validateSegmentName('valid');

            expect(result).to.be.null;
        });
    });

    describe('#get', function()
    {
        it('returns an error if a connection has not been established', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host });

            adapter.get('test', function(error)
            {
                should.exist(error);
                expect(error).to.be.an.instanceof(Error);
                error.message.should.equal('The connection is unavailable.');

                done();
            });
        });

        it('forwards Redis errors to its callback', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host });

            adapter.client =
            {
                'isReady': true,
                'send': function send()
                {
                    var deferred = P.defer();
                    deferred.reject(new Error('Simulated Redis error.'));
                    return deferred.promise;
                }
            };

            adapter.get('test', function(error)
            {
                should.exist(error);
                expect(error.message).to.equal('Simulated Redis error.');

                done();
            });
        });

        it('forwards `JSON.parse` exceptions to its callback', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host });

            adapter.client =
            {
                'isReady': true,
                'send': function send()
                {
                    return P('test');
                }
            };

            adapter.get('test', function(error)
            {
                should.exist(error);
                done();
            });
        });

        it('returns an error for malformed envelope contents', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host });

            adapter.client =
            {
                'isReady': true,
                'send': function send()
                {
                    return P('{ "item": "false" }');
                }
            };

            adapter.get('test', function(error)
            {
                should.exist(error);
                should.exist(error.envelope);
                expect(error.message).to.equal('Invalid envelope structure.');

                done();
            });
        });

        it('can retrieve stored objects', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host, 'partition': 'wwwtest' }),
                key = { 'id': 'test', 'segment': 'test' };

            adapter.start(function(error)
            {
                should.not.exist(error);
                adapter.set(key, 'myvalue', 200, function(error)
                {
                    should.not.exist(error);

                    adapter.get(key, function(error, result)
                    {
                        should.not.exist(error);
                        should.exist(result);
                        expect(result.item).to.equal('myvalue');

                        done();
                    });
                });
            });
        });

        it('should return `null` for missing items', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host, 'partition': 'wwwtest'}),
                key = { 'id': 'notfound', 'segment': 'notfound' };

            adapter.start(function(error)
            {
                should.not.exist(error);

                adapter.get(key, function(error, result)
                {
                    should.not.exist(error);
                    expect(result).to.be.null;

                    done();
                });
            });
        });
    });

    describe('#set', function()
    {
        it('returns an error if a connection has not been established', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host });
            adapter.set('test1', 'test1', 3600, function(error)
            {
                should.exist(error);
                expect(error).to.be.an.instanceof(Error);
                error.message.should.equal('The connection is unavailable.');

                done();
            });
        });

        it('forwards Redis errors to its callback', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host });

            adapter.client =
            {
                'isReady': true,
                'send': function send()
                {
                    var deferred = P.defer();
                    deferred.reject(new Error('Simulated Redis error.'));
                    return deferred.promise;
                }
            };

            adapter.set('test', 'test', 3600, function(error)
            {
                should.exist(error);
                expect(error).to.be.an.instanceof(Error);
                expect(error.message).to.equal('Simulated Redis error.');

                done();
            });
        });

        it('forwards `JSON.stringify` exceptions to its callback', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host }),
                key = { 'id': 'cyclic', 'segment': 'structure' };

            adapter.client =
            {
                'isReady': true,
                'send': function send()
                {
                    return P('OK');
                }
            };

            var value = [];
            value.push(value);

            adapter.set(key, value, 200, function(error)
            {
                should.exist(error);
                expect(error).to.be.an.instanceof(TypeError);
                done();
            });
        });
    });

    describe('#drop', function()
    {
        it('returns an error if a connection has not been established', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host });
            adapter.drop('test2', function(error)
            {
                should.exist(error);
                expect(error).to.be.an.instanceof(Error);
                error.message.should.equal('The connection is unavailable.');

                done();
            });
        });

        it('purges envelopes from Redis', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host, 'partition': 'wwwtest' }),
                key = { 'id': 'purged', 'segment': 'purged' };

            adapter.start(function(error)
            {
                should.not.exist(error);

                adapter.set(key, 'to-purge', 3600, function(error)
                {
                    should.not.exist(error);

                    adapter.drop(key, function(error)
                    {
                        should.not.exist(error);

                        adapter.get(key, function(error, result)
                        {
                            should.not.exist(error);
                            expect(result).to.be.null;

                            done();
                        });
                    });
                });
            });
        });
    });

    describe('#stop', function()
    {
        it('closes the underlying connection', function(done)
        {
            var adapter = new Adapter({ 'port': port, 'host': host });
            adapter.start(function()
            {
                adapter.isReady().should.be.true;

                adapter.client.once('end', function()
                {
                    adapter.isReady().should.be.false;
                    done();
                });

                adapter.stop();
            });
        });
    });
});