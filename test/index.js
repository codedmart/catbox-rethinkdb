// Load modules

var Code = require('code');
var Lab = require('lab');
var Catbox = require('catbox');
var R = require('rethinkdb');
var RethinkDB = require('..');


// Declare internals

var internals = {};


// Test shortcuts

var lab = exports.lab = Lab.script();
var expect = Code.expect;
var describe = lab.describe;
var it = lab.test;


describe('RethinkDB', function () {

    it('throws an error if not created with new', function (done) {

        var fn = function () {

            var rethinkdb = RethinkDB();
        };

        expect(fn).to.throw(Error);
        done();
    });

    it('creates a new connection', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            expect(client.isReady()).to.equal(true);
            done();
        });
    });

    it('closes the connection', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            expect(client.isReady()).to.equal(true);
            client.stop();
            expect(client.isReady()).to.equal(false);
            done();
        });
    });

    it('inserts an item', function(done) {

        var options = {
            host: '127.0.0.1',
            port: 28015
        };

        var rethinkdb = new RethinkDB(options);
        rethinkdb.start(function (err) {

            var ttl = 500;
            var expiresAt = new Date();
            expiresAt.setMilliseconds(expiresAt.getMilliseconds() + ttl);
            var record = {
                id: 'someId',
                value: 'asdf',
                stored: new Date(),
                ttl: ttl,
                expiresAt: expiresAt
            };

            rethinkdb.insert(record, function (err) {

                expect(err).to.not.exist();
                done();
            });
        });
    });

    it('replaces an item', function(done) {

        var options = {
            host: '127.0.0.1',
            port: 28015
        };

        var rethinkdb = new RethinkDB(options);
        rethinkdb.start(function (err) {

            var ttl = 500;
            var expiresAt = new Date();
            expiresAt.setMilliseconds(expiresAt.getMilliseconds() + ttl);
            var record = {
                id: 'someId',
                value: 'asdf',
                stored: new Date(),
                ttl: ttl,
                expiresAt: expiresAt
            };

            rethinkdb.replace(record, function (err) {

                expect(err).to.not.exist();
                done();
            });
        });
    });

    it('gets an item after setting it', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            var key = { id: 'x', segment: 'test' };
            client.set(key, '123', 500, function (err) {

                expect(err).to.not.exist();
                client.get(key, function (err, result) {

                    expect(err).to.equal(null);
                    expect(result.item).to.equal('123');
                    done();
                });
            });
        });
    });

    it('fails setting an item circular references', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            var key = { id: 'x', segment: 'test' };
            var value = { a: 1 };
            value.b = value;
            client.set(key, value, 10, function (err) {

                expect(err.message).to.equal('RqlDriverError: Nesting depth limit exceeded');
                done();
            });
        });
    });

    it('ignored starting a connection twice on same event', function (done) {

        var client = new Catbox.Client(RethinkDB);
        var x = 2;
        var start = function () {

            client.start(function (err) {

                expect(client.isReady()).to.equal(true);
                --x;
                if (!x) {
                    done();
                }
            });
        };

        start();
        start();
    });

    it('ignored starting a connection twice chained', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            expect(err).to.not.exist();
            expect(client.isReady()).to.equal(true);

            client.start(function (err) {

                expect(err).to.not.exist();
                expect(client.isReady()).to.equal(true);
                done();
            });
        });
    });

    it('returns not found on get when using null key', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            client.get(null, function (err, result) {

                expect(err).to.equal(null);
                expect(result).to.equal(null);
                done();
            });
        });
    });

    it('returns not found on get when item expired', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            var key = { id: 'x', segment: 'test' };
            client.set(key, 'x', 1, function (err) {

                expect(err).to.not.exist();
                setTimeout(function () {

                    client.get(key, function (err, result) {

                        expect(err).to.equal(null);
                        expect(result).to.equal(null);
                        done();
                    });
                }, 2);
            });
        });
    });

    it('returns error on set when using null key', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            client.set(null, {}, 1000, function (err) {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on get when using invalid key', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            client.get({}, function (err) {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on drop when using invalid key', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            client.drop({}, function (err) {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on set when using invalid key', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            client.set({}, {}, 1000, function (err) {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('ignores set when using non-positive ttl value', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            var key = { id: 'x', segment: 'test' };
            client.set(key, 'y', 0, function (err) {

                expect(err).to.not.exist();
                done();
            });
        });
    });

    it('returns error on drop when using null key', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.start(function (err) {

            client.drop(null, function (err) {

                expect(err instanceof Error).to.equal(true);
                done();
            });
        });
    });

    it('returns error on get when stopped', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.stop();
        var key = { id: 'x', segment: 'test' };
        client.connection.get(key, function (err, result) {

            expect(err).to.exist();
            expect(result).to.not.exist();
            done();
        });
    });

    it('returns error on set when stopped', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.stop();
        var key = { id: 'x', segment: 'test' };
        client.connection.set(key, 'y', 1, function (err) {

            expect(err).to.exist();
            done();
        });
    });

    it('returns error on drop when stopped', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.stop();
        var key = { id: 'x', segment: 'test' };
        client.connection.drop(key, function (err) {

            expect(err).to.exist();
            done();
        });
    });

    it('returns error on missing segment name', function (done) {

        var config = {
            expiresIn: 50000
        };
        var fn = function () {

            var client = new Catbox.Client(RethinkDB);
            var cache = new Catbox.Policy(config, client, '');
        };
        expect(fn).to.throw(Error);
        done();
    });

    it('returns error on bad segment name', function (done) {

        var config = {
            expiresIn: 50000
        };
        var fn = function () {

            var client = new Catbox.Client(RethinkDB);
            var cache = new Catbox.Policy(config, client, 'a\0b');
        };
        expect(fn).to.throw(Error);
        done();
    });

    it('returns error when cache item dropped while stopped', function (done) {

        var client = new Catbox.Client(RethinkDB);
        client.stop();
        client.drop('a', function (err) {

            expect(err).to.exist();
            done();
        });
    });

    describe('#start', function () {

        it('sets client to when the connection succeeds', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015
            };

            var rethinkdb = new RethinkDB(options);

            rethinkdb.start(function (err) {

                expect(err).to.not.exist();
                expect(rethinkdb.client).to.exist();
                done();
            });
        });

        it('reuses the client when a connection is already started', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015
            };

            var rethinkdb = new RethinkDB(options);

            rethinkdb.start(function (err) {

                expect(err).to.not.exist();
                var client = rethinkdb.client;

                rethinkdb.start(function () {

                    expect(client).to.equal(rethinkdb.client);
                    done();
                });
            });
        });

        it('returns an error when connection fails', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 6380
            };

            var rethinkdb = new RethinkDB(options);

            rethinkdb.start(function (err) {

                expect(err).to.exist();
                expect(err).to.be.instanceOf(Error);
                expect(rethinkdb.client).to.not.exist();
                done();
            });
        });

        it('sends auth command when password is provided', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015,
                password: 'wrongpassword'
            };

            var rethinkdb = new RethinkDB(options);

            var log = console.log;
            console.log = function (message) {

                expect(message).to.contain('Warning');
                console.log = log;
            };

            rethinkdb.start(function (err) {
                done();
            });
        });
    });

    describe('#validateSegmentName', function () {

        it('returns an error when the name is empty', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015
            };

            var rethinkdb = new RethinkDB(options);

            var result = rethinkdb.validateSegmentName('');

            expect(result).to.be.instanceOf(Error);
            expect(result.message).to.equal('Empty string');
            done();
        });

        it('returns an error when the name has a null character', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015
            };

            var rethinkdb = new RethinkDB(options);

            var result = rethinkdb.validateSegmentName('\0test');

            expect(result).to.be.instanceOf(Error);
            done();
        });

        it('returns null when there aren\'t any errors', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015
            };

            var rethinkdb = new RethinkDB(options);

            var result = rethinkdb.validateSegmentName('valid');

            expect(result).to.not.be.instanceOf(Error);
            expect(result).to.equal(null);
            done();
        });
    });

    describe('#get', function () {

        it('passes an error to the callback when the connection is closed', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015
            };

            var rethinkdb = new RethinkDB(options);

            rethinkdb.get('test', function (err) {

                expect(err).to.exist();
                expect(err).to.be.instanceOf(Error);
                expect(err.message).to.equal('Connection not started');
                done();
            });
        });

        it('is able to retrieve an object thats stored when connection is started', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015,
                partition: 'wwwtest'
            };
            var key = {
                id: 'test',
                segment: 'test'
            };

            var rethinkdb = new RethinkDB(options);

            rethinkdb.start(function () {

                rethinkdb.set(key, 'myvalue', 200, function (err) {

                    expect(err).to.not.exist();
                    rethinkdb.get(key, function (err, result) {

                        expect(err).to.not.exist();
                        expect(result.item).to.equal('myvalue');
                        done();
                    });
                });
            });
        });

        it('returns null when unable to find the item', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015,
                partition: 'wwwtest'
            };
            var key = {
                id: 'notfound',
                segment: 'notfound'
            };

            var rethinkdb = new RethinkDB(options);

            rethinkdb.start(function () {

                rethinkdb.get(key, function (err, result) {

                    expect(err).to.not.exist();
                    expect(result).to.not.exist();
                    done();
                });
            });
        });
    });

    describe('#set', function () {

        it('passes an error to the callback when the connection is closed', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015
            };

            var rethinkdb = new RethinkDB(options);

            rethinkdb.set('test1', 'test1', 3600, function (err) {

                expect(err).to.exist();
                expect(err).to.be.instanceOf(Error);
                expect(err.message).to.equal('Connection not started');
                done();
            });
        });

    });

    describe('#drop', function () {

        it('passes an error to the callback when the connection is closed', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015
            };

            var rethinkdb = new RethinkDB(options);

            rethinkdb.drop('test2', function (err) {

                expect(err).to.exist();
                expect(err).to.be.instanceOf(Error);
                expect(err.message).to.equal('Connection not started');
                done();
            });
        });

        it('deletes an item that has been set', function (done) {

            var client = new Catbox.Client(RethinkDB);
            client.start(function (err) {

                var key = { id: 'Id', segment: 'some' };
                client.drop(key, function (err, result) {

                    expect(err).to.equal(null);
                    done();
                });
            });
        });

    });

    describe('#stop', function () {

        it('sets the client to null', function (done) {

            var options = {
                host: '127.0.0.1',
                port: 28015
            };

            var rethinkdb = new RethinkDB(options);

            rethinkdb.start(function () {

                expect(rethinkdb.client).to.exist();
                rethinkdb.stop();
                expect(rethinkdb.client).to.not.exist();
                done();
            });
        });
    });
});
