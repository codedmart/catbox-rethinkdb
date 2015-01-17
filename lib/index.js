var Hoek = require('hoek');
var RethinkDB = require('rethinkdb');

// Declare internals

var internals = {};


// TODO: add authKey options
internals.defaults = {
    host: '127.0.0.1',
    port: 28015,
    db: 'catbox',
    table: 'catbox',
    flushInterval: 60000
};


exports = module.exports = internals.Connection = function (options) {

    Hoek.assert(this.constructor === internals.Connection, 'RethinkDB cache client must be instantiated using new');

    this.settings = Hoek.applyToDefaults(internals.defaults, options);
    this.table = RethinkDB.db(this.settings.db).table(this.settings.table);
    this.client = null;
    this.isConnected = false;

    return this;
};


internals.Connection.prototype.flush = function() {

    setInterval(function() {

        try {

            var now = new Date().getTime();
            this.table.filter(RethinkDB.row('expiresAt').lt(now)).delete().run(this.client);
        }

        catch (err) {

            console.log(new Error(err));
        }
    }.bind(this), this.settings.flushInterval);
};


internals.Connection.prototype.createDb = function () {

    var self = this;

    return RethinkDB.dbList().run(this.client)
    .then(function(dbs) {

        if (!Hoek.contain(dbs, self.settings.db)) {

            return RethinkDB.dbCreate(self.settings.db).run(self.client);
        }
    });
};


internals.Connection.prototype.createTable = function () {

    var self = this;

    return RethinkDB.tableList().run(this.client)
    .then(function(tables) {

        if (!Hoek.contain(tables, self.settings.table)) {

            return RethinkDB.tableCreate(self.settings.table).run(self.client);
        }
    });
};


internals.Connection.prototype.createIndex = function () {

    var self = this;

    return RethinkDB.table(this.settings.table).indexList().run(this.client)
    .then(function(indexes) {

        if (!Hoek.contain(indexes, 'expiresAt')) {

            return self.table.indexCreate('expiresAt').run(self.client);
        }
    });
};


internals.Connection.prototype.start = function (callback) {

    var self = this;

    if (this.client) {
        return Hoek.nextTick(callback)();
    }

    // Create client
    return RethinkDB.connect({

        host: this.settings.host,
        port: this.settings.port,
        db: this.settings.db
    }, function(err, conn) {

        if (err) {
            self.stop();
            return callback(new Error(err));
        }

        self.isConnected = true;
        self.client = conn;

        // Ensure table is created
        return self.createDb()
        .then(function() {
            return self.createTable();
        })
        .then(function() {
            return self.createIndex();
        })
        .then(function() {
            self.flush(conn);
            return callback();
        })
        .error(function(err) {
            return callback(new Error(err));
        });
    });
};


internals.Connection.prototype.stop = function () {

    if (this.client) {
        this.client.close();
        this.client = null;
        this.isConnected = false;
    }
};


internals.Connection.prototype.isReady = function () {

    return this.isConnected;
};


internals.Connection.prototype.validateSegmentName = function (name) {

    if (!name) {
        return new Error('Empty string');
    }

    if (name.indexOf('\0') !== -1) {
        return new Error('Includes null character');
    }

    return null;
};


internals.Connection.prototype.insert = function(record, callback) {

    try {
        this.table.insert(record).run(this.client, function (err, result) {

            if (err) {
                return callback(err);
            }

            return callback();
        });
    }

    catch(err) {

        return callback(new Error(err));
    }
};


internals.Connection.prototype.replace = function(record, callback) {

    try {

        this.table.replace(record).run(this.client, function (err, result) {

            if (err) {
                return callback(err);
            }

            return callback();
        });
    }

    catch(err) {

        return callback(new Error(err));
    }
};


internals.Connection.prototype.get = function (key, callback) {

    var self = this;

    if (!this.client) {
        return callback(new Error('Connection not started'));
    }

    var cacheKey = this.generateKey(key);

    this.table.get(cacheKey).run(this.client, function (err, result) {

        if (err) {
            return callback(err);
        }

        if (!result) {
            return callback(null, null);
        }

        if (!result.value || !result.stored) {

            return callback(new Error('Incorrect result structure'));
        }

        var envelope = {
            item: result.value,
            stored: result.stored.getTime(),
            ttl: result.ttl
        };

        return callback(null, envelope);
    });
};


internals.Connection.prototype.set = function (key, value, ttl, callback) {

    var self = this;

    if (!this.client) {
        return callback(new Error('Connection not started'));
    }

    var cacheKey = this.generateKey(key);

    var expiresAt = new Date();
    expiresAt.setMilliseconds(expiresAt.getMilliseconds() + ttl);

    var record = {
        id: cacheKey,
        value: value,
        stored: new Date(),
        ttl: ttl,
        expiresAt: expiresAt
    };

    this.get(key, function(err, result) {

        if (err) {
            return callback(err);
        }

        if (!result) {

            self.insert(record, callback);
        }

        else {

            self.replace(record, callback);
        }
    });
};


internals.Connection.prototype.drop = function (key, callback) {

    if (!this.client) {
        return callback(new Error('Connection not started'));
    }

    var cacheKey = this.generateKey(key);

    this.table.get(cacheKey).delete().run(this.client, function (err, result) {

        if (err) {
            return callback(err);
        }

        return callback(null);
    });
};

internals.Connection.prototype.generateKey = function (key) {

    return encodeURIComponent(key.segment) + encodeURIComponent(key.id);
};
