var Hoek = require('hoek');
var RethinkDB;

// Declare internals

var internals = {};


// TODO: add authKey options
internals.defaults = {
    host: '127.0.0.1',
    port: 28015,
    db: 'catbox',
    table: 'catbox',
    flushInterval: 60000,
    max: 100
};

var isConnected = function() {
    return RethinkDB.getPoolMaster().getNumConnections() > 0;
};

exports = module.exports = internals.Connection = function (options) {

    Hoek.assert(this.constructor === internals.Connection, 'RethinkDB cache client must be instantiated using new');

    this.settings = Hoek.applyToDefaults(internals.defaults, options);

    RethinkDB = require('rethinkdbdash')(this.settings);
    this.table = RethinkDB.db(this.settings.db).table(this.settings.table);

    this.isConnected = isConnected;
    this.started = false;

    return this;
};


internals.Connection.prototype.flush = function() {

    setInterval(function() {

        try {
            if (this.client.isReady()) {

                this.table.between(RethinkDB.minval, RethinkDB.now(), {index: 'expiresAt'}).delete().run();
            }
        }

        catch (err) {

            console.log(new Error(err));
        }
    }.bind(this), this.settings.flushInterval);
};

internals.Connection.prototype.createTable = function () {

    var self = this;

    return RethinkDB.tableList().run()
      .then(function(tables) {

          if (!Hoek.contain(tables, self.settings.table)) {

              return RethinkDB.tableCreate(self.settings.table).run();
          }
      });
};

internals.Connection.prototype.createIndex = function () {

    var self = this;

    return RethinkDB.table(this.settings.table).indexList().run()
      .then(function(indexes) {

          if (!Hoek.contain(indexes, 'expiresAt')) {

              return self.table.indexCreate('expiresAt').run();
          }
      });
};


internals.Connection.prototype.handleConnection = function(callback) {

    var self = this;

    return self.createTable()
      .then(function(){
          self.createIndex();
      })
      .then(function() {
          self.flush();
          self.started = true;
          return callback();
      })
      .error(function(err) {
          return callback(new Error(err));
      });

};


internals.Connection.prototype.start = function (callback) {

    if (this.started) {
        return Hoek.nextTick(callback)();
    }

    return this.handleConnection(callback);
};


internals.Connection.prototype.stop = function () {
    RethinkDB.getPoolMaster().drain();
    this.started = false;
};


internals.Connection.prototype.isReady = function () {

    return (this.started && this.isConnected());
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
        this.table.insert(record).run(function (err, result) {

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

        this.table.get(record.id).replace(record).run(function (err, result) {

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

    if (!this.isConnected()) {
        return callback(new Error('Connection not started'));
    }

    var cacheKey = this.generateKey(key);

    this.table.get(cacheKey).run(function (err, result) {

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

    if (!this.isConnected()) {
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

    if (!this.isConnected()) {
        return callback(new Error('Connection not started'));
    }

    var cacheKey = this.generateKey(key);

    this.table.get(cacheKey).delete().run(function (err, result) {

        if (err) {
            return callback(err);
        }

        return callback(null);
    });
};

internals.Connection.prototype.generateKey = function (key) {

    return encodeURIComponent(key.segment) + encodeURIComponent(key.id);
};
