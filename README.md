catbox-rethinkdb
============

RethinkDB adapter for catbox

still only about 87% test coverage right now.

## Options

- `host` - the RethinkDB server hostname. Defaults to `'127.0.0.1'`.
- `port` - the RethinkDB server port or unix domain socket path. Defaults to `28015`.
- `db` - the RethinkDB database. Defaults to `catbox`
- `table` - The RethinkDB table under the db to store. Defaults to `catbox`
- `flushInterval` - Since RethinkDB does not have ttl yet this sets how often to flush expiredAt records. Defaults to `60000`

## Tests

The test suite expects a RethinkDB server to be running on port 28015.

```sh
rethinkdb && npm test
```
