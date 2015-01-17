catbox-redis
============

RethinkDB adapter for catbox

## Options

- `host` - the Redis server hostname. Defaults to `'127.0.0.1'`.
- `port` - the Redis server port or unix domain socket path. Defaults to `28015`.
- `db` - the Redis database. Defaults to `catbox`
- `table` - The RethinkDB table under the db to store. Defaults to `catbox`

## Tests

The test suite expects a RethinkDB server to be running on port 28015.

```sh
rethinkdb && npm test
```
