# xpgsql

A pure Lua module that provides a more straightforward API to work with the [luapgsql] Lua library (also works with [cqueues-pgsql]).

* Canonical repository: https://git.sr.ht/~mna/xpgsql
* Issue tracker: https://todo.sr.ht/~mna/xpgsql

## Install

Via Luarocks:

```
$ luarocks install xpgsql
```

Or simply copy the single xpgsql.lua file in your project or your `LUA_PATH`.

## API

Assuming `local xpgsql = require 'xpgsql'`. You can check out the tests for actual examples of using the API.

### `xpgsql.connect(connstr)`

Connects to the database specified by the connection string.  It may be an
empty string if the required settings are set in environment variables. If
no connection string is provided, an empty string is used.

Returns the connection object on success, or nil and an error message.

### `xpgsql.model(res, newf)`

Returns a table filled with the values of the first row of res, keyed by
the field names. Returns nil if res has no row. If newf is provided,
calls it with the generated table so that it can make further
initialization and returns the return value(s) of newf.

### `xpgsql.models(res, newf)`

Returns an array of models for each row in res. Works the same as
xpgsql.model, except for multiple rows, and if res contains no row, returns
an empty array instead of nil.

### `Connection:format_array(t)`

Formats array t so that it can be used in a SQL statement as
e.g. IN (...) or ANY (...). It returns a string that can safely
be inserted in the SQL statement, with each value comma-separated
and each string value properly escaped. If the array contains
values other than strings and numbers, it raises an error.

### `Connection:tx(f, ...)`

Executes f inside a transaction, passing the Connection as first argument
and any extra arguments passed to this function as subsequent arguments. On
exit, the transaction is committed if f executed without error, otherwise it
is rollbacked.

Returns the return values of f on success, or nil and an error message on
error.

### `Connection:query(stmt, ...)`

Executes a query statement and returns the result if it succeeds, or nil
and an error message. Note that INSERT .. RETURNING must use
Connection:query as it returns values. The statement may contain $1, $2,
etc.  placeholders, they will be replaced by the extra arguments
provided to the method.

### `Connection:exec(stmt, ...)`

Executes a non-query statement and returns the result if it
succeeds, or nil and an error message. Note that INSERT..RETURNING
must use Connection:query as it returns values. The statement may
contain $1, $2, etc.  placeholders, they will be replaced by the
extra arguments provided to the method.

### `Connection:close()`

Closes the connection and frees resources associated with it.

## Development

Clone the project and install the required development dependencies:

* luaunit (unit test runner)
* luacov (recommended, test coverage)

If like me you prefer to keep your dependencies locally, per-project, then I recommend using my [llrocks] wrapper of the `luarocks` cli, which by default uses a local `lua_modules/` tree.

```
$ llrocks install ...
```

To run tests, first make sure a postgres database is reachable with
an empty connection string (e.g. via environment variables) and make
sure it is ok to create tables and change data in that database. Then:

```
$ llrocks run test/main.lua
```

To view code coverage:

```
$ llrocks cover test/main.lua
```

## License

The [BSD 3-clause][bsd] license.

[bsd]: http://opensource.org/licenses/BSD-3-Clause
[llrocks]: https://git.sr.ht/~mna/llrocks
[luapgsql]: https://github.com/arcapos/luapgsql
[cqueues-pgsql]: https://github.com/daurnimator/cqueues-pgsql
