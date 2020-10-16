local pgsql = require 'pgsql'

local ok, cqpgsql = pcall(require, 'cqueues_pgsql')
local connectfn = ok and cqpgsql.connectdb or pgsql.connectdb

local function closeconn(conn)
  local rawconn = conn._conn
  if rawconn then
    conn._conn = nil
    rawconn:finish()
  end
end

local Connection = {
  __name = 'xpgsql.Connection',
  __gc = closeconn,
  __close = closeconn,
}
Connection.__index = Connection

local function new_connection(rawconn)
  local o = {_conn = rawconn}
  setmetatable(o, Connection)
  return o
end

-- Closes the connection and frees resources associated with it.
function Connection:close()
  closeconn(self)
end

-- Formats array t so that it can be used in a SQL statement as
-- e.g. IN (...) or ANY (...). It returns a string that can safely
-- be inserted in the SQL statement, with each value comma-separated
-- and each string value properly escaped. If the array contains
-- values other than strings and numbers, it raises an error.
function Connection:format_array(t)
  local rawconn = self._conn
  assert(rawconn, 'connection closed')

  local buf = {}
  for i, v in ipairs(t) do
    local typ = type(v)

    if typ == 'string' then
      table.insert(buf, assert(rawconn:escapeLiteral(v)))
    elseif typ == 'number' then
      if math.type(v) == 'integer' then
        table.insert(buf, string.format('%d', v))
      else
        table.insert(buf, string.format('%f', v))
      end
    else
      error(string.format('invalid value type at index %d: %s', i, typ))
    end
  end
  return table.concat(buf, ',')
end

-- Executes f inside a transaction, passing the Connection as first argument
-- and any extra arguments passed to this function as subsequent arguments. On
-- exit, the transaction is committed if f executed without error, otherwise it
-- is rollbacked. The Connection.transaction field is set to true before calling
-- f, and is reset to its old value before returning.
--
-- Returns the return values of f on success, or nil and an error message on
-- error.
function Connection:tx(f, ...)
  do
    local ok, err = self:exec('BEGIN TRANSACTION')
    if not ok then return nil, err end
  end

  local old_tx = self.transaction
  self.transaction = true
  local res = table.pack(pcall(f, self, ...))
  self.transaction = old_tx

  if res[1] then
    local ok, err = self:exec('COMMIT')
    if not ok then return nil, err end
    return table.unpack(res, 2, res.n)
  else
    self:exec('ROLLBACK')
    return nil, res[2]
  end
end

-- Similar to Connection:tx, ensuretx starts a transaction only if Connection
-- is not already inside one, and calls f with the Connection as first argument
-- and any extra arguments passed to this function as subsequent arguments.
-- If a transaction was started, it is closed after the call to f with a commit
-- if f succeeded, or a rollback if it raised an error. If a transaction was
-- not started (if Connection was already in a transaction before the call to
-- ensuretx), the transaction is not terminated after the call to f.
--
-- Returns the return values of f on success, or nil and an error message on
-- error.
function Connection:ensuretx(f, ...)
  if not self.transaction then
    return self:tx(f, ...)
  end
  return self:with(false, f, ...)
end

-- Calls f with the Connection as first argument and any extra arguments passed
-- to this function as subsequent arguments. If close is true, the connection
-- is closed after the call to f.
--
-- Returns the return values of f on success, or nil and an error message on
-- error.
function Connection:with(close, f, ...)
  local res = table.pack(pcall(f, self, ...))
  if close then self:close() end
  if res[1] then
    return table.unpack(res, 2, res.n)
  else
    return nil, res[2]
  end
end

local function exec_or_query(rawconn, stmt, success, ...)
  assert(rawconn, 'connection closed')

  local res = rawconn:execParams(stmt, ...)
  if not res then
    return nil, rawconn:errorMessage(), rawconn:status()
  end
  local status = res:status()
  if status == success then
    return res
  else
    return nil, res:errorMessage(), status
  end
end

-- Executes a query statement and returns the result if it succeeds, or nil, an
-- error message and the status code. Note that INSERT .. RETURNING must use
-- Connection:query as it returns values. The statement may contain $1, $2,
-- etc.  placeholders, they will be replaced by the extra arguments provided to
-- the method.
function Connection:query(stmt, ...)
  return exec_or_query(self._conn, stmt, pgsql.PGRES_TUPLES_OK, ...)
end

-- Executes a non-query statement and returns the result if it succeeds, or
-- nil, an error message and the status code. Note that INSERT..RETURNING must
-- use Connection:query as it returns values. The statement may contain $1, $2,
-- etc.  placeholders, they will be replaced by the extra arguments provided to
-- the method.
function Connection:exec(stmt, ...)
  return exec_or_query(self._conn, stmt, pgsql.PGRES_COMMAND_OK, ...)
end


local M = {}

-- Connects to the database specified by the connection string.  It may be an
-- empty string if the required settings are set in environment variables. If
-- no connection string is provided, an empty string is used.
--
-- Returns the connection object on success, or nil, an error message and the
-- status code.
function M.connect(connstr)
  connstr = connstr or ''
  local conn = connectfn(connstr)
  local status = conn:status()
  if status == pgsql.CONNECTION_OK then
    return new_connection(conn)
  else
    local err = conn:errorMessage()
    conn:finish()
    return nil, err, status
  end
end

-- Returns a table filled with the values of the first row of
-- res, keyed by the field names. Returns nil if res has no
-- row. If newf is provided, calls it with the generated table
-- so that it can make further initialization and returns the
-- return value(s) of newf.
function M.model(res, newf)
  if res:ntuples() == 0 then return nil end

  newf = newf or function(t) return t end

  local o = {}
  for i = 1, res:nfields() do
    o[res:fname(i)] = res[1][i]
  end
  return newf(o)
end

-- Returns an array of models for each row in res. Works the same as
-- xpgsql.model, except for multiple rows, and if res contains no row, returns
-- an empty array instead of nil.
function M.models(res, newf)
  newf = newf or function(t) return t end

  local ar = {}
  for r = 1, res:ntuples() do
    local row = {}
    for c = 1, res:nfields() do
      row[res:fname(c)] = res[r][c]
    end
    table.insert(ar, newf(row))
  end
  return ar
end

return M
