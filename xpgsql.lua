local pgsql = require 'pgsql'

local okreq, cqpgsql = pcall(require, 'cqueues_pgsql')
local connectfn = okreq and cqpgsql.connectdb or pgsql.connectdb

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

local M = {}

local function new_connection(rawconn)
  local o = {_conn = rawconn}
  return setmetatable(o, Connection)
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
-- error. As a special-case, to allow asserting on the call, it returns true if
-- f succeeded and did not return anything (it can still return nil
-- explicitly and Connection:tx will then return nil).
function Connection:tx(f, ...)
  do
    local ok, err = self:exec('BEGIN TRANSACTION') -- TODO: all exec error values
    if not ok then return nil, err end
  end

  local old_tx = self.transaction
  self.transaction = true
  local res = table.pack(pcall(f, self, ...))
  self.transaction = old_tx

  if res[1] then
    local ok, err = self:exec('COMMIT') -- TODO: exec can return a bunch of error values
    if not ok then return nil, err end
		-- return true if f did not return anything
		if res.n == 1 then
			return true
		end
    return table.unpack(res, 2, res.n)
  else
    self:exec('ROLLBACK')
    return nil, res[2] -- TODO: all error values...
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
-- error. As a special-case, to allow asserting on the call, it returns true if
-- f succeeded and did not return anything (it can still return nil explicitly
-- and Connection:ensuretx will then return nil).
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
-- error. As a special-case, to allow asserting on the call, it returns true if
-- f succeeded and did not return anything (it can still return nil explicitly
-- and Connection:with will then return nil).
function Connection:with(close, f, ...)
  local res = table.pack(pcall(f, self, ...))
  if close then self:close() end
  if res[1] then
		-- return true if f did not return anything
		if res.n == 1 then
			return true
		end
    return table.unpack(res, 2, res.n)
  else
    return nil, res[2] -- TODO: all error values
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
    return nil, res:errorMessage(), status, res:resStatus(status), res:errorField(pgsql.PG_DIAG_SQLSTATE)
  end
end

-- Executes a query statement and returns the result if it succeeds, or nil, an
-- error message and the status code (number). If the error is not related to the
-- connection, then it also returns the string version of the status (e.g. PGRES_FATAL_ERROR)
-- and the SQL state code (e.g. 42P01, see https://www.postgresql.org/docs/current/errcodes-appendix.html).
--
-- Note that INSERT .. RETURNING must use Connection:query as it returns
-- values. The statement may contain $1, $2, etc. placeholders, they will be
-- replaced by the extra arguments provided to the method.
function Connection:query(stmt, ...)
  return exec_or_query(self._conn, stmt, pgsql.PGRES_TUPLES_OK, ...)
end

-- Executes a non-query statement and returns the result if it succeeds, or
-- nil, an error message and the status code (number). If the error is not
-- related to the connection, then it also returns the string version of the
-- status (e.g. PGRES_FATAL_ERROR) and the SQL state code (e.g. 42P01, see
-- https://www.postgresql.org/docs/current/errcodes-appendix.html).
--
-- Note that INSERT..RETURNING must use Connection:query as it returns values.
-- The statement may contain $1, $2, etc. placeholders, they will be replaced
-- by the extra arguments provided to the method.
function Connection:exec(stmt, ...)
  return exec_or_query(self._conn, stmt, pgsql.PGRES_COMMAND_OK, ...)
end

-- Combines a call to :query with a call to .model to return the first row
-- decoded into a table with column names as keys. If the last parameter
-- after the statement is a function, it is used as the 'newf' argument to
-- the call to .model, to provide further initialization of the row's table.
-- Returns the resulting table, or nil if the query did not return any row.
-- It returns nil along with any error in case of failure, as returned by
-- :query.
function Connection:get(stmt, ...)
	local newf

	local args = table.pack(...)
	if args.n > 0 then
		local last = args[#args]
		if type(last) == 'function' then
			newf = last
			args.n = args.n - 1
		end
	end

	local res, e1, e2, e3, e4 = self:query(stmt, table.unpack(args, 1, args.n))
	if not res then
		return e1, e2, e3, e4
	end
	return M.model(res, newf)
end

-- Combines a call to :query with a call to .models to return an array of rows
-- each decoded into a table with column names as keys. If the last parameter
-- after the statement is a function, it is used as the 'newf' argument to
-- the call to .models, to provide further initialization of each row's table.
-- Returns the resulting array (which is empty if the query returned no row),
-- or nil and any error as returned by :query.
function Connection:select(stmt, ...)
	local newf

	local args = table.pack(...)
	if args.n > 0 then
		local last = args[#args]
		if type(last) == 'function' then
			newf = last
			args.n = args.n - 1
		end
	end

	local res, e1, e2, e3, e4 = self:query(stmt, table.unpack(args, 1, args.n))
	if not res then
		return e1, e2, e3, e4
	end
	return M.models(res, newf)
end

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
