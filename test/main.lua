local lu = require 'luaunit'
local pgsql = require 'pgsql'
local xpgsql = require 'xpgsql'

local mainConn = assert(xpgsql.connect())

local function drop_table()
  assert(mainConn:exec [[
    DROP TABLE IF EXISTS test_xpgsql
  ]])
end

drop_table()

local function count_connections()
  local res = mainConn:query [[
    SELECT
      count(*) as conns
    FROM
      pg_stat_activity
  ]]
  return tonumber(res[1][1])
end

local function insert_rows(...)
  local n = select('#', ...)
  for i = 1, n do
    assert(mainConn:exec([[
      INSERT INTO
        test_xpgsql (val)
      VALUES
        ($1)
    ]], (select(i, ...))))
  end
end

local function ensure_table()
  assert(mainConn:exec [[
    CREATE TABLE IF NOT EXISTS test_xpgsql (
      id  SERIAL NOT NULL,
      val VARCHAR(100) NULL,

      PRIMARY KEY (id),
      UNIQUE (val)
    )
  ]])
end

TestXpgsql = {}
function TestXpgsql.test_connect_fail()
  local conn, err, code = xpgsql.connect('postgresql://invalid:pwd@localhost:1234/no-such-db')
  lu.assertNil(conn)
  lu.assertStrContains(err, 'Connection refused')
  lu.assertEquals(code, pgsql.CONNECTION_BAD)
end

function TestXpgsql.test_connect_ok()
  local before = count_connections()
  lu.assertTrue(before >= 1)

  local conn, err = xpgsql.connect()
  lu.assertNil(err)
  lu.assertNotNil(conn)

  local after = count_connections()
  lu.assertTrue(after > before)

  conn:close()

  local atend = count_connections()
  lu.assertTrue(atend < after)
end

function TestXpgsql.test_exec_fail()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local res, err, code = conn:exec([[
    INSERT INTO
      test_xpgsql (NO_SUCH_VAL)
    VALUES
      ($1)
  ]], 'a')

  lu.assertNil(res)
  lu.assertStrContains(err, 'column "no_such_val"')
  lu.assertEquals(code, pgsql.PGRES_FATAL_ERROR)

  conn:close()
end

function TestXpgsql.test_exec_ok()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local res, err = conn:exec([[
    INSERT INTO
      test_xpgsql (val)
    VALUES
      ($1)
  ]], 'a')

  lu.assertNil(err)
  -- number of rows affected
  lu.assertEquals(tonumber(res:cmdTuples()), 1)

  conn:close()
end

function TestXpgsql.test_query_fail()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local res, err, code = conn:query([[
    SELECT
      *
    FROM
      no_shcu_table
    WHERE
      id = $1
  ]], 123)

  lu.assertNil(res)
  lu.assertStrContains(err, 'relation "no_shcu_table"')
  lu.assertEquals(code, pgsql.PGRES_FATAL_ERROR)

  conn:close()
end

function TestXpgsql.test_query_ok()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local res, err = conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'no-such-val')

  lu.assertNil(err)
  lu.assertEquals(res:ntuples(), 0)
  lu.assertEquals(res:nfields(), 2)

  conn:close()
end

function TestXpgsql.test_insert_returning_fail()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local res, err, code = conn:query([[
    INSERT INTO
      test_xpgsql (val)
    VALUES
      ($1)
    RETURNING
      no_such_col
  ]], 'c')

  lu.assertNil(res)
  lu.assertStrContains(err, 'column "no_such_col"')
  lu.assertEquals(code, pgsql.PGRES_FATAL_ERROR)

  conn:close()
end

function TestXpgsql.test_insert_returning_ok()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local res, err = conn:query([[
    INSERT INTO
      test_xpgsql (val)
    VALUES
      ($1)
    RETURNING
      id
  ]], 'd')

  lu.assertNil(err)
  lu.assertEquals(res:ntuples(), 1)
  lu.assertEquals(res:nfields(), 1)
  lu.assertTrue(tonumber(res[1][1]) > 0)

  conn:close()
end

function TestXpgsql.test_model_none()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local res = xpgsql.model(conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'no-such-val'))

  lu.assertNil(res)

  conn:close()
end

function TestXpgsql.test_model_some()
  ensure_table()
  insert_rows('e')

  local conn = assert(xpgsql.connect())
  local res = xpgsql.model(conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'e'))

  lu.assertEquals(res.val, 'e')
  lu.assertString(res.id)
  lu.assertTrue(tonumber(res.id) > 0)

  conn:close()
end

local function newmodel(res)
  res.id = tonumber(res.id)
  return res
end

function TestXpgsql.test_model_some_fn()
  ensure_table()
  insert_rows('f')

  local conn = assert(xpgsql.connect())
  local res = xpgsql.model(conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'f'), newmodel)

  lu.assertEquals(res.val, 'f')
  lu.assertNumber(res.id)
  lu.assertTrue(res.id > 0)

  conn:close()
end

function TestXpgsql.test_models_none()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local res = xpgsql.models(conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'no-such-val'))

  lu.assertEquals(res, {})

  conn:close()
end

function TestXpgsql.test_models_some()
  ensure_table()
  insert_rows('g', 'h')

  local conn = assert(xpgsql.connect())
  local res = xpgsql.models(conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val IN ($1, $2)
    ORDER BY
      val
  ]], 'g', 'h'))

  lu.assertEquals(#res, 2)
  lu.assertEquals(res[1].val, 'g')
  lu.assertEquals(res[2].val, 'h')

  conn:close()
end

function TestXpgsql.test_models_some_fn()
  ensure_table()
  insert_rows('i', 'j')

  local conn = assert(xpgsql.connect())
  local res = xpgsql.models(conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val IN ($1, $2)
    ORDER BY
      val
  ]], 'i', 'j'), newmodel)

  lu.assertEquals(#res, 2)
  lu.assertNumber(res[1].id)
  lu.assertNumber(res[2].id)

  conn:close()
end

function TestXpgsql.test_format_array_string()
  ensure_table()
  insert_rows('k', 'l')

  local conn = assert(xpgsql.connect())
  local res, err = conn:query(string.format([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val IN (%s)
  ]], conn:format_array{'k', 'l'}))

  lu.assertNil(err)
  lu.assertEquals(res:ntuples(), 2)

  conn:close()
end

function TestXpgsql.test_format_array_int()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local res, err = conn:query(string.format([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      id IN (%s)
  ]], conn:format_array{1,2,3}))

  lu.assertNil(err)
  lu.assertNumber(res:ntuples())

  conn:close()
end

function TestXpgsql.test_format_array_empty()
  local conn = assert(xpgsql.connect())
  lu.assertEquals(conn:format_array{}, '')
  conn:close()
end

function TestXpgsql.test_format_array_invalid()
  local conn = assert(xpgsql.connect())
  lu.assertErrorMsgContains('invalid value type', function()
    conn:format_array{{}, {}}
  end)
  conn:close()
end

function TestXpgsql.test_tx_fail()
  local conn = assert(xpgsql.connect())

  local ok, err = conn:tx(function(c, arg)
    lu.assertTrue(c.transaction)
    assert(c:exec([[
      INSERT INTO
        test_xpgsql (val)
      VALUES
        ($1)
    ]], arg))
    error('rollback')
  end, 'm')
  lu.assertTrue(not conn.transaction)

  local res = assert(conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'm'))

  lu.assertNil(ok)
  lu.assertStrContains(err, 'rollback')
  lu.assertEquals(res:ntuples(), 0)

  conn:close()
end

function TestXpgsql.test_tx_ok()
  local conn = assert(xpgsql.connect())

  local ok, err = conn:tx(function(c, arg)
    lu.assertTrue(c.transaction)
    return assert(c:exec([[
      INSERT INTO
        test_xpgsql (val)
      VALUES
        ($1)
    ]], arg))
  end, 'n')
  lu.assertTrue(not conn.transaction)

  local res = assert(conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'n'))

  lu.assertNil(err)
  lu.assertEquals(ok:cmdTuples(), '1')
  lu.assertEquals(res:ntuples(), 1)

  conn:close()
end

local code = lu.LuaUnit.run()
mainConn:close()
os.exit(code)
