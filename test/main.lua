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

function TestXpgsql.test_get_none()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local res = conn:get([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'no-such-val')

  lu.assertNil(res)

  conn:close()
end

function TestXpgsql.test_get_some()
  ensure_table()
  insert_rows('ee')

  local conn = assert(xpgsql.connect())
  local res = conn:get([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'ee')

  lu.assertEquals(res.val, 'ee')
  lu.assertString(res.id)
  lu.assertTrue(tonumber(res.id) > 0)

  conn:close()
end

function TestXpgsql.test_get_some_fn()
  ensure_table()
  insert_rows('ff')

  local conn = assert(xpgsql.connect())
  local res = conn:get([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'ff', newmodel)

  lu.assertEquals(res.val, 'ff')
  lu.assertNumber(res.id)
  lu.assertTrue(res.id > 0)

  conn:close()
end

function TestXpgsql.test_select_none()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local res = conn:select([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'no-such-val')

  lu.assertEquals(res, {})

  conn:close()
end

function TestXpgsql.test_select_some()
  ensure_table()
  insert_rows('gg', 'hh')

  local conn = assert(xpgsql.connect())
  local res = conn:select([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val IN ($1, $2)
    ORDER BY
      val
  ]], 'gg', 'hh')

  lu.assertEquals(#res, 2)
  lu.assertEquals(res[1].val, 'gg')
  lu.assertEquals(res[2].val, 'hh')

  conn:close()
end

function TestXpgsql.test_select_some_fn()
  ensure_table()
  insert_rows('ii', 'jj')

  local conn = assert(xpgsql.connect())
  local res = conn:select([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val IN ($1, $2)
    ORDER BY
      val
  ]], 'ii', 'jj', newmodel)

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
  ensure_table()

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
  ensure_table()

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

function TestXpgsql.test_ensuretx_ok()
  ensure_table()

  local conn = assert(xpgsql.connect())

  local ok, err = conn:ensuretx(function(c, arg)
    return assert(c:exec([[
      INSERT INTO
        test_xpgsql (val)
      VALUES
        ($1)
    ]], arg))
  end, 'o')

  local res = assert(conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'o'))

  lu.assertNil(err)
  lu.assertEquals(ok:cmdTuples(), '1')
  lu.assertEquals(res:ntuples(), 1)

  conn:close()
end

function TestXpgsql.test_ensuretx_existing_ok()
  ensure_table()

  local conn = assert(xpgsql.connect())

  local ok, err = conn:tx(function(c, arg)
    return assert(c:ensuretx(function(c, arg)
      return assert(c:exec([[
        INSERT INTO
          test_xpgsql (val)
        VALUES
          ($1)
      ]], arg))
    end, arg))
  end, 'p')

  local res = assert(conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'p'))

  lu.assertNil(err)
  lu.assertEquals(ok:cmdTuples(), '1')
  lu.assertEquals(res:ntuples(), 1)

  conn:close()
end

function TestXpgsql.test_ensuretx_existing_fail()
  ensure_table()

  local conn = assert(xpgsql.connect())

  local ok, err = conn:tx(function(c, arg)
    -- insert 'q', will be rollbacked
    assert(c:exec([[
      INSERT INTO
        test_xpgsql (val)
      VALUES
        ($1)
    ]], arg))
    return assert(c:ensuretx(function(c, arg)
      return assert(c:exec([[
        INSERT INTO
          test_xpgsql (val)
        VALUES
          (z)
      ]]))
    end, arg))
  end, 'q')

  lu.assertNil(ok)
  lu.assertStrContains(err, 'column "z" does not exist')

  local res = assert(conn:query([[
    SELECT
      *
    FROM
      test_xpgsql
    WHERE
      val = $1
  ]], 'q'))

  lu.assertEquals(res:ntuples(), 0)

  conn:close()
end

function TestXpgsql.test_with_close()
  ensure_table()
  insert_rows('r')

  local conn = assert(xpgsql.connect())
  local ok, err = conn:with(true, function(c, arg)
    return assert(c:query([[
      SELECT
        *
      FROM
        test_xpgsql
      WHERE
        val = $1
    ]], arg))
  end, 'r')

  lu.assertNil(err)
  lu.assertEquals(ok:ntuples(), 1)
  lu.assertEquals(ok[1].val, 'r')

  ok, err = pcall(conn.query, conn, 'SELECT 1')
  lu.assertFalse(ok)
  lu.assertStrContains(err, 'connection closed')

  conn:close()
end

function TestXpgsql.test_with_noclose()
  ensure_table()
  insert_rows('s')

  local conn = assert(xpgsql.connect())
  local ok, err = conn:with(false, function(c, arg)
    return assert(c:query([[
      SELECT
        *
      FROM
        test_xpgsql
      WHERE
        val = $1
    ]], arg))
  end, 's')

  lu.assertNil(err)
  lu.assertEquals(ok:ntuples(), 1)
  lu.assertEquals(ok[1].val, 's')

  ok, res = pcall(conn.query, conn, 'SELECT 1')
  lu.assertTrue(ok)
  lu.assertEquals(res:ntuples(), 1)
  lu.assertEquals(res[1][1], '1')

  conn:close()
end

function TestXpgsql.test_tx_noreturn()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local ok, err = conn:tx(function(c)
    assert(c:query([[
      SELECT
        COUNT(*)
      FROM
        test_xpgsql
    ]]))
  end)

	conn:close()
  lu.assertNil(err)
	lu.assertTrue(ok)
end

function TestXpgsql.test_tx_returnnil()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local ok, err = conn:tx(function(c)
    assert(c:query([[
      SELECT
        COUNT(*)
      FROM
        test_xpgsql
    ]]))
		return nil
  end)

	conn:close()
  lu.assertNil(err)
	lu.assertNil(ok)
end

function TestXpgsql.test_ensuretx_noreturn()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local ok, err = conn:ensuretx(function(c)
    assert(c:query([[
      SELECT
        COUNT(*)
      FROM
        test_xpgsql
    ]]))
  end)

	conn:close()
  lu.assertNil(err)
	lu.assertTrue(ok)
end

function TestXpgsql.test_ensuretx_returnnil()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local ok, err = conn:ensuretx(function(c)
    assert(c:query([[
      SELECT
        COUNT(*)
      FROM
        test_xpgsql
    ]]))
		return nil
  end)

	conn:close()
  lu.assertNil(err)
	lu.assertNil(ok)
end

function TestXpgsql.test_with_noreturn()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local ok, err = conn:with(true, function(c)
    assert(c:query([[
      SELECT
        COUNT(*)
      FROM
        test_xpgsql
    ]]))
  end)

  lu.assertNil(err)
	lu.assertTrue(ok)
end

function TestXpgsql.test_with_returnnil()
  ensure_table()

  local conn = assert(xpgsql.connect())
  local ok, err = conn:with(true, function(c)
    assert(c:query([[
      SELECT
        COUNT(*)
      FROM
        test_xpgsql
    ]]))
		return nil
  end)

  lu.assertNil(err)
	lu.assertNil(ok)
end

function TestXpgsql.test_tx_err()
  ensure_table()
  insert_rows('t')

  local conn = assert(xpgsql.connect())
	-- no extra error information, due to assert raising only the error message
  local ok, err, extra = conn:tx(function(c)
    assert(c:exec([[
      INSERT INTO
        test_xpgsql (val)
      VALUES
        ($1)
    ]], 't'))
  end)

	conn:close()
	lu.assertNil(ok)
  lu.assertNotNil(err)
  lu.assertNil(extra)
	lu.assertStrContains(err, 'duplicate key value violates unique constraint')
end

function TestXpgsql.test_with_err()
  ensure_table()
  insert_rows('u')

  local conn = assert(xpgsql.connect())
	-- no extra error information, due to assert raising only the error message
  local ok, err, extra = conn:with(true, function(c)
    assert(c:exec([[
      INSERT INTO
        test_xpgsql (val)
      VALUES
        ($1)
    ]], 'u'))
  end)

	lu.assertNil(ok)
  lu.assertNotNil(err)
  lu.assertNil(extra)
	lu.assertStrContains(err, 'duplicate key value violates unique constraint')
end

function TestXpgsql.test_tx_errtfm()
	xpgsql.transform_error = function(msg, code, status, state)
		return {msg = msg, code = code, status = status, state = state}
	end

  ensure_table()
  insert_rows('v')

  local conn = assert(xpgsql.connect())
  local ok, err, extra = conn:tx(function(c)
    assert(c:exec([[
      INSERT INTO
        test_xpgsql (val)
      VALUES
        ($1)
    ]], 'v'))
  end)

	conn:close()
	xpgsql.transform_error = nil
	lu.assertNil(ok)
  lu.assertNotNil(err)
  lu.assertNil(extra)
	lu.assertStrContains(err.msg, 'duplicate key value violates unique constraint')
	err.msg = nil
	lu.assertEquals({code = 7, state = '23505', status = 'PGRES_FATAL_ERROR'}, err)
end

function TestXpgsql.test_with_errtfm()
	xpgsql.transform_error = function(msg, code, status, state)
		return {msg = msg, code = code, status = status, state = state}
	end

  ensure_table()
  insert_rows('w')

  local conn = assert(xpgsql.connect())
  local ok, err, extra = conn:with(true, function(c)
    assert(c:exec([[
      INSERT INTO
        test_xpgsql (val)
      VALUES
        ($1)
    ]], 'w'))
  end)

	xpgsql.transform_error = nil
	lu.assertNil(ok)
  lu.assertNotNil(err)
  lu.assertNil(extra)
	lu.assertStrContains(err.msg, 'duplicate key value violates unique constraint')
	err.msg = nil
	lu.assertEquals({code = 7, state = '23505', status = 'PGRES_FATAL_ERROR'}, err)
end

local code = lu.LuaUnit.run()
mainConn:close()
os.exit(code)
