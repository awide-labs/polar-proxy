-- Workload for checking client-visible RFQ LSN consistency through sysbench.
--
-- The script works in two modes:
--
--   1. unmodified sysbench + LD_PRELOAD tracer
--   2. sysbench patched with polar-rfq native Lua methods
--
-- In native mode, con:polar_query(sql, kind) records the RFQ LSN into the
-- same two contexts as the tracer: session/global and transaction/local.

local native_polar = false

local function has_native_polar(con)
  return type(con.polar_query) == "function" and
         type(con.polar_reset_context) == "function"
end

local function print_polar_report(label, report)
  if report == nil then
    return
  end

  print(string.format(
    "SYSBENCH_POLAR_LSN label=%s kind=%s has_lsn=%s lsn=%s " ..
    "target=%s verdict=%s session_target=%s txn_target=%s " ..
    "txn_active=%s",
    label,
    tostring(report.kind),
    tostring(report.has_lsn),
    tostring(report.lsn),
    tostring(report.target_lsn),
    tostring(report.verdict),
    tostring(report.context.session_target_lsn),
    tostring(report.context.txn_target_lsn),
    tostring(report.context.txn_active)))
end

local function polar_query(sql, kind, label)
  if native_polar then
    local rs, report = con:polar_query(sql, kind)
    print_polar_report(label, report)
    return rs
  end

  return con:query(sql)
end

function thread_init()
  drv = sysbench.sql.driver()
  con = drv:connect()
  native_polar = has_native_polar(con)

  if native_polar then
    con:polar_reset_context()
  end

  polar_query([[
    CREATE TABLE IF NOT EXISTS polardb_lsn_check (
      id bigint PRIMARY KEY,
      val bigint NOT NULL DEFAULT 0
    )
  ]], "observe", "setup")
end

function event()
  local id = sysbench.rand.uniform(1, 1000000)

  polar_query("BEGIN", "begin", "begin")
  polar_query(string.format([[
    INSERT INTO polardb_lsn_check(id, val)
      VALUES (%d, 1)
      ON CONFLICT (id) DO UPDATE SET val = polardb_lsn_check.val + 1
  ]], id), "write", "write")
  polar_query(string.format(
    "SELECT val FROM polardb_lsn_check WHERE id = %d", id),
    "read", "read")
  polar_query("COMMIT", "commit", "commit")
end

function thread_done()
  con:disconnect()
end
