/* =====================================================================
   Build a Real-Time BI Dashboard with Interactive Warehouses
   ---------------------------------------------------------------------
   Single idempotent setup script — safe to re-run.
   Starts as ACCOUNTADMIN only to create a dedicated demo role, then
   switches to that role to create and own every demo object. Run in a
   region that supports interactive warehouses and Snowpark Container
   Services.

   Region availability:
     https://docs.snowflake.com/en/user-guide/interactive#region-availability
     https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview#available-regions

   What this creates:
     0.  INTERACTIVE_ANALYTICS_ROLE  demo role that owns everything below
     1.  Database + schema
     2.  INTERACTIVE_ANALYTICS_WH    standard warehouse (DDL, tasks, general queries)
     3.  ORDERS                      standard table (current tick snapshot)
     4.  INTERACTIVE_ANALYTICS_IWH   interactive warehouse (sub-second dashboard reads)
     5.  ORDERS_HISTORY              regular table (accumulates all ticks)
     6.  SIMULATION_CONTROL          control table (start/stop/config channel)
     7.  SP_SIMULATE_TICK            stored procedure (writes orders each tick)
     8.  INTERACTIVE_ANALYTICS_POOL  compute pool (runs the SPCS simulation service)
     9.  SIM_REPO                    image repository (private Docker registry)
     10. APP_STAGE                   stage (Streamlit app files)
     11. INTERACTIVE_ANALYTICS_AUTOSTOP        serverless task (idle auto-stop)
     12. INTERACTIVE_ANALYTICS_HISTORY_CLEANUP warehouse task (daily cleanup)
     13. Seed ORDERS with 2,000 rows so the dashboard renders immediately.
   ===================================================================== */

-- -------------------------------------------------------------------------
-- 0. Demo role
--    ACCOUNTADMIN is used only for this block.  The demo role gets just the
--    account-level privileges it needs to create its own objects, so the
--    long-running SPCS service never holds an ACCOUNTADMIN session.
-- -------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS INTERACTIVE_ANALYTICS_ROLE
  COMMENT = 'Owns all objects for the real-time BI dashboard demo';

GRANT CREATE DATABASE, CREATE WAREHOUSE, CREATE COMPUTE POOL,
      EXECUTE TASK, EXECUTE MANAGED TASK
  ON ACCOUNT TO ROLE INTERACTIVE_ANALYTICS_ROLE;

GRANT ROLE INTERACTIVE_ANALYTICS_ROLE TO ROLE SYSADMIN;

SET MY_USER = CURRENT_USER();
GRANT ROLE INTERACTIVE_ANALYTICS_ROLE TO USER IDENTIFIER($MY_USER);

USE ROLE INTERACTIVE_ANALYTICS_ROLE;

-- -------------------------------------------------------------------------
-- 1. Database and schema
-- -------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS INTERACTIVE_STREAMING_DEMO;
CREATE SCHEMA   IF NOT EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC;

-- -------------------------------------------------------------------------
-- 2. Standard warehouse
--    Handles DDL, stored procedure execution, task runs, and all queries
--    against ORDERS_HISTORY and SIMULATION_CONTROL. Interactive warehouses
--    reject DML and CALL statements, so this separate warehouse handles
--    every write and stored procedure call.
-- -------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS INTERACTIVE_ANALYTICS_WH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE INTERACTIVE_ANALYTICS_WH;

-- -------------------------------------------------------------------------
-- 3. ORDERS — standard table (current tick snapshot)
--    Interactive warehouses query standard tables directly (zero-copy), so
--    no special table type is needed.  CLUSTER BY matches the dashboard's
--    WHERE clause: a recent time window filtered by region.  Truncating to
--    the minute keeps the clustering key low-cardinality and prunes
--    efficiently for "last N minutes" queries.
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS (
  ORDER_ID          STRING,
  ORDER_TS          TIMESTAMP_NTZ,
  REGION            STRING,
  PRODUCT_CATEGORY  STRING,
  QUANTITY          INT,
  AMOUNT            NUMBER(10,2)
)
CLUSTER BY (DATE_TRUNC('MINUTE', ORDER_TS), REGION);

-- -------------------------------------------------------------------------
-- 4. Interactive warehouse — attach ORDERS so its cache stays warm
--    ADD TABLES is optional for zero-copy queries, but it tells the
--    warehouse to warm ORDERS proactively instead of on first query.
--    NOTE: interactive warehouses have a 24-hour minimum billing period.
--    Drop the warehouse in the Clean Up step as soon as you are done.
-- -------------------------------------------------------------------------
CREATE INTERACTIVE WAREHOUSE IF NOT EXISTS INTERACTIVE_ANALYTICS_IWH
  WAREHOUSE_SIZE      = 'XSMALL'
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE;

ALTER WAREHOUSE INTERACTIVE_ANALYTICS_IWH RESUME IF SUSPENDED;

ALTER WAREHOUSE INTERACTIVE_ANALYTICS_IWH
  ADD TABLES (INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS);

-- Queries that exceed the interactive warehouse's 5-second limit (for
-- example, while the cache is still cold right after a resume) are
-- transparently re-run on the standard warehouse instead of failing.
ALTER WAREHOUSE INTERACTIVE_ANALYTICS_IWH
  SET FALLBACK_WAREHOUSE = INTERACTIVE_ANALYTICS_WH;

-- CREATE INTERACTIVE WAREHOUSE makes the new warehouse current for this
-- session. Interactive warehouses cannot run DML, so switch back to the
-- standard warehouse before the INSERT statements below.
USE WAREHOUSE INTERACTIVE_ANALYTICS_WH;

-- -------------------------------------------------------------------------
-- 5. ORDERS_HISTORY — regular table (accumulates all ticks)
--    Used for time-series charts and the Region x Category heatmap.
--    Rows older than 1 day are purged by the HISTORY_CLEANUP task.
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS_HISTORY (
  ORDER_ID          STRING,
  ORDER_TS          TIMESTAMP_NTZ,
  REGION            STRING,
  PRODUCT_CATEGORY  STRING,
  QUANTITY          NUMBER,
  AMOUNT            NUMBER(10,2),
  TICK_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- -------------------------------------------------------------------------
-- 6. SIMULATION_CONTROL — command channel between dashboard and SPCS service
-- -------------------------------------------------------------------------
CREATE OR REPLACE TABLE INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL (
  is_running      BOOLEAN       NOT NULL DEFAULT FALSE,
  mode            VARCHAR       NOT NULL DEFAULT 'normal',
  flash_category  VARCHAR,
  rate_mult       FLOAT         NOT NULL DEFAULT 1.0,
  last_active_at  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  total_orders    NUMBER        NOT NULL DEFAULT 0
);

INSERT INTO INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL
  (is_running, mode, flash_category, rate_mult, last_active_at, total_orders)
VALUES (FALSE, 'normal', NULL, 1.0, CURRENT_TIMESTAMP(), 0);

-- -------------------------------------------------------------------------
-- 7. SP_SIMULATE_TICK — the heart of the simulation
--    Called by the SPCS service every second.  In a single round-trip it:
--      a) Replaces the ORDERS snapshot with INSERT OVERWRITE.
--      b) Appends the new rows to ORDERS_HISTORY.
--      c) Increments TOTAL_ORDERS in SIMULATION_CONTROL.
--
--    EXECUTE AS OWNER runs the writes with the owner's privileges
--    (INTERACTIVE_ANALYTICS_ROLE), so the procedure is the single entry
--    point that modifies ORDERS, ORDERS_HISTORY, and SIMULATION_CONTROL.
--
--    Each region has a preferred product category (graduated 28→8 %
--    probability spectrum), creating a visible diagonal in the heatmap.
-- -------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE INTERACTIVE_STREAMING_DEMO.PUBLIC.SP_SIMULATE_TICK(
    MODE           VARCHAR,
    FLASH_CATEGORY VARCHAR,
    NUM_ROWS       FLOAT,
    DOMINANT       VARCHAR,
    DOM_PCT        FLOAT
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS
$$
    var flash   = FLASH_CATEGORY || null;
    var numRows = Math.floor(NUM_ROWS);

    // ---- Tick categories ------------------------------------------------
    // Pick 2-5 categories randomly for this tick.
    // If a Flash Sale is active, the promoted category is always included.
    var all_cats = ['Electronics','Apparel','Home','Beauty','Sports','Toys'];
    for (var i = all_cats.length - 1; i > 0; i--) {   // Fisher-Yates shuffle
        var j = Math.floor(Math.random() * (i + 1));
        var tmp = all_cats[i]; all_cats[i] = all_cats[j]; all_cats[j] = tmp;
    }
    var n_cats    = Math.floor(Math.random() * 4) + 2;
    var tick_cats = all_cats.slice(0, n_cats);
    if (MODE === 'flash_sale' && flash && tick_cats.indexOf(flash) === -1) {
        tick_cats[tick_cats.length - 1] = flash;
    }

    var n         = tick_cats.length;
    var quoted    = tick_cats.map(function(c) { return "'" + c + "'"; });
    var cat_array = 'ARRAY_CONSTRUCT(' + quoted.join(',') + ')';
    var cat_idx   = 'UNIFORM(0,' + (n - 1) + ',RANDOM())';

    var cat_sql, amount_sql;

    if (MODE === 'flash_sale' && flash) {
        // Flash Sale: 50 % of orders go to the promoted category; prices elevated.
        cat_sql    = "CASE WHEN UNIFORM(0,99,RANDOM()) < 50 THEN '" + flash + "'"
                   + " ELSE " + cat_array + "[" + cat_idx + "]::STRING END";
        amount_sql = "CASE WHEN PRODUCT_CATEGORY = '" + flash + "'"
                   + " THEN UNIFORM(50,800,RANDOM()) ELSE UNIFORM(5,400,RANDOM()) END";
    } else {
        // Each region has a preferred product category with a graduated
        // probability spectrum (28 % down to 8 %), creating a visible
        // diagonal gradient in the Region x Category heatmap.
        var rw = {
            'North America': {'Electronics':28,'Sports':22,'Beauty':18,'Home':14,'Toys':10,'Apparel':8},
            'Europe':        {'Apparel':28,'Electronics':22,'Beauty':20,'Toys':14,'Home':8,'Sports':8},
            'APAC':          {'Toys':28,'Home':22,'Electronics':18,'Beauty':14,'Apparel':10,'Sports':8},
            'LATAM':         {'Home':28,'Apparel':22,'Beauty':18,'Sports':14,'Toys':10,'Electronics':8},
            'MEA':           {'Beauty':28,'Toys':22,'Home':18,'Apparel':14,'Electronics':10,'Sports':8}
        };
        var regs   = ['North America','Europe','APAC','LATAM','MEA'];
        var rcases = [];
        for (var ri = 0; ri < regs.length; ri++) {
            var reg = regs[ri];
            var w   = rw[reg];
            var wl  = tick_cats.map(function(c) { return {cat: c, w: w[c] || 8}; });
            wl.sort(function(a, b) { return b.w - a.w; });
            var tot = 0;
            for (var wi = 0; wi < wl.length; wi++) { tot += wl[wi].w; }
            var cum = 0;
            var inn = [];
            for (var k = 0; k < wl.length - 1; k++) {
                cum += Math.round(wl[k].w * 100 / tot);
                inn.push('WHEN rv < ' + cum + " THEN '" + wl[k].cat + "'");
            }
            inn.push("ELSE '" + wl[wl.length - 1].cat + "'");
            // One UNIFORM draw per row via scalar subquery for accurate distribution.
            rcases.push(
                "WHEN REGION = '" + reg + "' THEN "
                + '(SELECT CASE ' + inn.join(' ') + ' END'
                + ' FROM (SELECT UNIFORM(0,99,RANDOM()) AS rv) _t)'
            );
        }
        rcases.push('ELSE ' + cat_array + '[' + cat_idx + ']::STRING');
        cat_sql    = 'CASE ' + rcases.join(' ') + ' END';
        amount_sql = 'UNIFORM(5,400,RANDOM())';
    }

    var sql = [
        'INSERT OVERWRITE INTO INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS',
        'SELECT ORDER_ID, ORDER_TS, REGION, PRODUCT_CATEGORY, QUANTITY,',
        '       ROUND(UNIT_PRICE * QUANTITY, 2) AS AMOUNT',
        'FROM (',
        '  SELECT UUID_STRING() AS ORDER_ID,',
        "    DATEADD('second',-UNIFORM(0,300,RANDOM()),CURRENT_TIMESTAMP())",
        '      ::TIMESTAMP_NTZ AS ORDER_TS,',
        "    ARRAY_CONSTRUCT('North America','Europe','APAC','LATAM','MEA')",
        '      [UNIFORM(0,4,RANDOM())]::STRING AS REGION,',
        '    ' + cat_sql + ' AS PRODUCT_CATEGORY,',
        '    UNIFORM(1,5,RANDOM()) AS QUANTITY,',
        '    ' + amount_sql + ' AS UNIT_PRICE',
        '  FROM TABLE(GENERATOR(ROWCOUNT => ' + numRows + '))',
        ')'
    ];
    snowflake.execute({sqlText: sql.join(' ')});

    snowflake.execute({sqlText:
        'INSERT INTO INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS_HISTORY'
        + ' (ORDER_ID, ORDER_TS, REGION, PRODUCT_CATEGORY, QUANTITY, AMOUNT, TICK_AT)'
        + ' SELECT ORDER_ID, ORDER_TS, REGION, PRODUCT_CATEGORY, QUANTITY, AMOUNT,'
        + ' CURRENT_TIMESTAMP() FROM INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS'
    });

    snowflake.execute({sqlText:
        'UPDATE INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL'
        + ' SET TOTAL_ORDERS = COALESCE(TOTAL_ORDERS, 0) + ' + numRows
        + ',    LAST_ACTIVE_AT = CURRENT_TIMESTAMP()'
    });
    return 'OK';
$$;

-- -------------------------------------------------------------------------
-- 8. Compute pool + image repository
--    The compute pool hosts the SPCS simulation service container.
--    The image repository is a Snowflake-managed private Docker registry.
-- -------------------------------------------------------------------------
CREATE COMPUTE POOL IF NOT EXISTS INTERACTIVE_ANALYTICS_POOL
  MIN_NODES         = 1
  MAX_NODES         = 1
  INSTANCE_FAMILY   = CPU_X64_XS
  AUTO_RESUME       = TRUE
  AUTO_SUSPEND_SECS = 3600;

CREATE IMAGE REPOSITORY IF NOT EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.SIM_REPO;

-- Copy the repository_url value from this output — you will use it when
-- building and pushing the Docker image in the next step.
SHOW IMAGE REPOSITORIES LIKE 'SIM_REPO'
  IN SCHEMA INTERACTIVE_STREAMING_DEMO.PUBLIC;

-- -------------------------------------------------------------------------
-- 9. App stage  (Streamlit app files)
-- -------------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.APP_STAGE;

-- -------------------------------------------------------------------------
-- 10. Maintenance tasks
--     AUTOSTOP    — serverless task; stops the simulation after 1 h idle.
--     HISTORY_CLEANUP — warehouse task; purges ORDERS_HISTORY rows > 1 day.
-- -------------------------------------------------------------------------
CREATE OR REPLACE TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_AUTOSTOP
  SCHEDULE                                 = '15 MINUTE'
  USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
  COMMENT                                  = 'Auto-stop simulation when idle > 1 h'
AS
  UPDATE INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL
  SET is_running = FALSE
  WHERE is_running = TRUE
    AND last_active_at < DATEADD('hour', -1, CURRENT_TIMESTAMP());

CREATE OR REPLACE TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_HISTORY_CLEANUP
  WAREHOUSE = INTERACTIVE_ANALYTICS_WH
  SCHEDULE  = 'USING CRON 0 2 * * * UTC'
  COMMENT   = 'Delete ORDERS_HISTORY rows older than 1 day'
AS
  DELETE FROM INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS_HISTORY
  WHERE TICK_AT < DATEADD('day', -1, CURRENT_TIMESTAMP());

ALTER TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_AUTOSTOP RESUME;
ALTER TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_HISTORY_CLEANUP RESUME;

-- -------------------------------------------------------------------------
-- 11. Seed ORDERS with 2,000 rows
--     Ensures the dashboard renders immediately before the simulation starts.
-- -------------------------------------------------------------------------
INSERT OVERWRITE INTO INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS
SELECT
  UUID_STRING()                                                       AS ORDER_ID,
  DATEADD('second', -UNIFORM(0, 1800, RANDOM()), CURRENT_TIMESTAMP())
    ::TIMESTAMP_NTZ                                                   AS ORDER_TS,
  ARRAY_CONSTRUCT('North America','Europe','APAC','LATAM','MEA')
    [UNIFORM(0, 4, RANDOM())]::STRING                                 AS REGION,
  ARRAY_CONSTRUCT('Electronics','Apparel','Home','Beauty','Sports','Toys')
    [UNIFORM(0, 5, RANDOM())]::STRING                                 AS PRODUCT_CATEGORY,
  UNIFORM(1, 5, RANDOM())                                             AS QUANTITY,
  ROUND(UNIFORM(5, 400, RANDOM()) * UNIFORM(1, 5, RANDOM()), 2)      AS AMOUNT
FROM TABLE(GENERATOR(ROWCOUNT => 2000));

-- =========================================================================
-- Clean up (run when done to stop incurring costs)
-- =========================================================================
-- NOTE: Interactive warehouses have a 24-hour minimum billing period.
--       Drop the warehouse as soon as you are finished.
--
-- USE ROLE INTERACTIVE_ANALYTICS_ROLE;
-- USE WAREHOUSE INTERACTIVE_ANALYTICS_WH;
-- UPDATE INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL SET is_running = FALSE;
-- ALTER TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_AUTOSTOP SUSPEND;
-- ALTER TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_HISTORY_CLEANUP SUSPEND;
-- DROP SERVICE      IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_SVC;
-- DROP STREAMLIT    IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.LIVE_ORDERS_DASHBOARD;
-- DROP TASK         IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_AUTOSTOP;
-- DROP TASK         IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_HISTORY_CLEANUP;
-- DROP PROCEDURE    IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.SP_SIMULATE_TICK(VARCHAR, VARCHAR, FLOAT, VARCHAR, FLOAT);
-- DROP WAREHOUSE    IF EXISTS INTERACTIVE_ANALYTICS_IWH;
-- DROP WAREHOUSE    IF EXISTS INTERACTIVE_ANALYTICS_WH;
-- DROP COMPUTE POOL IF EXISTS INTERACTIVE_ANALYTICS_POOL;
-- DROP DATABASE     IF EXISTS INTERACTIVE_STREAMING_DEMO;
--
-- USE ROLE ACCOUNTADMIN;
-- DROP ROLE         IF EXISTS INTERACTIVE_ANALYTICS_ROLE;
