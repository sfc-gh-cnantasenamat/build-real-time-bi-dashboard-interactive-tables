# Build a Real-Time BI Dashboard with Interactive Warehouses

A complete, self-contained demo that generates live synthetic e-commerce orders inside Snowflake and serves a continuously updating BI dashboard. No Kafka, no external message bus, no infrastructure outside Snowflake.

![Live Orders Dashboard walkthrough: stopped, running, Flash Sale, and the Region x Category heatmap](quickstart/build-real-time-bi-dashboard-interactive-warehouses/assets/demo-screencast.gif)

## Architecture

![Architecture diagram](quickstart/build-real-time-bi-dashboard-interactive-warehouses/assets/architecture-diagram.png)

The system is built entirely inside a single Snowflake account across three layers:

1. A containerized **SPCS simulation service** runs a tight loop, calling a JavaScript stored procedure every second to generate synthetic orders. The stored procedure writes those orders into a clustered **standard table** that an **interactive warehouse** queries in place (zero-copy) with a warm cache, enabling sub-second reads by the dashboard.

   ```
   INTERACTIVE_ANALYTICS_SVC (SPCS)
     │  polls SIMULATION_CONTROL for is_running / mode / config
     └─ CALL SP_SIMULATE_TICK
             ├─ INSERT OVERWRITE ──▶ ORDERS (Table, clustered)
             │                            └─ INTERACTIVE_ANALYTICS_IWH serves low-latency reads
             ├─ INSERT INTO ──────▶ ORDERS_HISTORY (Table)
             └─ UPDATE ───────────▶ SIMULATION_CONTROL (total_orders, last_active_at)
   ```

2. A **Streamlit in Snowflake** app surfaces live KPI tiles, charts, and simulation controls. It reads the current snapshot through the interactive warehouse and queries accumulated history through the standard warehouse.

   ```
   LIVE_ORDERS_DASHBOARD
     ├─ reads ORDERS via INTERACTIVE_ANALYTICS_IWH    (KPI tiles, live chart)
     ├─ reads ORDERS_HISTORY via INTERACTIVE_ANALYTICS_WH  (time-series, heatmap)
     ├─ reads/writes SIMULATION_CONTROL              (Start / Stop / Flash Sale / New session)
     └─ resumes INTERACTIVE_ANALYTICS_SVC when suspended
   ```

3. Two background tasks act as safety nets: one clears a stale "running" flag when no ticks have landed for an hour, the other purges old history rows daily.

   ```
   INTERACTIVE_ANALYTICS_AUTOSTOP (serverless task, every 15 min)
     └─ sets SIMULATION_CONTROL.is_running = FALSE when is_running = TRUE
        but last_active_at is > 1 hour old (ticks stopped landing)

   INTERACTIVE_ANALYTICS_HISTORY_CLEANUP (warehouse task, daily 2am UTC)
     └─ uses INTERACTIVE_ANALYTICS_WH
     └─ deletes ORDERS_HISTORY rows older than 1 day
   ```

## Prerequisites

| Requirement | Notes |
|---|---|
| Snowflake account with `ACCOUNTADMIN` (used once to create the demo role) | Must be in a [region that supports interactive warehouses](https://docs.snowflake.com/en/user-guide/interactive#region-availability) and [SPCS](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview#available-regions) |
| [Docker Desktop](https://docs.docker.com/get-docker/) | For building the simulation container |
| [Git](https://git-scm.com/) | For cloning this repo |

## Quickstart

### 1. Run the setup SQL

Open a Snowsight worksheet and run `scripts/setup.sql`. It uses `ACCOUNTADMIN` only to create `INTERACTIVE_ANALYTICS_ROLE`, then switches to that role, which creates and owns every demo object. The script is idempotent, so it is safe to re-run.

When the script finishes, copy the `repository_url` value from the `SHOW IMAGE REPOSITORIES` output. It looks like:
```
<orgname>-<accountname>.registry.snowflakecomputing.com/interactive_streaming_demo/public/sim_repo
```

### 2. Build and push the container image

```bash
# From the repo root
docker build -t simulation ./spcs/

# Apple Silicon? Add --platform linux/amd64
# docker build --platform linux/amd64 -t simulation ./spcs/

docker login <your_registry_url>

docker tag simulation <your_registry_url>/simulation:latest
docker push <your_registry_url>/simulation:latest
```

### 3. Deploy the SPCS simulation service

```sql
USE ROLE INTERACTIVE_ANALYTICS_ROLE;
USE WAREHOUSE INTERACTIVE_ANALYTICS_WH;

CREATE SERVICE IF NOT EXISTS
  INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_SVC
  IN COMPUTE POOL INTERACTIVE_ANALYTICS_POOL
  FROM SPECIFICATION $$
  spec:
    container:
    - name: simulator
      image: /interactive_streaming_demo/public/sim_repo/simulation:latest
      env:
        SNOWFLAKE_WAREHOUSE: INTERACTIVE_ANALYTICS_WH
        SIM_INTERVAL: "1"
        IDLE_SUSPEND_SECS: "3600"
        ROWS_BASE: "100"
        CTRL_CACHE_TICKS: "5"
  $$
  MIN_INSTANCES = 1
  MAX_INSTANCES = 1;

-- Wait until RUNNING (typically 30-60 seconds)
SHOW SERVICES LIKE 'INTERACTIVE_ANALYTICS_SVC'
  IN SCHEMA INTERACTIVE_STREAMING_DEMO.PUBLIC;
```

### 4. Deploy the Streamlit dashboard

Using SnowSQL or the Snowflake Python connector, from the `streamlit/` directory:

```sql
USE ROLE INTERACTIVE_ANALYTICS_ROLE;

PUT file://streamlit_app.py
    @INTERACTIVE_STREAMING_DEMO.PUBLIC.APP_STAGE/streamlit/
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

PUT file://environment.yml
    @INTERACTIVE_STREAMING_DEMO.PUBLIC.APP_STAGE/streamlit/
    OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

CREATE OR REPLACE STREAMLIT INTERACTIVE_STREAMING_DEMO.PUBLIC.LIVE_ORDERS_DASHBOARD
  ROOT_LOCATION   = '@INTERACTIVE_STREAMING_DEMO.PUBLIC.APP_STAGE/streamlit'
  MAIN_FILE       = 'streamlit_app.py'
  QUERY_WAREHOUSE = INTERACTIVE_ANALYTICS_WH
  TITLE           = 'Live Orders Dashboard';
```

Open the app in Snowsight under **Projects > Streamlit > LIVE_ORDERS_DASHBOARD**.

### 5. Explore the dashboard

Click **Start** in the sidebar. The simulation begins generating orders immediately. Try:
- **Rate multiplier**: scale order volume up or down
- **Flash Sale**: spike a product category to 50 % of all orders
- **New session**: clear history and reset counters for a fresh demo

## Cost notes

- **Interactive warehouse** (`INTERACTIVE_ANALYTICS_IWH`) has a **24-hour minimum billing period** once created. Drop it as soon as you are done.
- **Compute pool** (`INTERACTIVE_ANALYTICS_POOL`) auto-suspends after 1 hour of idle time.
- **Click Stop when you are done.** Every tick refreshes `last_active_at`, so a running simulation keeps running (and keeps the pool busy) even if nobody has the dashboard open. After you click Stop, the service self-suspends once it has been idle for `IDLE_SUSPEND_SECS` (1 hour).
- **AUTOSTOP task** is a safety net only: it clears `is_running` if the flag is still TRUE but no tick has landed for 1 hour, for example after the service crashed.

## Clean up

Run the commented-out `Clean up` block at the bottom of `scripts/setup.sql`, or paste:

```sql
USE ROLE INTERACTIVE_ANALYTICS_ROLE;
USE WAREHOUSE INTERACTIVE_ANALYTICS_WH;
UPDATE INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL SET is_running = FALSE;
ALTER TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_AUTOSTOP SUSPEND;
ALTER TASK INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_HISTORY_CLEANUP SUSPEND;
DROP SERVICE      IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_SVC;
DROP STREAMLIT    IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.LIVE_ORDERS_DASHBOARD;
DROP TASK         IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_AUTOSTOP;
DROP TASK         IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_HISTORY_CLEANUP;
DROP PROCEDURE    IF EXISTS INTERACTIVE_STREAMING_DEMO.PUBLIC.SP_SIMULATE_TICK(VARCHAR, VARCHAR, FLOAT, VARCHAR, FLOAT);
DROP WAREHOUSE    IF EXISTS INTERACTIVE_ANALYTICS_IWH;
DROP WAREHOUSE    IF EXISTS INTERACTIVE_ANALYTICS_WH;
DROP COMPUTE POOL IF EXISTS INTERACTIVE_ANALYTICS_POOL;
DROP DATABASE     IF EXISTS INTERACTIVE_STREAMING_DEMO;

USE ROLE ACCOUNTADMIN;
DROP ROLE         IF EXISTS INTERACTIVE_ANALYTICS_ROLE;
```

## Full tutorial

See the [step-by-step quickstart guide](quickstart/build-real-time-bi-dashboard-interactive-warehouses/build-real-time-bi-dashboard-interactive-warehouses.md) for a detailed walkthrough with explanations of each Snowflake object.
