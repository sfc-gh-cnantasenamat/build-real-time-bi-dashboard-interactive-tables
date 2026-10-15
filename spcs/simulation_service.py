"""Simulation service for the interactive warehouse demo.

DB / schema    : INTERACTIVE_STREAMING_DEMO.PUBLIC
Role           : INTERACTIVE_ANALYTICS_ROLE
Compute pool   : INTERACTIVE_ANALYTICS_POOL

Runs as a long-lived SPCS container. Every SIM_INTERVAL seconds (default 1)
it calls SP_SIMULATE_TICK (EXECUTE AS OWNER = INTERACTIVE_ANALYTICS_ROLE) which does
INSERT OVERWRITE into the ORDERS table and updates
SIMULATION_CONTROL in one round-trip.

Optimisations:
- ROWS_BASE=100 (rows generated per tick, scaled by business-hours curve)
- CTRL_CACHE_TICKS=5  (re-read SIMULATION_CONTROL every 5 ticks)
- UPDATE last_active_at + total_orders merged into SP (no extra round-trip)

Cost-saving behaviour
---------------------
When is_running = FALSE and last_active_at idle >= IDLE_SUSPEND_SECS
(default 3600 = 1 h), the container issues ALTER SERVICE ... SUSPEND and
exits. The Streamlit dashboard's Start button resumes it automatically.

Authentication uses the SPCS-internal OAuth token — no key-pair needed.
"""

from __future__ import annotations

import datetime
import logging
import math
import os
import random
import sys
import time

import snowflake.connector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SERVICE_FQN = "INTERACTIVE_STREAMING_DEMO.PUBLIC.INTERACTIVE_ANALYTICS_SVC"
TABLE = "INTERACTIVE_STREAMING_DEMO.PUBLIC.ORDERS"
CONTROL = "INTERACTIVE_STREAMING_DEMO.PUBLIC.SIMULATION_CONTROL"

INTERVAL = float(os.environ.get("SIM_INTERVAL", "1"))
IDLE_SUSPEND_SECS = int(os.environ.get("IDLE_SUSPEND_SECS", "3600"))
WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE", "INTERACTIVE_ANALYTICS_WH")
# SPCS OAuth sessions run as the service's owner role.
ROLE = os.environ.get("SNOWFLAKE_ROLE", "INTERACTIVE_ANALYTICS_ROLE")
ROWS_BASE = int(os.environ.get("ROWS_BASE", "100"))   # rows per tick before scaling
# Re-read SIMULATION_CONTROL every N ticks; use cached value in between.
# The SP already updates last_active_at, so idle detection still works.
CTRL_CACHE_TICKS = int(os.environ.get("CTRL_CACHE_TICKS", "5"))

REGIONS = ["North America", "Europe", "APAC", "LATAM", "MEA"]

# ---------------------------------------------------------------------------
# Snowflake connection (SPCS internal OAuth — no key-pair needed)
# ---------------------------------------------------------------------------
TOKEN_FILE = "/snowflake/session/token"


def connect() -> snowflake.connector.SnowflakeConnection:
    with open(TOKEN_FILE) as f:
        token = f.read().strip()
    return snowflake.connector.connect(
        host=os.environ["SNOWFLAKE_HOST"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        authenticator="oauth",
        token=token,
        database="INTERACTIVE_STREAMING_DEMO",
        schema="PUBLIC",
        role=ROLE,
        warehouse=WAREHOUSE,
    )


# ---------------------------------------------------------------------------
# Tick parameters
# ---------------------------------------------------------------------------
def _compute_tick(mode: str, flash_cat: str | None, rate_mult: float) -> dict:
    """Return parameters for one SP_SIMULATE_TICK call."""
    now = datetime.datetime.utcnow()
    h = now.hour + now.minute / 60.0

    biz = 0.20 + 0.80 * max(
        math.exp(-0.5 * ((h - 10.0) / 2.0) ** 2),
        math.exp(-0.5 * ((h - 15.0) / 2.0) ** 2),
    )

    base = int(ROWS_BASE * biz * float(rate_mult or 1.0))
    if mode == "flash_sale":
        base = int(base * 2.5)
    if random.random() < 0.10:
        base = int(base * random.uniform(1.5, 3.0))
    rows = max(3, min(100, base))

    dominant = REGIONS[int(h / (24.0 / len(REGIONS))) % len(REGIONS)]
    return {
        "rows": rows,
        "dominant": dominant,
        "dom_pct": 40,
    }


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main() -> None:
    logging.info(
        "Simulation service starting (interval=%.1fs, idle_suspend=%ds, rows_base=%d, cache_ticks=%d)",
        INTERVAL, IDLE_SUSPEND_SECS, ROWS_BASE, CTRL_CACHE_TICKS,
    )

    conn = connect()
    cur = conn.cursor()
    cur.execute(f"USE WAREHOUSE {WAREHOUSE}")
    logging.info("Connected to Snowflake as %s", ROLE)

    # Reset idle timer on startup (SP handles last_active_at during active ticks)
    cur.execute(f"UPDATE {CONTROL} SET last_active_at = CURRENT_TIMESTAMP()")
    logging.info("Idle timer reset")

    consecutive_errors = 0
    tick = 0
    ctrl_cache: tuple | None = None   # (is_running, mode, flash_cat, rate_mult, idle_secs)
    ticks_since_ctrl_read = 0

    while True:
        try:
            # Re-read SIMULATION_CONTROL every CTRL_CACHE_TICKS ticks or on first tick.
            if ctrl_cache is None or ticks_since_ctrl_read >= CTRL_CACHE_TICKS:
                cur.execute(
                    f"SELECT is_running, mode, flash_category, rate_mult, "
                    f"DATEDIFF('second', last_active_at, CURRENT_TIMESTAMP()) AS idle_secs "
                    f"FROM {CONTROL} LIMIT 1"
                )
                ctrl_cache = cur.fetchone()
                ticks_since_ctrl_read = 0
            else:
                ticks_since_ctrl_read += 1

            row = ctrl_cache

            if row and row[0]:  # is_running = TRUE
                mode = row[1] or "normal"
                flash_cat = row[2]
                rate_mult = float(row[3] or 1.0)

                params = _compute_tick(mode, flash_cat, rate_mult)

                # SP runs as its owner (INTERACTIVE_ANALYTICS_ROLE) for INSERT OVERWRITE.
                # Also updates last_active_at + total_orders inside the SP,
                # saving one round-trip vs doing it here.
                cur.execute(
                    "CALL INTERACTIVE_STREAMING_DEMO.PUBLIC.SP_SIMULATE_TICK(%s, %s, %s, %s, %s)",
                    (mode, flash_cat, float(params["rows"]), params["dominant"], float(params["dom_pct"])),
                )

                tick += 1
                logging.info(
                    "tick %d: %d rows | dominant=%s | mode=%s",
                    tick, params["rows"], params["dominant"], mode,
                )

            else:
                idle_secs = int(row[4]) if row else IDLE_SUSPEND_SECS + 1
                logging.debug("Paused. Idle for %ds / %ds", idle_secs, IDLE_SUSPEND_SECS)

                if idle_secs >= IDLE_SUSPEND_SECS:
                    logging.info(
                        "Idle for %d seconds (>= %d). Suspending service.",
                        idle_secs, IDLE_SUSPEND_SECS,
                    )
                    try:
                        cur.execute(f"ALTER SERVICE {SERVICE_FQN} SUSPEND")
                    except Exception as suspend_exc:
                        logging.error("SUSPEND failed: %s", suspend_exc)
                    logging.info("SUSPEND issued. Exiting.")
                    sys.exit(0)

            consecutive_errors = 0

        except SystemExit:
            raise

        except Exception as exc:
            consecutive_errors += 1
            ctrl_cache = None  # force re-read after error
            logging.error("Tick error (%d consecutive): %s", consecutive_errors, exc)
            if consecutive_errors >= 3:
                logging.warning("3 consecutive errors — reconnecting...")
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    conn = connect()
                    cur = conn.cursor()
                    cur.execute(f"USE WAREHOUSE {WAREHOUSE}")
                    consecutive_errors = 0
                    logging.info("Reconnected.")
                except Exception as re_exc:
                    logging.error("Reconnect failed: %s", re_exc)

        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
