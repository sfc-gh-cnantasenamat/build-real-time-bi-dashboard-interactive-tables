"""Live BI dashboard for the interactive warehouse quickstart.

Runs in Streamlit in Snowflake against the ORDERS standard table, served zero-copy by
the INTERACTIVE_ANALYTICS_IWH interactive warehouse. An st.fragment refreshes only the charts every few
seconds so the dashboard tracks the incoming order stream in near real time.

The data feed is driven by INTERACTIVE_ANALYTICS_SVC, a dedicated SPCS container that
writes to the ORDERS table server-side. This app only reads from ORDERS and
writes to SIMULATION_CONTROL (to start/stop/configure the feed). No INSERT
logic runs inside the Streamlit process.

Cost-saving behaviour
---------------------
The service auto-suspends after 1 hour of inactivity (is_running = FALSE).
Clicking "Start" from this app will resume a suspended service automatically
before enabling the feed. Clicking "Stop" starts the idle countdown; the
container will self-suspend ~1 hour later with no manual action needed.
"""

import time

import altair as alt
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

DB = "INTERACTIVE_STREAMING_DEMO"
SERVICE_FQN = f"{DB}.PUBLIC.INTERACTIVE_ANALYTICS_SVC"
TABLE   = f"{DB}.PUBLIC.ORDERS"
HISTORY = f"{DB}.PUBLIC.ORDERS_HISTORY"
CONTROL = f"{DB}.PUBLIC.SIMULATION_CONTROL"
CATEGORIES = ["Electronics", "Apparel", "Home", "Beauty", "Sports", "Toys"]
STANDARD_WH    = "INTERACTIVE_ANALYTICS_WH"   # for SIMULATION_CONTROL / ORDERS_HISTORY
INTERACTIVE_WH = "INTERACTIVE_ANALYTICS_IWH"  # for low-latency ORDERS reads

st.set_page_config(page_title="Live Orders", page_icon="⚡", layout="wide")
session = get_active_session()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def run_df(sql: str) -> pd.DataFrame:
    """Run against the standard warehouse — always resets warehouse first."""
    session.use_warehouse(STANDARD_WH)
    return session.sql(sql).to_pandas()


def run_df_it(sql: str) -> pd.DataFrame:
    """Query the ORDERS table via INTERACTIVE_ANALYTICS_IWH."""
    session.use_warehouse(INTERACTIVE_WH)
    try:
        return session.sql(sql).to_pandas()
    finally:
        session.use_warehouse(STANDARD_WH)


def get_control() -> dict:
    row = run_df(
        f"SELECT is_running, mode, flash_category, rate_mult, total_orders, "
        f"DATEDIFF('minute', last_active_at, CURRENT_TIMESTAMP()) AS idle_min "
        f"FROM {CONTROL} LIMIT 1"
    ).iloc[0]
    return {
        "is_running": bool(row["IS_RUNNING"]),
        "mode": str(row["MODE"]),
        "flash_category": row["FLASH_CATEGORY"],
        "rate_mult": float(row["RATE_MULT"]),
        "total_orders": int(row["TOTAL_ORDERS"] or 0),
        "idle_min": int(row["IDLE_MIN"]),
    }


_SQL_LITERAL = object()  # sentinel — value is already a SQL expression


def set_control(**kwargs) -> None:
    """Write one or more SIMULATION_CONTROL columns in a single UPDATE.

    Pass raw_last_active_at=True to use CURRENT_TIMESTAMP() (SQL expr, not a
    Python string that would be quoted).
    """
    parts = []
    for k, v in kwargs.items():
        col = k.upper()
        if v is None:
            parts.append(f"{col} = NULL")
        elif isinstance(v, bool):
            parts.append(f"{col} = {'TRUE' if v else 'FALSE'}")
        elif isinstance(v, str):
            safe = v.replace("'", "''")
            parts.append(f"{col} = '{safe}'")
        else:
            parts.append(f"{col} = {v}")
    session.use_warehouse(STANDARD_WH)
    session.sql(f"UPDATE {CONTROL} SET {', '.join(parts)}").collect()


def _start_simulation() -> None:
    """Set is_running=TRUE and reset the idle timer in one statement."""
    session.use_warehouse(STANDARD_WH)
    session.sql(
        f"UPDATE {CONTROL} "
        "SET is_running = TRUE, last_active_at = CURRENT_TIMESTAMP()"
    ).collect()


def get_service_status() -> str:
    """Return the SPCS service status string (e.g. RUNNING, SUSPENDED, DONE)."""
    try:
        rows = session.sql(
            f"SHOW SERVICES LIKE 'INTERACTIVE_ANALYTICS_SVC' "
            f"IN SCHEMA INTERACTIVE_STREAMING_DEMO.PUBLIC"
        ).collect()
        if rows:
            return str(rows[0]["status"]).upper()
    except Exception:
        pass
    return "UNKNOWN"


def ensure_service_running() -> str:
    """Resume the service if suspended; return the resulting status."""
    status = get_service_status()
    if status == "SUSPENDED":
        session.sql(f"ALTER SERVICE {SERVICE_FQN} RESUME").collect()
        # Give the container up to 30 s to reach RUNNING before proceeding
        for _ in range(6):
            time.sleep(5)
            status = get_service_status()
            if status == "RUNNING":
                break
    return status


# ---------------------------------------------------------------------------
# Button callbacks
# Streamlit runs on_click / on_change callbacks before the script body on the
# next rerun. The live_view fragment re-runs every few seconds; if it fires
# while a full rerun is in flight, the full rerun is restarted and an inline
# `if st.button(...)` branch can lose its click. Callbacks are never dropped,
# so every control that writes to SIMULATION_CONTROL uses one.
# ---------------------------------------------------------------------------
def _on_start() -> None:
    ensure_service_running()
    _start_simulation()


def _on_stop() -> None:
    set_control(is_running=False, mode="normal", flash_category=None)


def _on_reset_confirm() -> None:
    set_control(is_running=False, mode="normal", flash_category=None)
    # The service re-reads SIMULATION_CONTROL every CTRL_CACHE_TICKS (5) ticks,
    # so a few in-flight ticks can still land after the stop. Wait them out
    # before truncating so the new session really starts from zero.
    time.sleep(6)
    session.sql(f"TRUNCATE TABLE {HISTORY}").collect()
    set_control(total_orders=0)
    st.session_state.pop("_confirm_reset", None)


def _on_rate_change() -> None:
    set_control(rate_mult=float(st.session_state["rate_mult_ui"]))


def _on_flash_start() -> None:
    set_control(mode="flash_sale", flash_category=st.session_state["flash_cat"])


def _on_flash_stop() -> None:
    set_control(mode="normal", flash_category=None)


# ---------------------------------------------------------------------------
# Sidebar: simulation controls
# ---------------------------------------------------------------------------
# No native Streamlit API for sidebar width — CSS injection is the only way.
st.markdown("""<style></style>""", unsafe_allow_html=True)

st.title("⚡ Live Orders Dashboard")
st.caption(
    "Data written by `INTERACTIVE_ANALYTICS_SVC` (SPCS) · `ORDERS` is a standard table · `INTERACTIVE_ANALYTICS_IWH` is an interactive warehouse"
)

with st.sidebar:
    ctrl = get_control()
    svc_status = get_service_status()

    with st.container(border=True):
        st.subheader(":material/play_circle: Simulation")

        # --- Start / Stop buttons -----------------------------------------------
        start_label = "Start"
        if svc_status == "SUSPENDED" and not ctrl["is_running"]:
            start_label = "Start (resumes service)"

        col_start, col_stop = st.columns(2)
        with col_start:
            st.button(start_label, icon=":material/play_arrow:", key="sim_start", disabled=ctrl["is_running"], use_container_width=True, on_click=_on_start)
        with col_stop:
            st.button("Stop", icon=":material/stop:", key="sim_stop", disabled=not ctrl["is_running"], use_container_width=True, on_click=_on_stop)

        # --- Status display -----------------------------------------------------
        st.write("")
        if ctrl["is_running"]:
            if ctrl["mode"] == "flash_sale":
                st.success(f"Running · Flash Sale: {ctrl['flash_category']}")
            else:
                st.success("Running")
        elif svc_status == "SUSPENDED":
            st.warning("Service suspended — click Start to resume")
        else:
            idle_left = max(0, 60 - ctrl["idle_min"])
            if idle_left > 0:
                st.info(f"Stopped · auto-suspend in ~{idle_left} min")
            else:
                st.info("Stopped")

        # status_color = {
        #     "RUNNING": "green",
        #     "SUSPENDED": "orange",
        #     "UNKNOWN": "gray",
        # }.get(svc_status, "gray")
        with st.expander("Details"):
            st.caption(
                # f"- Status: :{status_color}[{svc_status}] \n"
                f"- Service: `INTERACTIVE_ANALYTICS_SVC` \n"
                "- Compute: `INTERACTIVE_ANALYTICS_POOL` \n"
                "- Table: `ORDERS` (standard)  \n"
                "- Interactive warehouse: `INTERACTIVE_ANALYTICS_IWH` (tick reads) \n"
                "- Standard warehouse: `INTERACTIVE_ANALYTICS_WH` (history/control)  \n"
                "- Auto-suspends 1 h after last Stop."
            )

        # --- New session --------------------------------------------------------
        st.divider()
        st.button(
            "New session", icon=":material/restart_alt:", key="new_session_btn",
            use_container_width=True,
            help="Clear all order history and start a fresh simulation.",
            on_click=lambda: st.session_state.update(_confirm_reset=True),
        )

        if st.session_state.get("_confirm_reset"):
            st.warning(
                "This will **clear all order history** and reset counters. "
                "Simulation will be stopped.",
                icon=":material/warning:",
            )
            col_yes, col_no = st.columns(2)
            with col_yes:
                st.button(
                    "Confirm", icon=":material/check:", key="confirm_reset_yes",
                    use_container_width=True, type="primary",
                    on_click=_on_reset_confirm,
                )
            with col_no:
                st.button(
                    "Cancel", icon=":material/close:", key="confirm_reset_no",
                    use_container_width=True,
                    on_click=lambda: st.session_state.pop("_confirm_reset", None),
                )

    with st.container(border=True):
        # --- Settings -----------------------------------------------------------
        st.subheader(":material/tune: Settings")
        st.write("")
        _WIN_OPTS = ["5 min", "15 min", "30 min", "60 min", "All time"]
        window_opt = st.selectbox("Time window", _WIN_OPTS, index=1)
        all_time = (window_opt == "All time")
        window_min = 0 if all_time else int(window_opt.split()[0])
        refresh_sec = st.slider("Tick interval (s)", 1, 30, 5)

        # --- Rate multiplier ----------------------------------------------------
        # Use a stable key so session state owns the slider value.
        # Without a key, any error-recovery re-run resets the slider to the DB
        # value (ctrl["rate_mult"]), causing the visible "bounce" effect.
        if "rate_mult_ui" not in st.session_state:
            st.session_state["rate_mult_ui"] = float(ctrl["rate_mult"])
        st.write("")
        st.slider(
            "Rate multiplier", 0.5, 3.0, step=0.1,
            key="rate_mult_ui",
            on_change=_on_rate_change,
            help="Scales order volume per tick. Takes effect within ~5 seconds. "
                 "Impact on windowed counts builds up over the time window.",
        )

        # --- Flash Sale controls ------------------------------------------------
        st.write("")
        st.selectbox("Flash Sale category", CATEGORIES, index=0, key="flash_cat")
        col_flash, col_end = st.columns(2)
        with col_flash:
            st.button(
                "Start", icon=":material/play_arrow:", key="flash_start",
                disabled=not ctrl["is_running"],
                use_container_width=True,
                on_click=_on_flash_start,
            )
        with col_end:
            st.button(
                "Stop", icon=":material/stop:", key="flash_stop",
                disabled=ctrl["mode"] != "flash_sale",
                use_container_width=True,
                on_click=_on_flash_stop,
            )


# ---------------------------------------------------------------------------
# History loader — all ORDERS_HISTORY queries batched in one function.
# Note: @st.cache_data is intentionally omitted. With tick_now advancing every
# refresh_sec seconds, every call is a cache miss anyway, so the decorator
# adds no benefit but can cause warehouse-state confusion in SiS.
# ---------------------------------------------------------------------------
def _fetch_history(where_hist: str, bucket_sec: int, tick_now: int = 0):
    hist_kpis = run_df(f"""
        SELECT
            COUNT(*)                                                              AS orders,
            COALESCE(SUM(AMOUNT), 0)                                             AS revenue,
            COUNT(DISTINCT REGION)                                               AS regions,
            COUNT(*) / NULLIF(DATEDIFF('minute', MIN(TICK_AT),
                              CURRENT_TIMESTAMP()), 0)                           AS orders_per_min
        FROM {HISTORY}
        {where_hist}
    """).iloc[0]
    ts = run_df(f"""
        SELECT
            TIME_SLICE(TICK_AT, {bucket_sec}, 'SECOND') AS bucket,
            COUNT(*)               AS orders,
            ROUND(SUM(AMOUNT), 2)  AS revenue
        FROM {HISTORY}
        {where_hist}
        GROUP BY 1
        ORDER BY 1
    """)
    region = run_df(f"""
        SELECT REGION, ROUND(SUM(AMOUNT), 2) AS revenue
        FROM {HISTORY}
        {where_hist}
        GROUP BY REGION
        ORDER BY revenue DESC
    """)
    cats = run_df(f"""
        SELECT PRODUCT_CATEGORY, COUNT(*) AS orders, ROUND(SUM(AMOUNT), 2) AS revenue
        FROM {HISTORY}
        {where_hist}
        GROUP BY PRODUCT_CATEGORY
        ORDER BY orders DESC
    """)
    heatmap = run_df(f"""
        SELECT REGION, PRODUCT_CATEGORY,
               COUNT(*) AS orders,
               ROUND(SUM(AMOUNT), 0) AS revenue
        FROM {HISTORY}
        {where_hist}
        GROUP BY REGION, PRODUCT_CATEGORY
    """)
    return hist_kpis, ts, region, cats, heatmap


# ---------------------------------------------------------------------------
# Main panel: auto-refreshing metrics and charts
# ---------------------------------------------------------------------------
# Stop auto-refreshing when simulation is paused — no new data arrives.
_run_interval = None if not ctrl["is_running"] else f"{refresh_sec}s"
@st.fragment(run_every=_run_interval)
def live_view():
    where = (
        f"WHERE ORDER_TS >= DATEADD('minute', -{window_min}, CURRENT_TIMESTAMP())"
    )

    # KPI row — total_orders comes from the control table (cumulative, never decreases)
    ctrl_now = get_control()

    # When stopped, ignore the time-window filter — show all accumulated history
    _show_all = all_time or not ctrl_now["is_running"]
    where_hist = (
        ""
        if _show_all
        else f"WHERE TICK_AT >= DATEADD('minute', -{window_min}, CURRENT_TIMESTAMP())"
    )
    bucket_sec  = 60 if _show_all else 10
    rev_key     = "rev_x_max_all" if _show_all else "rev_x_max_win"
    # All ORDERS_HISTORY reads in one cached call.
    # tick_now forces a new cache key every tick, guaranteeing fresh data.
    _tick_now = int(pd.Timestamp.now().timestamp() / refresh_sec)
    hist_kpis, ts, region, cats, heatmap = _fetch_history(where_hist, bucket_sec, _tick_now)
    # Single IWH round-trip: tick count + active regions + active categories
    _snap = run_df_it(f"""
        SELECT
            COUNT(*)                             AS n,
            ARRAY_AGG(DISTINCT REGION)           AS tick_regions,
            ARRAY_AGG(DISTINCT PRODUCT_CATEGORY) AS tick_cats
        FROM {TABLE}
    """).iloc[0]
    tick_count        = int(_snap["N"])
    _tick_regions_raw = _snap["TICK_REGIONS"]
    _tick_cats_raw    = _snap["TICK_CATS"]
    # ARRAY_AGG returns a Python list in Snowpark; guard against None on empty table
    import json
    def _to_set(val):
        if val is None: return set()
        if isinstance(val, list): return set(val)
        try: return set(json.loads(val))
        except Exception: return set()
    _tick_region_set = _to_set(_tick_regions_raw)
    _tick_cat_set    = _to_set(_tick_cats_raw)

    def _compact(n: float) -> str:
        """Short number format (12.3K, 4.5M) so KPI tiles fit narrow columns."""
        n = float(n or 0)
        for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
            if abs(n) >= div:
                return f"{n / div:.1f}{suffix}"
        return f"{n:,.0f}"

    with st.container(border=True):
        c1, c2, c3, c4, c5 = st.columns([1, 1.1, 1.25, 0.75, 1])
        c1.metric("Total orders", _compact(ctrl_now["total_orders"]),
                  delta=f"+{int(tick_count):,}/tick")
        orders_label = "Orders (all)" if (all_time or not ctrl_now["is_running"]) else f"Orders ({window_min}m)"
        c2.metric(orders_label, _compact(hist_kpis["ORDERS"]))
        c3.metric("Revenue", f"${_compact(hist_kpis['REVENUE'])}")
        c4.metric("Regions", int(hist_kpis["REGIONS"]))
        _opm = hist_kpis["ORDERS_PER_MIN"]
        c5.metric("Orders/min", "—" if pd.isna(_opm) else _compact(_opm))

    # Warn prominently when simulation is stopped
    if not ctrl_now["is_running"]:
        st.warning(
            "**Simulation is stopped.** Click **▶ Start** in the sidebar to resume data flow.",
            icon="⚠️",
        )

    chart_where = where_hist

    # Orders over time

    y_label = f"Orders / {bucket_sec}s"

    st.subheader(":material/show_chart: Orders over time")
    if ts.empty:
        st.info("No history data yet — click Start to begin the simulation.")
    else:
        # Y-axis: fresh scale each render (no lock) so small-data periods aren't dwarfed
        y_max = max(int(ts["ORDERS"].max() * 1.3), 50)

        # X-axis: auto-scale to the actual data range — avoids timezone-mismatch issues
        # with fixed UTC domain strings vs Snowflake TIMESTAMP_NTZ values.
        x_enc = alt.X(
            "BUCKET:T",
            axis=alt.Axis(format="%H:%M:%S", tickCount=6, labelOverlap="greedy"),
            title="Time",
        )

        area = (
            alt.Chart(ts)
            .mark_area(opacity=0.4, line=True)
            .encode(
                x=x_enc,
                y=alt.Y("ORDERS:Q",
                        scale=alt.Scale(domain=[0, y_max]),
                        title=y_label),
                tooltip=["BUCKET:T", "ORDERS:Q", "REVENUE:Q"],
            )
        )

        # Highlight the latest bucket in orange
        latest = ts.iloc[[-1]]
        latest_rule = (
            alt.Chart(latest)
            .mark_rule(color="orange", strokeWidth=1.5, strokeDash=[4, 3], opacity=0.7)
            .encode(x=alt.X("BUCKET:T"))
        )
        latest_dot = (
            alt.Chart(latest)
            .mark_point(size=120, color="orange", filled=True)
            .encode(
                x=alt.X("BUCKET:T"),
                y=alt.Y("ORDERS:Q", scale=alt.Scale(domain=[0, y_max])),
                tooltip=["BUCKET:T", "ORDERS:Q", "REVENUE:Q"],
            )
        )

        chart = (
            alt.layer(area, latest_rule, latest_dot)
            .properties(height=260)
        )
        st.altair_chart(chart, use_container_width=True, key="orders_over_time")

    # Breakdown charts: heatmap gets a full-width row so its cells stay
    # legible; region revenue and top categories sit side by side below it.
    col2 = st.container()
    col1, col3 = st.columns(2)

    with col1:
        st.subheader(":material/bar_chart: Revenue by region")
        if not region.empty:
            # Regions active in the current tick get a vivid color; others gray
            tick_regions = _tick_region_set
            _REGION_COLORS = {
                "North America": "#FF6B6B",
                "Europe":        "#4ECDC4",
                "APAC":          "#45B7D1",
                "LATAM":         "#FF9F43",
                "MEA":           "#A29BFE",
            }
            region["BAR_COLOR"] = region["REGION"].apply(
                lambda r: _REGION_COLORS.get(r, "#adb5bd") if r in tick_regions else "#555555"
            )
            x_max_rev = int(region["REVENUE"].max() * 1.2)
            if rev_key not in st.session_state or x_max_rev > st.session_state[rev_key]:
                st.session_state[rev_key] = max(x_max_rev, 1000)
            bar = (
                alt.Chart(region)
                .mark_bar()
                .encode(
                    x=alt.X("REVENUE:Q",
                            scale=alt.Scale(domain=[0, st.session_state[rev_key]]),
                            title="Revenue"),
                    y=alt.Y("REGION:N", sort="-x", title=None),
                    color=alt.Color("BAR_COLOR:N", scale=None, legend=None),
                    tooltip=["REGION:N", "REVENUE:Q"],
                )
                .properties(height=260)
            )
            st.altair_chart(bar, use_container_width=True, key="revenue_by_region")

    with col2:
        st.subheader(":material/grid_on: Orders by Region × Category")
        if not heatmap.empty:
            _CAT_ORDER    = ["Electronics", "Apparel", "Home", "Sports", "Toys", "Beauty"]
            _REGION_ORDER = ["North America", "Europe", "APAC", "LATAM", "MEA"]
            _REGION_ABBR  = {"North America": "NOAM", "Europe": "EUR",
                              "APAC": "APAC", "LATAM": "LATAM", "MEA": "MEA"}
            _ABBR_ORDER   = [_REGION_ABBR[r] for r in _REGION_ORDER]
            heatmap["REGION_ABBR"] = heatmap["REGION"].map(_REGION_ABBR)
            _max_orders = int(heatmap["ORDERS"].max())

            _heat_rect = (
                alt.Chart(heatmap)
                .mark_rect(stroke="black", strokeWidth=0.5)
                .encode(
                    x=alt.X("REGION_ABBR:N", sort=_ABBR_ORDER, title=None,
                            axis=alt.Axis(labelOverlap=False, labelLimit=120)),
                    y=alt.Y("PRODUCT_CATEGORY:N", sort=_CAT_ORDER, title=None,
                            axis=alt.Axis(labelOverlap=False, labelLimit=120)),
                    color=alt.Color(
                        "ORDERS:Q",
                        scale=alt.Scale(range=["#0d1f33", "#4C78A8"]),
                        legend=alt.Legend(title="Orders", orient="right",
                                          gradientLength=80, gradientThickness=8,
                                          titleFontSize=10, labelFontSize=9),
                    ),
                    tooltip=["REGION:N", "PRODUCT_CATEGORY:N",
                             alt.Tooltip("ORDERS:Q", format=","),
                             alt.Tooltip("REVENUE:Q", format="$,.0f")],
                )
            )
            _heat_text = (
                alt.Chart(heatmap)
                .mark_text(fontSize=11, fontWeight="bold")
                .encode(
                    x=alt.X("REGION_ABBR:N", sort=_ABBR_ORDER),
                    y=alt.Y("PRODUCT_CATEGORY:N", sort=_CAT_ORDER),
                    text=alt.Text("ORDERS:Q", format=","),
                    color=alt.condition(
                        alt.datum.ORDERS > _max_orders * 0.55,
                        alt.value("white"),
                        alt.value("#888"),
                    ),
                )
            )
            st.altair_chart(
                alt.layer(_heat_rect, _heat_text).properties(height=300),
                use_container_width=True,
                key="region_cat_heatmap",
            )

    with col3:
        st.subheader(":material/category: Top product categories")
        if not cats.empty:
            tick_set = _tick_cat_set

            # Rainbow palette — one vivid color per category
            _CAT_COLORS = {
                "Electronics": "#FF6B6B",
                "Apparel":     "#FF9F43",
                "Home":        "#F9CA24",
                "Sports":      "#6AB04C",
                "Toys":        "#4ECDC4",
                "Beauty":      "#A29BFE",
            }

            rows_html = ""
            for _, row in cats.iterrows():
                cat  = row["PRODUCT_CATEGORY"]
                col  = _CAT_COLORS.get(cat, "#adb5bd")
                in_t = cat in tick_set
                if in_t:
                    badge = (
                        f'<span style="background:{col}28;color:{col};'
                        f'border:1px solid {col}88;border-radius:12px;'
                        f'padding:2px 12px;font-weight:600;'
                        f'font-size:0.82em;white-space:nowrap">{cat}</span>'
                    )
                else:
                    badge = (
                        f'<span style="background:#88888818;color:#888;'
                        f'border:1px solid #88888855;border-radius:12px;'
                        f'padding:2px 12px;font-weight:500;'
                        f'font-size:0.82em;white-space:nowrap">{cat}</span>'
                    )
                rows_html += (
                    f"<tr>"
                    f'<td style="padding:7px 10px">{badge}</td>'
                    f'<td style="padding:7px 10px;text-align:right;color:#ccc">{int(row["ORDERS"]):,}</td>'
                    f'<td style="padding:7px 10px;text-align:right;color:#ccc">${row["REVENUE"]:,.2f}</td>'
                    f"</tr>"
                )

            st.html(f"""
            <table style="width:100%;border-collapse:collapse;font-size:0.9em;font-family:sans-serif">
              <thead>
                <tr style="border-bottom:1px solid #333">
                  <th style="padding:6px 10px;text-align:left;color:#777;font-weight:500">Category</th>
                  <th style="padding:6px 10px;text-align:right;color:#777;font-weight:500">Orders</th>
                  <th style="padding:6px 10px;text-align:right;color:#777;font-weight:500">Revenue</th>
                </tr>
              </thead>
              <tbody>{rows_html}</tbody>
            </table>
            """)

    st.caption(f"Last updated {pd.Timestamp.utcnow():%H:%M:%S} UTC")


live_view()
