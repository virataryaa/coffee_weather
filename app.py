import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="Colombia Weather Dashboard", layout="wide")

# -------------------------------------------------------
# THEME — HardMiner palette
# -------------------------------------------------------
BG      = "#fafafa"
SURFACE = "#ffffff"
INK     = "#1d1d1f"
INK_2   = "#424245"
INK_3   = "#6e6e73"
INK_4   = "#aeaeb2"
BORDER  = "rgba(0,0,0,0.08)"
GRID    = "rgba(0,0,0,0.06)"
RED     = "#c0392b"
NAVY    = "#0a2463"

ALL_YEAR_COLORS = {
    "2026":    RED,
    "2025":    INK,
    "2024":    "#2980b9",
    "2023":    "#27ae60",
    "normals": INK_4,
}

MONTH_TICKS  = ["01-01","02-01","03-01","04-01","05-01","06-01",
                 "07-01","08-01","09-01","10-01","11-01","12-01"]
MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun",
                "Jul","Aug","Sep","Oct","Nov","Dec"]

st.markdown(f"""
<style>
  [data-testid="stAppViewContainer"], [data-testid="stApp"] {{
      background:{BG} !important; color:{INK} !important;
  }}
  [data-testid="stSidebar"] {{
      background:{SURFACE} !important;
      border-right:1px solid {BORDER} !important;
  }}
  [data-testid="stSidebar"] * {{ color:{INK} !important; }}
  .block-container {{ padding-top:2rem !important; }}
  h2.section-header {{
      font-family: Helvetica Neue, sans-serif;
      font-size: 0.65rem;
      font-weight: 600;
      letter-spacing: .18em;
      text-transform: uppercase;
      color: {INK_3};
      border-bottom: 1px solid {BORDER};
      padding-bottom: .5rem;
      margin: 2rem 0 0.2rem 0;
  }}
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------------
# SETTINGS
# -------------------------------------------------------
URL = "https://api.weatherdesk.xweather.com/2e621a7f-2b1e-4f3e-af6a-5a986a68b398/services/gwi/v1/timeseries"

station_region = {
    "80009": "Colombia", "80036": "Colombia", "80063": "Colombia",
    "80091": "Colombia", "80110": "Colombia", "80112": "Colombia",
    "80210": "Colombia", "80211": "Colombia", "80214": "Colombia",
    "80222": "Colombia",
}

stations       = list(station_region.keys())
years_to_fetch = ["2023", "2024", "2025", "2026", "normals"]
MAX_WORKERS    = 20

# -------------------------------------------------------
# DATA FETCHING
# -------------------------------------------------------
def _fetch_station(station, parameter):
    params = {
        "station": station, "parameter": parameter,
        "start": "01-01", "end": "12-31", "model": "0", "metric": "1",
    }
    r = requests.get(URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json().get("output", {})
    records = []
    for y in years_to_fetch:
        if y in data:
            for d in data[y]:
                rec = {"station": station, "year": y, "date": d["date"]}
                if parameter == "PRCP":
                    rec.update({"prcp": d.get("prcp"), "prcp_sum": d.get("prcp_sum")})
                else:
                    rec.update({"tavg": d.get("tavg")})
                records.append(rec)
    return records


@st.cache_data(ttl=3600, show_spinner=False)
def load_data(parameter: str) -> pd.DataFrame:
    all_records, errors = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_station, s, parameter): s for s in stations}
        for future in as_completed(futures):
            station = futures[future]
            try:
                all_records.extend(future.result())
            except Exception as e:
                errors.append(f"Station {station}: {e}")
    if errors:
        st.warning(f"Some stations failed: {'; '.join(errors)}")
    return pd.DataFrame(all_records)


# -------------------------------------------------------
# DATA PROCESSING
# -------------------------------------------------------
@st.cache_data(show_spinner=False)
def process_precipitation(raw_df: pd.DataFrame, today: pd.Timestamp):
    df = raw_df.copy()
    df["region"] = df["station"].map(station_region)
    df = df[df["date"] != "02-29"].reset_index(drop=True)

    latest_year = df[df["year"] != "normals"]["year"].astype(int).max()
    df["full_date"] = pd.to_datetime(df.apply(
        lambda r: f"{latest_year}-{r['date']}" if r["year"] == "normals" else f"{r['year']}-{r['date']}",
        axis=1,
    ))
    df["tag"] = df.apply(
        lambda r: "realized" if r["year"] == "normals" or r["full_date"] <= today else "forecast",
        axis=1,
    )
    daily_avg = (
        df.groupby(["region", "year", "date"])
        .agg(prcp_avg=("prcp", "mean"), prcp_sum_avg=("prcp_sum", "mean"))
        .reset_index()
    )
    daily_avg["full_date"] = pd.to_datetime(daily_avg.apply(
        lambda r: f"{latest_year}-{r['date']}" if r["year"] == "normals" else f"{r['year']}-{r['date']}",
        axis=1,
    ))
    daily_avg["tag"] = daily_avg.apply(
        lambda r: "realized" if r["year"] == "normals" or r["full_date"] <= today else "forecast",
        axis=1,
    )
    return daily_avg, latest_year


@st.cache_data(show_spinner=False)
def process_temperature(raw_df: pd.DataFrame, today: pd.Timestamp):
    df = raw_df.copy()
    df["region"] = df["station"].map(station_region)
    df = df[df["date"] != "02-29"].reset_index(drop=True)

    latest_year = df[df["year"] != "normals"]["year"].astype(int).max()
    daily_avg = (
        df.groupby(["region", "year", "date"])
        .agg(tavg_avg=("tavg", "mean"))
        .reset_index()
    )
    daily_avg["tag"] = daily_avg.apply(
        lambda r: "realized"
        if r["year"] == "normals"
        else ("realized" if pd.to_datetime(f"{r['year']}-{r['date']}") <= today else "forecast"),
        axis=1,
    )
    return daily_avg, latest_year


@st.cache_data(show_spinner=False)
def process_rolling(daily_avg: pd.DataFrame, today: pd.Timestamp):
    df = daily_avg.copy()
    df["full_date"] = pd.to_datetime(df["full_date"])
    df = df[df["date"] != "02-29"].copy().reset_index(drop=True)
    df["rolling_date"] = df.apply(
        lambda r: pd.to_datetime(f"1950-{r['date']}") if r["year"] == "normals" else r["full_date"],
        axis=1,
    )

    normals_df  = df[df["year"] == "normals"].copy()
    normals_pre = normals_df.copy()
    normals_pre["rolling_date"] = normals_pre["rolling_date"].apply(
        lambda d: pd.Timestamp(year=1949, month=d.month, day=d.day)
    )
    normals_ext = pd.concat([normals_pre, normals_df], ignore_index=True).sort_values(["region", "rolling_date"])
    agg_normals = normals_ext.groupby(["region", "year", "rolling_date"], as_index=False).agg(
        prcp_avg=("prcp_avg", "mean"), prcp_sum_avg=("prcp_sum_avg", "mean")
    )

    def _roll(group):
        group = group.sort_values("rolling_date").copy()
        group["prcp_avg_30d_sum"] = group.rolling("30D", on="rolling_date", min_periods=1)["prcp_avg"].sum()
        return group

    agg_normals = agg_normals.groupby("region", group_keys=False).apply(_roll)
    agg_normals["xdate"] = agg_normals["rolling_date"].apply(
        lambda d: pd.Timestamp(year=2026, month=d.month, day=d.day)
    )
    agg_normals_final = agg_normals[agg_normals["rolling_date"].dt.year != 1949].reset_index(drop=True)

    other_df  = df[df["year"] != "normals"].copy()
    agg_other = other_df.groupby(["region", "year", "rolling_date"], as_index=False).agg(
        prcp_avg=("prcp_avg", "mean"), prcp_sum_avg=("prcp_sum_avg", "mean")
    )
    agg_other = agg_other.groupby("region", group_keys=False).apply(_roll)
    agg_other["xdate"] = agg_other["rolling_date"].apply(
        lambda d: pd.Timestamp(year=2026, month=d.month, day=d.day)
    )
    return (
        pd.concat([agg_other, agg_normals_final], ignore_index=True)
        .sort_values(["region", "rolling_date"])
        .reset_index(drop=True)
    )


# -------------------------------------------------------
# CHART HELPERS
# -------------------------------------------------------
def _base_layout(title: str, y_title: str, height: int = 420) -> dict:
    return dict(
        title=dict(
            text=title,
            font=dict(size=15, color=INK, family="Helvetica Neue, sans-serif"),
            x=0, xanchor="left",
        ),
        xaxis=dict(
            gridcolor=GRID, tickfont=dict(size=11, color=INK_3),
            linecolor=BORDER, zerolinecolor=BORDER,
        ),
        yaxis=dict(
            title=dict(text=y_title, font=dict(color=INK_2, size=12)),
            gridcolor=GRID, tickfont=dict(size=11, color=INK_3),
            linecolor=BORDER, zerolinecolor=BORDER,
        ),
        legend=dict(
            title=dict(text="Year", font=dict(color=INK_3, size=11)),
            bgcolor=SURFACE, bordercolor=BORDER, borderwidth=1,
            font=dict(size=11, color=INK_2),
        ),
        plot_bgcolor=SURFACE,
        paper_bgcolor=BG,
        font=dict(color=INK, family="Helvetica Neue, sans-serif"),
        hovermode="x unified",
        hoverlabel=dict(bgcolor=SURFACE, bordercolor=BORDER, font=dict(color=INK, size=12)),
        height=height,
        margin=dict(l=60, r=20, t=50, b=55),
    )


def _add_year_traces(fig, df_year, year, y_col, hover_unit, year_colors):
    """
    Datetime x-axis (reference year 2001) ensures seamless connection between
    realized (solid) and forecast (dotted) at adjacent datetime positions.
    """
    color    = year_colors.get(year, INK_4)
    df_all   = df_year.copy()
    df_all["_px"] = pd.to_datetime("2001-" + df_all["date"])
    df_all   = df_all.sort_values("_px")
    realized = df_all[df_all["tag"] == "realized"]
    forecast = df_all[df_all["tag"] == "forecast"]

    # --- Realized: solid ---
    if not realized.empty:
        fig.add_trace(go.Scatter(
            x=realized["_px"], y=realized[y_col],
            mode="lines", name=year, legendgroup=year, showlegend=True,
            line=dict(color=color, width=2, dash="solid"),
            connectgaps=True,
            hovertemplate=f"<b>{year}</b>  %{{x|%b %d}}  %{{y:.1f}} {hover_unit}<extra></extra>",
        ))

    # --- Forecast: dotted, bridged from last realized point so no gap ---
    if not forecast.empty:
        if not realized.empty:
            forecast = pd.concat([realized.iloc[[-1]], forecast], ignore_index=True)
        fig.add_trace(go.Scatter(
            x=forecast["_px"], y=forecast[y_col],
            mode="lines", name=f"{year} fcst", legendgroup=year, showlegend=True,
            line=dict(color=color, width=2, dash="dot"),
            connectgaps=True,
            hovertemplate=f"<b>{year} fcst</b>  %{{x|%b %d}}  %{{y:.1f}} {hover_unit}<extra></extra>",
        ))


_DT_XAXIS = dict(
    tickformat="%b",
    dtick="M1",
    tick0="2001-01-01",
    range=["2001-01-01", "2001-12-31"],
    gridcolor=GRID,
    tickfont=dict(size=11, color=INK_3),
    linecolor=BORDER,
    zerolinecolor=BORDER,
)


def build_cumulative_precip(daily_avg, region, year_colors):
    df_r = daily_avg[daily_avg["region"] == region].copy()
    fig  = go.Figure()
    for year in sorted(df_r["year"].unique()):
        _add_year_traces(fig, df_r[df_r["year"] == year], year, "prcp_sum_avg", "mm", year_colors)
    layout = _base_layout(f"Cumulative Precipitation  —  {region}", "mm")
    layout["xaxis"] = _DT_XAXIS.copy()
    fig.update_layout(**layout)
    return fig


def build_temperature(daily_avg_temp, region, year_colors):
    df_r    = daily_avg_temp[daily_avg_temp["region"] == region].copy()
    years_h = [y for y in ["2023", "2024", "2025"] if y in df_r["year"].values]
    df_minmax = (
        df_r[df_r["year"].isin(years_h)]
        .groupby("date", as_index=False)
        .agg(tavg_min=("tavg_avg", "min"), tavg_max=("tavg_avg", "max"))
    )
    # Convert to reference datetime for consistent x-axis
    df_minmax["_px"] = pd.to_datetime("2001-" + df_minmax["date"])
    df_minmax = df_minmax.sort_values("_px")

    fig = go.Figure()
    if not df_minmax.empty:
        fig.add_trace(go.Scatter(
            x=list(df_minmax["_px"]) + list(df_minmax["_px"])[::-1],
            y=list(df_minmax["tavg_max"]) + list(df_minmax["tavg_min"])[::-1],
            fill="toself", fillcolor="rgba(0,0,0,0.05)",
            line=dict(color="rgba(0,0,0,0)"),
            name="Hist. Range", hoverinfo="skip",
        ))
    for year in sorted(df_r["year"].unique()):
        _add_year_traces(fig, df_r[df_r["year"] == year], year, "tavg_avg", "°C", year_colors)
    layout = _base_layout(f"Average Temperature  —  {region}", "°C")
    layout["xaxis"] = _DT_XAXIS.copy()
    fig.update_layout(**layout)
    return fig


def build_rolling_precip(agg_df, region, today, year_colors):
    df_r = agg_df[(agg_df["region"] == region) & (agg_df["year"] != "2023")].copy()
    df_r = df_r[~((df_r["year"] == "2026") & (df_r["xdate"] > today))]
    fig  = go.Figure()
    for year in sorted(df_r["year"].unique()):
        df_y = df_r[df_r["year"] == year].sort_values("xdate")
        fig.add_trace(go.Scatter(
            x=df_y["xdate"], y=df_y["prcp_avg_30d_sum"],
            mode="lines", name=str(year),
            line=dict(color=year_colors.get(str(year), INK_4), width=2),
            connectgaps=True,
            hovertemplate=f"<b>{year}</b>  %{{x|%b %d}}  %{{y:.1f}} mm<extra></extra>",
        ))
    layout = _base_layout(f"30-Day Rolling Precipitation  —  {region}", "Rolling Sum (mm)")
    layout["xaxis"].update(tickformat="%b", dtick="M1")
    fig.update_layout(**layout)
    return fig


# -------------------------------------------------------
# STREAMLIT UI
# -------------------------------------------------------
st.markdown(
    f"<h1 style='font-family:Helvetica Neue,sans-serif;font-weight:600;"
    f"color:{NAVY};letter-spacing:-.02em;margin-bottom:.15rem'>"
    f"Colombia Weather Dashboard</h1>"
    f"<p style='color:{INK_4};font-size:.78rem;margin-top:0'>"
    f"WeatherDesk XWeather API &nbsp;·&nbsp; Cache refreshes every hour</p>",
    unsafe_allow_html=True,
)

today = pd.Timestamp.today().normalize()

# ---------- SIDEBAR ----------
with st.sidebar:
    st.markdown(
        f"<p style='font-size:.65rem;font-weight:700;letter-spacing:.14em;"
        f"text-transform:uppercase;color:{INK_3};margin-bottom:.6rem'>Filters</p>",
        unsafe_allow_html=True,
    )

    selected_years = st.multiselect(
        "Years",
        options=years_to_fetch,
        default=years_to_fetch,
        help="Select which years to display on all charts",
    )

    st.markdown(
        f"<hr style='border:none;border-top:1px solid {BORDER};margin:.8rem 0'>",
        unsafe_allow_html=True,
    )

    if st.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown(
        f"<hr style='border:none;border-top:1px solid {BORDER};margin:.8rem 0'>",
        unsafe_allow_html=True,
    )

    st.markdown(
        f"<p style='font-size:.65rem;font-weight:700;letter-spacing:.14em;"
        f"text-transform:uppercase;color:{INK_3};margin-bottom:.6rem'>Year colors</p>",
        unsafe_allow_html=True,
    )
    for yr, col in ALL_YEAR_COLORS.items():
        st.markdown(
            f"<div style='display:flex;align-items:center;gap:8px;margin-bottom:4px'>"
            f"<span style='width:12px;height:12px;border-radius:2px;"
            f"background:{col};display:inline-block'></span>"
            f"<span style='font-size:.82rem;color:{INK_2}'>{yr}</span></div>",
            unsafe_allow_html=True,
        )
    st.markdown(
        f"<p style='font-size:.75rem;color:{INK_4};margin-top:.8rem'>"
        f"Today: {today.strftime('%Y-%m-%d')}</p>",
        unsafe_allow_html=True,
    )

# Warn if nothing selected
if not selected_years:
    st.warning("Select at least one year from the sidebar.")
    st.stop()

# Filter color map to selected years only
active_colors = {y: c for y, c in ALL_YEAR_COLORS.items() if y in selected_years}

# ---------- LOAD & PROCESS ----------
c1, c2 = st.columns(2)
with c1:
    with st.spinner("Fetching precipitation..."):
        raw_prcp = load_data("PRCP")
with c2:
    with st.spinner("Fetching temperature..."):
        raw_temp = load_data("TAVG")

if raw_prcp.empty or raw_temp.empty:
    st.error("Failed to load data. Check API connectivity.")
    st.stop()

daily_avg,      _ = process_precipitation(raw_prcp, today)
daily_avg_temp, _ = process_temperature(raw_temp, today)
agg_df            = process_rolling(daily_avg, today)

# Apply year filter
daily_avg      = daily_avg[daily_avg["year"].isin(selected_years)]
daily_avg_temp = daily_avg_temp[daily_avg_temp["year"].isin(selected_years)]
agg_df         = agg_df[agg_df["year"].isin(selected_years)]

regions = sorted(daily_avg["region"].unique())

# ---------- DASHBOARD — all sections on one page ----------
for region in regions:

    # ---- Cumulative Precipitation ----
    st.markdown(
        f"<h2 class='section-header'>Cumulative Precipitation &nbsp;—&nbsp; {region}</h2>",
        unsafe_allow_html=True,
    )
    st.plotly_chart(
        build_cumulative_precip(daily_avg, region, active_colors),
        use_container_width=True, key=f"cum_{region}",
    )

    # ---- Temperature ----
    st.markdown(
        f"<h2 class='section-header'>Temperature &nbsp;—&nbsp; {region}</h2>",
        unsafe_allow_html=True,
    )
    st.plotly_chart(
        build_temperature(daily_avg_temp, region, active_colors),
        use_container_width=True, key=f"tmp_{region}",
    )

    # ---- 30-Day Rolling ----
    st.markdown(
        f"<h2 class='section-header'>30-Day Rolling Precipitation &nbsp;—&nbsp; {region}</h2>",
        unsafe_allow_html=True,
    )
    st.plotly_chart(
        build_rolling_precip(agg_df, region, today, active_colors),
        use_container_width=True, key=f"rol_{region}",
    )

st.markdown(
    f"<p style='font-size:.72rem;color:{INK_4};margin-top:1.5rem;border-top:1px solid {BORDER};"
    f"padding-top:.8rem'>Generated: {pd.Timestamp.today().strftime('%Y-%m-%d %H:%M')}</p>",
    unsafe_allow_html=True,
)
