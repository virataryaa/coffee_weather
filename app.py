import streamlit as st
import requests
import pandas as pd
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(page_title="Coffee Weather Dashboard", layout="wide")

# -------------------------------------------------------
# THEME
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
    "2026": RED, "2025": INK, "2024": "#2980b9",
    "2023": "#27ae60", "2022": "#8e44ad", "normals": INK_4,
}

# Brazil crop year colors — most recent first
CROP_COLOR_PALETTE = [RED, INK, "#f1948a", "#82e0aa", INK_4]

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
      font-size: 0.65rem; font-weight: 600;
      letter-spacing: .18em; text-transform: uppercase;
      color: {INK_3}; border-bottom: 1px solid {BORDER};
      padding-bottom: .5rem; margin: 2rem 0 0.2rem 0;
  }}
</style>
""", unsafe_allow_html=True)

# -------------------------------------------------------
# ORIGIN CONFIGS
# -------------------------------------------------------
URL         = "https://api.weatherdesk.xweather.com/2e621a7f-2b1e-4f3e-af6a-5a986a68b398/services/gwi/v1/timeseries"
MAX_WORKERS = 20

ORIGINS = {
    "Brazil": {
        "stations": {
            "86827": "Espirito Santo", "86828": "Espirito Santo", "86829": "Espirito Santo",
            "86805": "Espirito Santo", "86785": "Espirito Santo", "86853": "Espirito Santo",
            "86804": "Espirito Santo", "83550": "Espirito Santo",
            "83595": "Minas Gerais",  "86743": "Minas Gerais",  "83442": "Minas Gerais",
            "83579": "Minas Gerais",  "83384": "Minas Gerais",  "83582": "Minas Gerais",
            "86850": "Minas Gerais",  "86800": "Minas Gerais",  "86799": "Minas Gerais",
            "86718": "Minas Gerais",  "86846": "Minas Gerais",  "86761": "Minas Gerais",
            "83592": "Minas Gerais",  "86719": "Minas Gerais",  "86794": "Minas Gerais",
            "83566": "Minas Gerais",  "86822": "Minas Gerais",  "86780": "Minas Gerais",
            "83538": "Minas Gerais",  "86797": "Minas Gerais",  "86798": "Minas Gerais",
            "86820": "Minas Gerais",  "83574": "Minas Gerais",  "86783": "Minas Gerais",
            "86782": "Minas Gerais",  "86757": "Minas Gerais",  "86821": "Minas Gerais",
            "86742": "Minas Gerais",  "86758": "Minas Gerais",  "83692": "Minas Gerais",
            "83687": "Minas Gerais",  "86825": "Minas Gerais",  "86784": "Minas Gerais",
            "86871": "Minas Gerais",  "83437": "Minas Gerais",  "86852": "Minas Gerais",
            "86823": "Minas Gerais",  "83479": "Minas Gerais",  "86873": "Minas Gerais",
            "86819": "Minas Gerais",  "83531": "Minas Gerais",  "86778": "Minas Gerais",
            "83393": "Minas Gerais",  "83483": "Minas Gerais",  "86795": "Minas Gerais",
            "86741": "Minas Gerais",  "86849": "Minas Gerais",  "86763": "Minas Gerais",
            "83492": "Minas Gerais",  "86801": "Minas Gerais",  "86779": "Minas Gerais",
            "86776": "Minas Gerais",  "86738": "Minas Gerais",  "86848": "Minas Gerais",
            "86824": "Minas Gerais",
            "86816": "Sao Paulo", "86865": "Sao Paulo", "86844": "Sao Paulo",
            "83630": "Sao Paulo", "86869": "Sao Paulo", "86817": "Sao Paulo",
            "86839": "Sao Paulo", "86866": "Sao Paulo", "86868": "Sao Paulo",
            "86842": "Sao Paulo", "83716": "Sao Paulo", "86864": "Sao Paulo",
            "83726": "Sao Paulo", "86838": "Sao Paulo", "86815": "Sao Paulo",
        },
        "years": ["2022", "2023", "2024", "2025", "2026", "normals"],
        "type":  "crop_year",
    },
    "Colombia": {
        "stations": {
            "80009": "Colombia", "80036": "Colombia", "80063": "Colombia",
            "80091": "Colombia", "80110": "Colombia", "80112": "Colombia",
            "80210": "Colombia", "80211": "Colombia", "80214": "Colombia",
            "80222": "Colombia",
        },
        "years": ["2023", "2024", "2025", "2026", "normals"],
        "type":  "calendar",
    },
    "Honduras": {
        "stations": {
            "78708": "Honduras", "78714": "Honduras", "78717": "Honduras",
            "78718": "Honduras", "78719": "Honduras", "78720": "Honduras",
        },
        "years": ["2023", "2024", "2025", "2026", "normals"],
        "type":  "calendar",
    },
    "Super 4": {
        "stations": {
            "84050": "Super 4", "84105": "Super 4", "84135": "Super 4",
            "84143": "Super 4", "84226": "Super 4",
        },
        "years": ["2023", "2024", "2025", "2026", "normals"],
        "type":  "calendar",
    },
    "Vietnam": {
        "stations": {
            "48875": "Vietnam", "48866": "Vietnam", "48900": "Vietnam",
        },
        "years": ["2023", "2024", "2025", "2026", "normals"],
        "type":  "calendar",
    },
}

CALENDAR_YEARS = ["2023", "2024", "2025", "2026", "normals"]

# -------------------------------------------------------
# DATA FETCHING  (cached per origin + parameter)
# -------------------------------------------------------
def _fetch_one(station, parameter, years_to_fetch):
    params = {"station": station, "parameter": parameter,
              "start": "01-01", "end": "12-31", "model": "0", "metric": "1"}
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
def load_origin_data(origin_name: str, parameter: str) -> pd.DataFrame:
    cfg             = ORIGINS[origin_name]
    station_region  = cfg["stations"]
    years_to_fetch  = cfg["years"]
    stations        = list(station_region.keys())
    all_records, errors = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_fetch_one, s, parameter, years_to_fetch): s for s in stations}
        for future in as_completed(futures):
            stn = futures[future]
            try:
                all_records.extend(future.result())
            except Exception as e:
                errors.append(f"{stn}: {e}")
    if errors:
        st.warning(f"{origin_name} — {len(errors)} station(s) failed")
    df = pd.DataFrame(all_records)
    if not df.empty:
        df["region"] = df["station"].map(station_region)
    return df


# -------------------------------------------------------
# CALENDAR-YEAR PROCESSING  (Colombia / Honduras / Super4 / Vietnam)
# -------------------------------------------------------
@st.cache_data(show_spinner=False)
def process_precipitation(raw_df: pd.DataFrame, today: pd.Timestamp):
    df = raw_df[raw_df["date"] != "02-29"].copy().reset_index(drop=True)
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
    return daily_avg


@st.cache_data(show_spinner=False)
def process_temperature(raw_df: pd.DataFrame, today: pd.Timestamp):
    df = raw_df[raw_df["date"] != "02-29"].copy().reset_index(drop=True)
    daily_avg = (
        df.groupby(["region", "year", "date"])
        .agg(tavg_avg=("tavg", "mean"))
        .reset_index()
    )
    daily_avg["tag"] = daily_avg.apply(
        lambda r: "realized" if r["year"] == "normals"
        else ("realized" if pd.to_datetime(f"{r['year']}-{r['date']}") <= today else "forecast"),
        axis=1,
    )
    return daily_avg


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
# BRAZIL PROCESSING  (crop year Sep-Aug)
# -------------------------------------------------------
@st.cache_data(show_spinner=False)
def process_brazil(raw_prcp: pd.DataFrame, today: pd.Timestamp):
    df = raw_prcp[raw_prcp["date"] != "02-29"].copy().reset_index(drop=True)

    df_real    = df[df["year"] != "normals"].copy()
    df_normals = df[df["year"] == "normals"].copy()

    # --- Real data ---
    df_real["year_int"]  = df_real["year"].astype(int)
    df_real["full_date"] = pd.to_datetime(
        df_real["year_int"].astype(str) + "-" + df_real["date"], errors="coerce"
    )
    df_real = df_real[df_real["full_date"].notna()].copy()

    def _crop_label(dt):
        if dt.month >= 9:
            return f"{dt.year % 100:02d}/{(dt.year + 1) % 100:02d}"
        return f"{(dt.year - 1) % 100:02d}/{dt.year % 100:02d}"

    df_real["crop_year"] = df_real["full_date"].apply(_crop_label)
    # Map to reference timeline: Sep-Dec → year 2000, Jan-Aug → year 2001
    df_real["xdate"] = df_real["full_date"].apply(
        lambda dt: pd.Timestamp(2000 if dt.month >= 9 else 2001, dt.month, dt.day)
    )
    df_real["tag"] = df_real["full_date"].apply(
        lambda d: "realized" if d <= today else "forecast"
    )

    real_daily = (
        df_real.groupby(["region", "crop_year", "xdate", "tag"], as_index=False)
        .agg(prcp_avg=("prcp", "mean"))
        .sort_values("xdate")
    )
    real_daily["cumulative_prcp"] = (
        real_daily.groupby(["region", "crop_year"])["prcp_avg"].cumsum()
    )
    # Only keep crop years >= 22/23
    real_daily = real_daily[real_daily["crop_year"] >= "22/23"].copy()

    # Color mapping: most recent = red, then black, pink, green, gray
    def _cy_end(cy):
        return int(cy.split("/")[1])

    crop_years_sorted  = sorted(real_daily["crop_year"].unique(), key=_cy_end)
    crop_year_colors   = {
        cy: CROP_COLOR_PALETTE[i] if i < len(CROP_COLOR_PALETTE) else INK_4
        for i, cy in enumerate(reversed(crop_years_sorted))
    }
    latest_crop_year = crop_years_sorted[-1] if crop_years_sorted else None

    # --- Normals ---
    df_normals["month"] = df_normals["date"].str[:2].astype(int)
    df_normals["day"]   = df_normals["date"].str[3:].astype(int)
    df_normals["xdate"] = df_normals.apply(
        lambda r: pd.Timestamp(2000 if r["month"] >= 9 else 2001, r["month"], r["day"]),
        axis=1,
    )
    normals_daily = (
        df_normals.groupby(["region", "xdate"], as_index=False)
        .agg(prcp_avg=("prcp", "mean"))
        .sort_values("xdate")
    )
    normals_daily["cumulative_prcp"] = (
        normals_daily.groupby("region")["prcp_avg"].cumsum()
    )

    return real_daily, normals_daily, crop_years_sorted, crop_year_colors, latest_crop_year


# -------------------------------------------------------
# CHART HELPERS
# -------------------------------------------------------
def _base_layout(title: str, y_title: str, height: int = 420) -> dict:
    return dict(
        title=dict(text=title, font=dict(size=15, color=INK, family="Helvetica Neue, sans-serif"),
                   x=0, xanchor="left"),
        xaxis=dict(gridcolor=GRID, tickfont=dict(size=11, color=INK_3),
                   linecolor=BORDER, zerolinecolor=BORDER),
        yaxis=dict(title=dict(text=y_title, font=dict(color=INK_2, size=12)),
                   gridcolor=GRID, tickfont=dict(size=11, color=INK_3),
                   linecolor=BORDER, zerolinecolor=BORDER),
        legend=dict(title=dict(text="Year", font=dict(color=INK_3, size=11)),
                    bgcolor=SURFACE, bordercolor=BORDER, borderwidth=1,
                    font=dict(size=11, color=INK_2)),
        plot_bgcolor=SURFACE, paper_bgcolor=BG,
        font=dict(color=INK, family="Helvetica Neue, sans-serif"),
        hovermode="x unified",
        hoverlabel=dict(bgcolor=SURFACE, bordercolor=BORDER, font=dict(color=INK, size=12)),
        height=height,
        margin=dict(l=60, r=20, t=50, b=55),
    )


# Datetime x-axis for calendar-year charts (reference year 2001)
_DT_XAXIS_CAL = dict(
    tickformat="%b", dtick="M1", tick0="2001-01-01",
    range=["2001-01-01", "2001-12-31"],
    gridcolor=GRID, tickfont=dict(size=11, color=INK_3),
    linecolor=BORDER, zerolinecolor=BORDER,
)

# Datetime x-axis for Brazil crop-year charts (Sep 2000 → Aug 2001)
_DT_XAXIS_BRAZIL = dict(
    tickformat="%b", dtick="M1", tick0="2000-09-01",
    range=["2000-09-01", "2001-08-31"],
    gridcolor=GRID, tickfont=dict(size=11, color=INK_3),
    linecolor=BORDER, zerolinecolor=BORDER,
)


def _add_year_traces(fig, df_year, year, y_col, hover_unit, year_colors):
    """
    Realized = solid, forecast = dotted, bridged at boundary.
    Uses reference-year 2001 datetime x-axis for smooth continuous lines.
    """
    color    = year_colors.get(year, INK_4)
    df_all   = df_year.copy()
    df_all["_px"] = pd.to_datetime("2001-" + df_all["date"])
    df_all   = df_all.sort_values("_px")
    realized = df_all[df_all["tag"] == "realized"]
    forecast = df_all[df_all["tag"] == "forecast"]

    if not realized.empty:
        fig.add_trace(go.Scatter(
            x=realized["_px"], y=realized[y_col],
            mode="lines", name=year, legendgroup=year, showlegend=True,
            line=dict(color=color, width=2, dash="solid"), connectgaps=True,
            hovertemplate=f"<b>{year}</b>  %{{x|%b %d}}  %{{y:.1f}} {hover_unit}<extra></extra>",
        ))
    if not forecast.empty:
        if not realized.empty:
            forecast = pd.concat([realized.iloc[[-1]], forecast], ignore_index=True)
        fig.add_trace(go.Scatter(
            x=forecast["_px"], y=forecast[y_col],
            mode="lines", name=f"{year} fcst", legendgroup=year, showlegend=True,
            line=dict(color=color, width=2, dash="dot"), connectgaps=True,
            hovertemplate=f"<b>{year} fcst</b>  %{{x|%b %d}}  %{{y:.1f}} {hover_unit}<extra></extra>",
        ))


# -------------------------------------------------------
# CALENDAR-YEAR CHART BUILDERS
# -------------------------------------------------------
def build_cumulative_precip(daily_avg, region, year_colors):
    df_r = daily_avg[daily_avg["region"] == region].copy()
    fig  = go.Figure()
    for year in sorted(df_r["year"].unique()):
        _add_year_traces(fig, df_r[df_r["year"] == year], year, "prcp_sum_avg", "mm", year_colors)
    layout = _base_layout(f"Cumulative Precipitation  —  {region}", "mm")
    layout["xaxis"] = _DT_XAXIS_CAL.copy()
    fig.update_layout(**layout)
    return fig


def build_temperature(daily_avg_temp, region, year_colors):
    df_r      = daily_avg_temp[daily_avg_temp["region"] == region].copy()
    years_h   = [y for y in ["2023", "2024", "2025"] if y in df_r["year"].values]
    df_minmax = (
        df_r[df_r["year"].isin(years_h)]
        .groupby("date", as_index=False)
        .agg(tavg_min=("tavg_avg", "min"), tavg_max=("tavg_avg", "max"))
    )
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
    layout["xaxis"] = _DT_XAXIS_CAL.copy()
    fig.update_layout(**layout)
    return fig


def build_rolling_precip(agg_df, region, today, year_colors):
    latest_yr = str(agg_df[agg_df["year"] != "normals"]["year"].astype(str).max())
    df_r = agg_df[agg_df["region"] == region].copy()
    df_r = df_r[~((df_r["year"] == latest_yr) & (df_r["xdate"] > today))]
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
# BRAZIL CHART BUILDER  (crop year, Sep-Aug x-axis)
# -------------------------------------------------------
def build_brazil_cumulative(real_daily, normals_daily, region,
                             crop_years_sorted, crop_year_colors,
                             latest_crop_year, selected_crop_years):
    df_r = real_daily[real_daily["region"] == region].copy()
    df_n = normals_daily[normals_daily["region"] == region].sort_values("xdate")
    fig  = go.Figure()

    for cy in crop_years_sorted:
        if cy not in selected_crop_years:
            continue
        color = crop_year_colors.get(cy, INK_4)
        cy_df = df_r[df_r["crop_year"] == cy].sort_values("xdate")

        if cy == latest_crop_year:
            # Solid realized, dotted forecast — bridged for continuity
            realized = cy_df[cy_df["tag"] == "realized"]
            forecast = cy_df[cy_df["tag"] == "forecast"]
            if not realized.empty:
                fig.add_trace(go.Scatter(
                    x=realized["xdate"], y=realized["cumulative_prcp"],
                    mode="lines", name=cy, legendgroup=cy, showlegend=True,
                    line=dict(color=color, width=2, dash="solid"), connectgaps=True,
                    hovertemplate=f"<b>{cy}</b>  %{{x|%b %d}}  %{{y:.1f}} mm<extra></extra>",
                ))
            if not forecast.empty:
                if not realized.empty:
                    forecast = pd.concat([realized.iloc[[-1]], forecast], ignore_index=True)
                fig.add_trace(go.Scatter(
                    x=forecast["xdate"], y=forecast["cumulative_prcp"],
                    mode="lines", name=f"{cy} fcst", legendgroup=cy, showlegend=True,
                    line=dict(color=color, width=2, dash="dot"), connectgaps=True,
                    hovertemplate=f"<b>{cy} fcst</b>  %{{x|%b %d}}  %{{y:.1f}} mm<extra></extra>",
                ))
        else:
            fig.add_trace(go.Scatter(
                x=cy_df["xdate"], y=cy_df["cumulative_prcp"],
                mode="lines", name=cy,
                line=dict(color=color, width=2, dash="solid"), connectgaps=True,
                hovertemplate=f"<b>{cy}</b>  %{{x|%b %d}}  %{{y:.1f}} mm<extra></extra>",
            ))

    # Normals — dashed gray
    if not df_n.empty:
        fig.add_trace(go.Scatter(
            x=df_n["xdate"], y=df_n["cumulative_prcp"],
            mode="lines", name="Normals",
            line=dict(color=INK_4, width=2.5, dash="dash"), connectgaps=True,
            hovertemplate="<b>Normals</b>  %{x|%b %d}  %{y:.1f} mm<extra></extra>",
        ))

    layout = _base_layout(f"Cumulative Precipitation — Crop Year  ({region})", "mm")
    layout["xaxis"] = _DT_XAXIS_BRAZIL.copy()
    layout["legend"]["title"]["text"] = "Crop Year"
    fig.update_layout(**layout)
    return fig


# -------------------------------------------------------
# HELPER: render a full calendar-year origin tab
# -------------------------------------------------------
def render_calendar_tab(origin_name, selected_years, today):
    c1, c2 = st.columns(2)
    with c1:
        with st.spinner(f"Fetching {origin_name} precipitation..."):
            raw_prcp = load_origin_data(origin_name, "PRCP")
    with c2:
        with st.spinner(f"Fetching {origin_name} temperature..."):
            raw_temp = load_origin_data(origin_name, "TAVG")

    if raw_prcp.empty or raw_temp.empty:
        st.error(f"No data for {origin_name}.")
        return

    daily_avg      = process_precipitation(raw_prcp, today)
    daily_avg_temp = process_temperature(raw_temp, today)
    agg_df         = process_rolling(daily_avg, today)

    # Apply year filter
    daily_avg      = daily_avg[daily_avg["year"].isin(selected_years)]
    daily_avg_temp = daily_avg_temp[daily_avg_temp["year"].isin(selected_years)]
    agg_df         = agg_df[agg_df["year"].isin(selected_years)]

    active_colors = {y: c for y, c in ALL_YEAR_COLORS.items() if y in selected_years}
    regions       = sorted(daily_avg["region"].unique())

    for region in regions:
        st.markdown(
            f"<h2 class='section-header'>Cumulative Precipitation &nbsp;—&nbsp; {region}</h2>",
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            build_cumulative_precip(daily_avg, region, active_colors),
            use_container_width=True, key=f"cum_{origin_name}_{region}",
        )
        st.markdown(
            f"<h2 class='section-header'>Temperature &nbsp;—&nbsp; {region}</h2>",
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            build_temperature(daily_avg_temp, region, active_colors),
            use_container_width=True, key=f"tmp_{origin_name}_{region}",
        )
        st.markdown(
            f"<h2 class='section-header'>30-Day Rolling Precipitation &nbsp;—&nbsp; {region}</h2>",
            unsafe_allow_html=True,
        )
        st.plotly_chart(
            build_rolling_precip(agg_df, region, today, active_colors),
            use_container_width=True, key=f"rol_{origin_name}_{region}",
        )


# -------------------------------------------------------
# STREAMLIT UI
# -------------------------------------------------------
st.markdown(
    f"<h1 style='font-family:Helvetica Neue,sans-serif;font-weight:600;"
    f"color:{NAVY};letter-spacing:-.02em;margin-bottom:.15rem'>"
    f"Coffee Weather Dashboard</h1>"
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
        "Years (calendar origins)",
        options=CALENDAR_YEARS,
        default=CALENDAR_YEARS,
        help="Applies to Colombia, Honduras, Super 4, Vietnam",
    )
    st.markdown(f"<hr style='border:none;border-top:1px solid {BORDER};margin:.8rem 0'>",
                unsafe_allow_html=True)

    if st.button("Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.markdown(f"<hr style='border:none;border-top:1px solid {BORDER};margin:.8rem 0'>",
                unsafe_allow_html=True)
    st.markdown(
        f"<p style='font-size:.65rem;font-weight:700;letter-spacing:.14em;"
        f"text-transform:uppercase;color:{INK_3};margin-bottom:.6rem'>Year colors</p>",
        unsafe_allow_html=True,
    )
    for yr, col in ALL_YEAR_COLORS.items():
        st.markdown(
            f"<div style='display:flex;align-items:center;gap:8px;margin-bottom:4px'>"
            f"<span style='width:12px;height:12px;border-radius:2px;background:{col};"
            f"display:inline-block'></span>"
            f"<span style='font-size:.82rem;color:{INK_2}'>{yr}</span></div>",
            unsafe_allow_html=True,
        )
    st.markdown(f"<p style='font-size:.75rem;color:{INK_4};margin-top:.8rem'>"
                f"Today: {today.strftime('%Y-%m-%d')}</p>", unsafe_allow_html=True)

if not selected_years:
    st.warning("Select at least one year from the sidebar.")
    st.stop()

# ---------- TABS ----------
tab_brazil, tab_colombia, tab_honduras, tab_super4, tab_vietnam = st.tabs([
    "Brazil", "Colombia", "Honduras", "Super 4", "Vietnam",
])

# ---- BRAZIL ----
with tab_brazil:
    with st.spinner("Fetching Brazil precipitation..."):
        raw_brazil = load_origin_data("Brazil", "PRCP")

    if raw_brazil.empty:
        st.error("No data for Brazil.")
    else:
        real_daily, normals_daily, crop_years_sorted, crop_year_colors, latest_cy = \
            process_brazil(raw_brazil, today)

        # Crop year filter inside tab
        selected_crop_years = st.multiselect(
            "Crop Years",
            options=crop_years_sorted,
            default=crop_years_sorted,
            key="brazil_cy_filter",
        )

        regions_brazil = sorted(real_daily["region"].unique())
        for region in regions_brazil:
            st.markdown(
                f"<h2 class='section-header'>Cumulative Precipitation — Crop Year &nbsp;—&nbsp; {region}</h2>",
                unsafe_allow_html=True,
            )
            st.plotly_chart(
                build_brazil_cumulative(
                    real_daily, normals_daily, region,
                    crop_years_sorted, crop_year_colors, latest_cy,
                    selected_crop_years,
                ),
                use_container_width=True, key=f"bra_cum_{region}",
            )

# ---- COLOMBIA ----
with tab_colombia:
    render_calendar_tab("Colombia", selected_years, today)

# ---- HONDURAS ----
with tab_honduras:
    render_calendar_tab("Honduras", selected_years, today)

# ---- SUPER 4 ----
with tab_super4:
    render_calendar_tab("Super 4", selected_years, today)

# ---- VIETNAM ----
with tab_vietnam:
    render_calendar_tab("Vietnam", selected_years, today)

st.markdown(
    f"<p style='font-size:.72rem;color:{INK_4};margin-top:1.5rem;"
    f"border-top:1px solid {BORDER};padding-top:.8rem'>"
    f"Generated: {pd.Timestamp.today().strftime('%Y-%m-%d %H:%M')}</p>",
    unsafe_allow_html=True,
)
