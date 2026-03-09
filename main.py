import io
import os
import json
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import xlwt
from datetime import date

from database import fetch_data
from ml_engine import (
    train_all_models, predict_ptp_kept, predict_payment_likelihood,
    best_contact_analysis, compute_risk_scores
)
from queries import EWB_RECOVERY, EWB_PORTFOLIO, EWB_PTP_DAILY, EWB_150DPD, EWB_FIELD_RESULTS, EWB_150DPD_PTP, EWB_150DPD_FIELD, ewb_150dpd_efforts_query

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="EWB Portfolio Monitor",
    page_icon="🏦",
    layout="wide",
)

TODAY      = date.today()
TODAY_STR  = TODAY.strftime("%Y-%m-%d")
TODAY_DISP = TODAY.strftime("%B %d, %Y")
PALETTE    = px.colors.qualitative.Set2

# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR NAVIGATION
# ─────────────────────────────────────────────────────────────────────────────
st.sidebar.title("🏦 EWB Portfolio Monitor")
st.sidebar.markdown("---")
page = st.sidebar.radio("Select Portfolio", ["EWB Recovery", "EWB 150 DPD"])


# ─────────────────────────────────────────────────────────────────────────────
# SHARED UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def is_payment(s: pd.Series) -> pd.Series:
    return s.str.contains("PAYMENT|PAID|COLL", case=False, na=False)

def is_ptp(s: pd.Series) -> pd.Series:
    return s.str.contains("PTP", case=False, na=False)

def show_raw_data(df: pd.DataFrame, filename: str):
    with st.expander("📋 View Raw Data", expanded=False):
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button(
            "⬇ Download CSV",
            df.to_csv(index=False).encode(),
            file_name=filename,
            mime="text/csv",
        )

def dedupe_ptp(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["BARCODE_DATE"] = pd.to_datetime(df["BARCODE_DATE"], errors="coerce")
    df["AMOUNT"]       = pd.to_numeric(df["AMOUNT"], errors="coerce")
    return (
        df.sort_values("BARCODE_DATE")
        .drop_duplicates(subset=["CH_CODE", "AMOUNT"], keep="first")
    )

def df_to_xls_bytes(df: pd.DataFrame, sheet_name: str = "Sheet1") -> bytes:
    wb  = xlwt.Workbook(encoding="utf-8")
    ws  = wb.add_sheet(sheet_name)
    hdr = xlwt.easyxf(
        "font: bold true, name Arial, height 200; "
        "pattern: pattern solid, fore_colour grey25; "
        "borders: bottom thin;"
    )
    dat = xlwt.easyxf("font: name Arial, height 200;")
    for ci, col in enumerate(df.columns):
        ws.write(0, ci, str(col), hdr)
        ws.col(ci).width = max(len(str(col)) * 367, 3500)
    for ri, row in enumerate(df.itertuples(index=False), start=1):
        for ci, val in enumerate(row):
            if not isinstance(val, str) and pd.isnull(val):
                ws.write(ri, ci, "", dat)
            elif isinstance(val, pd.Timestamp):
                ws.write(ri, ci, val.strftime("%Y-%m-%d"), dat)
            else:
                ws.write(ri, ci, val, dat)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# ─────────────────────────────────────────────────────────────────────────────
# EWB RECOVERY ── SIDEBAR WIDGETS
# ─────────────────────────────────────────────────────────────────────────────

def recovery_target_sidebar(df: pd.DataFrame) -> dict:
    st.sidebar.markdown("---")
    st.sidebar.subheader("🎯 Bank Target")
    targets  = {}
    overall  = st.sidebar.number_input(
        "Overall Bank Target (₱)", min_value=0.0, value=0.0,
        step=10000.0, format="%.2f", key="tgt_overall"
    )
    targets["__overall__"] = overall
    placements = sorted(df["PLACEMENT"].dropna().unique()) if "PLACEMENT" in df.columns else []
    if placements:
        with st.sidebar.expander("📂 Target per Placement", expanded=False):
            for p in placements:
                val = st.number_input(
                    str(p), min_value=0.0, value=0.0,
                    step=10000.0, format="%.2f", key=f"tgt_{p}"
                )
                targets[p] = val
    return targets


def recovery_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 Filters")
    df = df.copy()
    df["BARCODE_DATE"] = pd.to_datetime(df["BARCODE_DATE"], errors="coerce")
    df["AMOUNT"]       = pd.to_numeric(df["AMOUNT"], errors="coerce")

    valid = df["BARCODE_DATE"].dropna()
    if not valid.empty:
        min_d = valid.min().date()
        max_d = valid.max().date()
        default_end   = min(TODAY, max_d)
        default_start = default_end
        rng = st.sidebar.date_input(
            "📅 Barcode Date Range",
            value=(default_start, default_end),
            min_value=min_d,
            max_value=max_d,
            key="rec_date",
            help="Defaults to today. Expand range to backtrack.",
        )
        if isinstance(rng, (list, tuple)) and len(rng) == 2:
            s, e = rng
            df = df[(df["BARCODE_DATE"].dt.date >= s) & (df["BARCODE_DATE"].dt.date <= e)]
        elif isinstance(rng, date):
            df = df[df["BARCODE_DATE"].dt.date == rng]

    for col, label, key in [
        ("AGENT",     "Agent",     "rec_agent"),
        ("STATUS",    "Status",    "rec_status"),
        ("PLACEMENT", "Placement", "rec_place"),
    ]:
        if col in df.columns:
            opts = sorted(df[col].dropna().unique())
            sel  = st.sidebar.multiselect(label, opts, key=key)
            if sel:
                df = df[df[col].isin(sel)]

    st.sidebar.caption(f"Showing **{len(df):,}** records")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# EWB RECOVERY ── PORTFOLIO OVERVIEW
# ─────────────────────────────────────────────────────────────────────────────

def render_portfolio(df_port: pd.DataFrame):
    st.markdown("## 📁 Portfolio Overview")
    df_port = df_port.copy()
    df_port["OB"] = pd.to_numeric(df_port["OB"], errors="coerce")

    k1, k2, k3 = st.columns(3)
    k1.metric("🏦 Total Accounts",           f"{df_port['ACCT_NO'].nunique():,}")
    k2.metric("💰 Total Outstanding Balance", f"₱{df_port['OB'].sum():,.2f}")
    k3.metric("📂 Placements",
              f"{df_port['PLACEMENT'].nunique():,}" if "PLACEMENT" in df_port.columns else "—")

    if "PLACEMENT" in df_port.columns:
        st.markdown("---")
        place_grp = (
            df_port.groupby("PLACEMENT", dropna=False)
            .agg(Accounts=("ACCT_NO", "nunique"), Total_OB=("OB", "sum"))
            .reset_index()
            .rename(columns={"PLACEMENT": "Placement"})
            .sort_values("Total_OB", ascending=False)
        )
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("#### Per Placement Summary")
            st.dataframe(
                place_grp, use_container_width=True, hide_index=True,
                column_config={
                    "Total_OB": st.column_config.NumberColumn("Total OB (₱)", format="₱%.2f"),
                    "Accounts": st.column_config.NumberColumn("Accounts", format="%d"),
                }
            )
        with c2:
            fig = px.bar(
                place_grp, x="Placement", y="Total_OB",
                text=place_grp["Total_OB"].apply(lambda x: f"₱{x:,.0f}"),
                color="Placement", color_discrete_sequence=PALETTE,
                title="OB per Placement"
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(showlegend=False, yaxis_title="Total OB (₱)", height=380)
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.markdown("#### 🔎 Client / Account Search")
    search = st.text_input(
        "Search by Account No, CH Code, or Name",
        placeholder="e.g. 1234567  or  JUAN DELA CRUZ",
        key="port_search",
    )
    if search.strip():
        mask = (
            df_port["ACCT_NO"].astype(str).str.contains(search, case=False, na=False)
            | df_port["CH_CODE"].astype(str).str.contains(search, case=False, na=False)
            | df_port["CH_NAME"].astype(str).str.contains(search, case=False, na=False)
        )
        result = df_port[mask]
        st.caption(f"Found **{len(result):,}** record(s)")
        st.dataframe(
            result[["ACCT_NO", "CH_CODE", "CH_NAME", "PLACEMENT", "OB"]],
            use_container_width=True, hide_index=True,
            column_config={"OB": st.column_config.NumberColumn(format="₱%.2f")},
        )
    else:
        st.caption("Type above to search accounts.")


# ─────────────────────────────────────────────────────────────────────────────
# EWB RECOVERY ── NEW ENDORSEMENT CONSOLIDATION
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# EWB RECOVERY — ENDORSEMENT PERSISTENCE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

ENDO_STORE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "endorsements.json")

def _normalize_endo(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]
    aliases = {
        "ACCTNO": "ACCT_NO", "ACCOUNT_NO": "ACCT_NO", "ACCOUNT": "ACCT_NO",
        "CHCODE": "CH_CODE", "CHNAME": "CH_NAME", "NAME": "CH_NAME",
        "OUTSTANDING_BALANCE": "OB", "BALANCE": "OB",
        "ENDORSE_DATE": "ENDO_DATE", "ENDORSEMENT_DATE": "ENDO_DATE",
    }
    df.rename(columns=aliases, inplace=True)
    return df

def _load_saved_batches() -> list[dict]:
    """Load all previously saved endorsement batches from disk."""
    if not os.path.exists(ENDO_STORE):
        return []
    try:
        with open(ENDO_STORE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []

def _save_batch(batch_name: str, df: pd.DataFrame):
    """Append a new batch to the saved store on disk."""
    os.makedirs(os.path.dirname(ENDO_STORE), exist_ok=True)
    batches = _load_saved_batches()
    # Avoid duplicate filenames — overwrite if same name uploaded again
    batches = [b for b in batches if b["name"] != batch_name]
    batches.append({
        "name":     batch_name,
        "uploaded": TODAY_STR,
        "rows":     len(df),
        "data":     df.to_json(orient="records", date_format="iso"),
    })
    with open(ENDO_STORE, "w", encoding="utf-8") as f:
        json.dump(batches, f, ensure_ascii=False)

def _delete_batch(batch_name: str):
    batches = [b for b in _load_saved_batches() if b["name"] != batch_name]
    os.makedirs(os.path.dirname(ENDO_STORE), exist_ok=True)
    with open(ENDO_STORE, "w", encoding="utf-8") as f:
        json.dump(batches, f, ensure_ascii=False)

def _all_endorsements_df() -> pd.DataFrame:
    """Combine all saved batches into one DataFrame."""
    batches = _load_saved_batches()
    if not batches:
        return pd.DataFrame()
    frames = []
    for b in batches:
        df = pd.read_json(b["data"], orient="records")
        df["_batch"] = b["name"]
        df["_uploaded"] = b["uploaded"]
        frames.append(df)
    return pd.concat(frames, ignore_index=True)


def _render_endo_charts(df_endo: pd.DataFrame, title_suffix: str = ""):
    """Shared chart block for endorsement data."""
    if df_endo.empty:
        st.info("No endorsement data.")
        return

    total_accts = df_endo["ACCT_NO"].nunique() if "ACCT_NO" in df_endo.columns else len(df_endo)
    ob_series   = pd.to_numeric(df_endo.get("OB", pd.Series(dtype=float)), errors="coerce")
    total_ob    = ob_series.sum()
    placements  = df_endo["PLACEMENT"].nunique() if "PLACEMENT" in df_endo.columns else 0

    k1, k2, k3 = st.columns(3)
    k1.metric("🏦 Accounts", f"{total_accts:,}")
    k2.metric("💰 Total OB", f"₱{total_ob:,.2f}")
    k3.metric("📂 Placements", f"{placements}")

    c1, c2 = st.columns(2)
    if "PLACEMENT" in df_endo.columns:
        acct_col = "ACCT_NO" if "ACCT_NO" in df_endo.columns else df_endo.columns[0]
        with c1:
            place_cnt = (
                df_endo.groupby("PLACEMENT")[acct_col]
                .nunique().reset_index(name="Accounts")
                .sort_values("Accounts", ascending=False)
            )
            fig = px.bar(place_cnt, x="PLACEMENT", y="Accounts", text="Accounts",
                         color="PLACEMENT", color_discrete_sequence=PALETTE,
                         title=f"Accounts per Placement {title_suffix}")
            fig.update_traces(textposition="outside")
            fig.update_layout(showlegend=False, height=360)
            st.plotly_chart(fig, use_container_width=True)

        if "OB" in df_endo.columns:
            df_endo["OB"] = pd.to_numeric(df_endo["OB"], errors="coerce")
            with c2:
                place_ob = (
                    df_endo.groupby("PLACEMENT")["OB"].sum()
                    .reset_index(name="Total_OB")
                    .sort_values("Total_OB", ascending=False)
                )
                fig = px.bar(place_ob, x="PLACEMENT", y="Total_OB",
                             text=place_ob["Total_OB"].apply(lambda x: f"₱{x:,.0f}"),
                             color="PLACEMENT", color_discrete_sequence=PALETTE,
                             title=f"Total OB per Placement {title_suffix}")
                fig.update_traces(textposition="outside")
                fig.update_layout(showlegend=False, height=360, yaxis_title="OB (₱)")
                st.plotly_chart(fig, use_container_width=True)

    if "OB" in df_endo.columns:
        df_endo["OB"] = pd.to_numeric(df_endo["OB"], errors="coerce")
        fig = px.histogram(df_endo, x="OB", nbins=30, title="OB Distribution",
                           color_discrete_sequence=["#1f77b4"])
        fig.update_layout(xaxis_title="OB (₱)", yaxis_title="Accounts", height=280)
        st.plotly_chart(fig, use_container_width=True)

    if "ENDO_DATE" in df_endo.columns:
        df_endo["ENDO_DATE"] = pd.to_datetime(df_endo["ENDO_DATE"], errors="coerce")
        daily = (
            df_endo.dropna(subset=["ENDO_DATE"])
            .groupby(df_endo["ENDO_DATE"].dt.date).size()
            .reset_index(name="Accounts")
        )
        daily.columns = ["Endo Date", "Accounts"]
        if not daily.empty:
            fig = px.bar(daily, x="Endo Date", y="Accounts", text="Accounts",
                         color_discrete_sequence=["#9467bd"],
                         title="Endorsement Date Timeline")
            fig.update_layout(height=280)
            st.plotly_chart(fig, use_container_width=True)


def render_endorsement_consolidation():
    st.markdown("## 📥 New Endorsement Consolidation")
    st.caption(
        "Upload files here — they are **automatically saved** to disk. "
        "Your data persists even after closing or refreshing."
    )

    # ── Upload + auto-save ────────────────────────────────────────────────
    uploaded = st.file_uploader(
        "Drop endorsement file here (.xlsx, .xls, or .csv)",
        type=["xlsx", "xls", "csv"],
        key="endo_upload",
    )

    if uploaded is not None:
        try:
            df_new = (
                pd.read_csv(uploaded)
                if uploaded.name.endswith(".csv")
                else pd.read_excel(uploaded)
            )
            df_new = _normalize_endo(df_new)
            if not df_new.empty:
                _save_batch(uploaded.name, df_new)
                st.success(f"✅ **{uploaded.name}** saved — {len(df_new):,} rows added to consolidated store.")
        except Exception as e:
            st.error(f"Could not read file: {e}")

    # ── Saved batches manager ─────────────────────────────────────────────
    batches = _load_saved_batches()

    if not batches:
        st.info("📂 No endorsement files saved yet. Upload a file above to begin.")
        return

    st.markdown("---")
    st.markdown(f"#### 📂 Saved Batches ({len(batches)} file(s))")

    for b in batches:
        col_info, col_del = st.columns([5, 1])
        with col_info:
            st.markdown(f"📄 **{b['name']}** — {b['rows']:,} rows — uploaded {b['uploaded']}")
        with col_del:
            if st.button("🗑 Delete", key=f"del_{b['name']}"):
                _delete_batch(b["name"])
                st.rerun()

    st.markdown("---")

    # ── Tabs: view by batch or consolidated ──────────────────────────────
    tab_all, tab_batch = st.tabs(["📊 Consolidated View", "🗂 View by Batch"])

    with tab_all:
        st.markdown("#### All Batches Combined")
        df_all = _all_endorsements_df()
        _render_endo_charts(df_all, "(All Batches)")
        with st.expander("📋 Full Consolidated Table", expanded=False):
            st.dataframe(df_all, use_container_width=True, hide_index=True)
            st.download_button(
                "⬇ Download Consolidated CSV",
                df_all.to_csv(index=False).encode(),
                file_name=f"endorsement_consolidated_{TODAY_STR}.csv",
                mime="text/csv",
                key="dl_all_endo",
            )

    with tab_batch:
        batch_names = [b["name"] for b in batches]
        sel = st.selectbox("Select batch to view", batch_names, key="endo_batch_sel")
        if sel:
            b_data = next(b for b in batches if b["name"] == sel)
            df_b   = pd.read_json(b_data["data"], orient="records")
            df_b   = _normalize_endo(df_b)
            st.markdown(f"#### {sel} — {len(df_b):,} rows — uploaded {b_data['uploaded']}")
            _render_endo_charts(df_b, f"({sel})")
            with st.expander("📋 Full Table", expanded=False):
                st.dataframe(df_b, use_container_width=True, hide_index=True)
                st.download_button(
                    "⬇ Download This Batch CSV",
                    df_b.to_csv(index=False).encode(),
                    file_name=f"{sel}_{TODAY_STR}.csv",
                    mime="text/csv",
                    key=f"dl_batch_{sel}",
                )


# ─────────────────────────────────────────────────────────────────────────────
# EWB RECOVERY ── KPIs + TARGET TRACKER
# ─────────────────────────────────────────────────────────────────────────────

def recovery_kpis(df: pd.DataFrame, df_port: pd.DataFrame, targets: dict):
    df      = df.copy()
    df_port = df_port.copy()
    df["AMOUNT"]  = pd.to_numeric(df["AMOUNT"], errors="coerce")
    df_port["OB"] = pd.to_numeric(df_port["OB"], errors="coerce")

    total_accts   = df_port["ACCT_NO"].nunique()
    total_ob      = df_port["OB"].sum()
    unique_agents = df["AGENT"].nunique()

    ptp_df    = df[is_ptp(df["STATUS"])]
    ptp_count = len(ptp_df)
    ptp_amt   = ptp_df["AMOUNT"].sum()

    this_mo = (
        (df["BARCODE_DATE"].dt.year  == TODAY.year) &
        (df["BARCODE_DATE"].dt.month == TODAY.month)
    )
    pay_mo = df[this_mo & is_payment(df["STATUS"])]
    posted = pay_mo["AMOUNT"].sum()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🏦 Total Accounts",     f"{total_accts:,}",   help="All endorsed accounts")
    c2.metric("💰 Total OB",           f"₱{total_ob:,.2f}")
    c3.metric("👤 Active Agents",      f"{unique_agents:,}")
    c4.metric("💳 PTP Count",          f"{ptp_count:,}",     f"₱{ptp_amt:,.2f}")
    c5.metric("✅ Posted (This Mo.)",  f"₱{posted:,.2f}")
    st.caption(
        f"📅 **Posted = {TODAY.strftime('%B %Y')} payment rows only.**  "
        "The date filter above is for browsing efforts — it does not affect the Posted figure."
    )

    overall_target = targets.get("__overall__", 0)
    if overall_target > 0:
        st.markdown("---")
        st.markdown("#### 🎯 Overall Target vs Posted")
        pct = posted / overall_target * 100
        var = posted - overall_target
        t1, t2, t3, t4 = st.columns(4)
        t1.metric("🎯 Bank Target", f"₱{overall_target:,.2f}")
        t2.metric("✅ Posted",      f"₱{posted:,.2f}")
        t3.metric("📊 % Achieved",  f"{pct:.1f}%",
                  delta="On target ✅" if pct >= 100 else "Below target ⚠️",
                  delta_color="normal" if pct >= 100 else "inverse")
        t4.metric("📉 Variance",    f"₱{var:,.2f}",
                  delta="Surplus" if var >= 0 else "Shortfall",
                  delta_color="normal" if var >= 0 else "inverse")

    place_targets = {k: v for k, v in targets.items() if k != "__overall__" and v > 0}
    if place_targets:
        st.markdown("#### 📂 Per Placement: Target vs Posted")
        rows = []
        for placement, tgt in place_targets.items():
            p_pay = pay_mo[pay_mo["PLACEMENT"] == placement]["AMOUNT"].sum()
            pct   = p_pay / tgt * 100 if tgt > 0 else 0
            rows.append({
                "Placement":    placement,
                "Target (₱)":  round(tgt, 2),
                "Posted (₱)":  round(p_pay, 2),
                "% Achieved":  f"{pct:.1f}%",
                "Variance (₱)": round(p_pay - tgt, 2),
                "Status":      "✅ On target" if pct >= 100 else "⚠️ Below",
            })
        st.dataframe(
            pd.DataFrame(rows), use_container_width=True, hide_index=True,
            column_config={
                "Target (₱)":   st.column_config.NumberColumn(format="₱%.2f"),
                "Posted (₱)":   st.column_config.NumberColumn(format="₱%.2f"),
                "Variance (₱)": st.column_config.NumberColumn(format="₱%.2f"),
            }
        )


# ─────────────────────────────────────────────────────────────────────────────
# EWB RECOVERY ── EFFORT CHARTS
# ─────────────────────────────────────────────────────────────────────────────

def recovery_charts(df: pd.DataFrame):
    pay_df = df[is_payment(df["STATUS"])]
    ptp_df = df[is_ptp(df["STATUS"])]

    st.markdown("### 📊 Agent Performance")
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Unique Worked Accounts per Agent**")
        worked = (
            df.dropna(subset=["AGENT", "ACCT_NO"])
            .drop_duplicates(subset=["AGENT", "ACCT_NO"])
            .groupby("AGENT")["ACCT_NO"].count()
            .reset_index(name="Unique Accounts")
            .sort_values("Unique Accounts", ascending=True)
        )
        fig = px.bar(worked, x="Unique Accounts", y="AGENT", orientation="h",
                     text="Unique Accounts", color="Unique Accounts",
                     color_continuous_scale="Blues")
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, coloraxis_showscale=False,
                          yaxis_title="", height=max(300, len(worked) * 30))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("**PTP Made per Agent**")
        if ptp_df.empty:
            st.info("No PTP records.")
        else:
            pa = (
                ptp_df.groupby("AGENT")
                .agg(PTP_Count=("RESULT_ID", "count"), PTP_Amount=("AMOUNT", "sum"))
                .reset_index().sort_values("PTP_Count", ascending=True)
            )
            fig = px.bar(pa, x="PTP_Count", y="AGENT", orientation="h",
                         text="PTP_Count", color="PTP_Amount",
                         color_continuous_scale="Greens",
                         hover_data={"PTP_Amount": ":,.2f"})
            fig.update_traces(textposition="outside")
            fig.update_layout(coloraxis_showscale=False, yaxis_title="",
                              height=max(300, len(pa) * 30))
            st.plotly_chart(fig, use_container_width=True)

    c3, c4 = st.columns(2)

    with c3:
        st.markdown("**Payment Posted per Agent** *(this month)*")
        pay_mo = pay_df[
            (pay_df["BARCODE_DATE"].dt.year  == TODAY.year) &
            (pay_df["BARCODE_DATE"].dt.month == TODAY.month)
        ]
        if pay_mo.empty:
            st.info("No payments this month.")
        else:
            pa = (
                pay_mo.groupby("AGENT")["AMOUNT"].sum()
                .reset_index(name="Amount")
                .sort_values("Amount", ascending=True)
            )
            fig = px.bar(pa, x="Amount", y="AGENT", orientation="h",
                         text=pa["Amount"].apply(lambda x: f"₱{x:,.0f}"),
                         color="Amount", color_continuous_scale="Oranges")
            fig.update_traces(textposition="outside")
            fig.update_layout(coloraxis_showscale=False, yaxis_title="",
                              height=max(300, len(pa) * 30))
            st.plotly_chart(fig, use_container_width=True)

    with c4:
        st.markdown("**Top 5 Status by Accounts**")
        top5 = df["STATUS"].fillna("Unknown").value_counts().head(5).reset_index()
        top5.columns = ["Status", "Count"]
        fig = px.bar(top5, x="Status", y="Count", text="Count",
                     color="Status", color_discrete_sequence=PALETTE)
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, xaxis_title="", height=360)
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("**💳 Payment per Placement** *(this month)*")
    pay_mo_all = pay_df[
        (pay_df["BARCODE_DATE"].dt.year  == TODAY.year) &
        (pay_df["BARCODE_DATE"].dt.month == TODAY.month)
    ]
    if not pay_mo_all.empty and "PLACEMENT" in pay_mo_all.columns:
        pp = (
            pay_mo_all.groupby("PLACEMENT")["AMOUNT"].sum()
            .reset_index(name="Amount")
            .sort_values("Amount", ascending=False)
        )
        fig = px.bar(pp, x="PLACEMENT", y="Amount",
                     text=pp["Amount"].apply(lambda x: f"₱{x:,.0f}"),
                     color="PLACEMENT", color_discrete_sequence=PALETTE)
        fig.update_traces(textposition="outside")
        fig.update_layout(showlegend=False, xaxis_title="Placement",
                          yaxis_title="Total Payment (₱)", height=380)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No placement/payment data this month.")

    st.markdown("**📅 Daily Effort Trend**")
    daily = (
        df.dropna(subset=["BARCODE_DATE"])
        .groupby(df["BARCODE_DATE"].dt.date)
        .size().reset_index(name="Efforts")
    )
    daily.columns = ["Date", "Efforts"]
    fig = px.line(daily, x="Date", y="Efforts", markers=True,
                  line_shape="spline", color_discrete_sequence=["#1f77b4"])
    fig.update_layout(height=300)
    st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# EWB RECOVERY ── PTP TRACKING
# ─────────────────────────────────────────────────────────────────────────────

def render_ptp_tracking(df_ptp_raw: pd.DataFrame):
    st.markdown("## 💳 PTP Tracking — " + TODAY.strftime("%B %Y"))

    if df_ptp_raw.empty:
        st.info("No PTP data available for this month.")
        return

    df = dedupe_ptp(df_ptp_raw)
    df["START_DATE"]   = pd.to_datetime(df["START_DATE"],   errors="coerce")
    df["BARCODE_DATE"] = pd.to_datetime(df["BARCODE_DATE"], errors="coerce")
    df["AMOUNT"]       = pd.to_numeric(df["AMOUNT"],        errors="coerce")

    k1, k2, k3 = st.columns(3)
    k1.metric("💳 Unique PTPs (deduped)", f"{len(df):,}")
    k2.metric("💰 Total PTP Amount",      f"₱{df['AMOUNT'].sum():,.2f}")
    k3.metric("🏦 Unique Accounts",
              f"{df['ACCT_NO'].nunique():,}" if "ACCT_NO" in df.columns else "—")

    placements = sorted(df["PLACEMENT"].dropna().unique()) if "PLACEMENT" in df.columns else []
    st.markdown("---")

    tab1, tab2, tab3 = st.tabs([
        "📅 Daily Gathered (Barcode Date)",
        "🗓️ PTP Schedule (Start Date)",
        "👤 Per Agent Daily",
    ])

    with tab1:
        sel = st.multiselect("Filter Placement", placements, default=[],
                             key="ptp_t1", placeholder="All placements")
        d   = df[df["PLACEMENT"].isin(sel)] if sel else df
        grp = (
            d.dropna(subset=["BARCODE_DATE"])
            .groupby([d["BARCODE_DATE"].dt.date, "PLACEMENT"])
            .agg(PTP_Count=("RESULT_ID", "count"), PTP_Amount=("AMOUNT", "sum"))
            .reset_index()
        )
        grp.columns = ["Date", "Placement", "PTP Count", "PTP Amount"]
        if grp.empty:
            st.info("No data.")
        else:
            ca, cb = st.columns(2)
            with ca:
                fig = px.bar(grp, x="Date", y="PTP Count", color="Placement",
                             barmode="group", text="PTP Count",
                             title="Daily PTP Count", color_discrete_sequence=PALETTE)
                fig.update_layout(height=360)
                st.plotly_chart(fig, use_container_width=True)
            with cb:
                fig = px.bar(grp, x="Date", y="PTP Amount", color="Placement",
                             barmode="group", title="Daily PTP Amount",
                             color_discrete_sequence=PALETTE)
                fig.update_layout(height=360, yaxis_title="Amount (₱)")
                st.plotly_chart(fig, use_container_width=True)
            st.dataframe(grp.sort_values("Date", ascending=False),
                         use_container_width=True, hide_index=True,
                         column_config={"PTP Amount": st.column_config.NumberColumn(format="₱%.2f")})

    with tab2:
        sel = st.multiselect("Filter Placement", placements, default=[],
                             key="ptp_t2", placeholder="All placements")
        d   = df[df["PLACEMENT"].isin(sel)] if sel else df
        grp = (
            d.dropna(subset=["START_DATE"])
            .groupby([d["START_DATE"].dt.date, "PLACEMENT"])
            .agg(PTP_Count=("RESULT_ID", "count"), PTP_Amount=("AMOUNT", "sum"))
            .reset_index()
        )
        grp.columns = ["Start Date", "Placement", "PTP Count", "PTP Amount"]
        if grp.empty:
            st.info("No schedule data.")
        else:
            ca, cb = st.columns(2)
            with ca:
                fig = px.bar(grp, x="Start Date", y="PTP Count", color="Placement",
                             barmode="stack", title="PTP Schedule Count",
                             color_discrete_sequence=PALETTE)
                fig.update_layout(height=360)
                st.plotly_chart(fig, use_container_width=True)
            with cb:
                fig = px.bar(grp, x="Start Date", y="PTP Amount", color="Placement",
                             barmode="stack", title="PTP Schedule Amount",
                             color_discrete_sequence=PALETTE)
                fig.update_layout(height=360, yaxis_title="Amount (₱)")
                st.plotly_chart(fig, use_container_width=True)
            st.dataframe(grp.sort_values("Start Date"),
                         use_container_width=True, hide_index=True,
                         column_config={"PTP Amount": st.column_config.NumberColumn(format="₱%.2f")})

    with tab3:
        sel_ag = st.multiselect("Filter Agent", sorted(df["AGENT"].dropna().unique()),
                                default=[], key="ptp_t3", placeholder="All agents")
        d      = df[df["AGENT"].isin(sel_ag)] if sel_ag else df
        ag_d   = (
            d.dropna(subset=["BARCODE_DATE", "AGENT"])
            .groupby([d["BARCODE_DATE"].dt.date, "AGENT"])
            .agg(PTP_Count=("RESULT_ID", "count"), PTP_Amount=("AMOUNT", "sum"))
            .reset_index()
        )
        ag_d.columns = ["Date", "Agent", "PTP Count", "PTP Amount"]
        if ag_d.empty:
            st.info("No agent PTP data.")
        else:
            fig = px.bar(ag_d, x="Date", y="PTP Count", color="Agent",
                         barmode="group", title="Daily PTPs per Agent",
                         color_discrete_sequence=PALETTE)
            fig.update_layout(height=420)
            st.plotly_chart(fig, use_container_width=True)
            summary = (
                d.groupby("AGENT")
                .agg(Total_PTP=("RESULT_ID", "count"), Total_Amount=("AMOUNT", "sum"))
                .reset_index()
                .rename(columns={"AGENT": "Agent", "Total_PTP": "Total PTP",
                                  "Total_Amount": "Total Amount (₱)"})
                .sort_values("Total PTP", ascending=False)
            )
            st.dataframe(summary, use_container_width=True, hide_index=True,
                         column_config={"Total Amount (₱)": st.column_config.NumberColumn(format="₱%.2f")})


# ─────────────────────────────────────────────────────────────────────────────
# EWB RECOVERY ── AGENT TABLE
# ─────────────────────────────────────────────────────────────────────────────

def recovery_agent_table(df: pd.DataFrame):
    df = df.copy()
    df["AMOUNT"] = pd.to_numeric(df["AMOUNT"], errors="coerce")
    rows = []
    for agent, grp in df.groupby("AGENT"):
        pay_mo = grp[
            is_payment(grp["STATUS"]) &
            (grp["BARCODE_DATE"].dt.year  == TODAY.year) &
            (grp["BARCODE_DATE"].dt.month == TODAY.month)
        ]
        ptp_rows  = grp[is_ptp(grp["STATUS"])]
        ptp_count = len(ptp_rows)
        rows.append({
            "Agent":              agent,
            "Worked Accts":       grp["ACCT_NO"].nunique(),
            "Total Efforts":      len(grp),
            "PTP Count":          ptp_count,
            "PTP Amount (₱)":     round(ptp_rows["AMOUNT"].sum(), 2),
            "Posted This Mo (₱)": round(pay_mo["AMOUNT"].sum(), 2),
            "PTP Rate":           f"{ptp_count / len(grp) * 100:.1f}%" if len(grp) else "0%",
        })
    st.markdown("#### 👤 Agent Summary Table")
    st.dataframe(
        pd.DataFrame(rows).sort_values("Worked Accts", ascending=False),
        use_container_width=True, hide_index=True,
        column_config={
            "PTP Amount (₱)":     st.column_config.NumberColumn(format="₱%.2f"),
            "Posted This Mo (₱)": st.column_config.NumberColumn(format="₱%.2f"),
        }
    )


# ─────────────────────────────────────────────────────────────────────────────
# EWB 150 DPD ── PULL OUT SECTION
# ─────────────────────────────────────────────────────────────────────────────

def compute_pullout_date(endo_date, cycle):
    """
    Pullout date = next month after endo_date, day = cycle number.
    Example: endo 2026-02-04, cycle 3 -> 2026-03-03
    """
    try:
        if pd.isnull(endo_date) or pd.isnull(cycle):
            return None
        endo = pd.Timestamp(endo_date)
        cycle_int = int(cycle)
        year  = endo.year + (1 if endo.month == 12 else 0)
        month = 1 if endo.month == 12 else endo.month + 1
        return date(year, month, cycle_int)
    except Exception:
        return None


def render_pullout_section(df: pd.DataFrame):
    st.markdown(f"## 📤 Pull Out — {TODAY_DISP}")

    df2 = df.copy()

    # Try DB PULLOUT_DATE first; if missing/null, compute from endo_date + cycle
    has_db_pullout = (
        "PULLOUT_DATE" in df2.columns and
        pd.to_datetime(df2["PULLOUT_DATE"], errors="coerce").notna().any()
    )

    if has_db_pullout:
        df2["PULLOUT_DATE"] = pd.to_datetime(df2["PULLOUT_DATE"], errors="coerce")
    else:
        endo_col  = next((c for c in ["leads_endo_date", "ENDO_DATE"] if c in df2.columns), None)
        cycle_col = "Cycle" if "Cycle" in df2.columns else None

        if endo_col and cycle_col:
            df2["PULLOUT_DATE"] = df2.apply(
                lambda r: compute_pullout_date(r[endo_col], r[cycle_col]), axis=1
            )
            df2["PULLOUT_DATE"] = pd.to_datetime(df2["PULLOUT_DATE"], errors="coerce")
            st.caption("ℹ️ Pull out date computed from Endo Date + Cycle (next month, day = cycle number).")
        else:
            st.warning("Cannot determine pull out date — need PULLOUT_DATE, Endo Date, or Cycle columns.")
            return

    pullout_df = df2[df2["PULLOUT_DATE"].dt.date == TODAY]

    if pullout_df.empty:
        st.success(f"✅ No accounts scheduled for pull out today ({TODAY_DISP}).")
        return

    st.warning(f"⚠️ **{len(pullout_df):,}** account(s) are scheduled for pull out today.")

    ch_col = next((c for c in ["leads_chcode", "CH_CODE"] if c in pullout_df.columns), None)

    preview_cols = [c for c in [ch_col, "AgentCode", "PULLOUT_DATE", "Status", "OB"] if c and c in pullout_df.columns]
    st.dataframe(pullout_df[preview_cols], use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("#### ⬇ Download Pull Out Files *(Excel 97-2003 .xls)*")

    d1, d2, d3 = st.columns(3)

    # File 1 — CH Code + POUT agent
    with d1:
        st.markdown("**File 1** — CH Code & Agent")
        if ch_col:
            f1 = pd.DataFrame({
                "CHCODE": pullout_df[ch_col].values,
                "AGENT":  "POUT",
            })
            st.download_button(
                "⬇ Download File 1",
                data=df_to_xls_bytes(f1, "PullOut_Agents"),
                file_name=f"pullout_agents_{TODAY_STR}.xls",
                mime="application/vnd.ms-excel",
                key="dl1",
            )
        else:
            st.warning("CH_CODE column not found.")

    # File 2 — Dispo template pre-filled
    with d2:
        st.markdown("**File 2** — Dispo Template")
        if ch_col:
            f2 = pd.DataFrame({
                "CHCODE":      pullout_df[ch_col].values,
                "STATUS":      "RETURNS",
                "SUB_STATUS":  "PULLOUT",
                "AMOUNT":      "",
                "START_DATE":  "",
                "END_DATE":    "",
                "OR_NUMBER":   "",
                "NOTES":       "PULLOUT",
                "NEW_ADDRESS": "",
                "NEW_CONTACT": "",
                "AGENT":       "POUT",
                "DATE":        TODAY_STR,
            })
            st.download_button(
                "⬇ Download File 2",
                data=df_to_xls_bytes(f2, "PullOut_Dispo"),
                file_name=f"pullout_dispo_{TODAY_STR}.xls",
                mime="application/vnd.ms-excel",
                key="dl2",
            )
        else:
            st.warning("CH_CODE column not found.")

    # File 3 — Full account info
    with d3:
        st.markdown("**File 3** — Full Account Info")
        avail = [c for c in [ch_col, "AgentCode", "Status", "Substatus",
                              "OB", "Cycle", "PULLOUT_DATE", "leads_endo_date"]
                 if c and c in pullout_df.columns]
        f3 = pullout_df[avail].rename(
            columns={ch_col: "CH_CODE", "AgentCode": "AGENT",
                     "leads_endo_date": "ENDO_DATE"}
        ).copy()
        f3["POUT_AGENT"]     = "POUT"
        f3["GENERATED_DATE"] = TODAY_STR
        st.download_button(
            "⬇ Download File 3",
            data=df_to_xls_bytes(f3, "PullOut_FullInfo"),
            file_name=f"pullout_fullinfo_{TODAY_STR}.xls",
            mime="application/vnd.ms-excel",
            key="dl3",
        )


# ─────────────────────────────────────────────────────────────────────────────
# EWB 150 DPD ── FILTERS + KPIs + CHARTS + CYCLE + AGENT
# ─────────────────────────────────────────────────────────────────────────────

def apply_filters_150(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔍 Filters")
    for col, label, key in [
        ("AgentCode", "Agent",     "dpd_agent"),
        ("Status",    "Status",    "dpd_status"),
        ("Substatus", "Substatus", "dpd_sub"),
        ("Cycle",     "Cycle",     "dpd_cycle"),
    ]:
        if col in df.columns:
            opts = sorted(df[col].dropna().unique())
            sel  = st.sidebar.multiselect(label, opts, key=key)
            if sel:
                df = df[df[col].isin(sel)]
    st.sidebar.caption(f"Showing **{len(df):,}** records")
    return df


def show_kpis_150(df: pd.DataFrame):
    total     = len(df)
    agents    = df["AgentCode"].nunique() if "AgentCode" in df.columns else 0
    contacted = int(df["Status"].notna().sum()) if "Status" in df.columns else 0
    ptp       = int(df["Status"].str.contains("PTP", case=False, na=False).sum()) if "Status" in df.columns else 0
    kept      = int(df["Substatus"].str.contains("KEPT", case=False, na=False).sum()) if "Substatus" in df.columns else 0
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🏦 Total Accounts", f"{total:,}")
    c2.metric("👤 Active Agents",  f"{agents:,}")
    c3.metric("📞 Contacted",      f"{contacted:,}", f"{contacted/total*100:.1f}%" if total else None)
    c4.metric("💳 PTP",            f"{ptp:,}",       f"{ptp/total*100:.1f}%"       if total else None)
    c5.metric("✅ Kept PTP",       f"{kept:,}")



# ─────────────────────────────────────────────────────────────────────────────
# EWB 150 DPD ── ACCOUNTS WITH NO EFFORT THIS MONTH
# ─────────────────────────────────────────────────────────────────────────────

def render_no_effort_section(df_raw: pd.DataFrame):
    """
    Show accounts that have NO effort recorded in the current month.
    Uses LastTouchDate from the 150 DPD query (latest result per account).
    An account is considered 'no effort this month' if:
      - LastTouchDate is NULL, OR
      - LastTouchDate is not in the current month/year
    """
    st.markdown("### 🚨 Accounts with No Effort — Current Month")
    st.caption(
        f"Accounts where the last touch is **NOT in {TODAY.strftime('%B %Y')}**. "
        "These need immediate attention."
    )

    df = df_raw.copy()

    # Identify the touch date column
    touch_col = next(
        (c for c in ["LastTouchDate", "BARCODE_DATE"] if c in df.columns), None
    )

    if touch_col is None:
        st.warning("No LastTouchDate column found.")
        return

    df[touch_col] = pd.to_datetime(df[touch_col], errors="coerce")

    # Flag accounts with NO effort this month
    this_month = (
        (df[touch_col].dt.month == TODAY.month) &
        (df[touch_col].dt.year  == TODAY.year)
    )
    no_effort_df = df[~this_month].copy()

    # Summary KPIs
    total_accts   = len(df)
    no_effort_cnt = len(no_effort_df)
    has_effort    = total_accts - no_effort_cnt
    pct_no_effort = no_effort_cnt / total_accts * 100 if total_accts else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📋 Total Accounts",        f"{total_accts:,}")
    k2.metric("✅ Worked This Month",     f"{has_effort:,}")
    k3.metric("🚨 No Effort This Month",  f"{no_effort_cnt:,}",
              delta=f"{pct_no_effort:.1f}% of portfolio",
              delta_color="inverse")
    k4.metric("📅 Reference Month",       TODAY.strftime("%B %Y"))

    if no_effort_df.empty:
        st.success("✅ All accounts have been touched this month!")
        return

    st.markdown("---")

    # Charts
    c1, c2 = st.columns(2)

    with c1:
        # Worked vs Not worked pie
        pie_data = pd.DataFrame({
            "Status": ["✅ Worked This Month", "🚨 No Effort"],
            "Count":  [has_effort, no_effort_cnt],
        })
        fig = px.pie(
            pie_data, names="Status", values="Count", hole=0.45,
            color="Status",
            color_discrete_map={
                "✅ Worked This Month": "#2ca02c",
                "🚨 No Effort":         "#d62728",
            },
            title="Effort Coverage This Month",
        )
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        # No-effort accounts per agent
        agent_col = next(
            (c for c in ["AgentCode", "AGENT", "LastTouch"] if c in no_effort_df.columns), None
        )
        if agent_col:
            agent_no = (
                no_effort_df.groupby(agent_col)
                .size().reset_index(name="No Effort Accounts")
                .sort_values("No Effort Accounts", ascending=True)
            )
            fig = px.bar(
                agent_no, x="No Effort Accounts", y=agent_col,
                orientation="h", text="No Effort Accounts",
                color="No Effort Accounts", color_continuous_scale="Reds",
                title="No-Effort Accounts per Agent",
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(
                coloraxis_showscale=False, yaxis_title="",
                height=max(300, len(agent_no) * 30),
            )
            st.plotly_chart(fig, use_container_width=True)

    # No-effort by cycle
    if "Cycle" in no_effort_df.columns:
        st.markdown("**No-Effort Accounts by Cycle**")
        cycle_no = (
            no_effort_df.groupby("Cycle")
            .size().reset_index(name="No Effort Accounts")
            .sort_values("Cycle")
        )
        fig = px.bar(
            cycle_no, x="Cycle", y="No Effort Accounts",
            text="No Effort Accounts",
            color="No Effort Accounts", color_continuous_scale="Oranges",
            title="No-Effort Accounts per Cycle",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(coloraxis_showscale=False, height=320)
        st.plotly_chart(fig, use_container_width=True)

    # Last touch date breakdown — how stale are these accounts?
    st.markdown("**⏱ How Long Since Last Touch?**")
    df_stale = no_effort_df.copy()
    df_stale["Days Since Touch"] = (
        pd.Timestamp(TODAY) - df_stale[touch_col]
    ).dt.days.fillna(9999).astype(int)

    bins   = [0, 7, 14, 30, 60, 90, 180, 9999]
    labels = ["1-7 days", "8-14 days", "15-30 days",
              "31-60 days", "61-90 days", "91-180 days", "180+ days / Never"]
    df_stale["Staleness"] = pd.cut(
        df_stale["Days Since Touch"], bins=bins, labels=labels, right=True
    )
    stale_counts = df_stale["Staleness"].value_counts().reindex(labels).fillna(0).reset_index()
    stale_counts.columns = ["Range", "Accounts"]
    fig = px.bar(
        stale_counts, x="Range", y="Accounts",
        text="Accounts", color="Accounts",
        color_continuous_scale="RdYlGn_r",
        title="Days Since Last Touch (No-Effort Accounts)",
    )
    fig.update_traces(textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=320, xaxis_title="Days Since Touch")
    st.plotly_chart(fig, use_container_width=True)

    # Full table with filter
    st.markdown("#### 📋 No-Effort Account List")

    # Filter by agent
    if agent_col and agent_col in no_effort_df.columns:
        agents = ["All"] + sorted(no_effort_df[agent_col].dropna().unique().tolist())
        sel_agent = st.selectbox(
            "Filter by Agent", agents, key="no_effort_agent_filter"
        )
        display_df = (
            no_effort_df if sel_agent == "All"
            else no_effort_df[no_effort_df[agent_col] == sel_agent]
        )
    else:
        display_df = no_effort_df

    # Select useful columns
    show_cols = [c for c in [
        "leads_chcode", "AgentCode", "Cycle", "Status", "Substatus",
        touch_col, "OB", "leads_endo_date", "PULLOUT_DATE",
    ] if c in display_df.columns]

    st.dataframe(
        display_df[show_cols].rename(columns={
            "leads_chcode":    "CH Code",
            "AgentCode":       "Agent",
            "leads_endo_date": "Endo Date",
            touch_col:         "Last Touch Date",
        }).sort_values("Last Touch Date", ascending=True, na_position="first"),
        use_container_width=True,
        hide_index=True,
        column_config={
            "OB": st.column_config.NumberColumn("OB (₱)", format="₱%.2f"),
        }
    )

    st.download_button(
        "⬇ Download No-Effort List",
        display_df[show_cols].to_csv(index=False).encode(),
        file_name=f"no_effort_{TODAY_STR}.csv",
        mime="text/csv",
        key="dl_no_effort",
    )



def show_charts_150(df: pd.DataFrame):
    c1, c2 = st.columns(2)

    with c1:
        if "Status" in df.columns:
            counts = df["Status"].fillna("Unknown").value_counts().reset_index()
            counts.columns = ["Status", "Count"]
            fig = px.bar(counts, x="Count", y="Status", orientation="h",
                         title="Accounts by Status", text="Count",
                         color="Status", color_discrete_sequence=PALETTE)
            fig.update_layout(showlegend=False, yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("**📅 Daily Payment**")
        if "PaymentDate" in df.columns and "Amount" in df.columns:
            pay = df[df["Status"].str.contains("PAYMENT|PAID|COLL", case=False, na=False)].copy()
            pay["PaymentDate"] = pd.to_datetime(pay["PaymentDate"], errors="coerce")
            pay["Amount"]      = pd.to_numeric(pay["Amount"], errors="coerce")
            if not pay.empty:
                dp = (
                    pay.dropna(subset=["PaymentDate"])
                    .groupby(pay["PaymentDate"].dt.date)
                    .agg(Amount=("Amount", "sum"), Transactions=("Amount", "count"))
                    .reset_index()
                )
                dp.columns = ["Date", "Amount", "Transactions"]
                fig = px.bar(dp, x="Date", y="Amount",
                             text=dp["Amount"].apply(lambda x: f"₱{x:,.0f}"),
                             hover_data={"Transactions": True},
                             color_discrete_sequence=["#2ca02c"])
                fig.update_traces(textposition="outside")
                fig.update_layout(yaxis_title="Payment Amount (₱)", height=360)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No payment records.")
        else:
            st.info("No payment data available.")

    c3, c4 = st.columns(2)
    with c3:
        if "AgentCode" in df.columns:
            counts = df["AgentCode"].value_counts().head(15).reset_index()
            counts.columns = ["Agent", "Count"]
            fig = px.bar(counts, x="Agent", y="Count", title="Top 15 Agents",
                         text="Count", color="Agent", color_discrete_sequence=PALETTE)
            fig.update_layout(showlegend=False, xaxis_tickangle=-35)
            st.plotly_chart(fig, use_container_width=True)

    with c4:
        if "LastTouchDate" in df.columns:
            d = df.copy()
            d["LastTouchDate"] = pd.to_datetime(d["LastTouchDate"], errors="coerce")
            daily = (
                d.dropna(subset=["LastTouchDate"])
                .groupby(d["LastTouchDate"].dt.date)
                .size().reset_index(name="Touches")
            )
            daily.columns = ["Date", "Touches"]
            fig = px.line(daily, x="Date", y="Touches", title="Daily Touch Activity",
                          markers=True, color_discrete_sequence=["#1f77b4"])
            st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# OB CURED — MANUAL REVIEW STORE
# ─────────────────────────────────────────────────────────────────────────────

OB_CURED_STORE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "ob_cured.json"
)

def _load_ob_cured() -> dict:
    if not os.path.exists(OB_CURED_STORE):
        return {}
    try:
        with open(OB_CURED_STORE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_ob_cured(store: dict):
    os.makedirs(os.path.dirname(OB_CURED_STORE), exist_ok=True)
    with open(OB_CURED_STORE, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False)

def _get_ob_cured_by_cycle() -> dict:
    store = _load_ob_cured()
    result = {}
    for key, val in store.items():
        if val.get("cured", False):
            cycle = str(val.get("cycle", "Unknown"))
            result[cycle] = result.get(cycle, 0.0) + float(val.get("ob", 0))
    return result


def render_ob_cured_review(df_raw: pd.DataFrame, df_ptp: pd.DataFrame = None):
    st.markdown("### \U0001f48a OB Cured Review")
    st.caption(
        "Review PTP accounts and mark each as **Cured** (payment confirmed) or **Not Cured**. "
        "Cured OB feeds directly into the Cycle Summary table."
    )

    if df_ptp is not None and not df_ptp.empty:
        df_review = df_ptp.copy()
        if "CH_CODE" in df_review.columns and "ACCT_NO" not in df_review.columns:
            df_review["ACCT_NO"] = df_review["CH_CODE"]
    else:
        df_review = df_raw.copy()
        if "Status" in df_review.columns:
            df_review = df_review[df_review["Status"].str.contains("PTP", case=False, na=False)]

    if df_review.empty:
        st.info("No PTP accounts found to review.")
        return

    df_review = df_review.copy()
    df_review["OB"] = pd.to_numeric(df_review.get("OB", 0), errors="coerce").fillna(0)

    acct_col  = next((c for c in ["ACCT_NO","CH_CODE","leads_chcode"] if c in df_review.columns), None)
    name_col  = next((c for c in ["CH_NAME","leads_chname"] if c in df_review.columns), None)
    cycle_col = "Cycle" if "Cycle" in df_review.columns else None
    agent_col = next((c for c in ["AGENT","AgentCode"] if c in df_review.columns), None)

    if acct_col is None:
        st.warning("No account number column found.")
        return

    df_review = df_review.sort_values("OB", ascending=False).drop_duplicates(subset=[acct_col], keep="first")
    store = _load_ob_cured()

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        view_filter = st.selectbox("Show", ["All", "\u2705 Cured", "\u274c Not Cured", "\u2b1c Unmarked"], key="ob_cured_view")
    with col_f2:
        cycle_filter = "All"
        if cycle_col:
            cycles = ["All"] + sorted(df_review[cycle_col].dropna().astype(str).unique().tolist())
            cycle_filter = st.selectbox("Cycle", cycles, key="ob_cured_cycle_filter")
    with col_f3:
        agent_filter = "All"
        if agent_col:
            agents = ["All"] + sorted(df_review[agent_col].dropna().unique().tolist())
            agent_filter = st.selectbox("Agent", agents, key="ob_cured_agent_filter")

    df_filtered = df_review.copy()
    if cycle_filter != "All" and cycle_col:
        df_filtered = df_filtered[df_filtered[cycle_col].astype(str) == cycle_filter]
    if agent_filter != "All" and agent_col:
        df_filtered = df_filtered[df_filtered[agent_col] == agent_filter]

    def _get_status(acct):
        k = str(acct)
        if k not in store: return "Unmarked"
        return "Cured" if store[k].get("cured") else "Not Cured"

    df_filtered = df_filtered.copy()
    df_filtered["_status"] = df_filtered[acct_col].apply(_get_status)
    if view_filter == "\u2705 Cured":
        df_filtered = df_filtered[df_filtered["_status"] == "Cured"]
    elif view_filter == "\u274c Not Cured":
        df_filtered = df_filtered[df_filtered["_status"] == "Not Cured"]
    elif view_filter == "\u2b1c Unmarked":
        df_filtered = df_filtered[df_filtered["_status"] == "Unmarked"]

    cured_rows = [v for v in store.values() if v.get("cured")]
    not_cured  = [v for v in store.values() if not v.get("cured")]
    cured_ob   = sum(float(v.get("ob", 0)) for v in cured_rows)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("PTP Accounts",   f"{len(df_review):,}")
    k2.metric("Marked Cured",   f"{len(cured_rows):,}")
    k3.metric("Not Cured",      f"{len(not_cured):,}")
    k4.metric("Total OB Cured", f"\u20b1{cured_ob:,.2f}")

    st.markdown("---")
    bc1, bc2, bc3 = st.columns(3)
    with bc1:
        if st.button("Mark ALL Filtered Cured", key="bulk_cure_all"):
            for _, row in df_filtered.iterrows():
                k = str(row[acct_col])
                store[k] = {"cured": True, "ob": float(row["OB"]),
                             "cycle": str(row[cycle_col]) if cycle_col else "Unknown",
                             "agent": str(row[agent_col]) if agent_col else ""}
            _save_ob_cured(store); st.rerun()
    with bc2:
        if st.button("Mark ALL Filtered Not Cured", key="bulk_not_cure"):
            for _, row in df_filtered.iterrows():
                k = str(row[acct_col])
                store[k] = {"cured": False, "ob": float(row["OB"]),
                             "cycle": str(row[cycle_col]) if cycle_col else "Unknown",
                             "agent": str(row[agent_col]) if agent_col else ""}
            _save_ob_cured(store); st.rerun()
    with bc3:
        if st.button("Reset ALL", key="reset_ob_cured"):
            _save_ob_cured({}); st.rerun()

    st.caption(f"Showing {len(df_filtered):,} accounts")

    for idx, row in df_filtered.iterrows():
        acct      = str(row[acct_col])
        ob_val    = float(row["OB"])
        cur_stat  = _get_status(acct)
        cycle_val = str(row[cycle_col]) if cycle_col else "—"
        agent_val = str(row[agent_col]) if agent_col else "—"
        name_val  = str(row[name_col])  if name_col  else acct
        amt_val   = float(pd.to_numeric(row.get("AMOUNT", 0), errors="coerce") or 0)
        status_icon = "\u2705" if cur_stat == "Cured" else ("\u274c" if cur_stat == "Not Cured" else "\u2b1c")

        col_info, col_cure, col_not = st.columns([6, 1, 1])
        with col_info:
            st.markdown(
                f"{status_icon} **{acct}** | {name_val} | "
                f"Cycle **{cycle_val}** | Agent **{agent_val}** | "
                f"OB \u20b1{ob_val:,.2f} | PTP \u20b1{amt_val:,.2f}"
            )
        with col_cure:
            if st.button("Cured", key=f"cure_{acct}_{idx}",
                         type="primary" if cur_stat == "Cured" else "secondary"):
                store[acct] = {"cured": True, "ob": ob_val, "cycle": cycle_val, "agent": agent_val}
                _save_ob_cured(store); st.rerun()
        with col_not:
            if st.button("Not", key=f"not_{acct}_{idx}",
                         type="primary" if cur_stat == "Not Cured" else "secondary"):
                store[acct] = {"cured": False, "ob": ob_val, "cycle": cycle_val, "agent": agent_val}
                _save_ob_cured(store); st.rerun()

    export = [{"ACCT_NO": k, "Cured": "Yes" if v.get("cured") else "No",
               "OB": v.get("ob",0), "Cycle": v.get("cycle",""), "Agent": v.get("agent","")}
              for k, v in store.items()]
    if export:
        st.download_button("Download OB Cured Review",
            pd.DataFrame(export).to_csv(index=False).encode(),
            file_name=f"ob_cured_{TODAY_STR}.csv", mime="text/csv", key="dl_ob_cured")




def _fmt(n) -> str:
    """Format number with comma thousands separator, 2 decimal places."""
    try:
        return f"{float(n):,.2f}"
    except Exception:
        return str(n)

def _fmti(n) -> str:
    """Format integer with comma separator."""
    try:
        return f"{int(n):,}"
    except Exception:
        return str(n)


def _cycle_summary_table(grp_df: pd.DataFrame, df_ptp: pd.DataFrame = None,
                         df_efforts: pd.DataFrame = None) -> pd.DataFrame:
    """Build cycle summary rows.
    df_ptp     — deduplicated PTP query for accurate PTP counts.
    df_efforts — all-efforts query for Worked Accounts + Total Efforts per cycle.
    """
    rows = []
    grp_df = grp_df.copy()
    grp_df["Cycle"] = grp_df["Cycle"].astype(str)

    # Pre-process PTP dataframe once
    df_ptp_clean = None
    if df_ptp is not None and not df_ptp.empty and "Cycle" in df_ptp.columns:
        df_ptp_clean = df_ptp.copy()
        df_ptp_clean["Cycle"]  = df_ptp_clean["Cycle"].astype(str)
        df_ptp_clean["AMOUNT"] = pd.to_numeric(df_ptp_clean.get("AMOUNT", pd.Series(dtype=float)), errors="coerce").fillna(0)
        sort_col = "BARCODE_DATE" if "BARCODE_DATE" in df_ptp_clean.columns else df_ptp_clean.columns[0]
        df_ptp_clean = df_ptp_clean.sort_values(sort_col)
        if "CH_CODE" in df_ptp_clean.columns:
            df_ptp_clean = df_ptp_clean.drop_duplicates(subset=["CH_CODE", "AMOUNT"], keep="first")

    # Pre-process efforts dataframe once
    df_eff_clean = None
    if df_efforts is not None and not df_efforts.empty:
        df_eff_clean = df_efforts.copy()
        if "Cycle" in df_eff_clean.columns:
            df_eff_clean["Cycle"] = df_eff_clean["Cycle"].astype(str)

    cured_map = _get_ob_cured_by_cycle()

    for cycle, grp in grp_df.groupby("Cycle", sort=True):
        total   = len(grp)
        ob      = pd.to_numeric(grp["OB"], errors="coerce").sum()

        # ── Worked accounts + Total efforts from dedicated efforts query ────
        if df_eff_clean is not None and "Cycle" in df_eff_clean.columns:
            cycle_eff = df_eff_clean[df_eff_clean["Cycle"] == cycle]
            worked    = cycle_eff["CH_CODE"].nunique() if "CH_CODE" in cycle_eff.columns else len(cycle_eff)
            efforts   = len(cycle_eff)
        else:
            # Fallback: derive from grp_df (one row per account — latest status)
            worked = int(grp[grp["Status"].notna() & (grp["Status"].str.strip() != "")].shape[0]) if "Status" in grp.columns else 0
            efforts = worked

        # ── Kept Amount: sum of Amount for ANY payment regardless of PTP status ──
        kept_amt = 0.0
        if "Amount" in grp.columns:
            kept_amt = pd.to_numeric(
                grp.loc[grp["Status"].str.contains("PAYMENT|PAID|COLL|KEPT", case=False, na=False), "Amount"],
                errors="coerce"
            ).sum() if "Status" in grp.columns else 0.0

        if df_ptp_clean is not None:
            cycle_ptp = df_ptp_clean[df_ptp_clean["Cycle"] == cycle]
            ptp       = len(cycle_ptp)
            ptp_amt   = cycle_ptp["AMOUNT"].sum()
        else:
            ptp     = int(grp["Status"].str.contains("PTP", case=False, na=False).sum()) if "Status" in grp.columns else 0
            ptp_amt = grp.loc[grp["Status"].str.contains("PTP", case=False, na=False), "Amount"].pipe(pd.to_numeric, errors="coerce").sum() if "Amount" in grp.columns else 0

        # OB-based target (10% of OB)
        target_ob    = round(ob * 0.10, 2)
        ob_cured     = round(cured_map.get(str(cycle), 0.0), 2)
        ob_variance  = round(target_ob - ob_cured, 2)
        pct_achieved = round(ob_cured / target_ob * 100, 1) if target_ob else 0.0

        # PTP OB = sum of OB of accounts that have a PTP
        ptp_ob = 0.0
        if df_ptp_clean is not None:
            cycle_ptp_codes = set(df_ptp_clean[df_ptp_clean["Cycle"] == cycle]["CH_CODE"].astype(str).tolist()) if "CH_CODE" in df_ptp_clean.columns else set()
            if cycle_ptp_codes and "leads_chcode" in grp.columns:
                ptp_ob = pd.to_numeric(
                    grp[grp["leads_chcode"].astype(str).isin(cycle_ptp_codes)]["OB"],
                    errors="coerce"
                ).sum()

        rows.append({
            "Cycle":            cycle,
            "Cycle Count":      _fmti(total),
            "Worked Accounts":  _fmti(worked),
            "Total Efforts":    _fmti(efforts),
            "OB":               _fmt(ob),
            "Target (10% OB)":  _fmt(target_ob),
            "PTP Count":        _fmti(ptp),
            "PTP OB":           _fmt(ptp_ob),
            "OB Cured":         _fmt(ob_cured),
            "Kept Amount":      _fmt(kept_amt),
            "Variance":         _fmt(ob_variance),
            "% Achieved":       f"{pct_achieved:.1f}%",
        })
    return pd.DataFrame(rows)


def _render_cycle_table_and_chart(summary_df: pd.DataFrame, key_suffix: str):
    """Render cycle summary table full-width, chart below."""
    if summary_df.empty:
        st.info("No data for this cut.")
        return

    # ── Full-width table (all columns visible, scroll down only) ─────────
    st.dataframe(
        summary_df,
        use_container_width=True,
        hide_index=True,
        height=min(400, (len(summary_df) + 1) * 38 + 10),
        column_config={c: st.column_config.TextColumn() for c in summary_df.columns if c != "Cycle"},
    )

    # ── Chart below ───────────────────────────────────────────────────────
    def _to_num(series):
        return pd.to_numeric(series.astype(str).str.replace(",", ""), errors="coerce").fillna(0)

    fig = go.Figure()
    if "Cycle Count" in summary_df.columns:
        vals = _to_num(summary_df["Cycle Count"])
        fig.add_trace(go.Bar(
            name="Cycle Count", x=summary_df["Cycle"], y=vals,
            marker_color="#1f77b4", text=summary_df["Cycle Count"], textposition="outside",
        ))
    if "Worked Accounts" in summary_df.columns:
        vals = _to_num(summary_df["Worked Accounts"])
        fig.add_trace(go.Bar(
            name="Worked", x=summary_df["Cycle"], y=vals,
            marker_color="#ff7f0e", text=summary_df["Worked Accounts"], textposition="outside",
        ))
    if "PTP Count" in summary_df.columns:
        vals = _to_num(summary_df["PTP Count"])
        fig.add_trace(go.Bar(
            name="PTP Count", x=summary_df["Cycle"], y=vals,
            marker_color="#2ca02c", text=summary_df["PTP Count"], textposition="outside",
        ))
    if "OB Cured" in summary_df.columns and "Target (10% OB)" in summary_df.columns:
        cured_vals  = _to_num(summary_df["OB Cured"])
        target_vals = _to_num(summary_df["Target (10% OB)"])
        fig.add_trace(go.Bar(
            name="OB Cured", x=summary_df["Cycle"], y=cured_vals,
            marker_color="#9467bd",
            text=summary_df["OB Cured"], textposition="outside",
        ))
        fig.add_trace(go.Scatter(
            name="Target (10% OB)", x=summary_df["Cycle"], y=target_vals,
            mode="lines+markers",
            line=dict(color="#d62728", width=2, dash="dash"),
        ))
    fig.update_layout(
        barmode="group", height=380, xaxis_title="Cycle",
        legend=dict(orientation="h", y=1.12),
        margin=dict(t=40),
    )
    st.plotly_chart(fig, use_container_width=True, key=f"cycle_chart_{key_suffix}")


def show_cycle_section(df: pd.DataFrame, df_ptp: pd.DataFrame = None, df_efforts: pd.DataFrame = None):
    if "Cycle" not in df.columns:
        return

    st.markdown("### 🔄 Cycle Performance")
    df = df.copy()
    df["Cycle"] = df["Cycle"].astype(str)

    # ── Detect endo date column ───────────────────────────────────────────
    endo_col = next((c for c in ["leads_endo_date", "ENDO_DATE"] if c in df.columns), None)

    if endo_col:
        df[endo_col] = pd.to_datetime(df[endo_col], errors="coerce")

        # Get the two most recent endo months present in data
        endo_months = (
            df.dropna(subset=[endo_col])[endo_col]
            .dt.to_period("M")
            .drop_duplicates()
            .sort_values(ascending=False)
            .head(2)          # latest two months
            .sort_values()    # oldest first for display
            .tolist()
        )

        # Build a tab per endo month + one overall tab
        tab_labels = []
        for p in endo_months:
            endo_month_name = p.strftime("%B %Y")
            cut_month_name  = (p + 1).strftime("%B %Y")
            tab_labels.append(f"📅 {endo_month_name} Endo → {cut_month_name} Cut")
        tab_labels.append("📊 Overall")

        tabs = st.tabs(tab_labels)

        for i, p in enumerate(endo_months):
            with tabs[i]:
                endo_month_name = p.strftime("%B %Y")
                cut_month_name  = (p + 1).strftime("%B %Y")
                st.markdown(
                    f"**Endorsed in {endo_month_name}** — "
                    f"Cut month: **{cut_month_name}**"
                )
                mask   = df[endo_col].dt.to_period("M") == p
                subset = df[mask]
                if subset.empty:
                    st.info(f"No accounts endorsed in {endo_month_name}.")
                else:
                    summary = _cycle_summary_table(subset, df_ptp=df_ptp, df_efforts=df_efforts)
                    _render_cycle_table_and_chart(summary, key_suffix=str(p))

                    # OB & Payment totals for this cut
                    total_ob  = pd.to_numeric(subset["OB"], errors="coerce").sum()
                    total_pay = pd.to_numeric(
                        subset.loc[
                            subset["Status"].str.contains("PAYMENT|PAID|COLL", case=False, na=False),
                            "Amount"
                        ], errors="coerce"
                    ).sum() if "Amount" in subset.columns else 0
                    a1, a2, a3 = st.columns(3)
                    a1.metric("🏦 Total Accounts", f"{subset['leads_chcode'].nunique() if 'leads_chcode' in subset.columns else len(subset):,}")
                    a2.metric("💰 Total OB",       f"₱{total_ob:,.2f}")
                    a3.metric("💳 Total Payment",  f"₱{total_pay:,.2f}")

        # Overall tab (last)
        with tabs[-1]:
            st.markdown("**All endorsement months combined**")
            summary_all = _cycle_summary_table(df, df_ptp=df_ptp, df_efforts=df_efforts)
            _render_cycle_table_and_chart(summary_all, key_suffix="overall")

    else:
        # No endo date — show overall only
        summary_df = _cycle_summary_table(df, df_ptp=df_ptp, df_efforts=df_efforts)
        _render_cycle_table_and_chart(summary_df, key_suffix="main")

    # ── OB Cured Review ──────────────────────────────────────────────────
    st.markdown("---")
    with st.expander("💊 OB Cured Review — Mark accounts as Cured / Not Cured", expanded=False):
        render_ob_cured_review(df, df_ptp=df_ptp)

    # ── Daily activity per cycle (always shown) ───────────────────────────
    st.markdown("#### 📅 Daily Accounts per Cycle")
    if "LastTouchDate" in df.columns:
        df2 = df.copy()
        df2["LastTouchDate"] = pd.to_datetime(df2["LastTouchDate"], errors="coerce")
        dc = (
            df2.dropna(subset=["LastTouchDate"])
            .groupby([df2["LastTouchDate"].dt.date, "Cycle"])
            .size().reset_index(name="Accounts")
        )
        dc.columns = ["Date", "Cycle", "Accounts"]
        fig = px.line(dc, x="Date", y="Accounts", color="Cycle",
                      markers=True, line_shape="spline",
                      color_discrete_sequence=PALETTE,
                      title="Daily Activity — Each Cycle")
        fig.update_layout(height=380, legend_title="Cycle")
        st.plotly_chart(fig, use_container_width=True)


def show_agent_table_150(df: pd.DataFrame, df_ptp: pd.DataFrame = None):
    if "AgentCode" not in df.columns:
        return

    # Build per-agent PTP count from PTP query if available
    ptp_by_agent = {}
    ptp_amt_by_agent = {}
    if df_ptp is not None and not df_ptp.empty and "AGENT" in df_ptp.columns:
        df_p = df_ptp.copy()
        df_p["AMOUNT"] = pd.to_numeric(df_p.get("AMOUNT", 0), errors="coerce").fillna(0)
        for ag, grp in df_p.groupby("AGENT"):
            ptp_by_agent[ag]     = len(grp)
            ptp_amt_by_agent[ag] = grp["AMOUNT"].sum()

    rows = []
    for agent, grp in df.groupby("AgentCode"):
        total      = len(grp)
        target_ptp = max(1, round(total * 0.10))
        ptp        = ptp_by_agent.get(agent,
                     int(grp["Status"].str.contains("PTP", case=False, na=False).sum())
                     if "Status" in grp.columns else 0)
        ptp_amt    = ptp_amt_by_agent.get(agent, 0)
        kept       = int(grp["Substatus"].str.contains("KEPT", case=False, na=False).sum()) if "Substatus" in grp.columns else 0
        # How many PTPs still needed to hit 10% target
        ptp_needed = max(0, target_ptp - ptp)
        # Payment needed = same as target (every PTP should convert)
        pay_needed = max(0, target_ptp - kept)
        rows.append({
            "Agent":              agent,
            "Accts Handled":      total,
            "Target PTP (10%)":   target_ptp,
            "Actual PTP":         ptp,
            "PTP Amount (₱)":     round(ptp_amt, 2),
            "PTP Needed":         ptp_needed,
            "Kept PTP":           kept,
            "Payments Needed":    pay_needed,
            "PTP Rate":           f"{ptp/total*100:.1f}%" if total else "0%",
            "Status":             "✅ Met" if ptp >= target_ptp else "❌ Below",
        })

    st.markdown("#### 👤 Agent Summary — 10% Target Tracker")
    agent_df = pd.DataFrame(rows).sort_values("Accts Handled", ascending=False)
    st.dataframe(
        agent_df, use_container_width=True, hide_index=True,
        column_config={
            "PTP Amount (₱)":   st.column_config.NumberColumn(format="₱%.2f"),
            "Target PTP (10%)": st.column_config.NumberColumn(format="%d"),
            "Actual PTP":       st.column_config.NumberColumn(format="%d"),
            "PTP Needed":       st.column_config.NumberColumn(format="%d"),
            "Kept PTP":         st.column_config.NumberColumn(format="%d"),
            "Payments Needed":  st.column_config.NumberColumn(format="%d"),
        }
    )

    # Visual: Actual PTP vs Target per agent
    c1, c2 = st.columns(2)
    with c1:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="Actual PTP", x=agent_df["Agent"], y=agent_df["Actual PTP"],
            marker_color="#2ca02c", text=agent_df["Actual PTP"], textposition="outside",
        ))
        fig.add_trace(go.Scatter(
            name="Target (10%)", x=agent_df["Agent"], y=agent_df["Target PTP (10%)"],
            mode="lines+markers", line=dict(color="#d62728", width=2, dash="dash"),
        ))
        fig.update_layout(barmode="group", height=360, title="PTP vs 10% Target per Agent",
                          xaxis_tickangle=-30, legend=dict(orientation="h", y=1.1))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        fig = px.bar(
            agent_df[agent_df["PTP Needed"] > 0],
            x="Agent", y="PTP Needed", text="PTP Needed",
            color="PTP Needed", color_continuous_scale="Reds",
            title="PTP Still Needed to Hit Target",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(coloraxis_showscale=False, height=360, xaxis_tickangle=-30)
        st.plotly_chart(fig, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# EWB RECOVERY — FIELD RESULTS TAB
# ─────────────────────────────────────────────────────────────────────────────

FIELD_STORE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "field_accounts.json")

def _save_field_accounts(filename: str, df: pd.DataFrame):
    os.makedirs(os.path.dirname(FIELD_STORE), exist_ok=True)
    try:
        with open(FIELD_STORE, "r", encoding="utf-8") as f:
            batches = json.load(f)
    except Exception:
        batches = []
    batches = [b for b in batches if b["name"] != filename]
    batches.append({
        "name": filename,
        "uploaded": TODAY_STR,
        "rows": len(df),
        "data": df.to_json(orient="records", date_format="iso"),
    })
    with open(FIELD_STORE, "w", encoding="utf-8") as f:
        json.dump(batches, f, ensure_ascii=False)

def _load_field_accounts() -> pd.DataFrame:
    if not os.path.exists(FIELD_STORE):
        return pd.DataFrame()
    try:
        with open(FIELD_STORE, "r", encoding="utf-8") as f:
            batches = json.load(f)
        if not batches:
            return pd.DataFrame()
        frames = []
        for b in batches:
            df = pd.read_json(b["data"], orient="records")
            df["_batch"] = b["name"]
            frames.append(df)
        return pd.concat(frames, ignore_index=True)
    except Exception:
        return pd.DataFrame()

def _delete_field_batch(name: str):
    try:
        with open(FIELD_STORE, "r", encoding="utf-8") as f:
            batches = json.load(f)
    except Exception:
        batches = []
    batches = [b for b in batches if b["name"] != name]
    with open(FIELD_STORE, "w", encoding="utf-8") as f:
        json.dump(batches, f, ensure_ascii=False)


def render_field_results(df_field_db: pd.DataFrame):
    """
    Field Results tab:
    - df_field_db : rows from EWB_FIELD_RESULTS query (actual field dispositions)
    - Dropbox    : upload list of accounts sent to field (CH_CODE / ACCT_NO)
    - Matching   : compare uploaded list vs actual results
    """
    st.markdown("## 🚗 Field Results")

    if df_field_db.empty:
        st.warning("No field result data from database.")

    df_field_db = df_field_db.copy()
    df_field_db["BARCODE_DATE"] = pd.to_datetime(df_field_db["BARCODE_DATE"], errors="coerce")
    df_field_db["AMOUNT"]       = pd.to_numeric(df_field_db["AMOUNT"], errors="coerce")

    # ── Top KPIs from DB ──────────────────────────────────────────────────
    total_efforts = len(df_field_db)
    unique_accts  = df_field_db["ACCT_NO"].nunique() if "ACCT_NO" in df_field_db.columns else 0
    unique_agents = df_field_db["AGENT"].nunique()
    ptp_f         = df_field_db[is_ptp(df_field_db["STATUS"])]
    pay_f         = df_field_db[
        is_payment(df_field_db["STATUS"]) &
        (df_field_db["BARCODE_DATE"].dt.year  == TODAY.year) &
        (df_field_db["BARCODE_DATE"].dt.month == TODAY.month)
    ]

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("📋 Field Efforts",     f"{total_efforts:,}")
    k2.metric("🏦 Accounts Fielded",  f"{unique_accts:,}")
    k3.metric("👤 Field Agents",      f"{unique_agents:,}")
    k4.metric("💳 PTP",               f"{len(ptp_f):,}", f"₱{ptp_f['AMOUNT'].sum():,.2f}")
    k5.metric("✅ Posted (This Mo.)", f"₱{pay_f['AMOUNT'].sum():,.2f}")

    st.markdown("---")

    # ── Main tabs ─────────────────────────────────────────────────────────
    t1, t2, t3 = st.tabs([
        "📊 Field Performance",
        "📂 Fielded Accounts List + Matching",
        "📋 Raw Field Data",
    ])

    # ── Tab 1: Performance charts ─────────────────────────────────────────
    with t1:
        if df_field_db.empty:
            st.info("No data.")
        else:
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Accounts Fielded per Agent**")
                worked = (
                    df_field_db.dropna(subset=["AGENT","ACCT_NO"])
                    .drop_duplicates(subset=["AGENT","ACCT_NO"])
                    .groupby("AGENT")["ACCT_NO"].count()
                    .reset_index(name="Accounts")
                    .sort_values("Accounts", ascending=True)
                )
                fig = px.bar(worked, x="Accounts", y="AGENT", orientation="h",
                             text="Accounts", color="Accounts",
                             color_continuous_scale="Blues")
                fig.update_traces(textposition="outside")
                fig.update_layout(showlegend=False, coloraxis_showscale=False,
                                  yaxis_title="", height=max(300, len(worked)*30))
                st.plotly_chart(fig, use_container_width=True)

            with c2:
                st.markdown("**Status Breakdown**")
                sc = df_field_db["STATUS"].fillna("Unknown").value_counts().reset_index()
                sc.columns = ["Status","Count"]
                fig = px.bar(sc, x="Count", y="Status", orientation="h",
                             text="Count", color="Status",
                             color_discrete_sequence=PALETTE)
                fig.update_traces(textposition="outside")
                fig.update_layout(showlegend=False,
                                  yaxis={"categoryorder":"total ascending"},
                                  height=max(300, len(sc)*32))
                st.plotly_chart(fig, use_container_width=True)

            c3, c4 = st.columns(2)
            with c3:
                st.markdown("**PTP per Field Agent**")
                if ptp_f.empty:
                    st.info("No field PTPs.")
                else:
                    pa = (
                        ptp_f.groupby("AGENT")
                        .agg(PTP_Count=("RESULT_ID","count"), PTP_Amount=("AMOUNT","sum"))
                        .reset_index().sort_values("PTP_Count", ascending=True)
                    )
                    fig = px.bar(pa, x="PTP_Count", y="AGENT", orientation="h",
                                 text="PTP_Count", color="PTP_Amount",
                                 color_continuous_scale="Greens",
                                 hover_data={"PTP_Amount":":,.2f"})
                    fig.update_traces(textposition="outside")
                    fig.update_layout(coloraxis_showscale=False, yaxis_title="",
                                      height=max(300, len(pa)*30))
                    st.plotly_chart(fig, use_container_width=True)

            with c4:
                st.markdown("**📅 Daily Field Activity**")
                daily = (
                    df_field_db.dropna(subset=["BARCODE_DATE"])
                    .groupby(df_field_db["BARCODE_DATE"].dt.date).size()
                    .reset_index(name="Efforts")
                )
                daily.columns = ["Date","Efforts"]
                fig = px.line(daily, x="Date", y="Efforts", markers=True,
                              line_shape="spline",
                              color_discrete_sequence=["#e377c2"])
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

            # Agent summary table
            st.markdown("#### 👤 Field Agent Summary")
            rows = []
            for agent, grp in df_field_db.groupby("AGENT"):
                p_mo     = grp[
                    is_payment(grp["STATUS"]) &
                    (grp["BARCODE_DATE"].dt.year  == TODAY.year) &
                    (grp["BARCODE_DATE"].dt.month == TODAY.month)
                ]
                ptp_rows  = grp[is_ptp(grp["STATUS"])]
                ptp_count = len(ptp_rows)
                rows.append({
                    "Agent":              agent,
                    "Accts Fielded":      grp["ACCT_NO"].nunique() if "ACCT_NO" in grp.columns else 0,
                    "Total Efforts":      len(grp),
                    "PTP Count":          ptp_count,
                    "PTP Amount (₱)":     round(ptp_rows["AMOUNT"].sum(), 2),
                    "Posted This Mo (₱)": round(p_mo["AMOUNT"].sum(), 2),
                    "PTP Rate":           f"{ptp_count/len(grp)*100:.1f}%" if len(grp) else "0%",
                })
            st.dataframe(
                pd.DataFrame(rows).sort_values("Accts Fielded", ascending=False),
                use_container_width=True, hide_index=True,
                column_config={
                    "PTP Amount (₱)":     st.column_config.NumberColumn(format="₱%.2f"),
                    "Posted This Mo (₱)": st.column_config.NumberColumn(format="₱%.2f"),
                }
            )

    # ── Tab 2: Upload fielded accounts list + matching ─────────────────────
    with t2:
        st.markdown("### 📂 Upload Fielded Accounts List")
        st.caption(
            "Upload the list of accounts that were **sent to field**. "
            "The system will match them against actual field results from the database "
            "to show which accounts have been worked and which have no result yet."
        )

        # Upload widget
        up = st.file_uploader(
            "Drop account list here (.xlsx, .xls, .csv)",
            type=["xlsx","xls","csv"],
            key="field_acct_upload",
        )
        if up is not None:
            try:
                df_up = pd.read_csv(up) if up.name.endswith(".csv") else pd.read_excel(up)
                # normalize columns
                df_up.columns = [c.strip().upper().replace(" ","_") for c in df_up.columns]
                for a,b in [("ACCTNO","ACCT_NO"),("ACCOUNT_NO","ACCT_NO"),
                             ("CHCODE","CH_CODE"),("CHNAME","CH_NAME")]:
                    if a in df_up.columns:
                        df_up.rename(columns={a:b}, inplace=True)
                _save_field_accounts(up.name, df_up)
                st.success(f"✅ Saved **{up.name}** — {len(df_up):,} accounts")
            except Exception as e:
                st.error(f"Could not read file: {e}")

        # Show saved batches
        try:
            with open(FIELD_STORE, "r", encoding="utf-8") as f:
                field_batches = json.load(f)
        except Exception:
            field_batches = []

        if field_batches:
            st.markdown("**Saved account lists:**")
            for b in field_batches:
                bc1, bc2 = st.columns([5,1])
                with bc1:
                    st.markdown(f"📄 **{b['name']}** — {b['rows']:,} accounts — {b['uploaded']}")
                with bc2:
                    if st.button("🗑", key=f"del_fa_{b['name']}"):
                        _delete_field_batch(b["name"])
                        st.rerun()

        st.markdown("---")

        # ── Matching ──────────────────────────────────────────────────────
        df_sent = _load_field_accounts()

        if df_sent.empty:
            st.info("Upload an account list above to see the match report.")
        elif df_field_db.empty:
            st.warning("No field results in database to match against.")
        else:
            st.markdown("### 🔗 Match Report")

            # Determine join key: prefer ACCT_NO, fallback CH_CODE
            sent_key   = "ACCT_NO" if "ACCT_NO" in df_sent.columns else (
                          "CH_CODE" if "CH_CODE" in df_sent.columns else None)
            result_key = "ACCT_NO" if "ACCT_NO" in df_field_db.columns else (
                          "CH_CODE" if "CH_CODE" in df_field_db.columns else None)

            if not sent_key or not result_key:
                st.error("Could not find ACCT_NO or CH_CODE column in uploaded file.")
            else:
                # Latest result per account
                latest = (
                    df_field_db.sort_values("BARCODE_DATE", ascending=False)
                    .drop_duplicates(subset=[result_key], keep="first")
                    [[result_key, "AGENT", "STATUS", "AMOUNT", "BARCODE_DATE"]]
                    .rename(columns={
                        result_key:    "MATCH_KEY",
                        "AGENT":       "Field Agent",
                        "STATUS":      "Field Status",
                        "AMOUNT":      "Amount",
                        "BARCODE_DATE":"Last Visit",
                    })
                )

                sent_keys = df_sent[sent_key].astype(str).str.strip().str.upper()
                latest["MATCH_KEY"] = latest["MATCH_KEY"].astype(str).str.strip().str.upper()

                df_sent2 = df_sent.copy()
                df_sent2["__key__"] = sent_keys

                merged = df_sent2.merge(
                    latest, left_on="__key__", right_on="MATCH_KEY", how="left"
                ).drop(columns=["__key__","MATCH_KEY"], errors="ignore")

                merged["Match Status"] = merged["Field Status"].apply(
                    lambda x: "✅ Has Result" if pd.notna(x) else "❌ No Result Yet"
                )

                # Summary KPIs
                total_sent    = merged[sent_key].nunique()
                has_result    = merged[merged["Match Status"]=="✅ Has Result"][sent_key].nunique()
                no_result     = total_sent - has_result
                match_rate    = has_result / total_sent * 100 if total_sent else 0

                m1, m2, m3, m4 = st.columns(4)
                m1.metric("📤 Accounts Sent to Field", f"{total_sent:,}")
                m2.metric("✅ Has Field Result",        f"{has_result:,}")
                m3.metric("❌ No Result Yet",           f"{no_result:,}")
                m4.metric("📊 Match Rate",              f"{match_rate:.1f}%")

                st.markdown("---")

                # Match pie chart
                c1, c2 = st.columns(2)
                with c1:
                    pie_data = pd.DataFrame({
                        "Status": ["✅ Has Result","❌ No Result Yet"],
                        "Count":  [has_result, no_result],
                    })
                    fig = px.pie(pie_data, names="Status", values="Count",
                                 color="Status",
                                 color_discrete_map={
                                     "✅ Has Result":  "#2ca02c",
                                     "❌ No Result Yet":"#d62728",
                                 },
                                 hole=0.45, title="Match Rate")
                    st.plotly_chart(fig, use_container_width=True)

                with c2:
                    # Status breakdown of matched accounts
                    matched_only = merged[merged["Match Status"]=="✅ Has Result"]
                    if not matched_only.empty:
                        sc = matched_only["Field Status"].fillna("Unknown").value_counts().reset_index()
                        sc.columns = ["Status","Count"]
                        fig = px.bar(sc, x="Status", y="Count", text="Count",
                                     color="Status", color_discrete_sequence=PALETTE,
                                     title="Field Status of Matched Accounts")
                        fig.update_traces(textposition="outside")
                        fig.update_layout(showlegend=False, height=340)
                        st.plotly_chart(fig, use_container_width=True)

                # Filter buttons
                view_opt = st.radio(
                    "View", ["All","✅ Has Result","❌ No Result Yet"],
                    horizontal=True, key="field_match_view"
                )
                display_df = merged if view_opt == "All" else merged[merged["Match Status"]==view_opt]

                st.dataframe(
                    display_df.drop(columns=["_batch"], errors="ignore"),
                    use_container_width=True, hide_index=True,
                    column_config={
                        "Amount": st.column_config.NumberColumn(format="₱%.2f"),
                    }
                )

                st.download_button(
                    "⬇ Download Match Report CSV",
                    display_df.drop(columns=["_batch"], errors="ignore")
                              .to_csv(index=False).encode(),
                    file_name=f"field_match_report_{TODAY_STR}.csv",
                    mime="text/csv",
                    key="dl_field_match",
                )

    # ── Tab 3: Raw field data ──────────────────────────────────────────────
    with t3:
        show_raw_data(df_field_db, "field_results.csv")



import numpy as np
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import joblib

ML_MODEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "models")

def _ensure_model_dir():
    os.makedirs(ML_MODEL_DIR, exist_ok=True)

def _build_account_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build one row per ACCT_NO with features for ML models.
    Uses: STATUS, AMOUNT, BARCODE_DATE, OB, AGENT, PLACEMENT.
    """
    df = df.copy()
    df["BARCODE_DATE"] = pd.to_datetime(df["BARCODE_DATE"], errors="coerce")
    df["AMOUNT"]       = pd.to_numeric(df["AMOUNT"], errors="coerce").fillna(0)
    df["OB"]           = pd.to_numeric(df["OB"], errors="coerce").fillna(0)

    ref_date = df["BARCODE_DATE"].max()
    if pd.isnull(ref_date):
        ref_date = pd.Timestamp(TODAY)

    def _agg(grp):
        last_touch = grp["BARCODE_DATE"].max()
        days_since = (ref_date - last_touch).days if pd.notna(last_touch) else 999
        ptp_mask   = grp["STATUS"].str.contains("PTP",                   case=False, na=False)
        pay_mask   = grp["STATUS"].str.contains("PAYMENT|PAID|COLL",     case=False, na=False)
        kept_mask  = grp["STATUS"].str.contains("PAYMENT|PAID|COLL|KEPT",case=False, na=False)
        return pd.Series({
            "total_efforts":       len(grp),
            "ptp_count":           int(ptp_mask.sum()),
            "payment_count":       int(pay_mask.sum()),
            "total_ptp_amount":    grp.loc[ptp_mask, "AMOUNT"].sum(),
            "total_pay_amount":    grp.loc[pay_mask, "AMOUNT"].sum(),
            "kept_count":          int(kept_mask.sum()),
            "days_since_last_touch": days_since,
            "ob":                  grp["OB"].max(),
            "latest_status":       grp.sort_values("BARCODE_DATE").iloc[-1]["STATUS"] if len(grp) else "",
            "latest_agent":        grp.sort_values("BARCODE_DATE").iloc[-1]["AGENT"]  if "AGENT" in grp.columns else "",
            "placement":           grp["PLACEMENT"].mode().iloc[0] if "PLACEMENT" in grp.columns and grp["PLACEMENT"].notna().any() else "",
            "ptp_kept_ratio":      float(kept_mask.sum()) / max(ptp_mask.sum(), 1),
            "contact_freq":        len(grp) / max(days_since, 1),
        })

    features = df.groupby("ACCT_NO").apply(_agg).reset_index()
    return features


def _encode_features(feat_df: pd.DataFrame) -> tuple:
    """Return numeric feature matrix X and column list."""
    num_cols = [
        "total_efforts", "ptp_count", "payment_count",
        "total_ptp_amount", "total_pay_amount", "kept_count",
        "days_since_last_touch", "ob", "ptp_kept_ratio", "contact_freq",
    ]
    X = feat_df[num_cols].fillna(0).values
    return X, num_cols


def train_all_models(df: pd.DataFrame) -> dict:
    """Train PTP kept model and Payment likelihood model. Save to disk."""
    _ensure_model_dir()
    results = {}
    features = _build_account_features(df)

    # ── PTP Kept Model ────────────────────────────────────────────────────
    ptp_feat = features[features["ptp_count"] > 0].copy()
    ptp_feat["label"] = (ptp_feat["payment_count"] > 0).astype(int)
    X, _ = _encode_features(ptp_feat)
    y    = ptp_feat["label"].values

    if len(ptp_feat) >= 20 and len(np.unique(y)) > 1:
        X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=42)
        clf = RandomForestClassifier(n_estimators=100, random_state=42, class_weight="balanced")
        clf.fit(X_tr, y_tr)
        acc = round(accuracy_score(y_te, clf.predict(X_te)) * 100, 1)
        joblib.dump(clf, os.path.join(ML_MODEL_DIR, "ptp_model.pkl"))
        results["ptp"] = {"status": "trained", "samples": len(ptp_feat), "accuracy": acc}
    else:
        results["ptp"] = {"status": "insufficient", "available": len(ptp_feat)}

    # ── Payment Likelihood Model ──────────────────────────────────────────
    pay_feat = features.copy()
    pay_feat["label"] = (pay_feat["payment_count"] > 0).astype(int)
    X2, _ = _encode_features(pay_feat)
    y2    = pay_feat["label"].values

    if len(pay_feat) >= 20 and len(np.unique(y2)) > 1:
        X_tr2, X_te2, y_tr2, y_te2 = train_test_split(X2, y2, test_size=0.2, random_state=42)
        clf2 = GradientBoostingClassifier(n_estimators=100, random_state=42)
        clf2.fit(X_tr2, y_tr2)
        acc2 = round(accuracy_score(y_te2, clf2.predict(X_te2)) * 100, 1)
        joblib.dump(clf2, os.path.join(ML_MODEL_DIR, "payment_model.pkl"))
        results["payment"] = {"status": "trained", "samples": len(pay_feat), "accuracy": acc2}
    else:
        results["payment"] = {"status": "insufficient", "available": len(pay_feat)}

    return results


def predict_ptp_kept(df: pd.DataFrame) -> pd.DataFrame:
    """Predict whether PTP accounts will be kept. Returns per-account dataframe."""
    features = _build_account_features(df)
    ptp_feat = features[features["ptp_count"] > 0].copy()

    if ptp_feat.empty:
        return pd.DataFrame()

    X, _ = _encode_features(ptp_feat)
    model_path = os.path.join(ML_MODEL_DIR, "ptp_model.pkl")

    if os.path.exists(model_path):
        clf  = joblib.load(model_path)
        prob = clf.predict_proba(X)[:, 1] * 100
    else:
        # Fallback: rule-based heuristic score
        prob = np.clip(
            (ptp_feat["ptp_kept_ratio"] * 60) +
            (np.clip(1 - ptp_feat["days_since_last_touch"] / 30, 0, 1) * 20) +
            (np.clip(ptp_feat["payment_count"] / 3, 0, 1) * 20),
            0, 100
        ).values

    ptp_feat = ptp_feat.copy()
    ptp_feat["ptp_kept_prob"]  = np.round(prob, 1)
    ptp_feat["ptp_kept_label"] = pd.cut(
        ptp_feat["ptp_kept_prob"],
        bins=[-1, 35, 65, 101],
        labels=["🔴 Likely Broken", "🟡 Uncertain", "🟢 Likely Kept"]
    )
    return ptp_feat.sort_values("ptp_kept_prob", ascending=False)


def predict_payment_likelihood(df: pd.DataFrame) -> pd.DataFrame:
    """Predict payment likelihood for all accounts."""
    features = _build_account_features(df)
    if features.empty:
        return pd.DataFrame()

    X, _ = _encode_features(features)
    model_path = os.path.join(ML_MODEL_DIR, "payment_model.pkl")

    if os.path.exists(model_path):
        clf  = joblib.load(model_path)
        prob = clf.predict_proba(X)[:, 1] * 100
    else:
        # Fallback heuristic
        ob_norm     = np.clip(features["ob"] / features["ob"].max().clip(lower=1), 0, 1)
        recency     = np.clip(1 - features["days_since_last_touch"] / 60, 0, 1)
        ptp_signal  = np.clip(features["ptp_count"] / 5, 0, 1)
        pay_history = np.clip(features["payment_count"] / 3, 0, 1)
        prob = np.clip(
            (ob_norm * 20) + (recency * 30) + (ptp_signal * 25) + (pay_history * 25),
            0, 100
        ).values

    features = features.copy()
    features["pay_prob"]  = np.round(prob, 1)
    features["pay_label"] = pd.cut(
        features["pay_prob"],
        bins=[-1, 35, 65, 101],
        labels=["🔴 Low", "🟡 Medium", "🟢 High"]
    )
    return features.sort_values("pay_prob", ascending=False)


def best_contact_analysis(df: pd.DataFrame) -> dict:
    """Analyze which day of week yields best PTP and payment rates."""
    df = df.copy()
    df["BARCODE_DATE"] = pd.to_datetime(df["BARCODE_DATE"], errors="coerce")
    df = df.dropna(subset=["BARCODE_DATE"])
    df["DOW"]       = df["BARCODE_DATE"].dt.day_name()
    df["is_ptp"]    = is_ptp(df["STATUS"]).astype(int)
    df["is_payment"]= is_payment(df["STATUS"]).astype(int)

    order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    stats = []
    for day in order:
        grp = df[df["DOW"] == day]
        if grp.empty:
            continue
        total    = len(grp)
        ptp_rate = grp["is_ptp"].mean()    * 100
        pay_rate = grp["is_payment"].mean()* 100
        stats.append({
            "Day":      day,
            "Efforts":  total,
            "PTP Rate": round(ptp_rate, 1),
            "Pay Rate": round(pay_rate, 1),
            "Score":    round((ptp_rate * 0.5) + (pay_rate * 0.5), 1),
        })

    dow_df   = pd.DataFrame(stats)
    best_day = dow_df.loc[dow_df["Score"].idxmax(), "Day"] if not dow_df.empty else "N/A"
    return {"dow_stats": dow_df, "best_day": best_day}


def compute_risk_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score every account 0-100.
    High score = high priority (large OB, recent contact, active PTP, no payments).
    """
    features = _build_account_features(df)
    if features.empty:
        return pd.DataFrame()

    f = features.copy()
    f["ob"]               = pd.to_numeric(f["ob"], errors="coerce").fillna(0)
    f["days_since_last_touch"] = pd.to_numeric(f["days_since_last_touch"], errors="coerce").fillna(999)

    ob_max    = f["ob"].max() or 1
    day_score = np.clip(1 - f["days_since_last_touch"] / 90, 0, 1)   # recent = higher score
    ob_score  = np.clip(f["ob"] / ob_max, 0, 1)
    ptp_score = np.clip(f["ptp_count"] / 5, 0, 1)
    no_pay    = np.clip(1 - f["payment_count"] / 5, 0, 1)             # no payments = higher risk

    risk = (ob_score * 35) + (day_score * 25) + (ptp_score * 20) + (no_pay * 20)
    f["risk_score"] = np.round(np.clip(risk * 100, 0, 100), 1)
    f["risk_label"] = pd.cut(
        f["risk_score"],
        bins=[-1, 35, 65, 101],
        labels=["🔵 Low Priority", "🟡 Medium Priority", "🔴 High Priority"]
    )
    return f.sort_values("risk_score", ascending=False)


# ─────────────────────────────────────────────────────────────────────────────
# ML TAB — MACHINE LEARNING INSIGHTS
# ─────────────────────────────────────────────────────────────────────────────

def render_ml_tab(df_recovery: pd.DataFrame):
    st.markdown("## 🤖 Machine Learning Insights")
    st.caption(
        "Models are trained on your actual historical data from the database. "
        "The more data available, the more accurate the predictions."
    )

    if df_recovery.empty:
        st.warning("No recovery data available to run ML models.")
        return

    # ── Train / retrain controls ──────────────────────────────────────────
    col_btn, col_status = st.columns([2, 3])
    with col_btn:
        if st.button("🔄 Train / Retrain All Models", type="primary", key="ml_train_btn"):
            with st.spinner("Training models on your data..."):
                results = train_all_models(df_recovery)
            for name, info in results.items():
                if info.get("status") == "trained":
                    st.success(
                        f"✅ **{name.upper()} model** trained — "
                        f"{info['samples']:,} samples, "
                        f"{info['accuracy']}% accuracy"
                    )
                else:
                    st.warning(
                        f"⚠️ **{name.upper()} model** — insufficient data "
                        f"({info.get('available', '?')} rows, need 20+)"
                    )

    with col_status:
        import os
        model_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "models")
        ptp_exists = os.path.exists(os.path.join(model_dir, "ptp_model.pkl"))
        pay_exists = os.path.exists(os.path.join(model_dir, "payment_model.pkl"))
        st.markdown(
            f"**Model status:** &nbsp;"
            f"{'✅' if ptp_exists else '⚪'} PTP &nbsp;|&nbsp; "
            f"{'✅' if pay_exists else '⚪'} Payment &nbsp;|&nbsp; "
            f"✅ Contact Time &nbsp;|&nbsp; ✅ Risk Score"
        )
        if not ptp_exists and not pay_exists:
            st.info("👆 Click **Train All Models** first to generate predictions.")

    st.markdown("---")

    # ── Four ML tabs ──────────────────────────────────────────────────────
    t1, t2, t3, t4 = st.tabs([
        "💳 PTP Kept Predictor",
        "💰 Payment Likelihood",
        "📅 Best Contact Time",
        "🎯 Account Risk Score",
    ])

    # ── Tab 1: PTP Kept ───────────────────────────────────────────────────
    with t1:
        st.markdown("### 💳 PTP Kept Predictor")
        st.caption("Predicts whether a client who made a PTP is likely to honor it.")
        try:
            with st.spinner("Running PTP predictions..."):
                ptp_pred = predict_ptp_kept(df_recovery)

            # Summary KPIs
            if "ptp_kept_label" in ptp_pred.columns:
                counts = ptp_pred["ptp_kept_label"].value_counts()
                k1, k2, k3 = st.columns(3)
                k1.metric("🟢 Likely Kept",   int(counts.get("🟢 Likely Kept",   0)))
                k2.metric("🟡 Uncertain",      int(counts.get("🟡 Uncertain",      0)))
                k3.metric("🔴 Likely Broken",  int(counts.get("🔴 Likely Broken",  0)))

            c1, c2 = st.columns(2)
            with c1:
                # Pie
                if "ptp_kept_label" in ptp_pred.columns:
                    pie = ptp_pred["ptp_kept_label"].value_counts().reset_index()
                    pie.columns = ["Label","Count"]
                    fig = px.pie(pie, names="Label", values="Count", hole=0.4,
                                 color="Label",
                                 color_discrete_map={
                                     "🟢 Likely Kept":  "#2ca02c",
                                     "🟡 Uncertain":     "#ff7f0e",
                                     "🔴 Likely Broken": "#d62728",
                                 },
                                 title="PTP Outcome Distribution")
                    st.plotly_chart(fig, use_container_width=True)

            with c2:
                # Histogram of probabilities
                if "ptp_kept_prob" in ptp_pred.columns:
                    fig = px.histogram(ptp_pred, x="ptp_kept_prob", nbins=20,
                                       title="PTP Kept Probability Distribution",
                                       color_discrete_sequence=["#1f77b4"],
                                       labels={"ptp_kept_prob": "Probability (%)"})
                    fig.update_layout(height=340)
                    st.plotly_chart(fig, use_container_width=True)

            # Filter by label
            filter_ptp = st.radio(
                "Show", ["All", "🟢 Likely Kept", "🟡 Uncertain", "🔴 Likely Broken"],
                horizontal=True, key="ml_ptp_filter"
            )
            disp = ptp_pred if filter_ptp == "All" else ptp_pred[ptp_pred["ptp_kept_label"] == filter_ptp]
            disp = disp.rename(columns={
                "ACCT_NO": "Account", "ptp_kept_prob": "Kept Prob (%)",
                "ptp_kept_label": "Prediction", "latest_agent": "Agent",
                "placement": "Placement", "latest_status": "Last Status",
                "ob": "OB (₱)", "ptp_count": "PTP Count",
                "payment_count": "Payments", "days_since_last_touch": "Days Since Touch",
            })
            st.dataframe(disp, use_container_width=True, hide_index=True,
                         column_config={"OB (₱)": st.column_config.NumberColumn(format="₱%.2f"),
                                        "Kept Prob (%)": st.column_config.ProgressColumn(
                                            format="%.1f%%", min_value=0, max_value=100)})
            st.download_button("⬇ Download PTP Predictions",
                               disp.to_csv(index=False).encode(),
                               file_name=f"ptp_predictions_{TODAY_STR}.csv",
                               key="dl_ptp_pred")
        except Exception as e:
            st.error(f"PTP model error: {e}")
            st.info("Try clicking **Train / Retrain All Models** above.")

    # ── Tab 2: Payment Likelihood ─────────────────────────────────────────
    with t2:
        st.markdown("### 💰 Payment Likelihood")
        st.caption("Predicts which accounts are most likely to make a payment this month.")
        try:
            with st.spinner("Running payment predictions..."):
                pay_pred = predict_payment_likelihood(df_recovery)

            if "pay_label" in pay_pred.columns:
                counts = pay_pred["pay_label"].value_counts()
                k1, k2, k3 = st.columns(3)
                k1.metric("🟢 High Likelihood",   int(counts.get("🟢 High",   0)))
                k2.metric("🟡 Medium Likelihood",  int(counts.get("🟡 Medium", 0)))
                k3.metric("🔴 Low Likelihood",     int(counts.get("🔴 Low",    0)))

            c1, c2 = st.columns(2)
            with c1:
                if "pay_label" in pay_pred.columns:
                    pie = pay_pred["pay_label"].value_counts().reset_index()
                    pie.columns = ["Label","Count"]
                    fig = px.pie(pie, names="Label", values="Count", hole=0.4,
                                 color="Label",
                                 color_discrete_map={
                                     "🟢 High":   "#2ca02c",
                                     "🟡 Medium": "#ff7f0e",
                                     "🔴 Low":    "#d62728",
                                 },
                                 title="Payment Likelihood Distribution")
                    st.plotly_chart(fig, use_container_width=True)

            with c2:
                # Top 10 high-probability accounts bar
                top10 = pay_pred.nlargest(10, "pay_prob")
                fig = px.bar(top10, x="pay_prob", y="ACCT_NO", orientation="h",
                             text=top10["pay_prob"].apply(lambda x: f"{x:.1f}%"),
                             color="pay_prob", color_continuous_scale="Greens",
                             title="Top 10 Accounts by Payment Probability",
                             labels={"pay_prob": "Probability (%)", "ACCT_NO": "Account"})
                fig.update_traces(textposition="outside")
                fig.update_layout(coloraxis_showscale=False, height=360)
                st.plotly_chart(fig, use_container_width=True)

            filter_pay = st.radio(
                "Show", ["All", "🟢 High", "🟡 Medium", "🔴 Low"],
                horizontal=True, key="ml_pay_filter"
            )
            disp = pay_pred if filter_pay == "All" else pay_pred[pay_pred["pay_label"] == filter_pay]
            disp = disp.rename(columns={
                "ACCT_NO": "Account", "pay_prob": "Pay Prob (%)",
                "pay_label": "Likelihood", "latest_agent": "Agent",
                "placement": "Placement", "ob": "OB (₱)",
                "ptp_count": "PTP Count", "payment_count": "Payments",
                "days_since_last_touch": "Days Since Touch",
            })
            st.dataframe(disp, use_container_width=True, hide_index=True,
                         column_config={"OB (₱)": st.column_config.NumberColumn(format="₱%.2f"),
                                        "Pay Prob (%)": st.column_config.ProgressColumn(
                                            format="%.1f%%", min_value=0, max_value=100)})
            st.download_button("⬇ Download Payment Predictions",
                               disp.to_csv(index=False).encode(),
                               file_name=f"payment_predictions_{TODAY_STR}.csv",
                               key="dl_pay_pred")
        except Exception as e:
            st.error(f"Payment model error: {e}")
            st.info("Try clicking **Train / Retrain All Models** above.")

    # ── Tab 3: Best Contact Time ──────────────────────────────────────────
    with t3:
        st.markdown("### 📅 Best Contact Time")
        st.caption(
            "Analyzes your historical effort data to find which day of the week "
            "produces the most PTPs and payments."
        )
        try:
            with st.spinner("Analyzing contact patterns..."):
                contact = best_contact_analysis(df_recovery)

            dow_df   = contact["dow_stats"]
            best_day = contact["best_day"]

            st.success(f"📅 **Best day to contact clients: {best_day}**")

            c1, c2 = st.columns(2)
            with c1:
                fig = px.bar(dow_df, x="Day", y=["PTP Rate","Pay Rate"],
                             barmode="group", title="PTP & Payment Rate by Day",
                             color_discrete_sequence=["#1f77b4","#2ca02c"],
                             labels={"value":"Rate (%)","variable":"Metric"})
                fig.update_layout(height=360)
                st.plotly_chart(fig, use_container_width=True)

            with c2:
                fig = px.bar(dow_df, x="Day", y="Score",
                             text=dow_df["Score"].apply(lambda x: f"{x:.1f}"),
                             color="Score", color_continuous_scale="RdYlGn",
                             title="Overall Contact Score by Day",
                             labels={"Score":"Combined Score"})
                fig.update_traces(textposition="outside")
                fig.update_layout(coloraxis_showscale=False, height=360)
                st.plotly_chart(fig, use_container_width=True)

            st.markdown("#### 📊 Detailed Day-of-Week Stats")
            st.dataframe(
                dow_df.sort_values("Score", ascending=False),
                use_container_width=True, hide_index=True,
                column_config={
                    "PTP Rate":  st.column_config.NumberColumn(format="%.1f%%"),
                    "Pay Rate":  st.column_config.NumberColumn(format="%.1f%%"),
                    "Score":     st.column_config.ProgressColumn(
                        format="%.1f", min_value=0,
                        max_value=float(dow_df["Score"].max()) if not dow_df.empty else 100
                    ),
                }
            )
        except Exception as e:
            st.error(f"Contact time analysis error: {e}")

    # ── Tab 4: Account Risk Score ─────────────────────────────────────────
    with t4:
        st.markdown("### 🎯 Account Risk Score")
        st.caption(
            "Scores every account from 0–100 based on OB, PTP history, recency, "
            "and payment behavior. Higher score = higher priority to contact."
        )
        try:
            with st.spinner("Computing risk scores..."):
                risk = compute_risk_scores(df_recovery)

            if "risk_label" in risk.columns:
                counts = risk["risk_label"].value_counts()
                k1, k2, k3 = st.columns(3)
                k1.metric("🔴 High Priority",   int(counts.get("🔴 High Priority",   0)))
                k2.metric("🟡 Medium Priority",  int(counts.get("🟡 Medium Priority",  0)))
                k3.metric("🔵 Low Priority",     int(counts.get("🔵 Low Priority",     0)))

            c1, c2 = st.columns(2)
            with c1:
                pie = risk["risk_label"].value_counts().reset_index()
                pie.columns = ["Label","Count"]
                fig = px.pie(pie, names="Label", values="Count", hole=0.4,
                             color="Label",
                             color_discrete_map={
                                 "🔴 High Priority":   "#d62728",
                                 "🟡 Medium Priority":  "#ff7f0e",
                                 "🔵 Low Priority":     "#1f77b4",
                             },
                             title="Risk Distribution")
                st.plotly_chart(fig, use_container_width=True)

            with c2:
                # Top 10 highest risk
                top10r = risk.head(10)
                fig = px.bar(top10r, x="risk_score", y="ACCT_NO", orientation="h",
                             text=top10r["risk_score"].apply(lambda x: f"{x:.1f}"),
                             color="risk_score", color_continuous_scale="RdYlGn_r",
                             title="Top 10 Highest Priority Accounts",
                             labels={"risk_score":"Risk Score","ACCT_NO":"Account"})
                fig.update_traces(textposition="outside")
                fig.update_layout(coloraxis_showscale=False, height=360)
                st.plotly_chart(fig, use_container_width=True)

            # Risk score by placement
            if "placement" in risk.columns:
                st.markdown("**Average Risk Score by Placement**")
                rp = risk.groupby("placement")["risk_score"].mean().reset_index()
                rp.columns = ["Placement","Avg Risk Score"]
                rp = rp.sort_values("Avg Risk Score", ascending=False)
                fig = px.bar(rp, x="Placement", y="Avg Risk Score",
                             text=rp["Avg Risk Score"].apply(lambda x: f"{x:.1f}"),
                             color="Avg Risk Score", color_continuous_scale="RdYlGn_r")
                fig.update_traces(textposition="outside")
                fig.update_layout(coloraxis_showscale=False, height=320)
                st.plotly_chart(fig, use_container_width=True)

            filter_risk = st.radio(
                "Show",
                ["All", "🔴 High Priority", "🟡 Medium Priority", "🔵 Low Priority"],
                horizontal=True, key="ml_risk_filter"
            )
            disp = risk if filter_risk == "All" else risk[risk["risk_label"] == filter_risk]
            disp = disp.rename(columns={
                "ACCT_NO": "Account", "risk_score": "Risk Score",
                "risk_label": "Priority", "latest_agent": "Agent",
                "placement": "Placement", "ob": "OB (₱)",
                "ptp_count": "PTP Count", "payment_count": "Payments",
                "days_since_last_touch": "Days Since Touch",
                "total_efforts": "Total Efforts", "latest_status": "Last Status",
            })
            st.dataframe(disp, use_container_width=True, hide_index=True,
                         column_config={
                             "OB (₱)":   st.column_config.NumberColumn(format="₱%.2f"),
                             "Risk Score": st.column_config.ProgressColumn(
                                 format="%.1f", min_value=0, max_value=100),
                         })
            st.download_button("⬇ Download Risk Scores",
                               disp.to_csv(index=False).encode(),
                               file_name=f"risk_scores_{TODAY_STR}.csv",
                               key="dl_risk")
        except Exception as e:
            st.error(f"Risk scoring error: {e}")



# ─────────────────────────────────────────────────────────────────────────────
# EWB 150 DPD ── ML INSIGHTS
# ─────────────────────────────────────────────────────────────────────────────

def render_ml_tab_150(df_raw: pd.DataFrame, df_ptp: pd.DataFrame):
    """ML Insights for EWB 150 DPD — reuses recovery ML engines with adapted features."""
    st.markdown("## 🤖 Machine Learning Insights — EWB 150 DPD")
    st.caption("Models trained on 150 DPD account data. Click Train to refresh.")

    # Build a recovery-compatible dataframe from 150 DPD raw data
    df = df_raw.copy()
    # Map 150 DPD columns to recovery-compatible names
    col_map = {
        "leads_chcode": "ACCT_NO",
        "AgentCode":    "AGENT",
        "Status":       "STATUS",
        "Amount":       "AMOUNT",
        "OB":           "OB",
        "LastTouchDate":"BARCODE_DATE",
        "Substatus":    "SUB_STATUS",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    if "ACCT_NO" not in df.columns and "leads_chcode" in df_raw.columns:
        df["ACCT_NO"] = df_raw["leads_chcode"]
    if "PLACEMENT" not in df.columns:
        df["PLACEMENT"] = "EWB 150 DPD"

    col_btn, col_status = st.columns([2, 3])
    with col_btn:
        if st.button("🔄 Train / Retrain Models", type="primary", key="ml150_train"):
            with st.spinner("Training on 150 DPD data..."):
                results = train_all_models(df)
            for name, info in results.items():
                if info.get("status") == "trained":
                    st.success(f"✅ **{name.upper()}** — {info['samples']:,} samples, {info['accuracy']}% acc")
                else:
                    st.warning(f"⚠️ **{name.upper()}** — insufficient data ({info.get('available','?')} rows)")
    with col_status:
        model_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "models")
        st.markdown(
            f"Models: {'✅' if os.path.exists(os.path.join(model_dir,'ptp_model.pkl')) else '⚪'} PTP &nbsp;|&nbsp; "
            f"{'✅' if os.path.exists(os.path.join(model_dir,'payment_model.pkl')) else '⚪'} Payment"
        )

    st.markdown("---")
    t1, t2, t3, t4 = st.tabs([
        "💳 PTP Kept Predictor",
        "💰 Payment Likelihood",
        "📅 Best Contact Time",
        "🎯 Account Risk Score",
    ])

    with t1:
        st.markdown("### 💳 PTP Kept Predictor")
        # Use actual PTP data for 150 DPD
        if df_ptp is not None and not df_ptp.empty:
            ptp_df = df_ptp.copy()
            ptp_df = ptp_df.rename(columns={"CH_CODE":"ACCT_NO","AgentCode":"AGENT"} )
            if "PLACEMENT" not in ptp_df.columns:
                ptp_df["PLACEMENT"] = "EWB 150 DPD"
            try:
                pred = predict_ptp_kept(ptp_df)
                if not pred.empty and "ptp_kept_label" in pred.columns:
                    counts = pred["ptp_kept_label"].value_counts()
                    k1,k2,k3 = st.columns(3)
                    k1.metric("🟢 Likely Kept",  int(counts.get("🟢 Likely Kept",  0)))
                    k2.metric("🟡 Uncertain",     int(counts.get("🟡 Uncertain",     0)))
                    k3.metric("🔴 Likely Broken", int(counts.get("🔴 Likely Broken", 0)))
                    c1,c2 = st.columns(2)
                    with c1:
                        pie = pred["ptp_kept_label"].value_counts().reset_index()
                        pie.columns=["Label","Count"]
                        fig=px.pie(pie,names="Label",values="Count",hole=0.4,
                                   color="Label",color_discrete_map={
                                       "🟢 Likely Kept":"#2ca02c","🟡 Uncertain":"#ff7f0e","🔴 Likely Broken":"#d62728"},
                                   title="PTP Outcome Distribution")
                        st.plotly_chart(fig,use_container_width=True)
                    with c2:
                        if "ptp_kept_prob" in pred.columns:
                            fig=px.histogram(pred,x="ptp_kept_prob",nbins=20,
                                             title="PTP Kept Probability",
                                             color_discrete_sequence=["#1f77b4"])
                            st.plotly_chart(fig,use_container_width=True)
                    st.dataframe(pred[["ACCT_NO","ptp_kept_prob","ptp_kept_label","ptp_count","payment_count"]].rename(
                        columns={"ACCT_NO":"Account","ptp_kept_prob":"Kept Prob (%)","ptp_kept_label":"Prediction",
                                 "ptp_count":"PTP Count","payment_count":"Payments"}),
                        use_container_width=True, hide_index=True,
                        column_config={"Kept Prob (%)": st.column_config.ProgressColumn(format="%.1f%%",min_value=0,max_value=100)})
                else:
                    st.info("Not enough PTP data for predictions.")
            except Exception as e:
                st.error(f"PTP model error: {e}")
        else:
            st.info("No PTP data available.")

    with t2:
        st.markdown("### 💰 Payment Likelihood")
        try:
            pred = predict_payment_likelihood(df)
            if not pred.empty and "pay_label" in pred.columns:
                counts = pred["pay_label"].value_counts()
                k1,k2,k3 = st.columns(3)
                k1.metric("🟢 High",   int(counts.get("🟢 High",  0)))
                k2.metric("🟡 Medium", int(counts.get("🟡 Medium",0)))
                k3.metric("🔴 Low",    int(counts.get("🔴 Low",   0)))
                c1,c2 = st.columns(2)
                with c1:
                    pie=pred["pay_label"].value_counts().reset_index()
                    pie.columns=["Label","Count"]
                    fig=px.pie(pie,names="Label",values="Count",hole=0.4,
                               color="Label",color_discrete_map={"🟢 High":"#2ca02c","🟡 Medium":"#ff7f0e","🔴 Low":"#d62728"},
                               title="Payment Likelihood")
                    st.plotly_chart(fig,use_container_width=True)
                with c2:
                    top10=pred.nlargest(10,"pay_prob")
                    fig=px.bar(top10,x="pay_prob",y="ACCT_NO",orientation="h",
                               text=top10["pay_prob"].apply(lambda x:f"{x:.1f}%"),
                               color="pay_prob",color_continuous_scale="Greens",
                               title="Top 10 Accounts")
                    fig.update_layout(coloraxis_showscale=False,height=360,yaxis_title="")
                    st.plotly_chart(fig,use_container_width=True)
                st.dataframe(pred[["ACCT_NO","pay_prob","pay_label","ptp_count","payment_count","ob"]].rename(
                    columns={"ACCT_NO":"Account","pay_prob":"Pay Prob (%)","pay_label":"Likelihood",
                             "ptp_count":"PTP Count","payment_count":"Payments","ob":"OB (₱)"}),
                    use_container_width=True,hide_index=True,
                    column_config={"Pay Prob (%)":st.column_config.ProgressColumn(format="%.1f%%",min_value=0,max_value=100),
                                   "OB (₱)":st.column_config.NumberColumn(format="₱%.2f")})
        except Exception as e:
            st.error(f"Payment model error: {e}")

    with t3:
        st.markdown("### 📅 Best Contact Time")
        try:
            contact = best_contact_analysis(df)
            dow_df  = contact["dow_stats"]
            st.success(f"📅 **Best day to contact: {contact['best_day']}**")
            c1,c2 = st.columns(2)
            with c1:
                fig=px.bar(dow_df,x="Day",y=["PTP Rate","Pay Rate"],barmode="group",
                           title="PTP & Payment Rate by Day",
                           color_discrete_sequence=["#1f77b4","#2ca02c"])
                st.plotly_chart(fig,use_container_width=True)
            with c2:
                fig=px.bar(dow_df,x="Day",y="Score",text=dow_df["Score"].apply(lambda x:f"{x:.1f}"),
                           color="Score",color_continuous_scale="RdYlGn",title="Contact Score by Day")
                fig.update_traces(textposition="outside")
                fig.update_layout(coloraxis_showscale=False)
                st.plotly_chart(fig,use_container_width=True)
            st.dataframe(dow_df.sort_values("Score",ascending=False),use_container_width=True,hide_index=True)
        except Exception as e:
            st.error(f"Contact analysis error: {e}")

    with t4:
        st.markdown("### 🎯 Account Risk Score")
        try:
            risk = compute_risk_scores(df)
            if not risk.empty and "risk_label" in risk.columns:
                counts = risk["risk_label"].value_counts()
                k1,k2,k3=st.columns(3)
                k1.metric("🔴 High Priority",  int(counts.get("🔴 High Priority", 0)))
                k2.metric("🟡 Medium Priority", int(counts.get("🟡 Medium Priority",0)))
                k3.metric("🔵 Low Priority",    int(counts.get("🔵 Low Priority",  0)))
                c1,c2=st.columns(2)
                with c1:
                    pie=risk["risk_label"].value_counts().reset_index()
                    pie.columns=["Label","Count"]
                    fig=px.pie(pie,names="Label",values="Count",hole=0.4,
                               color="Label",color_discrete_map={
                                   "🔴 High Priority":"#d62728","🟡 Medium Priority":"#ff7f0e","🔵 Low Priority":"#1f77b4"},
                               title="Risk Distribution")
                    st.plotly_chart(fig,use_container_width=True)
                with c2:
                    top10r=risk.head(10)
                    fig=px.bar(top10r,x="risk_score",y="ACCT_NO",orientation="h",
                               text=top10r["risk_score"].apply(lambda x:f"{x:.1f}"),
                               color="risk_score",color_continuous_scale="RdYlGn_r",
                               title="Top 10 High Priority")
                    fig.update_layout(coloraxis_showscale=False,height=360,yaxis_title="")
                    st.plotly_chart(fig,use_container_width=True)
                st.dataframe(risk[["ACCT_NO","risk_score","risk_label","ob","ptp_count","payment_count","days_since_last_touch"]].rename(
                    columns={"ACCT_NO":"Account","risk_score":"Risk Score","risk_label":"Priority",
                             "ob":"OB (₱)","ptp_count":"PTP Count","payment_count":"Payments","days_since_last_touch":"Days Since Touch"}),
                    use_container_width=True,hide_index=True,
                    column_config={"Risk Score":st.column_config.ProgressColumn(format="%.1f",min_value=0,max_value=100),
                                   "OB (₱)":st.column_config.NumberColumn(format="₱%.2f")})
                st.download_button("⬇ Download Risk Scores",
                                   risk.to_csv(index=False).encode(),
                                   file_name=f"risk_150dpd_{TODAY_STR}.csv",key="dl_risk150")
        except Exception as e:
            st.error(f"Risk scoring error: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# EWB 150 DPD ── ENDORSEMENT CONSOLIDATION (separate store from Recovery)
# ─────────────────────────────────────────────────────────────────────────────

ENDO_150_STORE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "endorsements_150.json")

def _save_endo_150(filename: str, df: pd.DataFrame):
    os.makedirs(os.path.dirname(ENDO_150_STORE), exist_ok=True)
    try:
        with open(ENDO_150_STORE, "r", encoding="utf-8") as f:
            batches = json.load(f)
    except Exception:
        batches = []
    batches = [b for b in batches if b["name"] != filename]
    batches.append({"name": filename, "uploaded": TODAY_STR,
                    "rows": len(df), "data": df.to_json(orient="records", date_format="iso")})
    with open(ENDO_150_STORE, "w", encoding="utf-8") as f:
        json.dump(batches, f, ensure_ascii=False)

def _load_endo_150_all() -> pd.DataFrame:
    if not os.path.exists(ENDO_150_STORE):
        return pd.DataFrame()
    try:
        with open(ENDO_150_STORE, "r", encoding="utf-8") as f:
            batches = json.load(f)
        if not batches:
            return pd.DataFrame()
        frames = []
        for b in batches:
            df = pd.read_json(b["data"], orient="records")
            df["_batch"] = b["name"]
            frames.append(df)
        return pd.concat(frames, ignore_index=True)
    except Exception:
        return pd.DataFrame()

def _delete_endo_150(name: str):
    try:
        with open(ENDO_150_STORE,"r",encoding="utf-8") as f:
            batches = json.load(f)
    except Exception:
        batches = []
    batches=[b for b in batches if b["name"]!=name]
    with open(ENDO_150_STORE,"w",encoding="utf-8") as f:
        json.dump(batches,f,ensure_ascii=False)

def render_endorsement_150():
    st.markdown("## 📥 150 DPD Endorsement Consolidation")
    st.caption("Upload 150 DPD endorsement files. Saved persistently to disk.")

    uploaded = st.file_uploader("Drop file (.xlsx, .xls, .csv)", type=["xlsx","xls","csv"], key="endo150_upload")
    if uploaded is not None:
        try:
            df_new = pd.read_csv(uploaded) if uploaded.name.endswith(".csv") else pd.read_excel(uploaded)
            df_new = _normalize_endo(df_new)
            if not df_new.empty:
                _save_endo_150(uploaded.name, df_new)
                st.success(f"✅ **{uploaded.name}** saved — {len(df_new):,} rows")
        except Exception as e:
            st.error(f"Could not read file: {e}")

    try:
        with open(ENDO_150_STORE,"r",encoding="utf-8") as f:
            batches=json.load(f)
    except Exception:
        batches=[]

    if not batches:
        st.info("📂 No files saved yet.")
        return

    st.markdown(f"#### 📂 Saved Batches ({len(batches)} file(s))")
    for b in batches:
        bc1,bc2=st.columns([5,1])
        with bc1:
            st.markdown(f"📄 **{b['name']}** — {b['rows']:,} rows — {b['uploaded']}")
        with bc2:
            if st.button("🗑",key=f"del_e150_{b['name']}"):
                _delete_endo_150(b["name"])
                st.rerun()

    st.markdown("---")
    tab_all, tab_batch = st.tabs(["📊 Consolidated View","🗂 View by Batch"])

    with tab_all:
        df_all = _load_endo_150_all()
        _render_endo_charts(df_all,"(All Batches)")
        with st.expander("📋 Full Table",expanded=False):
            st.dataframe(df_all,use_container_width=True,hide_index=True)
            st.download_button("⬇ Download CSV",df_all.to_csv(index=False).encode(),
                               file_name=f"endo150_consolidated_{TODAY_STR}.csv",
                               mime="text/csv",key="dl_endo150_all")

    with tab_batch:
        sel=st.selectbox("Select batch",[b["name"] for b in batches],key="endo150_sel")
        if sel:
            b_data=next(b for b in batches if b["name"]==sel)
            df_b=_normalize_endo(pd.read_json(b_data["data"],orient="records"))
            st.markdown(f"#### {sel} — {len(df_b):,} rows")
            _render_endo_charts(df_b,f"({sel})")


# ─────────────────────────────────────────────────────────────────────────────
# AREA BREAK — SHARED (used by both Recovery and 150 DPD)
# ─────────────────────────────────────────────────────────────────────────────

AREA_STORE_TEMPLATE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "area_data_{key}.json"
)

def _area_store(key: str) -> str:
    return AREA_STORE_TEMPLATE.format(key=key)

def _save_area_data(key: str, filename: str, df: pd.DataFrame):
    path = _area_store(key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "r", encoding="utf-8") as f:
            store = json.load(f)
    except Exception:
        store = {"accounts": [], "uncovered_areas": []}
    store["accounts"] = json.loads(df.to_json(orient="records", date_format="iso"))
    store["filename"]  = filename
    store["uploaded"]  = TODAY_STR
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False)

def _save_uncovered_areas(key: str, areas: list):
    path = _area_store(key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "r", encoding="utf-8") as f:
            store = json.load(f)
    except Exception:
        store = {"accounts": [], "uncovered_areas": []}
    store["uncovered_areas"] = areas
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False)

def _load_area_data(key: str) -> dict:
    path = _area_store(key)
    if not os.path.exists(path):
        return {"accounts": [], "uncovered_areas": [], "filename": None, "uploaded": None}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"accounts": [], "uncovered_areas": [], "filename": None, "uploaded": None}

def _extract_municipality(address: str) -> str:
    """
    Extracts municipality from an address string.
    Looks for known Philippine municipality/city keywords.
    Falls back to last meaningful comma-separated segment.
    """
    if not isinstance(address, str) or not address.strip():
        return "UNKNOWN"
    addr_upper = address.upper()

    # Common PH municipality/city patterns
    import re
    # Try to find "city of X", "municipality of X", or "X city/municipality"
    patterns = [
        r"(?:CITY OF|MUNICIPALITY OF)[\s]+([A-Z\s]+?)(?:,|$)",
        r"([A-Z\s]+)[\s]+(?:CITY|MUNICIPALITY|TOWN)(?:,|$|[\s])",
    ]
    for pat in patterns:
        m = re.search(pat, addr_upper)
        if m:
            return m.group(1).strip().title()

    # Fallback: second-to-last or last comma segment
    parts = [p.strip() for p in address.split(",") if p.strip()]
    if len(parts) >= 2:
        return parts[-2].strip().title()
    elif parts:
        return parts[-1].strip().title()
    return "UNKNOWN"

def _extract_final_area(address: str) -> str:
    """
    Extract the most specific area (barangay / subdivision / street level).
    Returns the first comma-separated segment.
    """
    if not isinstance(address, str) or not address.strip():
        return "UNKNOWN"
    parts = [p.strip() for p in address.split(",") if p.strip()]
    return parts[0].title() if parts else "UNKNOWN"


def render_area_break(portfolio_key: str):
    """
    Area Break tab — shared between Recovery and 150 DPD.
    portfolio_key: 'recovery' or 'dpd'
    """
    store_label = "EWB Recovery" if portfolio_key == "recovery" else "EWB 150 DPD"
    st.markdown(f"## 📍 Area Break — {store_label}")
    st.caption(
        "Upload a file with **Account No, Name, Address** columns. "
        "The system will extract Municipality and Final Area, then flag accounts "
        "in areas NOT covered by field visitation."
    )

    # ── Upload account list ───────────────────────────────────────────────
    st.markdown("#### 1️⃣ Upload Account List")
    up_acct = st.file_uploader(
        "Drop file with Account No, Name, Address (.xlsx/.xls/.csv)",
        type=["xlsx","xls","csv"],
        key=f"area_acct_{portfolio_key}",
    )
    if up_acct is not None:
        try:
            df_acct = pd.read_csv(up_acct) if up_acct.name.endswith(".csv") else pd.read_excel(up_acct)
            # Normalize columns
            df_acct.columns = [c.strip().upper().replace(" ","_") for c in df_acct.columns]
            for a,b in [("ACCTNO","ACCT_NO"),("ACCOUNT_NO","ACCT_NO"),("ACCOUNT","ACCT_NO"),
                        ("CHCODE","CH_CODE"),("CHNAME","CH_NAME"),("NAME","CH_NAME"),
                        ("ADDR","ADDRESS"),("FULL_ADDRESS","ADDRESS"),("CLIENT_ADDRESS","ADDRESS")]:
                if a in df_acct.columns and b not in df_acct.columns:
                    df_acct.rename(columns={a:b},inplace=True)

            addr_col = next((c for c in ["ADDRESS","FULL_ADDRESS","ADDR"] if c in df_acct.columns), None)
            if addr_col:
                df_acct["MUNICIPALITY"] = df_acct[addr_col].apply(_extract_municipality)
                df_acct["FINAL_AREA"]   = df_acct[addr_col].apply(_extract_final_area)
            else:
                st.warning("No ADDRESS column found. Add a column named ADDRESS, ADDR, or FULL_ADDRESS.")

            _save_area_data(portfolio_key, up_acct.name, df_acct)
            st.success(f"✅ **{up_acct.name}** saved — {len(df_acct):,} accounts, area break applied.")
        except Exception as e:
            st.error(f"Could not process file: {e}")

    # ── Upload uncovered areas list ───────────────────────────────────────
    st.markdown("#### 2️⃣ Upload / Enter Uncovered Areas")
    st.caption("Provide a list of municipalities or areas NOT reachable by field agents.")

    store_data = _load_area_data(portfolio_key)

    input_method = st.radio(
        "How to provide uncovered areas?",
        ["Type manually", "Upload text/CSV file"],
        horizontal=True,
        key=f"area_input_method_{portfolio_key}",
    )

    current_uncovered = store_data.get("uncovered_areas", [])

    if input_method == "Type manually":
        uncovered_text = st.text_area(
            "Enter one area per line (municipality or barangay name)",
            value="\n".join(current_uncovered),
            height=120,
            key=f"area_uncovered_text_{portfolio_key}",
            placeholder="e.g.\nMandaluyong\nSan Juan\nCaloocan",
        )
        if st.button("💾 Save Uncovered Areas", key=f"save_uncov_{portfolio_key}"):
            areas = [a.strip().title() for a in uncovered_text.splitlines() if a.strip()]
            _save_uncovered_areas(portfolio_key, areas)
            st.success(f"✅ Saved {len(areas)} uncovered area(s).")
            st.rerun()
    else:
        up_areas = st.file_uploader(
            "Drop text/CSV file (one area per row)",
            type=["txt","csv"],
            key=f"area_file_{portfolio_key}",
        )
        if up_areas is not None:
            try:
                raw = up_areas.read().decode("utf-8", errors="ignore")
                areas = [a.strip().title() for a in raw.splitlines() if a.strip() and not a.startswith(",")]
                _save_uncovered_areas(portfolio_key, areas)
                st.success(f"✅ Loaded {len(areas)} uncovered area(s) from file.")
                st.rerun()
            except Exception as e:
                st.error(f"Could not read file: {e}")

    # Reload after potential save
    store_data    = _load_area_data(portfolio_key)
    uncovered_set = set(a.title() for a in store_data.get("uncovered_areas", []))

    if uncovered_set:
        st.info(f"📋 **{len(uncovered_set)} uncovered area(s):** {', '.join(sorted(uncovered_set))}")

    # ── Area Break Report ─────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 📊 Area Break Report")

    acct_records = store_data.get("accounts", [])
    if not acct_records:
        st.info("Upload an account list above to generate the area break report.")
        return

    df_area = pd.DataFrame(acct_records)
    if "MUNICIPALITY" not in df_area.columns:
        st.warning("No MUNICIPALITY column — re-upload the account file with an ADDRESS column.")
        return

    # Flag out-of-area accounts
    if uncovered_set:
        df_area["COVERAGE"] = df_area["MUNICIPALITY"].apply(
            lambda x: "❌ Out of Area" if str(x).title() in uncovered_set else "✅ Covered"
        )
    else:
        df_area["COVERAGE"] = "✅ Covered"

    # KPIs
    total       = len(df_area)
    covered     = int((df_area["COVERAGE"] == "✅ Covered").sum())
    out_of_area = int((df_area["COVERAGE"] == "❌ Out of Area").sum())
    municipalities = df_area["MUNICIPALITY"].nunique()

    k1,k2,k3,k4 = st.columns(4)
    k1.metric("🏦 Total Accounts",   f"{total:,}")
    k2.metric("✅ Covered",          f"{covered:,}")
    k3.metric("❌ Out of Area",      f"{out_of_area:,}")
    k4.metric("🗺️ Municipalities",   f"{municipalities:,}")

    # Charts
    c1,c2 = st.columns(2)
    with c1:
        # Coverage pie
        pie_data = pd.DataFrame({
            "Status":["✅ Covered","❌ Out of Area"],
            "Count": [covered, out_of_area],
        })
        fig = px.pie(pie_data, names="Status", values="Count", hole=0.45,
                     color="Status",
                     color_discrete_map={"✅ Covered":"#2ca02c","❌ Out of Area":"#d62728"},
                     title="Coverage Distribution")
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        # Top municipalities bar
        muni_counts = (
            df_area.groupby(["MUNICIPALITY","COVERAGE"])
            .size().reset_index(name="Accounts")
            .sort_values("Accounts", ascending=False)
            .head(15)
        )
        fig = px.bar(muni_counts, x="MUNICIPALITY", y="Accounts",
                     color="COVERAGE", barmode="stack",
                     color_discrete_map={"✅ Covered":"#2ca02c","❌ Out of Area":"#d62728"},
                     title="Accounts per Municipality (Top 15)")
        fig.update_layout(xaxis_tickangle=-35, showlegend=True, height=360)
        st.plotly_chart(fig, use_container_width=True)

    # Final area breakdown
    if "FINAL_AREA" in df_area.columns:
        st.markdown("**📍 Final Area Breakdown**")
        area_counts = (
            df_area.groupby(["FINAL_AREA","MUNICIPALITY","COVERAGE"])
            .size().reset_index(name="Accounts")
            .sort_values("Accounts", ascending=False)
        )
        st.dataframe(area_counts, use_container_width=True, hide_index=True)

    # Filter view
    view_sel = st.radio(
        "View", ["All","✅ Covered","❌ Out of Area"],
        horizontal=True, key=f"area_view_{portfolio_key}",
    )
    disp = df_area if view_sel == "All" else df_area[df_area["COVERAGE"] == view_sel]
    st.dataframe(
        disp.drop(columns=["_batch"], errors="ignore"),
        use_container_width=True, hide_index=True,
    )
    st.download_button(
        "⬇ Download Area Break Report",
        disp.drop(columns=["_batch"], errors="ignore").to_csv(index=False).encode(),
        file_name=f"area_break_{portfolio_key}_{TODAY_STR}.csv",
        mime="text/csv",
        key=f"dl_area_{portfolio_key}",
    )



# ─────────────────────────────────────────────────────────────────────────────
# EWB 150 DPD ── PAYMENT UPLOAD + VISUALIZATION
# ─────────────────────────────────────────────────────────────────────────────

PAYMENT_STORE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "payment_upload.json"
)

def _save_payment_upload(filename: str, df: pd.DataFrame):
    os.makedirs(os.path.dirname(PAYMENT_STORE), exist_ok=True)
    try:
        with open(PAYMENT_STORE, "r", encoding="utf-8") as f:
            store = json.load(f)
    except Exception:
        store = []
    store = [b for b in store if b["name"] != filename]
    store.append({
        "name":     filename,
        "uploaded": TODAY_STR,
        "rows":     len(df),
        "data":     df.to_json(orient="records", date_format="iso"),
    })
    with open(PAYMENT_STORE, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False)

def _load_payment_uploads() -> pd.DataFrame:
    if not os.path.exists(PAYMENT_STORE):
        return pd.DataFrame()
    try:
        with open(PAYMENT_STORE, "r", encoding="utf-8") as f:
            store = json.load(f)
        if not store:
            return pd.DataFrame()
        frames = []
        for b in store:
            df = pd.read_json(b["data"], orient="records")
            df["_batch"] = b["name"]
            frames.append(df)
        return pd.concat(frames, ignore_index=True)
    except Exception:
        return pd.DataFrame()

def _delete_payment_batch(name: str):
    try:
        with open(PAYMENT_STORE, "r", encoding="utf-8") as f:
            store = json.load(f)
    except Exception:
        store = []
    store = [b for b in store if b["name"] != name]
    with open(PAYMENT_STORE, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False)


def _render_payment_tab(load_fn, save_fn, delete_fn, store_path, key_prefix="dpd", title="EWB 150 DPD"):
    """Shared payment upload + visualization logic for any portfolio."""
    st.markdown(f"## 💳 Payment Upload & Analysis — {title}")
    st.caption(
        "Upload a payment Excel/CSV file with **Account No, Touch Points, "
        "Source of Payment, Payment Amount** columns. "
        "Files are saved persistently."
    )

    # ── Upload ──────────────────────────────────────────────────────────
    uploaded = st.file_uploader(
        "Drop payment file here (.xlsx, .xls, .csv)",
        type=["xlsx", "xls", "csv"],
        key=f"payment_upload_file_{key_prefix}",
    )

    if uploaded is not None:
        try:
            df_new = (
                pd.read_csv(uploaded)
                if uploaded.name.endswith(".csv")
                else pd.read_excel(uploaded)
            )
            # Normalize columns
            df_new.columns = [c.strip().upper().replace(" ", "_") for c in df_new.columns]
            col_aliases = {
                "ACCTNO":           "ACCT_NO",
                "ACCOUNT_NO":       "ACCT_NO",
                "ACCOUNT":          "ACCT_NO",
                "CHCODE":           "ACCT_NO",
                "TOUCH_POINT":      "TOUCH_POINTS",
                "TOUCHPOINT":       "TOUCH_POINTS",
                "TOUCHPOINTS":      "TOUCH_POINTS",
                "SOURCE":           "SOURCE_OF_PAYMENT",
                "PAYMENT_SOURCE":   "SOURCE_OF_PAYMENT",
                "SOURCE_PAYMENT":   "SOURCE_OF_PAYMENT",
                "AMOUNT":           "PAYMENT_AMOUNT",
                "PAYMENT":          "PAYMENT_AMOUNT",
                "PAY_AMOUNT":       "PAYMENT_AMOUNT",
            }
            for old, new in col_aliases.items():
                if old in df_new.columns and new not in df_new.columns:
                    df_new.rename(columns={old: new}, inplace=True)

            if "PAYMENT_AMOUNT" in df_new.columns:
                df_new["PAYMENT_AMOUNT"] = pd.to_numeric(df_new["PAYMENT_AMOUNT"], errors="coerce").fillna(0)

            save_fn(uploaded.name, df_new)
            st.success(f"✅ **{uploaded.name}** saved — {len(df_new):,} rows")
        except Exception as e:
            st.error(f"Could not read file: {e}")

    # ── Saved files list ─────────────────────────────────────────────────
    try:
        with open(store_path, "r", encoding="utf-8") as f:
            batches = json.load(f)
    except Exception:
        batches = []

    if batches:
        st.markdown(f"**Saved files ({len(batches)}):**")
        for b in batches:
            bc1, bc2 = st.columns([5, 1])
            with bc1:
                st.markdown(f"📄 **{b['name']}** — {b['rows']:,} rows — {b['uploaded']}")
            with bc2:
                if st.button("🗑", key=f"del_pay_{key_prefix}_{b['name']}"):
                    delete_fn(b["name"])
                    st.rerun()

    st.markdown("---")

    # ── Load all saved payment data ───────────────────────────────────
    df_pay = load_fn()

    if df_pay.empty:
        st.info("Upload a payment file above to see the analysis.")
        return

    df_pay = df_pay.copy()
    if "PAYMENT_AMOUNT" in df_pay.columns:
        df_pay["PAYMENT_AMOUNT"] = pd.to_numeric(df_pay["PAYMENT_AMOUNT"], errors="coerce").fillna(0)

    # ── KPIs ─────────────────────────────────────────────────────────────
    total_accounts  = df_pay["ACCT_NO"].nunique() if "ACCT_NO" in df_pay.columns else len(df_pay)
    total_amount    = df_pay["PAYMENT_AMOUNT"].sum() if "PAYMENT_AMOUNT" in df_pay.columns else 0
    touch_types     = df_pay["TOUCH_POINTS"].nunique() if "TOUCH_POINTS" in df_pay.columns else 0
    source_types    = df_pay["SOURCE_OF_PAYMENT"].nunique() if "SOURCE_OF_PAYMENT" in df_pay.columns else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("🏦 Accounts",         f"{total_accounts:,}")
    k2.metric("💰 Total Payment",     f"₱{total_amount:,.2f}")
    k3.metric("📞 Touch Point Types", f"{touch_types:,}")
    k4.metric("🏷️ Payment Sources",   f"{source_types:,}")

    st.markdown("---")

    # ── Tab layout ───────────────────────────────────────────────────────
    t1, t2, t3 = st.tabs([
        "📞 By Touch Point",
        "🏷️ By Source of Payment",
        "📋 Raw Data",
    ])

    # ── Tab 1: Touch Points ──────────────────────────────────────────────
    with t1:
        if "TOUCH_POINTS" not in df_pay.columns:
            st.warning("TOUCH_POINTS column not found in uploaded file.")
        else:
            st.markdown("### 📞 Payment Analysis by Touch Point")

            c1, c2 = st.columns(2)
            with c1:
                # Count of accounts per touch point
                tp_count = (
                    df_pay.groupby("TOUCH_POINTS")["ACCT_NO"].nunique()
                    .reset_index(name="Accounts")
                    .sort_values("Accounts", ascending=False)
                ) if "ACCT_NO" in df_pay.columns else (
                    df_pay["TOUCH_POINTS"].value_counts().reset_index()
                    .rename(columns={"index":"TOUCH_POINTS","TOUCH_POINTS":"Accounts"})
                )
                fig = px.bar(
                    tp_count, x="TOUCH_POINTS", y="Accounts",
                    text="Accounts", color="TOUCH_POINTS",
                    color_discrete_sequence=PALETTE,
                    title="Accounts per Touch Point",
                )
                fig.update_traces(textposition="outside")
                fig.update_layout(showlegend=False, height=360, xaxis_tickangle=-30,
                                  xaxis_title="Touch Point")
                st.plotly_chart(fig, use_container_width=True)

            with c2:
                # Total payment amount per touch point
                if "PAYMENT_AMOUNT" in df_pay.columns:
                    tp_amt = (
                        df_pay.groupby("TOUCH_POINTS")["PAYMENT_AMOUNT"].sum()
                        .reset_index(name="Amount")
                        .sort_values("Amount", ascending=False)
                    )
                    fig = px.bar(
                        tp_amt, x="TOUCH_POINTS", y="Amount",
                        text=tp_amt["Amount"].apply(lambda x: f"₱{x:,.0f}"),
                        color="Amount", color_continuous_scale="Blues",
                        title="Total Payment per Touch Point",
                    )
                    fig.update_traces(textposition="outside")
                    fig.update_layout(coloraxis_showscale=False, height=360,
                                      xaxis_tickangle=-30, xaxis_title="Touch Point",
                                      yaxis_title="Payment (₱)")
                    st.plotly_chart(fig, use_container_width=True)

            # Pie chart of touch point distribution
            c3, c4 = st.columns(2)
            with c3:
                tp_pie = df_pay["TOUCH_POINTS"].value_counts().reset_index()
                tp_pie.columns = ["Touch Point", "Count"]
                fig = px.pie(
                    tp_pie, names="Touch Point", values="Count",
                    hole=0.4, title="Touch Point Distribution",
                    color_discrete_sequence=PALETTE,
                )
                st.plotly_chart(fig, use_container_width=True)

            with c4:
                # Average payment per touch point
                if "PAYMENT_AMOUNT" in df_pay.columns:
                    tp_avg = (
                        df_pay.groupby("TOUCH_POINTS")["PAYMENT_AMOUNT"].mean()
                        .reset_index(name="Avg Amount")
                        .sort_values("Avg Amount", ascending=True)
                    )
                    fig = px.bar(
                        tp_avg, x="Avg Amount", y="TOUCH_POINTS", orientation="h",
                        text=tp_avg["Avg Amount"].apply(lambda x: f"₱{x:,.0f}"),
                        color="Avg Amount", color_continuous_scale="Greens",
                        title="Avg Payment per Touch Point",
                    )
                    fig.update_traces(textposition="outside")
                    fig.update_layout(coloraxis_showscale=False, height=340,
                                      yaxis_title="", xaxis_title="Avg Amount (₱)")
                    st.plotly_chart(fig, use_container_width=True)

            # Detailed touch point table
            st.markdown("#### 📊 Touch Point Summary Table")
            if "PAYMENT_AMOUNT" in df_pay.columns:
                tp_summary = df_pay.groupby("TOUCH_POINTS").agg(
                    Accounts=("ACCT_NO", "nunique") if "ACCT_NO" in df_pay.columns else ("TOUCH_POINTS", "count"),
                    Total_Payment=("PAYMENT_AMOUNT", "sum"),
                    Avg_Payment=("PAYMENT_AMOUNT", "mean"),
                    Max_Payment=("PAYMENT_AMOUNT", "max"),
                    Count=("PAYMENT_AMOUNT", "count"),
                ).reset_index().rename(columns={"TOUCH_POINTS": "Touch Point"})
                tp_summary["Total_Payment"] = tp_summary["Total_Payment"].round(2)
                tp_summary["Avg_Payment"]   = tp_summary["Avg_Payment"].round(2)
                tp_summary["Max_Payment"]   = tp_summary["Max_Payment"].round(2)
                st.dataframe(
                    tp_summary.sort_values("Total_Payment", ascending=False),
                    use_container_width=True, hide_index=True,
                    column_config={
                        "Total_Payment": st.column_config.NumberColumn("Total (₱)", format="₱%.2f"),
                        "Avg_Payment":   st.column_config.NumberColumn("Avg (₱)",   format="₱%.2f"),
                        "Max_Payment":   st.column_config.NumberColumn("Max (₱)",   format="₱%.2f"),
                    }
                )

    # ── Tab 2: Source of Payment ─────────────────────────────────────────
    with t2:
        if "SOURCE_OF_PAYMENT" not in df_pay.columns:
            st.warning("SOURCE_OF_PAYMENT column not found in uploaded file.")
        else:
            st.markdown("### 🏷️ Payment Analysis by Source")

            c1, c2 = st.columns(2)
            with c1:
                src_count = (
                    df_pay.groupby("SOURCE_OF_PAYMENT")["ACCT_NO"].nunique()
                    .reset_index(name="Accounts")
                    .sort_values("Accounts", ascending=False)
                ) if "ACCT_NO" in df_pay.columns else (
                    df_pay["SOURCE_OF_PAYMENT"].value_counts().reset_index()
                    .rename(columns={"index":"SOURCE_OF_PAYMENT","SOURCE_OF_PAYMENT":"Accounts"})
                )
                fig = px.bar(
                    src_count, x="SOURCE_OF_PAYMENT", y="Accounts",
                    text="Accounts", color="SOURCE_OF_PAYMENT",
                    color_discrete_sequence=PALETTE,
                    title="Accounts per Payment Source",
                )
                fig.update_traces(textposition="outside")
                fig.update_layout(showlegend=False, height=360, xaxis_tickangle=-30,
                                  xaxis_title="Source")
                st.plotly_chart(fig, use_container_width=True)

            with c2:
                if "PAYMENT_AMOUNT" in df_pay.columns:
                    src_amt = (
                        df_pay.groupby("SOURCE_OF_PAYMENT")["PAYMENT_AMOUNT"].sum()
                        .reset_index(name="Amount")
                        .sort_values("Amount", ascending=False)
                    )
                    fig = px.bar(
                        src_amt, x="SOURCE_OF_PAYMENT", y="Amount",
                        text=src_amt["Amount"].apply(lambda x: f"₱{x:,.0f}"),
                        color="Amount", color_continuous_scale="Purples",
                        title="Total Payment per Source",
                    )
                    fig.update_traces(textposition="outside")
                    fig.update_layout(coloraxis_showscale=False, height=360,
                                      xaxis_tickangle=-30, xaxis_title="Source",
                                      yaxis_title="Payment (₱)")
                    st.plotly_chart(fig, use_container_width=True)

            c3, c4 = st.columns(2)
            with c3:
                src_pie = df_pay["SOURCE_OF_PAYMENT"].value_counts().reset_index()
                src_pie.columns = ["Source", "Count"]
                fig = px.pie(
                    src_pie, names="Source", values="Count",
                    hole=0.4, title="Source Distribution",
                    color_discrete_sequence=PALETTE,
                )
                st.plotly_chart(fig, use_container_width=True)

            with c4:
                if "PAYMENT_AMOUNT" in df_pay.columns:
                    src_avg = (
                        df_pay.groupby("SOURCE_OF_PAYMENT")["PAYMENT_AMOUNT"].mean()
                        .reset_index(name="Avg Amount")
                        .sort_values("Avg Amount", ascending=True)
                    )
                    fig = px.bar(
                        src_avg, x="Avg Amount", y="SOURCE_OF_PAYMENT", orientation="h",
                        text=src_avg["Avg Amount"].apply(lambda x: f"₱{x:,.0f}"),
                        color="Avg Amount", color_continuous_scale="Oranges",
                        title="Avg Payment per Source",
                    )
                    fig.update_traces(textposition="outside")
                    fig.update_layout(coloraxis_showscale=False, height=340,
                                      yaxis_title="", xaxis_title="Avg Amount (₱)")
                    st.plotly_chart(fig, use_container_width=True)

            # Cross-analysis: Touch Point x Source heatmap
            if "TOUCH_POINTS" in df_pay.columns and "PAYMENT_AMOUNT" in df_pay.columns:
                st.markdown("#### 🔥 Touch Point × Source Heatmap (Total Payment)")
                heatmap_data = (
                    df_pay.groupby(["TOUCH_POINTS", "SOURCE_OF_PAYMENT"])["PAYMENT_AMOUNT"]
                    .sum().reset_index()
                    .pivot(index="TOUCH_POINTS", columns="SOURCE_OF_PAYMENT", values="PAYMENT_AMOUNT")
                    .fillna(0)
                )
                fig = px.imshow(
                    heatmap_data,
                    color_continuous_scale="Blues",
                    title="Payment Amount: Touch Point vs Source",
                    text_auto=".0f",
                    aspect="auto",
                )
                fig.update_layout(height=max(300, len(heatmap_data) * 50))
                st.plotly_chart(fig, use_container_width=True)

            # Source summary table
            st.markdown("#### 📊 Source Summary Table")
            if "PAYMENT_AMOUNT" in df_pay.columns:
                src_summary = df_pay.groupby("SOURCE_OF_PAYMENT").agg(
                    Count=("PAYMENT_AMOUNT", "count"),
                    Total_Payment=("PAYMENT_AMOUNT", "sum"),
                    Avg_Payment=("PAYMENT_AMOUNT", "mean"),
                ).reset_index().rename(columns={"SOURCE_OF_PAYMENT": "Source"})
                src_summary["Total_Payment"] = src_summary["Total_Payment"].round(2)
                src_summary["Avg_Payment"]   = src_summary["Avg_Payment"].round(2)
                st.dataframe(
                    src_summary.sort_values("Total_Payment", ascending=False),
                    use_container_width=True, hide_index=True,
                    column_config={
                        "Total_Payment": st.column_config.NumberColumn("Total (₱)", format="₱%.2f"),
                        "Avg_Payment":   st.column_config.NumberColumn("Avg (₱)",   format="₱%.2f"),
                    }
                )

    # ── Tab 3: Raw data ──────────────────────────────────────────────────
    with t3:
        st.dataframe(
            df_pay.drop(columns=["_batch"], errors="ignore"),
            use_container_width=True, hide_index=True,
            column_config={
                "PAYMENT_AMOUNT": st.column_config.NumberColumn("Payment Amount (₱)", format="₱%.2f"),
            }
        )
        st.download_button(
            "⬇ Download Payment Data",
            df_pay.drop(columns=["_batch"], errors="ignore").to_csv(index=False).encode(),
            file_name=f"payment_upload_{TODAY_STR}.csv",
            mime="text/csv",
            key=f"dl_payment_upload_{key_prefix}",
        )



# ─────────────────────────────────────────────────────────────────────────────
# EWB RECOVERY ── PAYMENT UPLOAD + VISUALIZATION
# ─────────────────────────────────────────────────────────────────────────────

RECOVERY_PAYMENT_STORE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "recovery_payment_upload.json"
)

def _save_recovery_payment(filename: str, df: pd.DataFrame):
    os.makedirs(os.path.dirname(RECOVERY_PAYMENT_STORE), exist_ok=True)
    try:
        with open(RECOVERY_PAYMENT_STORE, "r", encoding="utf-8") as f:
            store = json.load(f)
    except Exception:
        store = []
    store = [b for b in store if b["name"] != filename]
    store.append({"name": filename, "uploaded": TODAY_STR,
                  "rows": len(df), "data": df.to_json(orient="records", date_format="iso")})
    with open(RECOVERY_PAYMENT_STORE, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False)

def _load_recovery_payments() -> pd.DataFrame:
    if not os.path.exists(RECOVERY_PAYMENT_STORE):
        return pd.DataFrame()
    try:
        with open(RECOVERY_PAYMENT_STORE, "r", encoding="utf-8") as f:
            store = json.load(f)
        if not store:
            return pd.DataFrame()
        frames = []
        for b in store:
            df = pd.read_json(b["data"], orient="records")
            df["_batch"] = b["name"]
            frames.append(df)
        return pd.concat(frames, ignore_index=True)
    except Exception:
        return pd.DataFrame()

def _delete_recovery_payment_batch(name: str):
    try:
        with open(RECOVERY_PAYMENT_STORE, "r", encoding="utf-8") as f:
            store = json.load(f)
    except Exception:
        store = []
    store = [b for b in store if b["name"] != name]
    with open(RECOVERY_PAYMENT_STORE, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False)


def render_payment_upload():
    """Payment upload tab for EWB 150 DPD."""
    _render_payment_tab(
        load_fn=_load_payment_uploads,
        save_fn=_save_payment_upload,
        delete_fn=_delete_payment_batch,
        store_path=PAYMENT_STORE,
        key_prefix="dpd",
        title="EWB 150 DPD",
    )


def render_payment_upload_recovery():
    """Payment upload & visualization tab for EWB Recovery."""
    _render_payment_tab(
        load_fn=_load_recovery_payments,
        save_fn=_save_recovery_payment,
        delete_fn=_delete_recovery_payment_batch,
        store_path=RECOVERY_PAYMENT_STORE,
        key_prefix="rec",
        title="EWB Recovery",
    )


# ─────────────────────────────────────────────────────────────────────────────
# ROUTING
# ─────────────────────────────────────────────────────────────────────────────

if page == "EWB Recovery":
    st.title("🏦 EWB Recovery")
    st.caption(f"Default view: **Today ({TODAY_DISP})**. Use the date filter in the sidebar to backtrack.")

    with st.spinner("Loading data..."):
        df_raw       = fetch_data(EWB_RECOVERY)
        df_port      = fetch_data(EWB_PORTFOLIO)
        df_ptp       = fetch_data(EWB_PTP_DAILY)
        df_field_db  = fetch_data(EWB_FIELD_RESULTS)

    if df_raw.empty and df_port.empty:
        st.warning("No data returned. Check your .env database credentials.")
        st.stop()

    df_raw["BARCODE_DATE"] = pd.to_datetime(df_raw["BARCODE_DATE"], errors="coerce")
    df_raw["AMOUNT"]       = pd.to_numeric(df_raw["AMOUNT"], errors="coerce")
    df_port["OB"]          = pd.to_numeric(df_port["OB"], errors="coerce")

    targets = recovery_target_sidebar(df_raw)
    df      = recovery_filters(df_raw)

    # ── Main tabs ──────────────────────────────────────────────────────────
    tab_main, tab_field, tab_ml, tab_endo, tab_area, tab_pay = st.tabs([
        "📊 Dashboard",
        "🚗 Field Results",
        "🤖 ML Insights",
        "📥 Endorsement Consolidation",
        "📍 Area Break",
        "💳 Payment Upload",
    ])

    with tab_main:
        render_portfolio(df_port)
        st.divider()
        recovery_kpis(df, df_port, targets)
        st.divider()
        recovery_charts(df)
        st.divider()
        render_ptp_tracking(df_ptp)
        st.divider()
        recovery_agent_table(df)
        st.divider()
        show_raw_data(df, "ewb_recovery.csv")

    with tab_field:
        render_field_results(df_field_db)

    with tab_ml:
        render_ml_tab(df_raw)

    with tab_endo:
        render_endorsement_consolidation()

    with tab_area:
        render_area_break("recovery")

    with tab_pay:
        render_payment_upload_recovery()


elif page == "EWB 150 DPD":
    st.title("🏦 EWB 150 DPD")

    with st.spinner("Loading data..."):
        df_raw        = fetch_data(EWB_150DPD)
        df_150ptp     = fetch_data(EWB_150DPD_PTP)
        df_150field   = fetch_data(EWB_150DPD_FIELD)
        df_150efforts = fetch_data(ewb_150dpd_efforts_query(TODAY.month, TODAY.year))

    if df_raw.empty:
        st.warning("No data returned. Check your .env database credentials.")
        st.stop()

    df = apply_filters_150(df_raw)

    # ── Main tabs ──────────────────────────────────────────────────────────
    tab_main, tab_field, tab_ml, tab_endo, tab_area, tab_pay = st.tabs([
        "📊 Dashboard",
        "🚗 Field Results",
        "🤖 ML Insights",
        "📥 Endorsement",
        "📍 Area Break",
        "💳 Payment Upload",
    ])

    with tab_main:
        show_kpis_150(df)
        if "OB" in df.columns:
            st.metric("💰 Total OB", f"₱{pd.to_numeric(df['OB'], errors='coerce').sum():,.2f}")
        st.divider()
        render_no_effort_section(df_raw)
        st.divider()
        render_pullout_section(df)
        st.divider()
        show_charts_150(df)
        st.divider()
        show_cycle_section(df, df_ptp=df_150ptp, df_efforts=df_150efforts)
        st.divider()
        show_agent_table_150(df, df_ptp=df_150ptp)
        st.divider()
        show_raw_data(df, "ewb_150dpd.csv")

    with tab_field:
        render_field_results(df_150field)

    with tab_ml:
        render_ml_tab_150(df_raw, df_150ptp)

    with tab_endo:
        render_endorsement_150()

    with tab_area:
        render_area_break("dpd")

    with tab_pay:
        render_payment_upload()
