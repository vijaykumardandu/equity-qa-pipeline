"""
Equity QA Pipeline — Streamlit Dashboard
Interactive monitoring interface for the public ownership QA dataset.
Run: streamlit run dashboard/streamlit_app.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "db/equity_qa.db")

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Equity QA Pipeline",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .metric-card {
        background: #1B2A4A; color: white; border-radius: 10px;
        padding: 1.2rem 1.5rem; margin-bottom: 0.5rem;
    }
    .metric-label { font-size: 0.75rem; color: #AABBDD; text-transform: uppercase; letter-spacing: 1px; }
    .metric-value { font-size: 2.2rem; font-weight: 700; margin: 0.2rem 0; }
    .critical { color: #EF5350; }
    .high     { color: #FFA726; }
    .medium   { color: #FFEE58; }
    .good     { color: #66BB6A; }
    .stDataFrame { border-radius: 8px; }
    div[data-testid="metric-container"] { background: #F7F9FC; border-radius: 8px; padding: 1rem; }
</style>
""", unsafe_allow_html=True)


# ── Data loaders ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=10)
def load_summary():
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute("SELECT COUNT(*) FROM shareholding_raw").fetchone()[0]
    latest = conn.execute("SELECT run_at, defect_rate_pct FROM audit_runs ORDER BY run_id DESC LIMIT 1").fetchone()
    sev = pd.read_sql("SELECT severity, COUNT(*) as count FROM defect_log WHERE status='Open' GROUP BY severity", conn)
    open_total = conn.execute("SELECT COUNT(*) FROM defect_log WHERE status='Open'").fetchone()[0]
    resolved   = conn.execute("SELECT COUNT(*) FROM defect_log WHERE status='Resolved'").fetchone()[0]
    conn.close()
    sev_map = dict(zip(sev["severity"], sev["count"])) if not sev.empty else {}
    return {
        "total": total, "open": open_total, "resolved": resolved,
        "critical": sev_map.get("Critical", 0),
        "high": sev_map.get("High", 0),
        "medium": sev_map.get("Medium", 0),
        "defect_rate": round(open_total / total * 100, 1) if total else 0,
        "last_run": latest[0][:19] if latest else "Never",
    }


@st.cache_data(ttl=10)
def load_defects(status="Open"):
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        "SELECT * FROM defect_log WHERE status=? ORDER BY defect_id DESC",
        conn, params=(status,)
    )
    conn.close()
    return df


@st.cache_data(ttl=10)
def load_rule_stats():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        """SELECT rule_id, rule_name, severity,
                  COUNT(*) as defects, COUNT(DISTINCT company_ticker) as companies
           FROM defect_log WHERE status='Open'
           GROUP BY rule_id ORDER BY defects DESC""",
        conn
    )
    conn.close()
    return df


@st.cache_data(ttl=10)
def load_company_risk():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        """SELECT company_ticker,
                  SUM(CASE WHEN severity='Critical' THEN 1 ELSE 0 END) AS critical,
                  SUM(CASE WHEN severity='High' THEN 1 ELSE 0 END) AS high,
                  SUM(CASE WHEN severity='Medium' THEN 1 ELSE 0 END) AS medium,
                  COUNT(*) AS total
           FROM defect_log WHERE status='Open'
           GROUP BY company_ticker ORDER BY total DESC""",
        conn
    )
    conn.close()
    if not df.empty:
        df["risk_score"] = df["critical"]*10 + df["high"]*6 + df["medium"]*3
        df["risk_label"] = df["risk_score"].apply(
            lambda s: "High Risk" if s >= 20 else "Medium Risk" if s >= 8 else "Low Risk"
        )
    return df


@st.cache_data(ttl=10)
def load_audit_history():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM audit_runs ORDER BY run_id DESC LIMIT 20", conn)
    conn.close()
    return df


@st.cache_data(ttl=10)
def load_source_breakdown():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql(
        """SELECT s.source,
                  COUNT(DISTINCT s.record_id) AS total_records,
                  COUNT(d.defect_id) AS defects
           FROM shareholding_raw s
           LEFT JOIN defect_log d ON s.record_id = d.record_id AND d.status='Open'
           GROUP BY s.source ORDER BY defects DESC""",
        conn
    )
    conn.close()
    if not df.empty:
        df["defect_rate"] = (df["defects"] / df["total_records"] * 100).round(1)
    return df


def run_audit_action():
    """Trigger QA audit from the dashboard."""
    import subprocess
    result = subprocess.run(
        ["python", "-c",
         "import sys; sys.path.insert(0,'.'); from pipeline.qa_engine import run_full_audit; r=run_full_audit(); print(r)"],
        capture_output=True, text=True, cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    )
    return result.returncode == 0


# ── Sidebar ────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🔍 Equity QA Pipeline")
    st.markdown("**Public Ownership Domain**")
    st.divider()

    page = st.radio("Navigation", [
        "📊 Overview",
        "🚨 Defect Log",
        "📋 Rule Performance",
        "🏢 Company Risk",
        "📈 Audit History",
        "🔗 Source Analysis",
    ])

    st.divider()
    st.markdown("**Quick Actions**")
    if st.button("▶ Run Full Audit", use_container_width=True, type="primary"):
        with st.spinner("Running 12 validation rules..."):
            ok = run_audit_action()
        if ok:
            st.success("Audit complete! Refresh page.")
            load_summary.clear()
            load_defects.clear()
            load_rule_stats.clear()
            load_company_risk.clear()
        else:
            st.error("Audit failed. Check logs.")

    st.divider()
    summary = load_summary()
    st.markdown(f"**Last Run:** `{summary['last_run']}`")
    st.markdown(f"**Records:** `{summary['total']:,}`")
    st.markdown(f"**Defect Rate:** `{summary['defect_rate']}%`")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Overview
# ══════════════════════════════════════════════════════════════════════════════
if page == "📊 Overview":
    st.title("Quality Audit Overview")
    st.caption(f"Public Ownership Shareholding Dataset  •  Last updated: {summary['last_run']}")

    # KPI row
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Records", f"{summary['total']:,}")
    with col2:
        st.metric("Open Defects", summary["open"], delta=f"{summary['defect_rate']}% rate", delta_color="inverse")
    with col3:
        st.metric("🔴 Critical", summary["critical"])
    with col4:
        st.metric("🟠 High", summary["high"])
    with col5:
        st.metric("✅ Resolved", summary["resolved"])

    st.divider()

    col_left, col_right = st.columns([1, 1])

    with col_left:
        st.subheader("Defects by Severity")
        sev_df = pd.DataFrame({
            "Severity": ["Critical", "High", "Medium"],
            "Count": [summary["critical"], summary["high"], summary["medium"]],
        })
        fig = px.pie(
            sev_df, values="Count", names="Severity",
            color="Severity",
            color_discrete_map={"Critical": "#EF5350", "High": "#FFA726", "Medium": "#FFEE58"},
            hole=0.45,
        )
        fig.update_traces(textposition="outside", textinfo="percent+label")
        fig.update_layout(margin=dict(t=10, b=10), showlegend=False, height=320)
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.subheader("Defects by Rule")
        rule_df = load_rule_stats()
        if not rule_df.empty:
            fig2 = px.bar(
                rule_df.head(8), x="defects", y="rule_id", orientation="h",
                color="severity",
                color_discrete_map={"Critical": "#EF5350", "High": "#FFA726", "Medium": "#FFEE58"},
                text="defects",
            )
            fig2.update_layout(
                margin=dict(t=10, b=10), height=320,
                xaxis_title="Defect Count", yaxis_title="Rule",
                legend_title="Severity", yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig2, use_container_width=True)

    # Company heatmap
    st.subheader("Company Risk Heatmap")
    risk_df = load_company_risk()
    if not risk_df.empty:
        fig3 = go.Figure(data=go.Bar(
            x=risk_df["company_ticker"],
            y=risk_df["risk_score"],
            marker_color=risk_df["risk_score"].apply(
                lambda s: "#EF5350" if s >= 20 else "#FFA726" if s >= 8 else "#66BB6A"
            ),
            text=risk_df["total"].apply(lambda x: f"{x} defects"),
            textposition="outside",
        ))
        fig3.update_layout(
            xaxis_title="Company", yaxis_title="Risk Score",
            margin=dict(t=10, b=10), height=300,
        )
        st.plotly_chart(fig3, use_container_width=True)

    # Source quality
    st.subheader("Data Source Quality")
    src_df = load_source_breakdown()
    if not src_df.empty:
        st.dataframe(src_df, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Defect Log
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🚨 Defect Log":
    st.title("Defect Log")

    col1, col2, col3 = st.columns(3)
    with col1:
        status_filter = st.selectbox("Status", ["Open", "Resolved", "Waived"])
    with col2:
        sev_filter = st.selectbox("Severity", ["All", "Critical", "High", "Medium"])
    with col3:
        rule_filter = st.text_input("Rule ID (e.g. QR-01)", "")

    df = load_defects(status_filter)

    if sev_filter != "All":
        df = df[df["severity"] == sev_filter]
    if rule_filter:
        df = df[df["rule_id"] == rule_filter.upper()]

    st.caption(f"Showing {len(df):,} records")

    def color_severity(val):
        colors = {"Critical": "background-color:#FDECEA; color:#B71C1C",
                  "High": "background-color:#FFF3E0; color:#E65100",
                  "Medium": "background-color:#FFFDE7; color:#F57F17"}
        return colors.get(val, "")

    display_cols = ["defect_id", "record_id", "company_ticker", "rule_id",
                    "rule_name", "severity", "field_affected", "field_value", "status"]
    if not df.empty:
        styled = df[display_cols].style.map(color_severity, subset=["severity"])
        st.dataframe(styled, use_container_width=True, hide_index=True)
    else:
        st.info("No defects match the selected filters.")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Rule Performance
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📋 Rule Performance":
    st.title("Validation Rule Performance")
    st.caption("12 business rules applied across all shareholding records")

    rule_df = load_rule_stats()
    if not rule_df.empty:
        # Bar chart
        fig = px.bar(
            rule_df, x="rule_id", y="defects", color="severity",
            color_discrete_map={"Critical": "#EF5350", "High": "#FFA726", "Medium": "#FFEE58"},
            text="defects", hover_data=["rule_name", "companies"],
        )
        fig.update_layout(
            xaxis_title="Rule ID", yaxis_title="Defects Found",
            margin=dict(t=20, b=10), height=350,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Table with preventive actions
        from pipeline.defect_classifier import PREVENTIVE_ACTIONS
        rule_df["preventive_action"] = rule_df["rule_id"].map(PREVENTIVE_ACTIONS)
        st.subheader("Rule Details & Preventive Actions")
        st.dataframe(rule_df, use_container_width=True, hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Company Risk
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🏢 Company Risk":
    st.title("Company Risk Profiles")

    risk_df = load_company_risk()
    if not risk_df.empty:
        col1, col2 = st.columns([1.2, 1])
        with col1:
            fig = px.bar(
                risk_df, x="company_ticker", y=["critical", "high", "medium"],
                color_discrete_map={"critical": "#EF5350", "high": "#FFA726", "medium": "#FFEE58"},
                barmode="stack", labels={"value": "Defects", "variable": "Severity"},
            )
            fig.update_layout(height=380, margin=dict(t=20, b=10))
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Risk Rankings")
            display = risk_df[["company_ticker", "risk_label", "risk_score", "total"]].copy()

            def color_risk(val):
                return {
                    "High Risk":   "background-color:#FDECEA; color:#B71C1C",
                    "Medium Risk": "background-color:#FFF3E0; color:#E65100",
                    "Low Risk":    "background-color:#E8F5E9; color:#1B5E20",
                }.get(val, "")

            st.dataframe(
                display.style.map(color_risk, subset=["risk_label"]),
                use_container_width=True, hide_index=True
            )

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Audit History
# ══════════════════════════════════════════════════════════════════════════════
elif page == "📈 Audit History":
    st.title("Audit Run History")

    hist_df = load_audit_history()
    if not hist_df.empty:
        fig = px.line(
            hist_df.sort_values("run_id"),
            x="run_at", y="defect_rate_pct",
            markers=True, labels={"defect_rate_pct": "Defect Rate (%)", "run_at": "Run Time"},
            line_shape="spline",
        )
        fig.add_hline(y=5, line_dash="dash", line_color="green",
                      annotation_text="Target: 5%", annotation_position="right")
        fig.update_layout(height=350, margin=dict(t=20, b=10))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Run Log")
        st.dataframe(hist_df, use_container_width=True, hide_index=True)
    else:
        st.info("No audit history yet. Run an audit first.")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE: Source Analysis
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🔗 Source Analysis":
    st.title("Data Source Analysis")
    st.caption("Quality breakdown by filing source")

    src_df = load_source_breakdown()
    if not src_df.empty:
        fig = px.scatter(
            src_df, x="total_records", y="defect_rate",
            size="defects", color="source", text="source",
            labels={"total_records": "Records Ingested", "defect_rate": "Defect Rate (%)"},
        )
        fig.update_traces(textposition="top center")
        fig.update_layout(height=380, margin=dict(t=20, b=10))
        st.plotly_chart(fig, use_container_width=True)

        st.subheader("Source Quality Table")
        st.dataframe(src_df, use_container_width=True, hide_index=True)

    st.divider()
    st.markdown("""
    **Insight:** Sources with high defect rates should be flagged for upstream remediation.
    Work with the source data team to apply validation rules at the point of filing ingestion,
    reducing downstream QA burden.
    """)