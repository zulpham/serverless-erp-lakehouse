"""Serverless ERP Lakehouse - Interactive Streamlit AI Analytics Dashboard.

Provides a business-friendly conversational interface to query the Medallion
Apache Iceberg Lakehouse using natural language powered by Amazon Bedrock
(Claude 3 Haiku / Titan Text), SQLGlot Security Guardrails, and Plotly Visualizations.

Architecture & Engineering Standards:
1. Zero Pandas Dependency: Uses pure Rust-backed Polars for all dataframe operations.
2. In-Memory Engine Caching: Utilizes @st.cache_resource to reuse global SDK connections.
3. Multi-Tab Visual Analytics: Segregates Insights, Plotly Charts, Data Tables, and SQL Audit Logs.
4. Resilient Error Handling: Gracefully presents AST security violations and Athena timeouts.
"""

import os
import pathlib
import sys
import plotly.express as px
import polars as pl
import streamlit as st

# Add project root directory to sys.path for clean module imports
root_dir = pathlib.Path(__file__).parent.parent
sys.path.append(str(root_dir))

from src.ai.sql_engine import LakehouseAIEngine

# Page Configuration & Metadata
st.set_page_config(
    page_title="Serverless ERP Lakehouse AI",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Corporate CSS Theme
st.markdown(
    """
<style>
    .main-header { font-size: 2.2rem; font-weight: 700; color: #1E88E5; margin-bottom: 0.2rem; }
    .sub-header { font-size: 1.05rem; color: #78909C; margin-bottom: 1.5rem; }
    .stMetric { background-color: #0E1117; padding: 0.8rem; border-radius: 8px; border: 1px solid #262730; }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_resource(show_spinner=False)
def get_ai_engine() -> LakehouseAIEngine:
    """Initializes and caches the Lakehouse AI Engine in global memory."""
    return LakehouseAIEngine()


# Initialize Engine Instance
engine = get_ai_engine()

# ------------------------------------------------------------------------------
# 1. SIDEBAR: Architecture Control Center & Sample Inquiries
# ------------------------------------------------------------------------------
with st.sidebar:
    st.image(
        "https://upload.wikimedia.org/wikipedia/commons/9/93/Amazon_Web_Services_Logo.svg",
        width=90,
    )
    st.title("Lakehouse Control Center")
    st.markdown("**Architecture:** 100% Serverless Medallion Lakehouse")
    st.markdown("**Table Format:** Apache Iceberg v2 (ZSTD Parquet)")
    st.markdown("**Engine:** Amazon Athena Engine v3 (Presto/Trino)")
    st.markdown("**Data Engine:** Rust-backed Polars (Zero Pandas)")

    st.divider()
    st.subheader("💡 Contoh Pertanyaan Bisnis")

    sample_queries = [
        "Tampilkan 5 pelanggan dengan total belanja terbesar di tahun 1997",
        "Siapa 3 karyawan dengan total nilai penjualan tertinggi?",
        "Tampilkan tren penjualan bulanan (Total Revenue) di tahun 1997",
        "Tampilkan 5 produk terlaris berdasarkan total kuantitas yang terjual",
        "Berapa total pendapatan dan rata-rata diskon per negara tujuan pengiriman (Ship Country)?",
    ]

    selected_sample = None
    for idx, q in enumerate(sample_queries):
        if st.button(q, key=f"sample_{idx}", use_container_width=True):
            selected_sample = q

    st.divider()
    st.markdown(
        "🛡️ **Security Safeguards:** SQLGlot AST Anti-Mutation & LIMIT 100"
    )
    st.markdown("⚡ **L3 Cache:** Athena 24-Hour Result Reuse Enabled")

# ------------------------------------------------------------------------------
# 2. MAIN VIEW: Conversational Query Interface
# ------------------------------------------------------------------------------
st.markdown(
    '<div class="main-header">⚡ Serverless ERP Lakehouse AI Analytics</div>',
    unsafe_allow_html=True,
)
st.markdown(
    '<div class="sub-header">Tanyakan data transaksi ERP Anda dalam bahasa alami — AI akan menyusun kueri Trino SQL, memindai tabel Apache Iceberg di Amazon Athena, dan menyajikan visualisasi data instan.</div>',
    unsafe_allow_html=True,
)

# User Query Input
user_query = st.text_input(
    "Masukkan pertanyaan analitik Anda:",
    value=selected_sample if selected_sample else "",
    placeholder="Contoh: Tampilkan 5 pelanggan dengan total belanja terbesar di tahun 1997",
)

btn_run = st.button("🚀 Eksekusi Kueri Analitik", type="primary")

# ------------------------------------------------------------------------------
# 3. PIPELINE EXECUTION & MULTI-TAB PRESENTATION
# ------------------------------------------------------------------------------
if (btn_run or selected_sample) and user_query:
    with st.spinner(
        "🤖 AI sedang menyusun SQL dan memindai tabel Apache Iceberg..."
    ):
        try:
            # Execute the complete Text-to-SQL Pipeline
            raw_sql, guarded_sql, df_result, summary = engine.run_pipeline(
                user_query
            )

            # Tabbed Presentation Layout
            tab_summary, tab_chart, tab_data, tab_sql = st.tabs(
                [
                    "📋 Ringkasan Eksekutif",
                    "📊 Visualisasi Grafik",
                    "🔢 Tabel Data (Polars)",
                    "🛠️ Kueri SQL Terverifikasi",
                ]
            )

            # ------------------------------------------------------------------
            # TAB 1: EXECUTIVE BUSINESS SUMMARY
            # ------------------------------------------------------------------
            with tab_summary:
                st.subheader("💡 Executive Insights & Strategic Takeaways")
                st.markdown(summary)

                # High-Level Metrics Overview
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Baris Ditemukan", f"{df_result.height} baris")

                with col2:
                    # Dynamically calculate primary financial metrics if present in Polars DataFrame
                    num_cols = [
                        col
                        for col, dtype in df_result.schema.items()
                        if dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
                    ]
                    if num_cols:
                        metric_name = num_cols[-1]
                        total_val = (
                            df_result[metric_name].sum()
                            if df_result[metric_name].is_not_null().any()
                            else 0
                        )
                        st.metric(
                            f"Total {metric_name.replace('_', ' ').title()}",
                            f"${total_val:,.2f}",
                        )
                    else:
                        st.metric("Status Kueri", "Berhasil Di-Scan")

                with col3:
                    st.metric("Status Keamanan", "AST Guardrail PASSED ✅")

            # ------------------------------------------------------------------
            # TAB 2: INTERACTIVE PLOTLY VISUALIZATION
            # ------------------------------------------------------------------
            with tab_chart:
                st.subheader("📈 Visualisasi Data Interaktif")
                if df_result.height > 0 and len(df_result.columns) >= 2:
                    # Categorize columns based on Polars Schema
                    cat_cols = [
                        col
                        for col, dtype in df_result.schema.items()
                        if dtype
                        in (pl.Utf8, pl.String, pl.Categorical, pl.Date)
                    ]
                    num_cols = [
                        col
                        for col, dtype in df_result.schema.items()
                        if dtype
                        in (pl.Float64, pl.Float32, pl.Int64, pl.Int32)
                    ]

                    # Convert Polars DataFrame to dict for Plotly Express (Zero Pandas dependency)
                    data_dict = df_result.to_dict(as_series=False)

                    if cat_cols and num_cols:
                        x_axis = cat_cols[0]
                        y_axis = num_cols[0]
                        fig = px.bar(
                            data_dict,
                            x=x_axis,
                            y=y_axis,
                            title=f"Analisis: {y_axis.replace('_', ' ').title()} berdasarkan {x_axis.replace('_', ' ').title()}",
                            template="plotly_dark",
                            color=y_axis,
                            color_continuous_scale="Blues",
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    elif len(num_cols) >= 2:
                        fig = px.line(
                            data_dict,
                            x=num_cols[0],
                            y=num_cols[1:],
                            template="plotly_dark",
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info(
                            "Dataframe tidak memuat kombinasi kolom kategori dan numerik untuk divisualisasikan."
                        )
                else:
                    st.info(
                        "Hasil kueri tidak memiliki data yang cukup untuk divisualisasikan."
                    )

            # ------------------------------------------------------------------
            # TAB 3: TABULAR DATA VIEW (POLARS NATIVE)
            # ------------------------------------------------------------------
            with tab_data:
                st.subheader("📋 Dataframe Hasil Kueri Apache Iceberg")
                st.dataframe(df_result, use_container_width=True)

            # ------------------------------------------------------------------
            # TAB 4: SQL CODE & AST SECURITY AUDIT LOGS
            # ------------------------------------------------------------------
            with tab_sql:
                st.subheader("🛡️ Log Kueri Trino SQL & Guardrails AST")
                st.markdown(
                    "**1. Kueri SQL Asli yang Dihasilkan AI (Pass-1 Model):**"
                )
                st.code(raw_sql, language="sql")

                st.markdown(
                    "**2. Kueri SQL yang Telah Divalidasi (SQLGlot AST Sanitized & Guarded):**"
                )
                st.code(guarded_sql, language="sql")

                st.info(
                    "Catatan Audit: Kueri telah melewati verifikasi pohon sintaksis abstrak (AST) "
                    "untuk memblokir seluruh perintah mutasi (DROP/DELETE/ALTER/INSERT) dan menyuntikkan batas LIMIT 100."
                )

        except Exception as e:
            st.error(f"❌ Terjadi kesalahan saat memproses kueri: {e}")