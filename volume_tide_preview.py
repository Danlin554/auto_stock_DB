"""
量能指標 — 獨立預覽圖表
執行方式：cd /mnt/c/Users/User/Desktop/FB-Market && venv/bin/streamlit run volume_tide_preview.py
"""
import os
import sqlite3
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'data', 'market.db')

TIME_MARKERS = [("09:00", "開盤"), ("09:30", "9:30"), ("10:00", "10:00"),
                ("12:00", "12:00"), ("13:00", "13:00")]


def add_time_markers(fig, date_str):
    """在圖表上加上關鍵時間垂直虛線標記"""
    for t, label in TIME_MARKERS:
        x_val = f"{date_str} {t}:00"
        fig.add_shape(type="line", x0=x_val, x1=x_val, y0=0, y1=1,
                      yref="paper", line=dict(color="#ccc", width=1, dash="dot"))
        fig.add_annotation(x=x_val, y=1, yref="paper", text=label,
                           showarrow=False, font=dict(size=10, color="#999"),
                           yshift=10)


st.set_page_config(page_title="量能指標預覽", layout="wide")
st.markdown("<h2 style='text-align:center;'>量能指標預覽</h2>", unsafe_allow_html=True)

# === 從 raw_snapshots 計算每個 snapshot_time 的上漲/下跌成交金額 ===
conn = sqlite3.connect(DB_PATH, timeout=10)
today = conn.execute("SELECT MAX(snapshot_time) FROM raw_snapshots").fetchone()[0][:10]

df = pd.read_sql_query("""
    SELECT
        snapshot_time,
        SUM(CASE WHEN change_percent > 0 THEN trade_value ELSE 0 END) AS up_value,
        SUM(CASE WHEN change_percent < 0 THEN trade_value ELSE 0 END) AS down_value,
        SUM(CASE WHEN change_percent > 0 THEN trade_value ELSE 0 END)
          + SUM(CASE WHEN change_percent < 0 THEN trade_value ELSE 0 END) AS total_value
    FROM raw_snapshots
    WHERE snapshot_time LIKE ?
      AND LENGTH(symbol) = 4 AND symbol GLOB '[1-9][0-9][0-9][0-9]'
      AND change_percent IS NOT NULL AND is_anomaly = 0
    GROUP BY snapshot_time
    ORDER BY snapshot_time
""", conn, params=(f"{today}%",))
conn.close()

if df.empty:
    st.warning("今日無資料")
    st.stop()

# === 計算指標 ===
df['net_flow'] = (df['up_value'] - df['down_value']) / 1e8
df['up_pct'] = df['up_value'] / df['total_value'].replace(0, float('nan')) * 100
df['down_pct'] = df['down_value'] / df['total_value'].replace(0, float('nan')) * 100

# 降採樣到每分鐘
df['snapshot_time'] = pd.to_datetime(df['snapshot_time'])
df = df.set_index('snapshot_time').resample('1min').last().dropna(how='all').reset_index()
df['snapshot_time'] = df['snapshot_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

# === 數值面板 ===
latest = df.iloc[-1]
c1, c2, c3 = st.columns(3)
with c1:
    net = latest['net_flow']
    color = "#e74c3c" if net > 0 else "#27ae60"
    st.markdown(f"""
    <div style="background:#f8f9fa; border-radius:8px; padding:12px; text-align:center;">
        <div style="font-size:28px; font-weight:700; color:{color};">{net:+,.0f} 億</div>
        <div style="font-size:12px; color:#888;">量能淨流入</div>
        <div style="font-size:11px; color:#aaa;">上漲股 - 下跌股 成交金額</div>
    </div>""", unsafe_allow_html=True)
with c2:
    up_pct = latest['up_pct']
    color = "#e74c3c" if up_pct > 50 else "#27ae60"
    st.markdown(f"""
    <div style="background:#f8f9fa; border-radius:8px; padding:12px; text-align:center;">
        <div style="font-size:28px; font-weight:700; color:{color};">{up_pct:.1f}%</div>
        <div style="font-size:12px; color:#888;">上漲股量能佔比</div>
        <div style="font-size:11px; color:#aaa;">上漲股成交金額 / 總成交金額</div>
    </div>""", unsafe_allow_html=True)
with c3:
    down_pct = latest['down_pct']
    color = "#27ae60" if down_pct > 50 else "#e74c3c"
    st.markdown(f"""
    <div style="background:#f8f9fa; border-radius:8px; padding:12px; text-align:center;">
        <div style="font-size:28px; font-weight:700; color:{color};">{down_pct:.1f}%</div>
        <div style="font-size:12px; color:#888;">下跌股量能佔比</div>
        <div style="font-size:11px; color:#aaa;">下跌股成交金額 / 總成交金額</div>
    </div>""", unsafe_allow_html=True)

st.markdown("---")

# === 圖表 1：量能淨流入 長條圖 ===
fig1 = go.Figure()
colors = ['#e74c3c' if v > 0 else '#27ae60' for v in df['net_flow']]
fig1.add_trace(go.Bar(
    x=df['snapshot_time'], y=df['net_flow'],
    marker_color=colors,
    name='量能淨流入',
    hovertemplate='%{x}<br>淨流入: %{y:+,.0f} 億<extra></extra>',
))
fig1.add_hline(y=0, line_color="#888", line_width=1)
add_time_markers(fig1, today)

fig1.update_layout(
    title=dict(text="量能淨流入 趨勢（上漲股 - 下跌股 成交金額，單位：億）", font=dict(size=14)),
    height=400,
    margin=dict(t=80, b=50, l=50, r=30),
    yaxis=dict(title="淨流入（億）"),
    xaxis=dict(title="時間", tickangle=-45),
    plot_bgcolor='white',
    hovermode='x unified',
)
st.plotly_chart(fig1, use_container_width=True)

# === 圖表 2：上漲/下跌股量能佔比 面積圖 ===
fig2 = go.Figure()
fig2.add_trace(go.Scatter(
    x=df['snapshot_time'], y=df['up_pct'],
    name='上漲股量能佔比',
    line=dict(color='#e74c3c', width=2),
    fill='tozeroy', fillcolor='rgba(231,76,60,0.15)',
    mode='lines',
    hovertemplate='上漲股: %{y:.1f}%<extra></extra>',
))
fig2.add_trace(go.Scatter(
    x=df['snapshot_time'], y=df['down_pct'],
    name='下跌股量能佔比',
    line=dict(color='#27ae60', width=2),
    fill='tozeroy', fillcolor='rgba(39,174,96,0.15)',
    mode='lines',
    hovertemplate='下跌股: %{y:.1f}%<extra></extra>',
))
# 50% 平衡線
fig2.add_hline(y=50, line_dash="dash", line_color="#888", line_width=1,
               annotation_text="多空平衡線 50%", annotation_position="bottom right")
add_time_markers(fig2, today)

fig2.update_layout(
    title=dict(text="多空量能佔比 趨勢（上漲股 vs 下跌股 成交金額佔比）", font=dict(size=14)),
    height=400,
    margin=dict(t=80, b=50, l=50, r=30),
    yaxis=dict(title="佔比（%）", range=[0, 100]),
    xaxis=dict(title="時間", tickangle=-45),
    plot_bgcolor='white',
    hovermode='x unified',
)
st.plotly_chart(fig2, use_container_width=True)

st.caption(f"資料日期：{today} | 資料點數：{len(df)} 筆（每分鐘一筆）")
