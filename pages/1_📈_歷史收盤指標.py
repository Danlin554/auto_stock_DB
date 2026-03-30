"""
歷史每日收盤指標 — Phase 4 UI/UX 大改版
- 統一柔和藍色系（副圖3 風格）
- 所有字體從設定檔讀取（預設放大）
- 2D 縮放 + scrollZoom
- 所有圖表加入統計帶 + IQR 離群值線
- legendgroup 點擊隔離（雙線圖）
- 完整統計摘要 expander（中位數/P25/P75/P10/P90/IQR）
- 資料表格支援全欄位顯示
- 快取刷新按鈕 + 資料完整性診斷
"""
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from lib.chart_config import load_chart_settings
from lib.db import get_connection, read_sql

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

st.set_page_config(page_title="歷史收盤指標", layout="wide", page_icon="📈")

# ── 載入設定 ──────────────────────────────────────────────────
_cfg = load_chart_settings()
_font = _cfg['font']
_chart = _cfg['chart']
_pal = _cfg['palette']
_tbl = _cfg['table']

_TITLE_SZ  = int(_font.get('title_size', 16))
_TICK_SZ   = int(_font.get('axis_tick_size', 11))
_LEGEND_SZ = int(_font.get('legend_size', 10))
_CARD_VAL  = float(_font.get('stat_card_value_rem', 1.6))
_CARD_LBL  = float(_font.get('stat_card_label_rem', 0.82))

_CHART_H      = int(_chart.get('height', 400))
_LW_RAW       = float(_chart.get('line_width_raw', 1.5))
_LW_MA5       = float(_chart.get('line_width_ma5', 2.5))
_RAW_ALPHA    = float(_chart.get('raw_alpha', 0.4))
_BAND_OUTER   = float(_chart.get('band_alpha_outer', 0.08))
_BAND_INNER   = float(_chart.get('band_alpha_inner', 0.18))
_SHOW_BANDS   = bool(_chart.get('show_bands', True))
_SHOW_MED     = bool(_chart.get('show_median', True))
_SHOW_P5P95   = bool(_chart.get('show_p5_p95', True))
_SHOW_IQR     = bool(_chart.get('show_iqr_outlier', True))

_PRIMARY  = _pal.get('primary', '#4A90D9')
_POS      = _pal.get('positive', '#E8756C')
_NEG      = _pal.get('negative', '#5BAA8A')
_SMA_COLS = _pal.get('sma_colors', ['#F5C26B', '#4A90D9', '#9B8EC4'])
_MED_COL   = _pal.get('median_color', '#F97316')
_IQR_COL   = _pal.get('iqr_outlier_color', '#EF4444')
_P5P95_COL = _pal.get('p5p95_color', '#8B5CF6')
_P25P75_COL = _pal.get('p25p75_color', '#F59E0B')

PLOTLY_CONFIG = {
    'scrollZoom': True,
    'displayModeBar': True,
    'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
}

# ── 指標主清單（唯一來源）───────────────────────────────────
# 所有指標欄位的完整定義，衍生出 band_cols / CARD_DEFS /
# display_cols / _DIAG_COLS / FULL_COL_LABELS，新增指標只改這裡。
# 欄位說明：
#   label              中文顯示名稱
#   compute_band       是否計算 rolling 統計帶（P5~P95 / IQR）
#   check_integrity    是否納入資料完整性診斷
#   show_card          是否顯示統計摘要卡片（含百分位排名）
#   higher_is_bullish  數值愈高代表偏多（百分位顏色依據）
#   decimals           顯示小數位數
#   suffix             顯示後綴（如 '%'）
#   signed             是否顯示 +/- 號
#   show_in_table      是否納入預設資料表格顯示
INDICATORS = {
    # ── 特殊 ──
    'date':                     {'label': '日期',             'show_in_table': True},
    'filtered_total':           {'label': '篩選總家數'},
    # ── 漲跌計數 ──
    'up_count':                 {'label': '上漲',   'compute_band': True, 'check_integrity': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 0,
                                 'show_in_table': True},
    'down_count':               {'label': '下跌',   'compute_band': True, 'check_integrity': True,
                                 'show_card': True, 'higher_is_bullish': False, 'decimals': 0,
                                 'show_in_table': True},
    'flat_count':               {'label': '持平',   'show_in_table': True},
    'red_k_count':              {'label': '紅K',    'compute_band': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 0,
                                 'show_in_table': True},
    'black_k_count':            {'label': '黑K',    'compute_band': True,
                                 'show_card': True, 'higher_is_bullish': False, 'decimals': 0,
                                 'show_in_table': True},
    'flat_k_count':             {'label': '十字K'},
    'tse_up_count':             {'label': '上市上漲'},
    'otc_up_count':             {'label': '上櫃上漲'},
    # ── 成交量值 ──
    'total_trade_value':        {'label': '成交金額(億)', 'compute_band': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 0,
                                 'show_in_table': True},
    'total_trade_volume':       {'label': '成交量'},
    # ── 核心指數 ──
    'sentiment_index':          {'label': '情緒指數',   'compute_band': True, 'check_integrity': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 2,
                                 'signed': True,   'show_in_table': True},
    'ad_ratio':                 {'label': '多空比',     'compute_band': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 2,
                                 'show_in_table': True},
    'volatility':               {'label': '波動度',     'compute_band': True,
                                 'show_card': True, 'higher_is_bullish': False, 'decimals': 2,
                                 'suffix': '%',    'show_in_table': True},
    'strength_index':           {'label': '強弱勢指數', 'compute_band': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 2,
                                 'suffix': '%', 'signed': True, 'show_in_table': True},
    'activity_rate':            {'label': '活躍度(%)',  'compute_band': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 1,
                                 'suffix': '%',    'show_in_table': True},
    # ── 漲跌幅分佈桶 ──
    'bucket_up_2_5':            {'label': '漲0-2.5%'},
    'bucket_up_5':              {'label': '漲2.5-5%'},
    'bucket_up_7_5':            {'label': '漲5-7.5%'},
    'bucket_up_above':          {'label': '漲>7.5%'},
    'bucket_down_2_5':          {'label': '跌0-2.5%'},
    'bucket_down_5':            {'label': '跌2.5-5%'},
    'bucket_down_7_5':          {'label': '跌5-7.5%'},
    'bucket_down_above':        {'label': '跌>7.5%'},
    # ── 強弱勢計數 ──
    'advantage_count':          {'label': '優勢股數'},
    'strong_count':             {'label': '強勢股',    'compute_band': True,
                                 'higher_is_bullish': True,  'show_in_table': True},
    'super_strong_count':       {'label': '超強勢',   'compute_band': True,
                                 'higher_is_bullish': True},
    'near_limit_up_count':      {'label': '接近漲停', 'compute_band': True,
                                 'higher_is_bullish': True},
    'disadvantage_count':       {'label': '劣勢股數'},
    'weak_count':               {'label': '弱勢股',    'compute_band': True,
                                 'higher_is_bullish': False, 'show_in_table': True},
    'super_weak_count':         {'label': '超弱勢',   'compute_band': True,
                                 'higher_is_bullish': False},
    'near_limit_down_count':    {'label': '接近跌停', 'compute_band': True,
                                 'higher_is_bullish': False},
    # ── 前日強弱勢追蹤 ──
    'prev_strong_count':        {'label': '前日強勢股數'},
    'prev_strong_avg_today':    {'label': '前日強勢今漲幅(%)'},
    'prev_strong_positive_rate':{'label': '前日強勢正報酬率(%)'},
    'prev_weak_count':          {'label': '前日弱勢股數'},
    'prev_weak_avg_today':      {'label': '前日弱勢今跌幅(%)'},
    'prev_weak_negative_rate':  {'label': '前日弱勢負報酬率(%)'},
    # ── 強弱百均漲幅 ──
    'top_n_avg':                {'label': '強百(%)',   'compute_band': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 2,
                                 'suffix': '%', 'signed': True, 'show_in_table': True},
    'bottom_n_avg':             {'label': '弱百(%)',   'compute_band': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 2,
                                 'suffix': '%', 'signed': True, 'show_in_table': True},
    # ── 權值股 ──
    'blue_chip_up_count':       {'label': '權值股上漲'},
    'blue_chip_total':          {'label': '權值股總數'},
    'blue_chip_avg_change':     {'label': '權值股均漲幅(%)', 'compute_band': True,
                                 'higher_is_bullish': True},
    # ── 量能潮汐 ──
    'volume_tide_up_value':     {'label': '上漲量能'},
    'volume_tide_down_value':   {'label': '下跌量能'},
    'volume_tide_net':          {'label': '量能潮汐淨值', 'compute_band': True,
                                 'higher_is_bullish': True},
    'volume_tide_up_pct':       {'label': '上漲量能佔比(%)', 'compute_band': True,
                                 'higher_is_bullish': True},
    'volume_tide_down_pct':     {'label': '下跌量能佔比(%)'},
    # ── 漲幅 >5% ──
    'above_5pct_count':         {'label': '>5%股數',  'compute_band': True,
                                 'check_integrity': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 0,
                                 'show_in_table': True},
    # ── 20日創新高/低 ──
    'new_high_20d_count':       {'label': '20日新高', 'compute_band': True,
                                 'check_integrity': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 0,
                                 'show_in_table': True},
    'new_low_20d_count':        {'label': '20日新低', 'compute_band': True,
                                 'check_integrity': True,
                                 'show_card': True, 'higher_is_bullish': False, 'decimals': 0,
                                 'show_in_table': True},
    # ── 均線結構 ──
    'above_5ma_count':          {'label': '站穩5MA股數'},
    'above_20ma_count':         {'label': '站穩20MA股數'},
    'above_60ma_count':         {'label': '站穩60MA股數'},
    'above_5ma_pct':            {'label': '站穩5MA%',  'compute_band': True,
                                 'check_integrity': True, 'higher_is_bullish': True},
    'above_20ma_pct':           {'label': '站穩20MA%', 'compute_band': True,
                                 'check_integrity': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 1,
                                 'suffix': '%', 'show_in_table': True},
    'above_60ma_pct':           {'label': '站穩60MA%', 'compute_band': True,
                                 'check_integrity': True,
                                 'show_card': True, 'higher_is_bullish': True,  'decimals': 1,
                                 'suffix': '%', 'show_in_table': True},
    # ── 融資 ──
    'margin_maintenance_rate':  {'label': '融資維持率(%)'},
}

# ── 從主清單自動產生衍生清單 ────────────────────────────────
_band_cols = [k for k, v in INDICATORS.items() if v.get('compute_band')]
FULL_COL_LABELS = {k: v['label'] for k, v in INDICATORS.items()}
_DIAG_COLS = [k for k, v in INDICATORS.items() if v.get('check_integrity')]
CARD_DEFS = [
    (k, v['label'], v.get('higher_is_bullish', True),
     v.get('decimals', 2), v.get('suffix', ''), v.get('signed', False))
    for k, v in INDICATORS.items() if v.get('show_card')
]
_DEFAULT_TABLE_COLS = [k for k, v in INDICATORS.items() if v.get('show_in_table')]

# ── CSS（字型從設定讀取）────────────────────────────────────
st.markdown(f"""<style>
.section-hdr {{
    font-size:1.05rem; font-weight:700; color:#2c3e50;
    border-left:4px solid {_PRIMARY}; padding:4px 10px;
    margin:14px 0 8px 0; background:#f8f9fa;
}}
.stat-card {{
    background:#fff; border:1px solid #e0e0e0; border-radius:8px;
    padding:10px 8px; text-align:center;
}}
.stat-val {{ font-size:{_CARD_VAL}rem; font-weight:700; color:#2c3e50; }}
.stat-lbl {{ font-size:{_CARD_LBL}rem; color:#999; margin-top:2px; }}
.stat-sub {{ font-size:0.72rem; margin-top:4px; }}
.stat-range {{ font-size:0.72rem; color:#888; margin-top:3px; }}
.rank-badge {{ display:inline-block; padding:2px 7px; border-radius:12px;
               font-size:0.70rem; font-weight:600; }}
.rank-extreme-low  {{ background:#ffeaea; color:#c0392b; }}
.rank-low          {{ background:#fff3e0; color:#e67e22; }}
.rank-mid          {{ background:#e8f5e9; color:#27ae60; }}
.rank-high         {{ background:#e3f2fd; color:#1565c0; }}
.rank-extreme-high {{ background:#fce4ec; color:#880e4f; }}
</style>""", unsafe_allow_html=True)


# ── 資料載入 ──────────────────────────────────────────────────
@st.cache_data(ttl=1800)
def _load_raw_all():
    try:
        conn = get_connection()
        try:
            df = read_sql("SELECT * FROM daily_closing ORDER BY date ASC", conn)
        finally:
            conn.close()
        return df
    except Exception as e:
        st.error(f"讀取 daily_closing 失敗：{e}")
        return pd.DataFrame()


@st.cache_data(ttl=1800)
def compute_bands(lookback: int):
    """計算所有指標的 rolling 統計帶（5MA、P5~P95、IQR 離群值邊界）。"""
    df = _load_raw_all()
    if df.empty:
        return df

    min_p = max(30, min(50, lookback // 10))
    result = df.copy()

    for col in _band_cols:
        if col not in result.columns:
            continue
        raw = pd.to_numeric(result[col], errors='coerce')
        result[f'{col}_ma5'] = raw.rolling(5, min_periods=1).mean()
        result[f'{col}_p5']  = raw.rolling(lookback, min_periods=min_p).quantile(0.05)
        result[f'{col}_p10'] = raw.rolling(lookback, min_periods=min_p).quantile(0.10)
        result[f'{col}_p25'] = raw.rolling(lookback, min_periods=min_p).quantile(0.25)
        result[f'{col}_p50'] = raw.rolling(lookback, min_periods=min_p).quantile(0.50)
        result[f'{col}_p75'] = raw.rolling(lookback, min_periods=min_p).quantile(0.75)
        result[f'{col}_p90'] = raw.rolling(lookback, min_periods=min_p).quantile(0.90)
        result[f'{col}_p95'] = raw.rolling(lookback, min_periods=min_p).quantile(0.95)
        # IQR 離群值邊界（不需額外 rolling，基於 P25/P75 加減）
        iqr = result[f'{col}_p75'] - result[f'{col}_p25']
        result[f'{col}_iqr_upper'] = result[f'{col}_p75'] + 1.5 * iqr
        result[f'{col}_iqr_lower'] = result[f'{col}_p25'] - 1.5 * iqr

    return result


# ── 輔助函式 ──────────────────────────────────────────────────
def _hex_rgb(h):
    h = h.lstrip('#')
    return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)


def percentile_rank(series, value):
    if series.empty or pd.isna(value):
        return float('nan')
    return round((series < value).sum() / len(series) * 100, 1)


def rank_badge(pct, higher_is_bullish=True):
    if pd.isna(pct):
        return ''
    if higher_is_bullish:
        if pct >= 90:   cls, lbl = 'rank-extreme-high', f'極端偏多({pct:.0f}%)'
        elif pct >= 70: cls, lbl = 'rank-high',         f'偏多({pct:.0f}%)'
        elif pct >= 30: cls, lbl = 'rank-mid',          f'中性({pct:.0f}%)'
        elif pct >= 10: cls, lbl = 'rank-low',          f'偏空({pct:.0f}%)'
        else:           cls, lbl = 'rank-extreme-low',  f'極端偏空({pct:.0f}%)'
    else:
        if pct >= 90:   cls, lbl = 'rank-extreme-low',  f'極端偏空({pct:.0f}%)'
        elif pct >= 70: cls, lbl = 'rank-low',          f'偏空({pct:.0f}%)'
        elif pct >= 30: cls, lbl = 'rank-mid',          f'中性({pct:.0f}%)'
        elif pct >= 10: cls, lbl = 'rank-high',         f'偏多({pct:.0f}%)'
        else:           cls, lbl = 'rank-extreme-high', f'極端偏多({pct:.0f}%)'
    return f'<span class="rank-badge {cls}">{lbl}</span>'


def fmt_val(v, decimals=2, suffix='', signed=False):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return 'N/A'
    fmt = f"{{:{'+'if signed else ''}.{decimals}f}}"
    return fmt.format(float(v)) + suffix


def _add_band(fig, df_p, col, color, legendgroup=None,
              outer_alpha=None, inner_alpha=None):
    """填充 P10-P90 和 P25-P75 統計帶。"""
    if not _SHOW_BANDS:
        return
    r, g, b = _hex_rgb(color)
    oa = outer_alpha if outer_alpha is not None else _BAND_OUTER
    ia = inner_alpha if inner_alpha is not None else _BAND_INNER
    for hi_c, lo_c, alpha in [
        (f'{col}_p90', f'{col}_p10', oa),
        (f'{col}_p75', f'{col}_p25', ia),
    ]:
        if hi_c not in df_p.columns or lo_c not in df_p.columns:
            continue
        mask = df_p[hi_c].notna() & df_p[lo_c].notna()
        if mask.sum() < 2:
            continue
        db = df_p[mask]
        xf = list(db['date']) + list(db['date'])[::-1]
        yf = list(db[hi_c]) + list(db[lo_c])[::-1]
        kw = dict(legendgroup=legendgroup) if legendgroup else {}
        fig.add_trace(go.Scatter(
            x=xf, y=yf, fill='toself',
            fillcolor=f'rgba({r},{g},{b},{alpha})',
            line=dict(color='rgba(0,0,0,0)'),
            hoverinfo='skip', showlegend=False, **kw
        ))


def _base_layout(title, height=None, zero_line=False, y_suffix='', yrange=None):
    h = height or _CHART_H
    layout = dict(
        title=dict(text=title, font=dict(size=_TITLE_SZ), y=0.97, yanchor='top'),
        height=h,
        margin=dict(t=44, b=42, l=58, r=20),
        hovermode='x unified',
        dragmode='pan',
        plot_bgcolor='white', paper_bgcolor='white',
        xaxis=dict(
            tickfont=dict(size=_TICK_SZ), tickangle=-30,
            showgrid=False, fixedrange=False,
            rangeselector=dict(
                buttons=[
                    dict(count=100, label="100天", step="day", stepmode="backward"),
                    dict(count=180, label="180天", step="day", stepmode="backward"),
                    dict(count=1,   label="1年",   step="year", stepmode="backward"),
                    dict(step="all", label="全部"),
                ],
                bgcolor='rgba(240,242,246,0.9)',
                activecolor='#3B82C4',
                font=dict(size=10),
                x=0, y=1.02,
            ),
        ),
        yaxis=dict(
            tickfont=dict(size=_TICK_SZ), showgrid=True,
            gridcolor='#f0f0f0', ticksuffix=y_suffix,
            fixedrange=False,
        ),
        legend=dict(
            orientation='h', yanchor='bottom', y=1.01,
            xanchor='right', x=1, font=dict(size=_LEGEND_SZ),
        ),
    )
    if yrange:
        layout['yaxis']['range'] = yrange
    return layout


def make_chart(df_p, col, label, height=None, zero_line=False,
               hover_fmt='.2f', y_suffix='', color=None):
    """單指標圖：原始值 + 5MA + rolling 統計帶 + IQR 線"""
    if col not in df_p.columns:
        return None
    clr = color or _PRIMARY
    raw = pd.to_numeric(df_p[col], errors='coerce')
    ma5 = df_p.get(f'{col}_ma5', raw)
    r, g, b = _hex_rgb(clr)

    fig = go.Figure()
    _add_band(fig, df_p, col, clr)

    # 中位數虛線（加粗）
    if _SHOW_MED and f'{col}_p50' in df_p.columns:
        fig.add_trace(go.Scatter(
            x=df_p['date'], y=df_p[f'{col}_p50'], mode='lines',
            line=dict(color=_MED_COL, width=2.5, dash='dash'),
            name='中位數(P50)', showlegend=False,
            hovertemplate=f'中位數(P50): %{{y:{hover_fmt}}}<extra></extra>'
        ))

    # P25/P75 四分位線（琥珀色）
    p25p75r, p25p75g, p25p75b = _hex_rgb(_P25P75_COL)
    for p_c, p_lbl in [(f'{col}_p75', 'P75'), (f'{col}_p25', 'P25')]:
        if p_c in df_p.columns:
            fig.add_trace(go.Scatter(
                x=df_p['date'], y=df_p[p_c], mode='lines',
                line=dict(color=f'rgba({p25p75r},{p25p75g},{p25p75b},0.85)', width=1.5, dash='dash'),
                name=p_lbl, showlegend=False,
                hovertemplate=f'{p_lbl}: %{{y:{hover_fmt}}}<extra></extra>'
            ))

    # P95/P5 極端分位線（紫色）
    if _SHOW_P5P95:
        pr, pg, pb = _hex_rgb(_P5P95_COL)
        for p_c, p_lbl in [(f'{col}_p95', 'P95'), (f'{col}_p5', 'P5')]:
            if p_c in df_p.columns:
                fig.add_trace(go.Scatter(
                    x=df_p['date'], y=df_p[p_c], mode='lines',
                    line=dict(color=f'rgba({pr},{pg},{pb},0.85)', width=1.5, dash='dot'),
                    name=p_lbl, showlegend=False,
                    hovertemplate=f'{p_lbl}: %{{y:{hover_fmt}}}<extra></extra>'
                ))

    # IQR 上界（多方警戒線，紅色）
    if _SHOW_IQR and f'{col}_iqr_upper' in df_p.columns:
        fig.add_trace(go.Scatter(
            x=df_p['date'], y=df_p[f'{col}_iqr_upper'], mode='lines',
            line=dict(color=_IQR_COL, width=1.8, dash='dashdot'),
            name='IQR上界', showlegend=False,
            hovertemplate=f'IQR上界: %{{y:{hover_fmt}}}<extra></extra>'
        ))

    # 原始值（半透明細線）
    fig.add_trace(go.Scatter(
        x=df_p['date'], y=raw, mode='lines', name=label,
        line=dict(color=f'rgba({r},{g},{b},{_RAW_ALPHA})', width=_LW_RAW),
        hovertemplate=f'{label}: %{{y:{hover_fmt}}}<extra></extra>'
    ))

    # 5MA 主線
    fig.add_trace(go.Scatter(
        x=df_p['date'], y=ma5, mode='lines', name=f'{label} 5MA',
        line=dict(color=clr, width=_LW_MA5),
        hovertemplate=f'5MA: %{{y:{hover_fmt}}}<extra></extra>'
    ))

    if zero_line:
        fig.add_hline(y=0, line_color='#ccc', line_width=1)

    fig.update_layout(**_base_layout(label, height, y_suffix=y_suffix))
    return fig


def make_dual_chart(df_p, col1, label1, col2, label2,
                    height=None, zero_line=False, hover_fmt='.0f',
                    color1=None, color2=None):
    """
    雙線圖：兩指標各自有原始值 + 5MA + 統計帶。
    使用 legendgroup 支援點擊隔離（點圖例可只看單指標）。
    """
    clr1 = color1 or _POS
    clr2 = color2 or _NEG
    fig = go.Figure()

    for col, label, clr in [(col1, label1, clr1), (col2, label2, clr2)]:
        if col not in df_p.columns:
            continue
        raw = pd.to_numeric(df_p[col], errors='coerce')
        ma5 = df_p.get(f'{col}_ma5', raw)
        r, g, b = _hex_rgb(clr)
        grp = f'grp_{col}'

        # 統計帶（預設極低 alpha，隔離後才明顯）
        _add_band(fig, df_p, col, clr, legendgroup=grp,
                  outer_alpha=_BAND_OUTER * 0.5, inner_alpha=_BAND_INNER * 0.5)

        # 中位數（加粗）
        if _SHOW_MED and f'{col}_p50' in df_p.columns:
            fig.add_trace(go.Scatter(
                x=df_p['date'], y=df_p[f'{col}_p50'], mode='lines',
                line=dict(color=_MED_COL, width=2.0, dash='dash'),
                legendgroup=grp, name=f'{label} 中位數', showlegend=False,
                hovertemplate=f'{label}中位數: %{{y:{hover_fmt}}}<extra></extra>'
            ))

        # P25/P75 四分位線
        p25p75r, p25p75g, p25p75b = _hex_rgb(_P25P75_COL)
        for p_c, p_lbl in [(f'{col}_p75', 'P75'), (f'{col}_p25', 'P25')]:
            if p_c in df_p.columns:
                fig.add_trace(go.Scatter(
                    x=df_p['date'], y=df_p[p_c], mode='lines',
                    line=dict(color=f'rgba({p25p75r},{p25p75g},{p25p75b},0.7)',
                              width=1.2, dash='dash'),
                    legendgroup=grp, name=f'{label} {p_lbl}', showlegend=False,
                    hovertemplate=f'{label}{p_lbl}: %{{y:{hover_fmt}}}<extra></extra>'
                ))

        # 原始值
        fig.add_trace(go.Scatter(
            x=df_p['date'], y=raw, mode='lines', name=label,
            line=dict(color=f'rgba({r},{g},{b},{_RAW_ALPHA})', width=_LW_RAW),
            legendgroup=grp, legendgrouptitle=dict(text=''),
            hovertemplate=f'{label}: %{{y:{hover_fmt}}}<extra></extra>'
        ))

        # 5MA
        fig.add_trace(go.Scatter(
            x=df_p['date'], y=ma5, mode='lines', name=f'{label} 5MA',
            line=dict(color=clr, width=_LW_MA5),
            legendgroup=grp,
            hovertemplate=f'{label}5MA: %{{y:{hover_fmt}}}<extra></extra>'
        ))

    if zero_line:
        fig.add_hline(y=0, line_color='#ccc', line_width=1)

    layout = _base_layout(f'{label1} / {label2}', height)
    layout['legend']['groupclick'] = 'togglegroup'
    fig.update_layout(**layout)
    return fig


def make_new_high_low_chart(df_p, height=None):
    """20日創新高/新低 — 雙向柱狀圖 + P25/P50/P75 水平參考線"""
    r1, g1, b1 = _hex_rgb(_POS)
    r2, g2, b2 = _hex_rgb(_NEG)
    fig = go.Figure()

    if 'new_high_20d_count' in df_p.columns:
        raw_h = pd.to_numeric(df_p['new_high_20d_count'], errors='coerce')
        ma5_h = pd.to_numeric(df_p.get('new_high_20d_count_ma5', raw_h), errors='coerce')
        fig.add_trace(go.Bar(
            x=df_p['date'], y=raw_h,
            name='20日新高', marker_color=f'rgba({r1},{g1},{b1},0.45)',
            hovertemplate='新高: %{y:.0f}<extra></extra>'
        ))
        fig.add_trace(go.Scatter(
            x=df_p['date'], y=ma5_h, mode='lines', name='新高 5MA',
            line=dict(color=_POS, width=2),
            hovertemplate='新高5MA: %{y:.0f}<extra></extra>'
        ))
        # P50/P75 水平參考線
        if 'new_high_20d_count_p50' in df_p.columns:
            p50_val = df_p['new_high_20d_count_p50'].dropna()
            if not p50_val.empty:
                fig.add_hline(y=float(p50_val.iloc[-1]),
                              line=dict(color=f'rgba({r1},{g1},{b1},0.5)', width=1, dash='dash'),
                              annotation_text=f'新高中位 {float(p50_val.iloc[-1]):.0f}',
                              annotation_font_size=9)
        if 'new_high_20d_count_p75' in df_p.columns:
            p75_val = df_p['new_high_20d_count_p75'].dropna()
            if not p75_val.empty:
                fig.add_hline(y=float(p75_val.iloc[-1]),
                              line=dict(color=f'rgba({r1},{g1},{b1},0.3)', width=1, dash='dot'),
                              annotation_text=f'新高P75 {float(p75_val.iloc[-1]):.0f}',
                              annotation_font_size=9)

    if 'new_low_20d_count' in df_p.columns:
        raw_l = pd.to_numeric(df_p['new_low_20d_count'], errors='coerce')
        ma5_l = pd.to_numeric(df_p.get('new_low_20d_count_ma5', raw_l), errors='coerce')
        fig.add_trace(go.Bar(
            x=df_p['date'], y=-raw_l,
            name='20日新低', marker_color=f'rgba({r2},{g2},{b2},0.45)',
            customdata=raw_l,
            hovertemplate='新低: %{customdata:.0f}<extra></extra>'
        ))
        fig.add_trace(go.Scatter(
            x=df_p['date'], y=-ma5_l, mode='lines', name='新低 5MA',
            line=dict(color=_NEG, width=2),
            customdata=ma5_l,
            hovertemplate='新低5MA: %{customdata:.0f}<extra></extra>'
        ))

    fig.add_hline(y=0, line_color='#888', line_width=1)
    layout = _base_layout('20日創新高/新低家數', height)
    layout['barmode'] = 'relative'
    fig.update_layout(**layout)
    return fig


def make_sma_structure_chart(df_p, height=None):
    """均線結構（站穩各均線的股票比例 %）+ 統計帶"""
    fig = go.Figure()
    configs = [
        ('above_5ma_pct',  '站穩5MA',  _SMA_COLS[0] if len(_SMA_COLS) > 0 else '#F5C26B'),
        ('above_20ma_pct', '站穩20MA', _SMA_COLS[1] if len(_SMA_COLS) > 1 else '#4A90D9'),
        ('above_60ma_pct', '站穩60MA', _SMA_COLS[2] if len(_SMA_COLS) > 2 else '#9B8EC4'),
    ]
    for col, label, clr in configs:
        if col not in df_p.columns:
            continue
        raw = pd.to_numeric(df_p[col], errors='coerce')
        ma5 = pd.to_numeric(df_p.get(f'{col}_ma5', raw), errors='coerce')
        r, g, b = _hex_rgb(clr)
        grp = f'grp_{col}'

        # 統計帶（預設低 alpha）
        _add_band(fig, df_p, col, clr, legendgroup=grp,
                  outer_alpha=_BAND_OUTER * 0.4, inner_alpha=_BAND_INNER * 0.4)

        fig.add_trace(go.Scatter(
            x=df_p['date'], y=raw, mode='lines', name=label,
            line=dict(color=f'rgba({r},{g},{b},{_RAW_ALPHA})', width=_LW_RAW),
            legendgroup=grp,
            hovertemplate=f'{label}: %{{y:.1f}}%<extra></extra>'
        ))
        fig.add_trace(go.Scatter(
            x=df_p['date'], y=ma5, mode='lines', name=f'{label} 5MA',
            line=dict(color=clr, width=_LW_MA5),
            legendgroup=grp,
            hovertemplate=f'{label}5MA: %{{y:.1f}}%<extra></extra>'
        ))

    fig.add_hline(y=50, line_color='#999', line_width=1, line_dash='dash')
    layout = _base_layout('全市場均線結構統計', height, y_suffix='%', yrange=[0, 100])
    layout['legend']['groupclick'] = 'togglegroup'
    fig.update_layout(**layout)
    return fig


def stat_card(col, label, higher_bullish, decimals, suffix, latest, df_all_col, signed=False):
    val = latest.get(col) if hasattr(latest, 'get') else None
    if val is None:
        try:
            val = latest[col]
        except Exception:
            val = None

    display_val = val
    if col == 'total_trade_value' and val is not None:
        display_val = round(float(val) / 1e8, 0)

    pct = percentile_rank(df_all_col.dropna(), display_val) if df_all_col is not None else float('nan')
    badge = rank_badge(pct, higher_bullish)
    val_str = fmt_val(display_val, decimals, suffix, signed)

    # P25-P75 區間
    p25_val = latest.get(f'{col}_p25') if hasattr(latest, 'get') else None
    p75_val = latest.get(f'{col}_p75') if hasattr(latest, 'get') else None
    if p25_val is None:
        try: p25_val = latest[f'{col}_p25']
        except (KeyError, TypeError): pass
    if p75_val is None:
        try: p75_val = latest[f'{col}_p75']
        except (KeyError, TypeError): pass

    range_html = ''
    if p25_val is not None and p75_val is not None and not pd.isna(p25_val) and not pd.isna(p75_val):
        if col == 'total_trade_value':
            p25_v = round(float(p25_val) / 1e8, 0)
            p75_v = round(float(p75_val) / 1e8, 0)
        else:
            p25_v, p75_v = float(p25_val), float(p75_val)
        p25_s = fmt_val(p25_v, decimals, suffix)
        p75_s = fmt_val(p75_v, decimals, suffix)
        range_html = f'<div class="stat-range">P25~P75: {p25_s}~{p75_s}</div>'

    st.markdown(
        f'<div class="stat-card">'
        f'<div class="stat-val">{val_str}</div>'
        f'<div class="stat-lbl">{label}</div>'
        f'<div class="stat-sub">{badge}</div>'
        f'{range_html}'
        f'</div>', unsafe_allow_html=True
    )


# ============================================================
#  Sidebar 控制項
# ============================================================
if 'hist_start' not in st.session_state:
    st.session_state.hist_start = date(2020, 1, 1)
if 'hist_end' not in st.session_state:
    st.session_state.hist_end = date.today()

# 如果 hist_end 遠落後於今天（超過 30 天），自動重設為今天
if (date.today() - st.session_state.hist_end).days > 30:
    st.session_state.hist_end = date.today()

with st.sidebar:
    st.markdown("### 📅 日期範圍")
    c1, c2 = st.columns(2)
    with c1:
        if st.button("近30天",  use_container_width=True):
            st.session_state.hist_start = date.today() - timedelta(days=30)
            st.session_state.hist_end   = date.today()
        if st.button("近3月",   use_container_width=True):
            st.session_state.hist_start = date.today() - timedelta(days=90)
            st.session_state.hist_end   = date.today()
        if st.button("近1年",   use_container_width=True):
            st.session_state.hist_start = date.today() - timedelta(days=365)
            st.session_state.hist_end   = date.today()
    with c2:
        if st.button("近100天", use_container_width=True):
            st.session_state.hist_start = date.today() - timedelta(days=100)
            st.session_state.hist_end   = date.today()
        if st.button("近180天", use_container_width=True):
            st.session_state.hist_start = date.today() - timedelta(days=180)
            st.session_state.hist_end   = date.today()
        if st.button("全部",    use_container_width=True):
            st.session_state.hist_start = date(2020, 1, 1)
            st.session_state.hist_end   = date.today()

    start_input = st.date_input("起始日期", value=st.session_state.hist_start)
    end_input   = st.date_input("結束日期", value=st.session_state.hist_end)
    st.session_state.hist_start = start_input
    st.session_state.hist_end   = end_input

    st.markdown("---")
    st.markdown("### 📏 圖表高度")
    chart_height = st.slider(
        "圖表高度 (px)", min_value=250, max_value=800,
        value=_CHART_H, step=25,
        help="調整每個圖表的上下高度"
    )

    st.markdown("---")
    st.markdown("### 📊 統計帶設定")
    # 從設定檔讀取 lookback 預設值
    _default_lookback = int(_cfg.get('lookback', 1000))
    lookback = st.slider(
        "統計回溯天數", min_value=200, max_value=1500,
        value=_default_lookback, step=50,
        help="統計帶基於「原始值」在近 N 個交易日的分布計算。可在⚙設定頁面變更預設值。"
    )
    yr_approx = lookback / 250
    st.caption(f"約 {yr_approx:.1f} 年歷史")

    st.markdown("---")
    if st.button("🔄 重新整理資料", use_container_width=True,
                 help="清除快取，強制從資料庫重新載入最新資料"):
        _load_raw_all.clear()
        compute_bands.clear()
        st.rerun()

start_str = start_input.strftime('%Y-%m-%d')
end_str   = end_input.strftime('%Y-%m-%d')

# ── 載入資料 ──────────────────────────────────────────────────
with st.spinner("計算統計帶中..."):
    df_full = compute_bands(lookback)

if df_full.empty:
    st.warning("⚠ 尚無歷史收盤指標資料。請先執行 `venv/bin/python backfill_history.py` 進行回填。")
    st.stop()

df = df_full[(df_full['date'] >= start_str) & (df_full['date'] <= end_str)].copy()
if df.empty:
    st.warning(f"所選日期範圍（{start_str} ~ {end_str}）內無資料，請調整日期。")
    st.stop()

latest      = df.iloc[-1]
latest_date = latest['date']
db_max_date = df_full['date'].max()

# ── 頁面標題 ──────────────────────────────────────────────────
st.markdown(
    "<h2 style='margin:0 0 4px; color:#2c3e50;'>📈 歷史每日收盤指標</h2>",
    unsafe_allow_html=True
)
st.caption(
    f"最新資料：**{latest_date}** | 顯示 **{len(df)}** 個交易日（{start_str} ~ {end_str}）"
    f" | 資料庫最新日：**{db_max_date}** | 統計帶回溯：**{lookback}** 天 | 歷史總筆數：**{len(df_full)}** 天"
)

# ── 資料完整性診斷（可展開）────────────────────────────────
_truncated = []
for _c in _DIAG_COLS:
    if _c in df_full.columns:
        _last = df_full[df_full[_c].notna()]['date'].max()
        if _last and _last < db_max_date:
            _truncated.append((_c, _last))

if _truncated:
    with st.expander(f"⚠ 資料完整性異常（{len(_truncated)} 個欄位有截斷）— 點此查看"):
        for _c, _last in _truncated:
            lbl = FULL_COL_LABELS.get(_c, _c)
            st.warning(f"**{lbl}** (`{_c}`)：資料只到 `{_last}`，資料庫最新為 `{db_max_date}`")
        st.info("解決方法：在側邊欄點「🔄 重新整理資料」，或重新執行 `venv/bin/python backfill_history.py --fill-rolling`")
else:
    pass  # 正常情況不顯示任何提示

# ============================================================
#  統計摘要卡片
# ============================================================
st.markdown('<div class="section-hdr">📊 今日數值 vs 歷史百分位</div>',
            unsafe_allow_html=True)

COLS_PER_ROW = 5
# 使用含統計帶欄位的 df_full（compute_bands 結果）做百分位計算
_latest_full = df_full.iloc[-1]

for row_start in range(0, len(CARD_DEFS), COLS_PER_ROW):
    row_defs = CARD_DEFS[row_start:row_start + COLS_PER_ROW]
    row_cols = st.columns(len(row_defs))
    for ci, (col, label, higher_b, dec, sfx, sgn) in enumerate(row_defs):
        with row_cols[ci]:
            series_for_rank = df_full[col] if col in df_full.columns else pd.Series(dtype=float)
            if col == 'total_trade_value' and not series_for_rank.empty:
                series_for_rank = series_for_rank / 1e8
            stat_card(col, label, higher_b, dec, sfx, _latest_full, series_for_rank, signed=sgn)

# ── 完整統計摘要 expander ────────────────────────────────────
with st.expander("📊 完整統計摘要（中位數 / P25~P75 / P10~P90 / IQR 離群值）", expanded=False):
    _stats_rows = []
    for col, label, _, dec, sfx, _ in CARD_DEFS:
        if col not in df_full.columns:
            continue
        _scale = 1e8 if col == 'total_trade_value' else 1.0
        def _f(key):
            v = _latest_full.get(key) if hasattr(_latest_full, 'get') else None
            if v is None:
                try: v = _latest_full[key]
                except (KeyError, TypeError): pass
            return fmt_val(float(v) / _scale if v is not None and not pd.isna(v) else v,
                           dec, sfx) if v is not None and not pd.isna(v) else 'N/A'
        _stats_rows.append({
            '指標': label,
            '最新值': _f(col),
            'P10': _f(f'{col}_p10'),
            'P25': _f(f'{col}_p25'),
            '中位數': _f(f'{col}_p50'),
            'P75': _f(f'{col}_p75'),
            'P90': _f(f'{col}_p90'),
            'IQR上界': _f(f'{col}_iqr_upper'),
            'IQR下界': _f(f'{col}_iqr_lower'),
        })
    if _stats_rows:
        st.dataframe(pd.DataFrame(_stats_rows), use_container_width=True, hide_index=True)
    st.caption(f"統計值基於完整歷史資料（全 {len(df_full)} 天），使用原始值（非 5MA）計算。"
               "IQR 上/下界 = Q3±1.5×IQR，超出此範圍屬統計上的極端值。")

# ============================================================
#  指標趨勢圖（Tabs 分組）
# ============================================================
st.markdown('<div class="section-hdr">📉 歷史趨勢圖（含 5MA 平滑線 + 統計帶）</div>',
            unsafe_allow_html=True)
st.caption(
    "💡 操作說明：框選矩形可放大（X/Y 雙向）；滾輪縮放；雙擊恢復全覽。"
    "雙線圖：點擊圖例可切換顯示，**點擊圖例項目**後再點其他可隔離單線查看其統計帶。"
)

tab1, tab2, tab3, tab4 = st.tabs(["😊 情緒面", "💪 強弱面", "⚡ 動能面", "🏗 結構面"])

# ──────────────────────────────────────────────────────────
with tab1:
    for fig in [
        make_chart(df, 'sentiment_index', '情緒指數', height=chart_height, zero_line=True, hover_fmt='.2f'),
        make_chart(df, 'ad_ratio', '多空比（上漲/下跌）', height=chart_height, hover_fmt='.3f'),
        make_dual_chart(df, 'up_count', '上漲家數', 'down_count', '下跌家數', height=chart_height, hover_fmt='.0f'),
        make_dual_chart(df, 'red_k_count', '紅K家數', 'black_k_count', '黑K家數', height=chart_height, hover_fmt='.0f'),
    ]:
        if fig:
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

# ──────────────────────────────────────────────────────────
with tab2:
    for fig in [
        make_chart(df, 'strength_index', '強弱勢漲跌幅指數',
                   height=chart_height, zero_line=True, hover_fmt='.2f', y_suffix='%'),
        make_chart(df, 'volatility', '波動度（漲跌幅標準差）',
                   height=chart_height, hover_fmt='.2f', y_suffix='%'),
        make_chart(df, 'activity_rate', '活躍度（強弱勢家數/總家數）',
                   height=chart_height, hover_fmt='.1f', y_suffix='%'),
        make_dual_chart(df, 'strong_count', '強勢家數(≥3%)',
                        'weak_count', '弱勢家數(≤-3%)', height=chart_height, hover_fmt='.0f'),
        make_chart(df, 'above_5pct_count', '漲幅>5% 家數', height=chart_height, hover_fmt='.0f'),
    ]:
        if fig:
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    if 'above_5pct_count' not in df.columns:
        st.info("「漲幅>5%家數」尚無資料，請執行：`venv/bin/python backfill_history.py --fill-rolling`")

# ──────────────────────────────────────────────────────────
with tab3:
    # 成交金額需要換算為億元
    c_val = df.copy()
    if 'total_trade_value' in c_val.columns:
        c_val['total_trade_value'] = pd.to_numeric(c_val['total_trade_value'], errors='coerce') / 1e8
        for sfx in ['_ma5', '_p5', '_p10', '_p25', '_p50', '_p75', '_p90', '_p95',
                    '_iqr_upper', '_iqr_lower']:
            k = f'total_trade_value{sfx}'
            if k in c_val.columns:
                c_val[k] = pd.to_numeric(c_val[k], errors='coerce') / 1e8

    for fig in [
        make_chart(c_val, 'total_trade_value', '全市場成交金額（億元）',
                   height=chart_height, hover_fmt='.0f', y_suffix='億'),
        make_chart(df, 'top_n_avg', '強勢百均漲幅（前100強平均）',
                   height=chart_height, zero_line=True, hover_fmt='.2f', y_suffix='%'),
        make_chart(df, 'bottom_n_avg', '弱勢百均跌幅（後100弱平均）',
                   height=chart_height, zero_line=True, hover_fmt='.2f', y_suffix='%'),
        make_chart(df, 'volume_tide_net', '量能潮汐淨值（億）',
                   height=chart_height, zero_line=True, hover_fmt='.1f', y_suffix='億'),
        make_chart(df, 'volume_tide_up_pct', '上漲量能佔比',
                   height=chart_height, hover_fmt='.1f', y_suffix='%'),
    ]:
        if fig:
            st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

# ──────────────────────────────────────────────────────────
with tab4:
    # 20日創新高/新低
    all_null_hl = ('new_high_20d_count' not in df.columns or
                   df['new_high_20d_count'].isna().all())
    if not all_null_hl:
        fig = make_new_high_low_chart(df, height=chart_height)
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
    else:
        st.info("「20日創新高/新低」尚無資料，請執行：`venv/bin/python backfill_history.py --fill-rolling`")

    # 均線結構
    all_null_sma = ('above_20ma_pct' not in df.columns or
                    df['above_20ma_pct'].isna().all())
    if not all_null_sma:
        fig = make_sma_structure_chart(df, height=chart_height)
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)
    else:
        st.info("「均線結構統計」尚無資料，請執行：`venv/bin/python backfill_history.py --fill-rolling`")

    # 超強/超弱勢家數
    fig = make_dual_chart(
        df, 'super_strong_count', '超強勢家數(≥7.5%)',
            'super_weak_count', '超弱勢家數(≤-7.5%)', height=chart_height, hover_fmt='.0f'
    )
    if fig:
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    fig = make_dual_chart(
        df, 'near_limit_up_count', '接近漲停家數(≥9.5%)',
            'near_limit_down_count', '接近跌停家數(≤-9.5%)', height=chart_height, hover_fmt='.0f'
    )
    if fig:
        st.plotly_chart(fig, use_container_width=True, config=PLOTLY_CONFIG)

    # 大盤融資維持率（佔位）
    st.info("📌 **大盤融資維持率**：資料來源研究中，預計未來版本加入。"
            "（參考閾值：樂觀警戒 170% / 斷頭警戒 140%）")

# ============================================================
#  歷史摘要表格
# ============================================================
st.markdown('<div class="section-hdr">📋 歷史每日收盤指標表</div>',
            unsafe_allow_html=True)

# 衍生欄位後綴（排除用）
_BAND_SFXS = ('_ma5','_p5','_p10','_p25','_p50','_p75','_p90','_p95',
               '_iqr_upper','_iqr_lower')

if _tbl.get('show_all_columns', False):
    # 全欄位模式：排除衍生統計帶欄位和 id
    _all_raw = [c for c in df.columns
                if not any(c.endswith(s) for s in _BAND_SFXS) and c != 'id']
    display_cols = _all_raw
else:
    display_cols = [c for c in _DEFAULT_TABLE_COLS if c in df.columns]

tbl_cols = [c for c in display_cols if c in df.columns]

# 欄位篩選器
_with_labels = [(c, FULL_COL_LABELS.get(c, c)) for c in tbl_cols]
_default_sel = tbl_cols[:min(15, len(tbl_cols))]
sel_cols = st.multiselect(
    "選擇顯示欄位（可自訂）",
    options=tbl_cols,
    default=_default_sel,
    format_func=lambda c: FULL_COL_LABELS.get(c, c),
)
if not sel_cols:
    sel_cols = tbl_cols

df_table = df[sel_cols].copy().sort_values('date', ascending=False)
if 'total_trade_value' in df_table.columns:
    df_table['total_trade_value'] = (
        pd.to_numeric(df_table['total_trade_value'], errors='coerce') / 1e8
    ).round(0)
df_table.columns = [FULL_COL_LABELS.get(c, c) for c in sel_cols]

_fmt = {
    '情緒指數': '{:+.2f}', '多空比': '{:.3f}', '波動度': '{:.2f}%',
    '強弱勢指數': '{:+.2f}%', '活躍度(%)': '{:.1f}%',
    '強百(%)': '{:+.2f}%', '弱百(%)': '{:+.2f}%',
    '站穩20MA%': '{:.1f}%', '站穩60MA%': '{:.1f}%',
    '站穩5MA%': '{:.1f}%', '成交金額': '{:.0f}億',
}

st.dataframe(
    df_table.style.format(_fmt, na_rep='N/A'),
    use_container_width=True,
    height=int(_tbl.get('height', 480)),
)

csv_bytes = df_table.to_csv(index=False).encode('utf-8-sig')
st.download_button(
    label="📥 下載 CSV",
    data=csv_bytes,
    file_name=f"daily_closing_{start_str}_{end_str}.csv",
    mime='text/csv',
)

# ── 頁尾 ──────────────────────────────────────────────────
st.markdown(
    "<hr><p style='text-align:center; color:#bbb; font-size:0.70rem;'>"
    "歷史收盤指標 | 資料來源：富邦 API（盤中）+ 證交所/櫃買中心 OHLCV API（歷史回填）<br>"
    "參考線：灰虛線=中位數，金點線=P5/P95，紅點虛線=IQR離群值邊界<br>"
    "⚙ 字型/色板/高度等顯示設定請至「設定」頁面調整</p>",
    unsafe_allow_html=True
)
