"""
⚙ 圖表設定頁面
提供「參數設定」和「顯示方式設定」兩個分頁。
設定儲存在 config/chart_settings.json，跨頁共用且重啟後保留。
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
from lib.chart_config import (
    load_chart_settings, save_chart_settings, DEFAULT_CHART_SETTINGS
)
import copy

st.set_page_config(page_title="設定", layout="wide", page_icon="⚙")

st.markdown("""<style>
.setting-section {
    font-size:1.05rem; font-weight:700; color:#2c3e50;
    border-left:4px solid #4A90D9; padding:4px 10px;
    margin:18px 0 10px 0; background:#f8f9fa;
}
.setting-note {
    font-size:0.78rem; color:#888; margin-top:4px;
}
</style>""", unsafe_allow_html=True)

st.markdown("<h2 style='margin:0 0 6px; color:#2c3e50;'>⚙ 圖表設定</h2>", unsafe_allow_html=True)
st.caption("設定會同時儲存到資料庫（雲端持久化）和 `config/chart_settings.json`，容器重啟後仍然保留。修改後切換到歷史收盤指標頁即可看到效果。")

cfg = load_chart_settings()

tab_params, tab_display = st.tabs(["📐 參數設定", "🎨 顯示方式設定"])

# ============================================================
#  Tab A：參數設定
# ============================================================
with tab_params:
    st.markdown('<div class="setting-section">📊 統計帶參數</div>', unsafe_allow_html=True)

    lookback = st.slider(
        "統計回溯天數（lookback）",
        min_value=200, max_value=1500,
        value=int(cfg.get('lookback', 1000)),
        step=50,
        help="統計帶（P10/P25/中位數/P75/P90）基於原始值在近 N 個交易日的分布計算。數字越大，帶越「穩定」但對近期市場環境反應較慢。"
    )
    yr = lookback / 250
    st.caption(f"約 {yr:.1f} 年歷史（台股每年約 250 個交易日）")

    st.markdown('<div class="setting-section">🔲 顯示開關</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        show_bands     = st.checkbox("顯示統計帶（P25-P75 / P10-P90）",
                                     value=bool(cfg['chart'].get('show_bands', True)))
        show_median    = st.checkbox("顯示中位數虛線",
                                     value=bool(cfg['chart'].get('show_median', True)))
    with col2:
        show_p5_p95    = st.checkbox("顯示 P5 / P95 極端參考線",
                                     value=bool(cfg['chart'].get('show_p5_p95', True)))
        show_iqr       = st.checkbox("顯示 IQR 離群值邊界線（Q3+1.5×IQR / Q1-1.5×IQR）",
                                     value=bool(cfg['chart'].get('show_iqr_outlier', True)))

    st.markdown('<div class="setting-note">💡 IQR 離群值邊界：超出此範圍代表數值極端異常，常用於識別市場極端情緒。</div>',
                unsafe_allow_html=True)

    st.markdown("---")
    col_a, col_b, _ = st.columns([1, 1, 3])
    with col_a:
        save_params = st.button("💾 儲存參數設定", use_container_width=True, type="primary")
    with col_b:
        reset_params = st.button("↩ 恢復預設值", use_container_width=True)

    if save_params:
        cfg['lookback'] = lookback
        cfg['chart']['show_bands'] = show_bands
        cfg['chart']['show_median'] = show_median
        cfg['chart']['show_p5_p95'] = show_p5_p95
        cfg['chart']['show_iqr_outlier'] = show_iqr
        save_chart_settings(cfg)
        st.success("✅ 參數設定已儲存！請切換到「歷史收盤指標」頁面查看效果。")

    if reset_params:
        defaults = DEFAULT_CHART_SETTINGS
        cfg['lookback'] = defaults['lookback']
        cfg['chart']['show_bands'] = defaults['chart']['show_bands']
        cfg['chart']['show_median'] = defaults['chart']['show_median']
        cfg['chart']['show_p5_p95'] = defaults['chart']['show_p5_p95']
        cfg['chart']['show_iqr_outlier'] = defaults['chart']['show_iqr_outlier']
        save_chart_settings(cfg)
        st.success("✅ 參數設定已恢復預設值！")
        st.rerun()


# ============================================================
#  Tab B：顯示方式設定
# ============================================================
with tab_display:

    # ── 字型大小 ─────────────────────────────────────────────
    st.markdown('<div class="setting-section">🔤 字型大小</div>', unsafe_allow_html=True)
    fc = cfg['font']
    col1, col2, col3 = st.columns(3)
    with col1:
        title_size = st.slider("圖表標題字型（px）", 10, 28, int(fc.get('title_size', 16)))
        axis_size  = st.slider("軸刻度字型（px）",   6,  18, int(fc.get('axis_tick_size', 11)))
    with col2:
        legend_size = st.slider("圖例字型（px）",    6,  16, int(fc.get('legend_size', 10)))
        card_val    = st.slider("統計卡片數值大小（rem × 10）", 10, 25,
                                int(round(float(fc.get('stat_card_value_rem', 1.6)) * 10)))
    with col3:
        card_lbl = st.slider("統計卡片標籤大小（rem × 10）", 6, 14,
                             int(round(float(fc.get('stat_card_label_rem', 0.82)) * 10)))
        st.markdown(f"""
<div style='margin-top:8px; padding:8px; background:#f0f4f8; border-radius:6px;'>
<span style='font-size:{card_val/10}rem; font-weight:700; color:#2c3e50;'>+12.34</span><br>
<span style='font-size:{card_lbl/10}rem; color:#999;'>指標名稱</span>
</div>""", unsafe_allow_html=True)

    # ── 圖表大小 ─────────────────────────────────────────────
    st.markdown('<div class="setting-section">📐 圖表大小</div>', unsafe_allow_html=True)
    chart_height = st.slider(
        "圖表高度（px）", 250, 700,
        int(cfg['chart'].get('height', 400)),
        step=25,
        help="每張圖表的像素高度。建議 350-500px。"
    )

    # ── 色板 ─────────────────────────────────────────────────
    st.markdown('<div class="setting-section">🎨 色板設定</div>', unsafe_allow_html=True)
    pal = cfg['palette']
    col1, col2, col3 = st.columns(3)
    with col1:
        primary_color  = st.color_picker("主色（單指標圖）",   pal.get('primary', '#DC2626'))
        positive_color = st.color_picker("正向色（上漲/強勢）", pal.get('positive', '#FB923C'))
        median_color   = st.color_picker("中位數線顏色",        pal.get('median_color', '#B8C4CE'))
    with col2:
        negative_color  = st.color_picker("負向色（下跌/弱勢）",   pal.get('negative', '#4ADE80'))
        iqr_color       = st.color_picker("IQR 離群值線顏色",       pal.get('iqr_outlier_color', '#F43F5E'))
        p25p75_color    = st.color_picker("P25/P75 四分位線顏色",   pal.get('p25p75_color', '#7B8FA2'))
        p5p95_color     = st.color_picker("P10/P90 極端線顏色",     pal.get('p5p95_color', '#3D5A73'))
    with col3:
        sma_colors = pal.get('sma_colors', ['#F5C26B', '#4A90D9', '#9B8EC4'])
        sma_c5  = st.color_picker("均線結構：5MA 色",  sma_colors[0] if len(sma_colors) > 0 else '#F5C26B')
        sma_c20 = st.color_picker("均線結構：20MA 色", sma_colors[1] if len(sma_colors) > 1 else '#4A90D9')
        sma_c60 = st.color_picker("均線結構：60MA 色", sma_colors[2] if len(sma_colors) > 2 else '#9B8EC4')

    # 色板預覽
    st.markdown(f"""
<div style='margin:8px 0; padding:10px; background:#fafafa; border:1px solid #e8e8e8; border-radius:6px;
            display:flex; gap:12px; align-items:center; flex-wrap:wrap;'>
  <span style='font-size:0.78rem; color:#999;'>預覽：</span>
  <span style='background:{primary_color}; color:#fff; padding:3px 10px; border-radius:4px; font-size:0.78rem;'>主色</span>
  <span style='background:{positive_color}; color:#fff; padding:3px 10px; border-radius:4px; font-size:0.78rem;'>正向</span>
  <span style='background:{negative_color}; color:#fff; padding:3px 10px; border-radius:4px; font-size:0.78rem;'>負向</span>
  <span style='background:{median_color}; color:#fff; padding:3px 10px; border-radius:4px; font-size:0.78rem;'>中位數</span>
  <span style='background:{p25p75_color}; color:#fff; padding:3px 10px; border-radius:4px; font-size:0.78rem;'>P25/P75</span>
  <span style='background:{p5p95_color}; color:#333; padding:3px 10px; border-radius:4px; font-size:0.78rem;'>P5/P95</span>
  <span style='background:{iqr_color}; color:#fff; padding:3px 10px; border-radius:4px; font-size:0.78rem;'>IQR</span>
  <span style='background:{sma_c5}; color:#fff; padding:3px 10px; border-radius:4px; font-size:0.78rem;'>5MA</span>
  <span style='background:{sma_c20}; color:#fff; padding:3px 10px; border-radius:4px; font-size:0.78rem;'>20MA</span>
  <span style='background:{sma_c60}; color:#fff; padding:3px 10px; border-radius:4px; font-size:0.78rem;'>60MA</span>
</div>""", unsafe_allow_html=True)

    # ── 線條樣式 ─────────────────────────────────────────────
    st.markdown('<div class="setting-section">📏 線條樣式</div>', unsafe_allow_html=True)
    col1, col2, col3 = st.columns(3)
    cc = cfg['chart']
    with col1:
        raw_lw    = st.slider("原始值線寬（px）", 0.5, 4.0,
                               float(cc.get('line_width_raw', 1.5)), step=0.5)
        ma5_lw    = st.slider("5MA 線寬（px）",   1.0, 5.0,
                               float(cc.get('line_width_ma5', 2.5)), step=0.5)
    with col2:
        raw_alpha = st.slider("原始值線透明度（0=全透明，1=不透明）", 0.1, 0.9,
                               float(cc.get('raw_alpha', 0.4)), step=0.05)
        band_outer = st.slider("外帶透明度（P10-P90）", 0.02, 0.3,
                                float(cc.get('band_alpha_outer', 0.08)), step=0.02)
    with col3:
        band_inner = st.slider("內帶透明度（P25-P75）", 0.05, 0.5,
                                float(cc.get('band_alpha_inner', 0.18)), step=0.02)

    # ── 資料表格 ─────────────────────────────────────────────
    st.markdown('<div class="setting-section">📋 資料表格</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        show_all_cols = st.checkbox(
            "顯示全部欄位（~56 個原始欄位）",
            value=bool(cfg['table'].get('show_all_columns', False)),
            help="關閉時只顯示常用的 21 個欄位；開啟時顯示資料庫所有原始欄位（不含衍生統計帶欄位）。"
        )
    with col2:
        tbl_height = st.slider("表格高度（px）", 300, 900,
                                int(cfg['table'].get('height', 480)), step=40)

    st.markdown("---")
    col_a, col_b, _ = st.columns([1, 1, 3])
    with col_a:
        save_display = st.button("💾 儲存顯示設定", use_container_width=True, type="primary")
    with col_b:
        reset_display = st.button("↩ 恢復預設值  ", use_container_width=True)

    if save_display:
        cfg['font']['title_size'] = title_size
        cfg['font']['axis_tick_size'] = axis_size
        cfg['font']['legend_size'] = legend_size
        cfg['font']['stat_card_value_rem'] = card_val / 10.0
        cfg['font']['stat_card_label_rem'] = card_lbl / 10.0
        cfg['chart']['height'] = chart_height
        cfg['chart']['line_width_raw'] = raw_lw
        cfg['chart']['line_width_ma5'] = ma5_lw
        cfg['chart']['raw_alpha'] = raw_alpha
        cfg['chart']['band_alpha_outer'] = band_outer
        cfg['chart']['band_alpha_inner'] = band_inner
        cfg['palette']['primary'] = primary_color
        cfg['palette']['positive'] = positive_color
        cfg['palette']['negative'] = negative_color
        cfg['palette']['median_color'] = median_color
        cfg['palette']['iqr_outlier_color'] = iqr_color
        cfg['palette']['p25p75_color'] = p25p75_color
        cfg['palette']['p5p95_color'] = p5p95_color
        cfg['palette']['sma_colors'] = [sma_c5, sma_c20, sma_c60]
        cfg['table']['show_all_columns'] = show_all_cols
        cfg['table']['height'] = tbl_height
        save_chart_settings(cfg)
        st.success("✅ 顯示設定已儲存！請切換到「歷史收盤指標」頁面查看效果。")

    if reset_display:
        defaults = DEFAULT_CHART_SETTINGS
        cfg['font']    = copy.deepcopy(defaults['font'])
        cfg['chart']['height']          = defaults['chart']['height']
        cfg['chart']['line_width_raw']  = defaults['chart']['line_width_raw']
        cfg['chart']['line_width_ma5']  = defaults['chart']['line_width_ma5']
        cfg['chart']['raw_alpha']       = defaults['chart']['raw_alpha']
        cfg['chart']['band_alpha_outer']= defaults['chart']['band_alpha_outer']
        cfg['chart']['band_alpha_inner']= defaults['chart']['band_alpha_inner']
        cfg['palette'] = copy.deepcopy(defaults['palette'])
        cfg['table']   = copy.deepcopy(defaults['table'])
        save_chart_settings(cfg)
        st.success("✅ 顯示設定已恢復預設值！")
        st.rerun()

# ── 頁尾說明 ─────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<p style='color:#bbb; font-size:0.75rem; text-align:center;'>
設定頁 — 所有設定儲存於資料庫（<code>app_settings</code> 表）及 <code>config/chart_settings.json</code>，雲端容器重啟後仍然保留。<br>
如果切換到歷史頁後畫面沒有更新，請點歷史頁側邊欄的「🔄 重新整理資料」按鈕。
</p>""", unsafe_allow_html=True)
