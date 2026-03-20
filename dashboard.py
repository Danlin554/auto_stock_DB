"""
盤中情緒監控系統 - Streamlit 儀表板
執行方式：cd /mnt/c/Users/User/Desktop/FB-Market && venv/bin/streamlit run dashboard.py
"""
import os
import sqlite3
import json
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
import streamlit as st

# === 路徑設定 ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'data', 'market.db')
SETTINGS_PATH = os.path.join(BASE_DIR, 'config', 'settings.json')
BLUE_CHIPS_PATH = os.path.join(BASE_DIR, 'config', 'blue_chips.csv')
TSE_TOP20_PATH = os.path.join(BASE_DIR, 'config', 'tse_top20.csv')
OTC_TOP20_PATH = os.path.join(BASE_DIR, 'config', 'otc_top20.csv')
STOCK_NAMES_PATH = os.path.join(BASE_DIR, 'stock_names.json')


# === 設定檔讀寫 ===

SETTINGS_ERROR_KEY = "_settings_load_error"
DEFAULT_SETTINGS = {
    "volume_filter": 0,
    "bucket_tiers": [2.5, 5.0, 7.5],
    "flat_threshold": 1.0,
    "limit_threshold": 9.5,
    "indicator_strong": 3.0,
    "indicator_super_strong": 7.5,
    "continuity_threshold": 5.0,
    "top_bottom_n": 100,
    "refresh_interval": 60,
    "high_price_threshold": 300,
    "font_base": 16,
    "chart_font_base": 10,
    "chart_height_base": 340,
}


def _default_settings():
    data = dict(DEFAULT_SETTINGS)
    data["bucket_tiers"] = list(DEFAULT_SETTINGS["bucket_tiers"])
    return data


def _normalize_bucket_tiers(tiers):
    defaults = list(DEFAULT_SETTINGS["bucket_tiers"])
    if not isinstance(tiers, (list, tuple)):
        return defaults

    parsed = []
    for value in tiers:
        try:
            fval = float(value)
        except (TypeError, ValueError):
            continue
        if fval > 0:
            parsed.append(fval)

    if len(parsed) < 3:
        return defaults

    parsed = sorted(parsed)
    return parsed[:3]


def _to_int(value, default, min_value=None):
    try:
        num = int(value)
    except (TypeError, ValueError):
        num = int(default)
    if min_value is not None:
        num = max(min_value, num)
    return num


def _to_float(value, default, min_value=None):
    try:
        num = float(value)
    except (TypeError, ValueError):
        num = float(default)
    if min_value is not None:
        num = max(min_value, num)
    return num


def _normalize_settings(raw):
    settings = _default_settings()
    if isinstance(raw, dict):
        settings.update(raw)

    settings["volume_filter"] = _to_int(settings.get("volume_filter"), DEFAULT_SETTINGS["volume_filter"], min_value=0)
    settings["bucket_tiers"] = _normalize_bucket_tiers(settings.get("bucket_tiers"))
    settings["flat_threshold"] = _to_float(settings.get("flat_threshold"), DEFAULT_SETTINGS["flat_threshold"], min_value=0.1)
    settings["limit_threshold"] = _to_float(settings.get("limit_threshold"), DEFAULT_SETTINGS["limit_threshold"])
    settings["indicator_strong"] = _to_float(settings.get("indicator_strong"), DEFAULT_SETTINGS["indicator_strong"])
    settings["indicator_super_strong"] = _to_float(settings.get("indicator_super_strong"), DEFAULT_SETTINGS["indicator_super_strong"])
    settings["continuity_threshold"] = _to_float(settings.get("continuity_threshold"), DEFAULT_SETTINGS["continuity_threshold"])
    settings["top_bottom_n"] = _to_int(settings.get("top_bottom_n"), DEFAULT_SETTINGS["top_bottom_n"], min_value=10)
    settings["refresh_interval"] = _to_int(settings.get("refresh_interval"), DEFAULT_SETTINGS["refresh_interval"], min_value=15)
    settings["high_price_threshold"] = _to_int(settings.get("high_price_threshold"), DEFAULT_SETTINGS["high_price_threshold"], min_value=50)
    settings["font_base"] = _to_int(settings.get("font_base"), DEFAULT_SETTINGS["font_base"], min_value=10)
    settings["chart_font_base"] = _to_int(settings.get("chart_font_base"), DEFAULT_SETTINGS["chart_font_base"], min_value=6)
    settings["chart_height_base"] = _to_int(settings.get("chart_height_base"), DEFAULT_SETTINGS["chart_height_base"], min_value=250)
    return settings


@st.cache_data(ttl=30)
def _read_settings_json():
    with open(SETTINGS_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_settings():
    try:
        raw = _read_settings_json()
        if not isinstance(raw, dict):
            raise ValueError("settings.json 內容不是物件格式")
        settings = _normalize_settings(raw)
        st.session_state.pop(SETTINGS_ERROR_KEY, None)
        return settings
    except Exception as e:
        st.session_state[SETTINGS_ERROR_KEY] = f"{type(e).__name__}: {e}"
        return _default_settings()

def save_settings(data):
    normalized = _normalize_settings(data)
    with open(SETTINGS_PATH, 'w', encoding='utf-8') as f:
        json.dump(normalized, f, ensure_ascii=False, indent=4)
    _read_settings_json.clear()
    st.session_state.pop(SETTINGS_ERROR_KEY, None)


@st.cache_data(ttl=300)
def load_csv_list(path):
    with open(path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def save_csv_list(path, items):
    with open(path, 'w', encoding='utf-8') as f:
        for item in items:
            f.write(item.strip() + '\n')
    load_csv_list.clear()

@st.cache_data(ttl=3600)
def load_stock_names():
    """從 stock_names.json 載入代號→名稱對照（快取 1 小時）"""
    try:
        with open(STOCK_NAMES_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('data', {})
    except Exception:
        return {}


SETTINGS = load_settings()
STOCK_NAMES = load_stock_names()

# === Streamlit 設定 ===
st.set_page_config(page_title="盤中情緒監控", layout="wide", page_icon="📊")

# === 動態字體 CSS ===
if 'font_base' not in st.session_state:
    st.session_state.font_base = SETTINGS.get('font_base', 16)
if 'chart_font_base' not in st.session_state:
    st.session_state.chart_font_base = SETTINGS.get('chart_font_base', 10)
if 'chart_height_base' not in st.session_state:
    st.session_state.chart_height_base = SETTINGS.get('chart_height_base', 340)

fb = st.session_state.font_base

st.markdown(f"""
<style>
    .main .block-container {{ padding-top: 0.5rem; max-width: 1600px; }}
    [data-testid="stVerticalBlock"] > div {{ gap: 0.3rem; }}

    .panel {{
        background: #fff; border-radius: 8px; padding: 16px;
        border: 1px solid #e0e0e0; box-shadow: 0 1px 3px rgba(0,0,0,0.06);
    }}
    .panel-title {{
        font-size: {fb * 0.85}px; font-weight: 600; color: #555;
        margin-bottom: 10px; padding-bottom: 5px; border-bottom: 1px solid #eee;
    }}
    .metric-box {{
        background: #f8f9fa; border-radius: 8px; padding: 8px 6px;
        text-align: center; margin: 3px 0;
    }}
    .big-num {{ font-size: {fb * 2.0}px; font-weight: 700; line-height: 1.1; }}
    .mid-num {{ font-size: {fb * 1.4}px; font-weight: 700; line-height: 1.1; }}
    .sm-num {{ font-size: {fb * 1.1}px; font-weight: 600; line-height: 1.1; }}
    .metric-num {{ font-size: {fb * 1.45}px; font-weight: 700; line-height: 1.1; }}
    .label {{ font-size: {fb * 0.7}px; color: #888; margin-top: 2px; }}
    .range-label {{ font-size: {fb * 0.7}px; color: #61758a; margin-top: 1px; font-weight: 500; }}
    .red {{ color: #e74c3c; }}
    .green {{ color: #27ae60; }}
    .gray {{ color: #888; }}
    .section-hdr {{
        font-size: {fb * 1.0}px; font-weight: 600; color: #333;
        border-left: 4px solid #e74c3c; padding-left: 10px;
        margin: 16px 0 8px 0;
    }}
    .trend-card {{
        background: #fff; border-radius: 8px; padding: 12px;
        border: 1px solid #e0e0e0; text-align: center;
    }}
    .trend-val {{ font-size: {fb * 1.3}px; font-weight: 700; }}
    .trend-lbl {{ font-size: {fb * 0.7}px; color: #888; }}

    /* 範本風格的水平指標條 */
    .hbar-row {{
        display: flex; align-items: center; margin: 2px 0;
        font-size: {fb * 0.8}px;
    }}
    .hbar-name {{ width: 90px; text-align: right; padding-right: 6px; color: #333; font-size: {fb * 0.75}px; }}
    .hbar-val {{ width: 55px; text-align: right; font-weight: 600; font-size: {fb * 0.75}px; }}
    .hbar-bar-wrap {{ flex: 1; height: 14px; background: #f0f0f0; border-radius: 3px; position: relative; }}
    .hbar-bar {{ height: 100%; border-radius: 3px; }}
    .hbar-pct {{ width: 45px; text-align: right; font-size: {fb * 0.75}px; font-weight: 600; }}

    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}

    /* 設定按鈕樣式 */
    .settings-btn button {{
        background: none; border: none; font-size: 1.5rem; cursor: pointer;
        padding: 2px 8px; margin-top: 5px;
    }}

    /* 情緒背景 */
    .panel-bullish {{ background: linear-gradient(135deg, #fff5f5, #ffffff); border-left: 3px solid #e74c3c; }}
    .panel-bearish {{ background: linear-gradient(135deg, #f0fff4, #ffffff); border-left: 3px solid #27ae60; }}
    .panel-neutral {{ background: #fff; }}
</style>
""", unsafe_allow_html=True)


# ============================================================
#  設定彈窗
# ============================================================

@st.dialog("⚙️ 設定", width="large")
def settings_dialog():
    tab_params, tab_stocks, tab_font = st.tabs(["📊 參數設定", "📋 自選股管理", "🔤 字體大小"])

    # ---- Tab 1: 參數設定 ----
    with tab_params:
        st.caption("修改後按「儲存」即可，main.py 會在下一次抓取時自動套用新參數。")

        current = load_settings()

        col1, col2 = st.columns(2)

        with col1:
            volume_filter = st.number_input(
                "成交量濾網（張）", value=int(current.get('volume_filter', 0)),
                min_value=0, step=100, key="s_volume_filter")
            limit_threshold = st.number_input(
                "漲跌停門檻（%）", value=float(current.get('limit_threshold', 9.5)),
                min_value=0.0, max_value=15.0, step=0.5, format="%.1f", key="s_limit")
            indicator_strong = st.number_input(
                "強勢判定（%）", value=float(current.get('indicator_strong', 3.0)),
                min_value=0.0, max_value=10.0, step=0.5, format="%.1f", key="s_strong")
            indicator_super_strong = st.number_input(
                "超強勢判定（%）", value=float(current.get('indicator_super_strong', 7.5)),
                min_value=0.0, max_value=15.0, step=0.5, format="%.1f", key="s_super")
            continuity_threshold = st.number_input(
                "延續性門檻（%）", value=float(current.get('continuity_threshold', 5.0)),
                min_value=0.0, max_value=15.0, step=0.5, format="%.1f", key="s_cont")

        with col2:
            top_bottom_n = st.number_input(
                "百大統計個數", value=int(current.get('top_bottom_n', 100)),
                min_value=10, max_value=500, step=10, key="s_topn")
            refresh_interval = st.number_input(
                "儀表板刷新間隔（秒）", value=int(current.get('refresh_interval', 60)),
                min_value=15, max_value=120, step=5, key="s_refresh")
            high_price_threshold = st.number_input(
                "高價股門檻（元）", value=int(current.get('high_price_threshold', 300)),
                min_value=50, max_value=2000, step=50, key="s_highprice")

        st.markdown("**分桶階級（%）**")
        tiers = current.get('bucket_tiers', [2.5, 5, 7.5])
        bc1, bc2, bc3 = st.columns(3)
        with bc1:
            t1 = st.number_input("第 1 級", value=float(tiers[0]) if len(tiers) > 0 else 2.5,
                                 min_value=0.5, max_value=10.0, step=0.5, format="%.1f", key="s_t1")
        with bc2:
            t2 = st.number_input("第 2 級", value=float(tiers[1]) if len(tiers) > 1 else 5.0,
                                 min_value=0.5, max_value=15.0, step=0.5, format="%.1f", key="s_t2")
        with bc3:
            t3 = st.number_input("第 3 級", value=float(tiers[2]) if len(tiers) > 2 else 7.5,
                                 min_value=0.5, max_value=20.0, step=0.5, format="%.1f", key="s_t3")

        if st.button("💾 儲存參數", key="save_params", width="stretch"):
            new_settings = load_settings()  # 保留現有欄位（如 font_base）
            new_settings.update({
                "volume_filter": volume_filter,
                "bucket_tiers": [t1, t2, t3],
                "limit_threshold": limit_threshold,
                "indicator_strong": indicator_strong,
                "indicator_super_strong": indicator_super_strong,
                "continuity_threshold": continuity_threshold,
                "top_bottom_n": top_bottom_n,
                "refresh_interval": refresh_interval,
                "high_price_threshold": high_price_threshold,
            })
            save_settings(new_settings)
            st.success("參數已儲存！")
            st.rerun()

    # ---- Tab 2: 自選股管理 ----
    with tab_stocks:
        st.caption("每行一個股票代號（4 碼數字）。修改後按「儲存」寫回 CSV 檔。")

        def format_stock_list(codes):
            """將代號清單格式化為帶名稱的顯示文字"""
            lines = []
            for code in codes:
                name = STOCK_NAMES.get(code, '')
                lines.append(f"{code}  {name}" if name else code)
            return '\n'.join(lines)

        def parse_stock_input(text):
            """從輸入文字解析出股票代號（忽略名稱部分）"""
            codes = []
            for line in text.strip().split('\n'):
                line = line.strip()
                if not line:
                    continue
                code = line.split()[0].strip()
                if code.isdigit() and len(code) == 4:
                    codes.append(code)
            return codes

        blue_chips_current = load_csv_list(BLUE_CHIPS_PATH)

        st.markdown("**權值股**（用於計算權值股指標）")
        blue_input = st.text_area(
            "權值股清單", value=format_stock_list(blue_chips_current),
            height=250, key="edit_blue", label_visibility="collapsed")

        st.markdown("---")

        sc1, sc2 = st.columns(2)
        with sc1:
            st.markdown("**上市市值前 N 大**（圖表用）")
            tse_input = st.text_area(
                "上市前 N 大", value=format_stock_list(load_csv_list(TSE_TOP20_PATH)),
                height=250, key="edit_tse", label_visibility="collapsed")
        with sc2:
            st.markdown("**上櫃市值前 N 大**（圖表用）")
            otc_input = st.text_area(
                "上櫃前 N 大", value=format_stock_list(load_csv_list(OTC_TOP20_PATH)),
                height=250, key="edit_otc", label_visibility="collapsed")

        if st.button("💾 儲存自選股", key="save_stocks", width="stretch"):
            blue_codes = parse_stock_input(blue_input)
            tse_codes = parse_stock_input(tse_input)
            otc_codes = parse_stock_input(otc_input)

            save_csv_list(BLUE_CHIPS_PATH, blue_codes)
            save_csv_list(TSE_TOP20_PATH, tse_codes)
            save_csv_list(OTC_TOP20_PATH, otc_codes)

            st.success(f"已儲存！權值股 {len(blue_codes)} 檔、上市 {len(tse_codes)} 檔、上櫃 {len(otc_codes)} 檔")
            st.rerun()

    # ---- Tab 3: 字體大小 ----
    with tab_font:
        st.caption("調整儀表板面板與圖表的字體大小。")

        st.markdown("**面板字體**（市場數據、極端值等面板內的數字與標籤）")
        new_size = st.slider(
            "面板基準字體（px）", min_value=10, max_value=28, value=st.session_state.font_base,
            step=1, key="font_slider")

        # 面板預覽
        st.markdown(f"""<div style="background:#f8f9fa; border-radius:8px; padding:12px; margin-top:6px;">
            <span style="font-size:{new_size * 2.0}px; font-weight:700; color:#e74c3c;">17.0</span>
            <span style="font-size:{new_size * 0.7}px; color:#888;"> 多空家數情緒指數</span>&emsp;
            <span style="font-size:{new_size * 1.4}px; font-weight:700; color:#27ae60;">15.2%</span>
            <span style="font-size:{new_size * 0.7}px; color:#888;"> 多空活躍度</span>
        </div>""", unsafe_allow_html=True)

        st.markdown("---")

        st.markdown("**圖表字體**（市值前20大、分級統計、趨勢圖等 Plotly 圖表）")
        new_chart_size = st.slider(
            "圖表基準字體（px）", min_value=6, max_value=20, value=st.session_state.chart_font_base,
            step=1, key="chart_font_slider")

        # 圖表預覽
        st.markdown(f"""<div style="background:#f8f9fa; border-radius:8px; padding:12px; margin-top:6px;">
            <span style="font-size:{new_chart_size}px; color:#333;">台積電(2330)</span>&emsp;
            <span style="font-size:{new_chart_size}px; font-weight:700; color:#e74c3c;">-4.76%</span>&emsp;
            <span style="font-size:{new_chart_size+2}px; font-weight:600; color:#555;">上市市值前20大</span>
        </div>""", unsafe_allow_html=True)

        st.markdown("---")

        st.markdown("**圖表高度**（趨勢圖、分布圖等圖表的上下高度）")
        new_chart_height = st.slider(
            "圖表基準高度（px）", min_value=250, max_value=600, value=st.session_state.chart_height_base,
            step=10, key="chart_height_slider")

        # 高度預覽
        st.markdown(f"""<div style="background:#f8f9fa; border-radius:8px; padding:8px; margin-top:6px; text-align:center;">
            <div style="height:{int(new_chart_height * 0.15)}px; background:linear-gradient(180deg, #e74c3c 0%, #f5b7b1 50%, #7dcea0 100%);
                        border-radius:4px; margin:4px auto; width:80%;"></div>
            <span style="font-size:11px; color:#999;">圖表高度預覽：{new_chart_height}px</span>
        </div>""", unsafe_allow_html=True)

        if st.button("💾 套用字體與圖表設定", key="save_font", width="stretch"):
            st.session_state.font_base = new_size
            st.session_state.chart_font_base = new_chart_size
            st.session_state.chart_height_base = new_chart_height
            current = load_settings()
            current['font_base'] = new_size
            current['chart_font_base'] = new_chart_size
            current['chart_height_base'] = new_chart_height
            save_settings(current)
            st.rerun()


# ============================================================
#  資料讀取
# ============================================================

def get_db_error():
    """回傳目前資料庫不可用的原因；可用時回傳 None"""
    db_dir = os.path.dirname(DB_PATH)
    if not os.path.isdir(db_dir):
        return f"資料庫資料夾不存在：{db_dir}"
    if not os.path.exists(DB_PATH):
        return f"資料庫檔案不存在：{DB_PATH}"
    try:
        conn = sqlite3.connect(DB_PATH, timeout=15)
        conn.close()
    except sqlite3.Error as e:
        return f"無法開啟資料庫：{e}"
    return None


def open_db():
    """統一用同一個路徑與 timeout 開啟 SQLite"""
    return sqlite3.connect(DB_PATH, timeout=15)


@st.cache_data(ttl=10)
def load_latest_stats():
    """最新統計（快取 10 秒，避免同一次頁面載入重複查詢）"""
    conn = open_db()
    try:
        conn.row_factory = sqlite3.Row
        row = conn.execute("SELECT * FROM computed_stats ORDER BY snapshot_time DESC LIMIT 1").fetchone()
    finally:
        conn.close()
    return dict(row) if row else None

@st.cache_data(ttl=10)
def load_stats_history(date_str):
    """當日歷史（快取 10 秒）"""
    conn = open_db()
    try:
        df = pd.read_sql_query(
            "SELECT * FROM computed_stats WHERE snapshot_time LIKE ? ORDER BY snapshot_time",
            conn, params=(f"{date_str}%",)
        )
    finally:
        conn.close()
    return df

@st.cache_data(ttl=10)
def load_latest_snapshot(vol_filter=0):
    """最新快照（快取 10 秒，volume_filter 變動時自動刷新，依 symbol 去重）"""
    conn = open_db()
    try:
        df = pd.read_sql_query("""
            SELECT symbol, name, market, change_percent, close_price,
                   trade_volume, trade_value
            FROM raw_snapshots
            WHERE id IN (
                SELECT MIN(id) FROM raw_snapshots
                WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM raw_snapshots)
                  AND LENGTH(symbol)=4 AND symbol GLOB '[1-9][0-9][0-9][0-9]'
                  AND change_percent IS NOT NULL AND is_anomaly = 0
                  AND trade_volume >= ?
                GROUP BY symbol
            )
        """, conn, params=(vol_filter,))
    finally:
        conn.close()
    return df

@st.cache_data(ttl=10)
def load_total_stock_count():
    """原始總股票數（不套成交量濾網，依 symbol 去重，快取 10 秒）"""
    conn = open_db()
    try:
        row = conn.execute("""
            SELECT COUNT(DISTINCT symbol) FROM raw_snapshots
            WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM raw_snapshots)
              AND LENGTH(symbol)=4 AND symbol GLOB '[1-9][0-9][0-9][0-9]'
              AND change_percent IS NOT NULL AND is_anomaly = 0
        """).fetchone()
    finally:
        conn.close()
    return row[0] if row else 0


@st.cache_data(ttl=300)
def load_daily_stocks_latest_date():
    """讀取 daily_stocks 最新交易日（YYYY-MM-DD）"""
    conn = open_db()
    try:
        row = conn.execute("SELECT MAX(date) FROM daily_stocks").fetchone()
    finally:
        conn.close()
    return row[0] if row and row[0] else None


def _previous_business_day(ref_date):
    day = ref_date
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def expected_daily_stocks_date(now):
    """推估目前時間點應該至少同步到哪個交易日"""
    market_close = now.replace(hour=13, minute=35, second=0, microsecond=0)
    ref = now.date()

    if now.weekday() >= 5:
        ref -= timedelta(days=1)
    elif now < market_close:
        ref -= timedelta(days=1)

    return _previous_business_day(ref)


def get_daily_stocks_freshness_warning(now):
    latest_date_str = load_daily_stocks_latest_date()
    if not latest_date_str:
        return "盤後資料（daily_stocks）尚未同步，前日強弱勢追蹤可能顯示 N/A。"

    try:
        latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d").date()
    except ValueError:
        return f"盤後資料日期格式異常：{latest_date_str}，前日強弱勢追蹤可能失真。"

    expected = expected_daily_stocks_date(now)
    if latest_date < expected:
        return (
            f"盤後資料最新日為 {latest_date_str}，落後預期 {expected.isoformat()}；"
            "前日強弱勢追蹤可能失真。"
        )

    return None


def format_metric_value(value, decimals=2, suffix='', signed=False):
    """格式化儀表板數值；無值時回傳 N/A"""
    if value is None or pd.isna(value):
        return "N/A"
    if decimals == 0:
        text = str(int(round(float(value))))
    else:
        fmt = f"{{:{'+' if signed else ''}.{decimals}f}}"
        text = fmt.format(float(value))
    return f"{text}{suffix}"


def format_range_text(history_df, column, decimals=2, suffix=''):
    """回傳當日某欄位的最小~最大範圍文字"""
    if history_df.empty or column not in history_df.columns:
        return "今日數據範圍 N/A"

    series = pd.to_numeric(history_df[column], errors='coerce').dropna()
    if series.empty:
        return "今日數據範圍 N/A"

    min_val = series.min()
    max_val = series.max()
    if decimals == 0:
        return f"今日數據範圍 {int(round(min_val))} ~ {int(round(max_val))}"

    fmt = f"{{:.{decimals}f}}"
    return f"今日數據範圍 {fmt.format(min_val)}{suffix} ~ {fmt.format(max_val)}{suffix}"


def format_ratio_range_text(history_df, numerator_col, denominator_col, decimals=2, suffix='%'):
    """回傳分子/分母型指標的當日最小~最大範圍文字"""
    required = {numerator_col, denominator_col}
    if history_df.empty or not required.issubset(history_df.columns):
        return "今日數據範圍 N/A"

    numerator = pd.to_numeric(history_df[numerator_col], errors='coerce')
    denominator = pd.to_numeric(history_df[denominator_col], errors='coerce')
    valid = denominator > 0
    if not valid.any():
        return "今日數據範圍 N/A"

    series = (numerator[valid] / denominator[valid]) * 100
    series = series.dropna()
    if series.empty:
        return "今日數據範圍 N/A"

    min_val = series.min()
    max_val = series.max()
    fmt = f"{{:.{decimals}f}}"
    return f"今日數據範圍 {fmt.format(min_val)}{suffix} ~ {fmt.format(max_val)}{suffix}"


def metric_box_html(value_html, label, range_text, num_class="mid-num", color_class="", style="", arrow=None):
    """建立市場數據總覽單一格 HTML。arrow: 'up'/'down'/None"""
    classes = " ".join(x for x in [num_class, color_class] if x)
    style_attr = f' style="{style}"' if style else ""
    range_html = f'<div class="range-label">{range_text}</div>' if range_text else ""
    arrow_html = ""
    if arrow == "up":
        arrow_html = ' <span style="font-size:0.7em; color:#e74c3c;">▲</span>'
    elif arrow == "down":
        arrow_html = ' <span style="font-size:0.7em; color:#27ae60;">▼</span>'
    return (
        '<div class="metric-box">'
        f'<div class="{classes}"{style_attr}>{value_html}{arrow_html}</div>'
        f'<div class="label">{label}</div>'
        f'{range_html}'
        '</div>'
    )


# === 時間標記（趨勢圖用）===
TREND_TIME_MARKERS = [("09:00", "開盤"), ("09:30", "9:30"), ("10:00", "10:00"),
                      ("12:00", "12:00"), ("13:00", "13:00")]


def add_time_markers(fig, date_str):
    """在圖表上加上關鍵時間垂直虛線標記"""
    for t, label in TREND_TIME_MARKERS:
        x_val = f"{date_str} {t}:00"
        fig.add_shape(type="line", x0=x_val, x1=x_val, y0=0, y1=1,
                      yref="paper", line=dict(color="#ddd", width=1, dash="dot"))
        fig.add_annotation(x=x_val, y=1, yref="paper", text=label,
                           showarrow=False, font=dict(size=9, color="#aaa"),
                           yshift=8)


def get_arrow(current, previous):
    """比較兩個數值，回傳箭頭方向"""
    if current is None or previous is None:
        return None
    try:
        c, p = float(current), float(previous)
        if c > p:
            return "up"
        elif c < p:
            return "down"
    except (TypeError, ValueError):
        pass
    return None


# === 量能指標載入 ===
@st.cache_data(ttl=10)
def load_volume_tide(date_str):
    """從 raw_snapshots 計算每個 snapshot_time 的上漲/下跌成交金額"""
    conn = open_db()
    try:
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
        """, conn, params=(f"{date_str}%",))
    finally:
        conn.close()
    if df.empty:
        return df
    df['net_flow'] = (df['up_value'] - df['down_value']) / 1e8
    total_for_pct = pd.to_numeric(df['total_value'], errors='coerce')
    zero_total_mask = total_for_pct.fillna(0) <= 0
    total_for_pct = total_for_pct.mask(zero_total_mask)
    df['up_pct'] = (pd.to_numeric(df['up_value'], errors='coerce') / total_for_pct) * 100
    df['down_pct'] = (pd.to_numeric(df['down_value'], errors='coerce') / total_for_pct) * 100
    # 無成交時段改以 0% 呈現，並保留標記供前端提示資料品質。
    df['pct_imputed'] = zero_total_mask
    df.loc[zero_total_mask, ['up_pct', 'down_pct']] = 0
    return df


# ============================================================
#  圖表
# ============================================================

def format_threshold(value):
    """門檻顯示格式化，避免 2.5 被顯示成 2.50"""
    return f"{float(value):g}"


def build_distribution_summary(snapshot_df, settings):
    """依目前分級門檻計算各級距家數與占比"""
    if snapshot_df.empty or 'change_percent' not in snapshot_df.columns:
        return {'labels': [], 'counts': [], 'percents': [], 'colors': [], 'total': 0}

    tiers = _normalize_bucket_tiers(settings.get('bucket_tiers'))
    t1, t2, t3 = tiers
    flat_thr = max(0.1, float(settings.get('flat_threshold', DEFAULT_SETTINGS['flat_threshold'])))
    p = pd.to_numeric(snapshot_df['change_percent'], errors='coerce').dropna()
    total = int(len(p))
    if total == 0:
        return {'labels': [], 'counts': [], 'percents': [], 'colors': [], 'total': 0}

    labels = [
        f"<-{format_threshold(t3)}%",
        f"-{format_threshold(t3)}~-{format_threshold(t2)}%",
        f"-{format_threshold(t2)}~-{format_threshold(t1)}%",
        f"-{format_threshold(t1)}~-{format_threshold(flat_thr)}%",
        "持平",
        f"{format_threshold(flat_thr)}~{format_threshold(t1)}%",
        f"{format_threshold(t1)}~{format_threshold(t2)}%",
        f"{format_threshold(t2)}~{format_threshold(t3)}%",
        f">{format_threshold(t3)}%",
    ]
    counts = [
        int((p < -t3).sum()),
        int(((p >= -t3) & (p < -t2)).sum()),
        int(((p >= -t2) & (p < -t1)).sum()),
        int(((p >= -t1) & (p < -flat_thr)).sum()),
        int(((p >= -flat_thr) & (p <= flat_thr)).sum()),
        int(((p > flat_thr) & (p <= t1)).sum()),
        int(((p > t1) & (p <= t2)).sum()),
        int(((p > t2) & (p <= t3)).sum()),
        int((p > t3).sum()),
    ]
    percents = [count / total * 100 for count in counts]
    colors = ['#1f9d4a', '#35aa54', '#56ba5d', '#8dc86d', '#a6adb4',
              '#f0b25b', '#ec8a4b', '#e2623f', '#c63d35']
    return {
        'labels': labels,
        'counts': counts,
        'percents': percents,
        'colors': colors,
        'total': total,
    }


def make_distribution_chart(snapshot_df, settings, title, subtitle=None, height=320):
    """多空分布圖：顯示家數與家數占比"""
    summary = build_distribution_summary(snapshot_df, settings)
    cfb = st.session_state.get('chart_font_base', 10)

    if summary['total'] == 0:
        fig = go.Figure()
        fig.update_layout(
            title=dict(text=title, font=dict(size=cfb+2)),
            height=height,
            margin=dict(t=50, b=30, l=30, r=20),
            plot_bgcolor='white',
            xaxis=dict(visible=False),
            yaxis=dict(visible=False),
        )
        fig.add_annotation(
            text="目前無符合樣本",
            x=0.5, y=0.5, xref='paper', yref='paper',
            showarrow=False, font=dict(size=cfb+2, color='#8893a0'),
        )
        if subtitle:
            fig.add_annotation(
                text=subtitle,
                x=0.5, y=1.08, xref='paper', yref='paper',
                showarrow=False, font=dict(size=cfb, color='#5f7a85'),
            )
        return fig

    labels = summary['labels']
    counts = summary['counts']
    percents = summary['percents']
    colors = summary['colors']
    max_count = max(counts) if counts else 0
    pct_offset = max(2, max_count * 0.12)
    count_font_color = '#4a5563'
    pct_font_color = '#4A90D9'

    fig = go.Figure(go.Bar(
        x=labels,
        y=counts,
        marker_color=colors,
        text=[str(v) for v in counts],
        textposition='outside',
        textfont=dict(size=cfb, color=count_font_color),
        cliponaxis=False,
        hovertemplate='級距: %{x}<br>家數: %{y}<extra></extra>',
    ))

    annotations = []
    for label, count, pct in zip(labels, counts, percents):
        annotations.append(dict(
            x=label,
            y=count + pct_offset,
            text=f"{pct:.1f}%",
            showarrow=False,
            font=dict(size=cfb-1, color=pct_font_color),
            yanchor='bottom',
        ))

    if subtitle:
        annotations.append(dict(
            text=subtitle,
            x=0.5, y=1.09, xref='paper', yref='paper',
            showarrow=False, font=dict(size=cfb, color='#5f7a85'),
        ))

    fig.update_layout(
        title=dict(text=title, font=dict(size=cfb+2)),
        height=height,
        margin=dict(t=60 if subtitle else 40, b=35, l=40, r=15),
        xaxis=dict(title="漲跌幅區間", tickfont=dict(size=cfb-1), tickangle=-28),
        yaxis=dict(title="家數", range=[0, max_count + pct_offset * 2.2]),
        showlegend=False,
        plot_bgcolor='white',
        annotations=annotations,
    )
    return fig


def make_gauge(sentiment, s_min=None, s_max=None):
    """情緒儀表板 — 與範本一致"""
    if sentiment is None:
        sentiment = 0
    cfb = st.session_state.get('chart_font_base', 10)
    bar_color = "#e74c3c" if sentiment > 0 else "#27ae60" if sentiment < 0 else "#888"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=sentiment,
        number={'font': {'size': cfb*4, 'color': bar_color}, 'suffix': '', 'valueformat': '.1f'},
        title={'text': "多空家數情緒", 'font': {'size': cfb+4, 'color': '#555'}},
        gauge={
            'axis': {'range': [-100, 100], 'tickwidth': 1, 'tickcolor': '#ccc',
                     'tickfont': {'size': cfb}, 'dtick': 50},
            'bar': {'color': bar_color, 'thickness': 0.25},
            'bgcolor': '#f5f5f5',
            'borderwidth': 0,
            'steps': [
                {'range': [-100, -50], 'color': '#1a9641'},
                {'range': [-50, -20], 'color': '#a6d96a'},
                {'range': [-20, 20], 'color': '#ffffbf'},
                {'range': [20, 50], 'color': '#fdae61'},
                {'range': [50, 100], 'color': '#d73027'},
            ],
            'threshold': {
                'line': {'color': "#333", 'width': 3},
                'thickness': 0.8, 'value': sentiment
            },
        }
    ))

    # 波動範圍標註
    annotations = []
    if s_min is not None and s_max is not None:
        annotations.append(dict(
            text=f"今日多空家數情緒範圍: {s_min:.2f} ~ {s_max:.2f}",
            x=0.5, y=-0.15, xref='paper', yref='paper',
            showarrow=False, font=dict(size=cfb, color='#888'),
        ))

    fig.update_layout(
        height=260, margin=dict(t=40, b=40, l=20, r=20),
        annotations=annotations,
    )
    return fig


def make_top_stocks_chart(snapshot_df, symbols, title):
    """市值前N大水平長條圖（按CSV順序排列，顯示漲跌幅+漲跌比例條）"""
    if snapshot_df.empty:
        return go.Figure()
    cfb = st.session_state.get('chart_font_base', 10)

    # 按 CSV 順序排（市值由大到小），但圖表 y 軸從下到上，所以反轉
    ordered = []
    for sym in symbols:
        row = snapshot_df[snapshot_df['symbol'] == sym]
        if not row.empty:
            ordered.append(row.iloc[0])
    if not ordered:
        return go.Figure()

    df = pd.DataFrame(ordered)
    df = df.iloc[::-1]  # 反轉讓最大市值在最上面

    colors = ['#e74c3c' if x > 0 else '#27ae60' if x < 0 else '#888' for x in df['change_percent']]
    labels = [f"{r['name']}({r['symbol']})" for _, r in df.iterrows()]

    fig = go.Figure(go.Bar(
        x=df['change_percent'], y=labels,
        orientation='h', marker_color=colors,
        text=[f"{x:+.2f}%" for x in df['change_percent']],
        textposition='outside', textfont=dict(size=cfb, color='#4A90D9'),
    ))
    fig.update_layout(
        title=dict(text=title, font=dict(size=cfb+2)),
        height=max(350, len(df) * max(22, cfb*2+2)),
        margin=dict(t=30, b=10, l=max(110, cfb*11), r=50),
        xaxis=dict(title="漲跌幅(%)", zeroline=True, zerolinecolor='#ccc', zerolinewidth=1),
        yaxis=dict(tickfont=dict(size=cfb)),
        plot_bgcolor='white', showlegend=False,
    )
    return fig


def make_timeline(df, y_cols, names, colors, title, y_title="", fmt=".2f", height=None):
    cfb = st.session_state.get('chart_font_base', 10)
    if height is None:
        height = st.session_state.get('chart_height_base', 340)
    fig = go.Figure()
    for col, name, color in zip(y_cols, names, colors):
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df['snapshot_time'], y=df[col],
                name=f'<span style="color:{color}">{name}</span>',
                line=dict(color=color, width=2), mode='lines',
                hovertemplate=f'{name}: %{{y:{fmt}}}<extra></extra>'))
    fig.update_layout(
        title=dict(text=title, font=dict(size=cfb+2), y=0.97, yanchor='top'),
        height=height, margin=dict(t=130, b=45, l=40, r=20),
        hovermode='x unified',
        xaxis=dict(title="時間", tickfont=dict(size=cfb, style='italic'), tickangle=-45),
        yaxis=dict(title=y_title),
        legend=dict(orientation="h", yanchor="top", y=1.22,
                    xanchor="center", x=0.5, font=dict(size=cfb)),
        plot_bgcolor='white',
    )
    return fig


def make_diverging_bar(df):
    """多空家數強弱勢統計分布圖 — 正負堆疊面積圖（消除薄柱突刺）"""
    cfb = st.session_state.get('chart_font_base', 10)
    fig = go.Figure()
    # 漲方（正方向堆疊面積）— 由外層到內層依序加入
    for col, name, color, fill_color in [
        ('bucket_up_5',    '漲 2.5~5%',  '#fdae61', 'rgba(253,174,97,0.7)'),
        ('bucket_up_7_5',  '漲 5~7.5%',  '#f46d43', 'rgba(244,109,67,0.7)'),
        ('bucket_up_above','漲 >7.5%',   '#d73027', 'rgba(215,48,39,0.7)'),
    ]:
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df['snapshot_time'], y=df[col],
                name=f'<span style="color:{color}">{name}</span>',
                line=dict(width=0, shape='spline', smoothing=1.3), mode='lines',
                fillcolor=fill_color, stackgroup='up',
                hovertemplate=f'{name}: %{{y:.0f}}<extra></extra>'))
    # 跌方（負方向堆疊面積）
    for col, name, color, fill_color in [
        ('bucket_down_5',    '跌 2.5~5%',  '#a6d96a', 'rgba(166,217,106,0.7)'),
        ('bucket_down_7_5',  '跌 5~7.5%',  '#66bd63', 'rgba(102,189,99,0.7)'),
        ('bucket_down_above','跌 >7.5%',   '#1a9850', 'rgba(26,152,80,0.7)'),
    ]:
        if col in df.columns:
            fig.add_trace(go.Scatter(
                x=df['snapshot_time'], y=-df[col].abs(),
                name=f'<span style="color:{color}">{name}</span>',
                line=dict(width=0, shape='spline', smoothing=1.3), mode='lines',
                fillcolor=fill_color, stackgroup='down',
                customdata=df[col].abs(),
                hovertemplate=f'{name}: %{{customdata:.0f}}<extra></extra>'))
    fig.update_layout(
        title=dict(text="多空家數強弱勢統計分布圖", font=dict(size=cfb+2), y=0.98, yanchor='top'),
        height=st.session_state.get('chart_height_base', 340) + 30,
        margin=dict(t=140, b=45, l=40, r=20),
        hovermode='x unified',
        xaxis=dict(tickfont=dict(size=cfb), tickangle=-45),
        yaxis=dict(title="家數", zeroline=True, zerolinecolor='#888', zerolinewidth=1),
        legend=dict(orientation="h", yanchor="top", y=1.22,
                    xanchor="center", x=0.5, font=dict(size=cfb)),
        plot_bgcolor='white',
    )
    return fig


def build_trend_payload(history_df, date_str):
    """建立趨勢區塊需要的文字與圖表，供 session_state 暫存重用"""
    current = load_settings()
    strong_thr = current.get('indicator_strong', 3)
    super_thr = current.get('indicator_super_strong', 7.5)
    payload = {'date_str': date_str, 'has_history': len(history_df) > 1}
    if len(history_df) <= 1:
        return payload

    latest = history_df.iloc[-1]
    first = history_df.iloc[0]
    str_d = (latest.get('strength_index', 0) or 0) - (first.get('strength_index', 0) or 0)
    sen_d = (latest.get('sentiment_index', 0) or 0) - (first.get('sentiment_index', 0) or 0)

    # --- 趨勢圖用降採樣資料（每分鐘取最後一筆），減少瀏覽器渲染負擔 ---
    chart_df = history_df.copy()
    chart_df['snapshot_time'] = pd.to_datetime(chart_df['snapshot_time'], errors='coerce')
    chart_df = chart_df.dropna(subset=['snapshot_time'])
    if chart_df.empty:
        payload['has_history'] = False
        payload['data_error'] = "趨勢時間欄位含異常值，已略過無效資料。"
        return payload
    chart_df = chart_df.set_index('snapshot_time').resample('1min').last().dropna(how='all').reset_index()
    if chart_df.empty:
        payload['has_history'] = False
        payload['data_error'] = "趨勢資料降採樣後無有效資料。"
        return payload
    chart_df['snapshot_time'] = chart_df['snapshot_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # --- 對 count 欄位做滾動平滑，避免折線圖突刺 ---
    smooth_df = chart_df.copy()
    count_cols = ['super_strong_count', 'strong_count', 'weak_count', 'super_weak_count']
    for col in count_cols:
        if col in smooth_df.columns:
            smooth_df[col] = smooth_df[col].rolling(window=3, center=True, min_periods=1).mean()

    fig_strength = make_timeline(
        smooth_df,
        count_cols,
        [f'超強勢(≥{super_thr}%)', f'強勢(≥{strong_thr}%)', f'弱勢(≤-{strong_thr}%)', f'超弱勢(≤-{super_thr}%)'],
        ['#DC2626', '#F5B7B1', '#7DCEA0', '#196F3D'],
        "極端股 vs 強弱股變化",
        "家數",
        fmt=".0f",
        height=st.session_state.get('chart_height_base', 340) + 60,
    )

    fig_sentiment = make_timeline(
        chart_df,
        ['sentiment_index'],
        ['多空家數情緒指數'],
        ['#3498db'],
        "多空情緒綜合指標趨勢",
        "情緒指數",
    )
    if 'strength_index' in chart_df.columns:
        fig_sentiment.add_trace(go.Scatter(
            x=chart_df['snapshot_time'], y=chart_df['strength_index'],
            name='<span style="color:#e74c3c">強弱勢漲跌幅指數</span>',
            line=dict(color='#e74c3c', width=2),
            mode='lines', yaxis='y2',
            hovertemplate='強弱勢漲跌幅指數: %{y:.2f}%<extra></extra>'))
        fig_sentiment.update_layout(yaxis2=dict(title='強弱指數 / 活躍度(%)', overlaying='y', side='right'))
    if 'activity_rate' in chart_df.columns:
        fig_sentiment.add_trace(go.Scatter(
            x=chart_df['snapshot_time'], y=chart_df['activity_rate'],
            name='<span style="color:#e67e22">多空活躍度(%)</span>',
            line=dict(color='#e67e22', width=2, dash='dot'),
            mode='lines', yaxis='y2',
            hovertemplate='多空活躍度: %{y:.2f}%<extra></extra>'))

    # --- 異常值偵測 + 限幅 + 滾動平滑（僅供分布圖使用）---
    bucket_cols = ['bucket_up_5', 'bucket_up_7_5', 'bucket_up_above',
                   'bucket_down_5', 'bucket_down_7_5', 'bucket_down_above']
    bar_df = chart_df.copy()
    if all(c in bar_df.columns for c in bucket_cols):
        # 降低資料密度：每 5 分鐘取一筆平均值，避免面積圖鋸齒
        bar_df['snapshot_time'] = pd.to_datetime(bar_df['snapshot_time'], errors='coerce')
        bar_df = bar_df.dropna(subset=['snapshot_time'])
        bar_df = bar_df.set_index('snapshot_time')
        bar_df = bar_df[bucket_cols].resample('5min').mean().dropna().reset_index()
        # 滾動平滑柔化剩餘波動
        for col in bucket_cols:
            bar_df[col] = bar_df[col].rolling(window=3, center=True, min_periods=1).mean()

    # --- 強勢百 / 弱勢百 分開兩張圖 ---
    fig_top = make_timeline(
        chart_df,
        ['top_n_avg'],
        ['強勢百均漲幅'],
        ['#e74c3c'],
        "強勢百 均漲幅趨勢",
        "漲跌幅(%)",
    )
    fig_bottom = make_timeline(
        chart_df,
        ['bottom_n_avg'],
        ['弱勢百均跌幅'],
        ['#27ae60'],
        "弱勢百 均跌幅趨勢",
        "漲跌幅(%)",
    )

    # --- 新增圖表：前日強勢股追蹤（雙軸）---
    fig_prev_strong = make_timeline(
        chart_df,
        ['prev_strong_avg_today'],
        ['前日強勢股今日均漲跌幅'],
        ['#e74c3c'],
        "前日強勢股 — 今日表現追蹤",
        "漲跌幅(%)",
    )
    if 'prev_strong_positive_rate' in chart_df.columns:
        fig_prev_strong.add_trace(go.Scatter(
            x=chart_df['snapshot_time'], y=chart_df['prev_strong_positive_rate'],
            name='<span style="color:#3498db">正報酬率家數(%)</span>',
            line=dict(color='#3498db', width=2, dash='dot'),
            mode='lines', yaxis='y2',
            hovertemplate='正報酬率家數: %{y:.1f}%<extra></extra>'))
        fig_prev_strong.update_layout(yaxis2=dict(title='報酬率(%)', overlaying='y', side='right'))

    # --- 新增圖表：前日弱勢股追蹤（雙軸）---
    fig_prev_weak = make_timeline(
        chart_df,
        ['prev_weak_avg_today'],
        ['前日弱勢股今日均漲跌幅'],
        ['#27ae60'],
        "前日弱勢股 — 今日表現追蹤",
        "漲跌幅(%)",
    )
    if 'prev_weak_negative_rate' in chart_df.columns:
        fig_prev_weak.add_trace(go.Scatter(
            x=chart_df['snapshot_time'], y=chart_df['prev_weak_negative_rate'],
            name='<span style="color:#e67e22">負報酬率家數(%)</span>',
            line=dict(color='#e67e22', width=2, dash='dot'),
            mode='lines', yaxis='y2',
            hovertemplate='負報酬率家數: %{y:.1f}%<extra></extra>'))
        fig_prev_weak.update_layout(yaxis2=dict(title='報酬率(%)', overlaying='y', side='right'))

    # --- 所有圖表加上時間標記 ---
    for fig in [fig_strength, fig_sentiment, fig_top, fig_bottom, fig_prev_strong, fig_prev_weak]:
        add_time_markers(fig, date_str)
    fig_bar = make_diverging_bar(bar_df)
    add_time_markers(fig_bar, date_str)

    # --- 量能潮汐圖表 ---
    vt_df = load_volume_tide(date_str)
    fig_vt_flow = None
    fig_vt_pct = None
    vt_imputed_points = 0
    if not vt_df.empty:
        if 'pct_imputed' in vt_df.columns:
            vt_imputed_points = int(vt_df['pct_imputed'].sum())

        # 降採樣到每分鐘
        vt_chart = vt_df.copy()
        vt_chart['snapshot_time'] = pd.to_datetime(vt_chart['snapshot_time'], errors='coerce')
        vt_chart = vt_chart.dropna(subset=['snapshot_time'])
        vt_chart = vt_chart.set_index('snapshot_time').resample('1min').last().dropna(how='all').reset_index()
        if vt_chart.empty:
            vt_imputed_points = 0
        else:
            vt_chart['snapshot_time'] = vt_chart['snapshot_time'].dt.strftime('%Y-%m-%d %H:%M:%S')

            cfb = st.session_state.get('chart_font_base', 10)
            ht = st.session_state.get('chart_height_base', 340)

            # 量能淨流入長條圖
            fig_vt_flow = go.Figure()
            colors = ['#e74c3c' if v > 0 else '#27ae60' for v in vt_chart['net_flow']]
            fig_vt_flow.add_trace(go.Bar(
                x=vt_chart['snapshot_time'], y=vt_chart['net_flow'],
                marker_color=colors, name='量能淨流入',
                hovertemplate='%{x}<br>淨流入: %{y:+,.0f} 億<extra></extra>',
            ))
            fig_vt_flow.add_hline(y=0, line_color="#888", line_width=1)
            add_time_markers(fig_vt_flow, date_str)
            fig_vt_flow.update_layout(
                title=dict(text="量能淨流入 趨勢（億）", font=dict(size=cfb+2), y=0.97, yanchor='top'),
                height=ht, margin=dict(t=130, b=45, l=40, r=20),
                yaxis=dict(title="淨流入（億）"),
                xaxis=dict(tickfont=dict(size=cfb), tickangle=-45),
                plot_bgcolor='white', hovermode='x unified',
            )

            # 多空量能佔比面積圖
            fig_vt_pct = go.Figure()
            fig_vt_pct.add_trace(go.Scatter(
                x=vt_chart['snapshot_time'], y=vt_chart['up_pct'],
                name='上漲股量能佔比', line=dict(color='#e74c3c', width=2),
                fill='tozeroy', fillcolor='rgba(231,76,60,0.15)', mode='lines',
                hovertemplate='上漲股: %{y:.1f}%<extra></extra>',
            ))
            fig_vt_pct.add_trace(go.Scatter(
                x=vt_chart['snapshot_time'], y=vt_chart['down_pct'],
                name='下跌股量能佔比', line=dict(color='#27ae60', width=2),
                fill='tozeroy', fillcolor='rgba(39,174,96,0.15)', mode='lines',
                hovertemplate='下跌股: %{y:.1f}%<extra></extra>',
            ))
            fig_vt_pct.add_hline(y=50, line_dash="dash", line_color="#888", line_width=1)
            add_time_markers(fig_vt_pct, date_str)
            fig_vt_pct.update_layout(
                title=dict(text="多空量能佔比 趨勢", font=dict(size=cfb+2), y=0.97, yanchor='top'),
                height=ht, margin=dict(t=130, b=45, l=40, r=20),
                yaxis=dict(title="佔比（%）", range=[0, 100]),
                xaxis=dict(tickfont=dict(size=cfb), tickangle=-45),
                plot_bgcolor='white', hovermode='x unified',
                legend=dict(orientation="h", yanchor="top", y=1.22,
                            xanchor="center", x=0.5, font=dict(size=cfb)),
            )

    payload.update({
        'snapshot_time': latest.get('snapshot_time', ''),
        'history_len': len(history_df),
        'str_d': str_d,
        'sen_d': sen_d,
        'sc1': "red" if str_d > 0 else "green",
        'sc2': "red" if sen_d > 0 else "green",
        'fig_strength': fig_strength,
        'fig_sentiment': fig_sentiment,
        'fig_bar': fig_bar,
        'fig_top': fig_top,
        'fig_bottom': fig_bottom,
        'fig_prev_strong': fig_prev_strong,
        'fig_prev_weak': fig_prev_weak,
        'fig_vt_flow': fig_vt_flow,
        'fig_vt_pct': fig_vt_pct,
        'vt_imputed_points': vt_imputed_points,
    })
    return payload


def render_trend_section(payload, updating=False):
    st.markdown('<div class="section-hdr">📉 即時趨勢監控（當日變化）</div>', unsafe_allow_html=True)
    key_prefix = "trend-updating" if updating else "trend"

    if updating and payload.get('snapshot_time'):
        st.caption(f"趨勢圖更新中，先顯示上一版資料（{payload['snapshot_time']}）。")
    if payload.get('data_error'):
        st.warning(payload['data_error'])

    if not payload.get('has_history'):
        st.markdown(
            '<div class="panel" style="text-align:center; padding:25px; margin:10px 0;">'
            '<p class="gray">📊 即時趨勢圖表需要盤中多筆快照資料。'
            '盤中執行 main.py 後此區會自動填充。</p></div>',
            unsafe_allow_html=True)
        return

    t1, t2 = st.columns(2)
    with t1:
        st.markdown(
            f'<div class="trend-card"><div class="trend-val {payload["sc1"]}">{payload["str_d"]:+.2f}%</div>'
            f'<div class="trend-lbl">強弱勢漲跌幅指數變化(前{payload["history_len"]}筆)</div></div>',
            unsafe_allow_html=True)
    with t2:
        st.markdown(
            f'<div class="trend-card"><div class="trend-val {payload["sc2"]}">{payload["sen_d"]:+.1f}</div>'
            f'<div class="trend-lbl">多空家數情緒變化(前{payload["history_len"]}筆)</div></div>',
            unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(payload['fig_strength'], width="stretch", key=f"{key_prefix}-strength")
    with c2:
        st.plotly_chart(payload['fig_sentiment'], width="stretch", key=f"{key_prefix}-sentiment")

    st.plotly_chart(payload['fig_bar'], width="stretch", key=f"{key_prefix}-bar")

    if 'fig_top' in payload and 'fig_bottom' in payload:
        tb1, tb2 = st.columns(2)
        with tb1:
            st.plotly_chart(payload['fig_top'], width="stretch", key=f"{key_prefix}-top")
        with tb2:
            st.plotly_chart(payload['fig_bottom'], width="stretch", key=f"{key_prefix}-bottom")

    if 'fig_prev_strong' in payload and 'fig_prev_weak' in payload:
        p1, p2 = st.columns(2)
        with p1:
            st.plotly_chart(payload['fig_prev_strong'], width="stretch", key=f"{key_prefix}-prev-strong")
        with p2:
            st.plotly_chart(payload['fig_prev_weak'], width="stretch", key=f"{key_prefix}-prev-weak")

    # --- 量能潮汐圖表（左右並列）---
    if payload.get('fig_vt_flow') and payload.get('fig_vt_pct'):
        v1, v2 = st.columns(2)
        with v1:
            st.plotly_chart(payload['fig_vt_flow'], width="stretch", key=f"{key_prefix}-vt-flow")
        with v2:
            st.plotly_chart(payload['fig_vt_pct'], width="stretch", key=f"{key_prefix}-vt-pct")
        if payload.get('vt_imputed_points', 0) > 0:
            st.caption(f"量能佔比含 {payload['vt_imputed_points']} 個無成交時段，該時段以 0% 呈現。")


# ============================================================
#  主頁面
# ============================================================

def main():
    db_error = get_db_error()
    if db_error:
        st.warning(db_error)
        st.caption(f"資料庫路徑：{DB_PATH}")
        st.info("請先執行 /mnt/c/Users/User/Desktop/FB-Market/main.py 建立資料庫並寫入盤中快照。")
        st.stop()

    settings_error = st.session_state.get(SETTINGS_ERROR_KEY)
    if settings_error:
        st.warning(f"settings.json 讀取失敗，已套用安全預設：{settings_error}")

    settings = load_settings()

    toolbar_spacer, toolbar_btn = st.columns([15, 1])
    with toolbar_btn:
        if st.button("⚙️", key="open_settings", help="開啟設定"):
            settings_dialog()
            st.stop()

    # 資料區塊（由 fragment 自動定時刷新，盤中 13:35 前生效）
    _refresh_sec = settings.get('refresh_interval', DEFAULT_SETTINGS['refresh_interval'])
    now = datetime.now()
    market_close = now.replace(hour=13, minute=35, second=0, microsecond=0)
    run_every = timedelta(seconds=_refresh_sec) if now < market_close else None

    # ---- 浮動時鐘按鈕 ----
    _is_market_open = 1 if now < market_close else 0
    st.html(f"""
    <div id="fb-float-btn">
      <div class="fb-panel">
        <div class="fb-time">--:--:--</div>
        <div class="fb-date">----/--/--</div>
        <div class="fb-countdown">
          下次更新&ensp;<span class="fb-cd-num">--</span>s
        </div>
      </div>
      <div class="fb-circle">⏱</div>
    </div>
    <style>
      #fb-float-btn {{
        position: fixed; bottom: 30px; right: 30px; z-index: 999999;
        user-select: none; touch-action: none;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      }}
      #fb-float-btn .fb-circle {{
        width: 52px; height: 52px; border-radius: 50%;
        background: linear-gradient(135deg, #c0392b, #e74c3c);
        color: #fff; display: flex; align-items: center; justify-content: center;
        cursor: grab; box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        font-size: 20px; font-weight: 700;
        transition: transform 0.2s, box-shadow 0.2s;
      }}
      #fb-float-btn .fb-circle:hover {{
        transform: scale(1.1); box-shadow: 0 6px 20px rgba(0,0,0,0.4);
      }}
      #fb-float-btn .fb-panel {{
        position: absolute; bottom: 62px; right: 0;
        background: rgba(30,30,30,0.92); backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border-radius: 16px; padding: 20px 24px; color: #fff;
        min-width: 210px; box-shadow: 0 8px 32px rgba(0,0,0,0.45);
        opacity: 0; transform: scale(0.8) translateY(10px);
        transform-origin: bottom right;
        pointer-events: none;
        transition: opacity 0.25s ease, transform 0.25s ease;
      }}
      #fb-float-btn.expanded .fb-panel {{
        opacity: 1; transform: scale(1) translateY(0); pointer-events: auto;
      }}
      #fb-float-btn .fb-time {{
        font-size: 44px; font-weight: 700; letter-spacing: 2px;
        line-height: 1.1; text-align: center; color: #fff;
      }}
      #fb-float-btn .fb-date {{
        font-size: 16px; color: rgba(255,255,255,0.6);
        text-align: center; margin-top: 4px; letter-spacing: 1px;
      }}
      #fb-float-btn .fb-countdown {{
        font-size: 14px; color: rgba(255,255,255,0.5);
        text-align: center; margin-top: 14px; padding-top: 12px;
        border-top: 1px solid rgba(255,255,255,0.12);
      }}
      #fb-float-btn .fb-cd-num {{
        font-size: 24px; font-weight: 700; color: #e74c3c;
      }}
    </style>
    <script>
    (function() {{
      if (window._fbFloatInterval) {{
        clearInterval(window._fbFloatInterval);
        window._fbFloatInterval = null;
      }}
      if (window._fbFloatRefreshHandler) {{
        window.removeEventListener('fb-fragment-refresh', window._fbFloatRefreshHandler);
        window._fbFloatRefreshHandler = null;
      }}
      if (window._fbFloatPointerHandlers && window._fbFloatPointerHandlers.btn) {{
        var old = window._fbFloatPointerHandlers;
        old.btn.removeEventListener('pointerdown', old.down);
        old.btn.removeEventListener('pointermove', old.move);
        old.btn.removeEventListener('pointerup', old.up);
      }}

      var REFRESH = {_refresh_sec};
      var IS_OPEN = {_is_market_open};
      var btn   = document.getElementById('fb-float-btn');
      if (!btn) return;
      var circle = btn.querySelector('.fb-circle');
      var timeEl = btn.querySelector('.fb-time');
      var dateEl = btn.querySelector('.fb-date');
      var cdNum  = btn.querySelector('.fb-cd-num');
      var cdWrap = btn.querySelector('.fb-countdown');
      var panel  = btn.querySelector('.fb-panel');

      /* --- 時鐘 + 日期 + 倒數 --- */
      var countdownLeft = REFRESH;
      function tick() {{
        var now = new Date();
        var h = String(now.getHours()).padStart(2,'0');
        var m = String(now.getMinutes()).padStart(2,'0');
        var s = String(now.getSeconds()).padStart(2,'0');
        timeEl.textContent = h + ':' + m + ':' + s;
        var y = now.getFullYear();
        var mo = String(now.getMonth()+1).padStart(2,'0');
        var d = String(now.getDate()).padStart(2,'0');
        dateEl.textContent = y + '/' + mo + '/' + d;
        if (IS_OPEN) {{
          cdWrap.style.display = '';
          cdNum.textContent = countdownLeft;
          countdownLeft--;
          if (countdownLeft < 0) countdownLeft = REFRESH;
        }} else {{
          cdWrap.style.display = 'none';
        }}
      }}
      tick();
      window._fbFloatInterval = setInterval(tick, 1000);

      /* 監聽 fragment 刷新事件，歸零倒數 */
      window._fbFloatRefreshHandler = function() {{
        countdownLeft = REFRESH;
      }};
      window.addEventListener('fb-fragment-refresh', window._fbFloatRefreshHandler);

      /* --- 拖曳 + 點擊 --- */
      var dragging = false, didDrag = false;
      var sx, sy, sl, st2;
      var THRESHOLD = 5;

      var onPointerDown = function(e) {{
        dragging = true; didDrag = false;
        sx = e.clientX; sy = e.clientY;
        var r = btn.getBoundingClientRect();
        sl = r.left; st2 = r.top;
        btn.setPointerCapture(e.pointerId);
        circle.style.cursor = 'grabbing';
        e.preventDefault();
      }};
      var onPointerMove = function(e) {{
        if (!dragging) return;
        var dx = e.clientX - sx, dy = e.clientY - sy;
        if (Math.abs(dx) > THRESHOLD || Math.abs(dy) > THRESHOLD) didDrag = true;
        if (didDrag) {{
          var nl = Math.max(0, Math.min(sl + dx, window.innerWidth  - btn.offsetWidth));
          var nt = Math.max(0, Math.min(st2 + dy, window.innerHeight - btn.offsetHeight));
          btn.style.right = 'auto'; btn.style.bottom = 'auto';
          btn.style.left = nl + 'px'; btn.style.top = nt + 'px';
        }}
      }};
      var onPointerUp = function(e) {{
        if (!dragging) return;
        dragging = false;
        circle.style.cursor = 'grab';
        btn.releasePointerCapture(e.pointerId);
        try {{ localStorage.setItem('fb-float-pos', JSON.stringify({{
          l: btn.style.left, t: btn.style.top, r: btn.style.right, b: btn.style.bottom
        }})); }} catch(x) {{}}
        if (!didDrag) {{
          btn.classList.toggle('expanded');
          adjustPanel();
        }}
      }};
      btn.addEventListener('pointerdown', onPointerDown);
      btn.addEventListener('pointermove', onPointerMove);
      btn.addEventListener('pointerup', onPointerUp);
      window._fbFloatPointerHandlers = {{
        btn: btn, down: onPointerDown, move: onPointerMove, up: onPointerUp
      }};

      /* --- 面板方向自適應 --- */
      function adjustPanel() {{
        var r = btn.getBoundingClientRect();
        if (r.top < 280) {{
          panel.style.bottom = 'auto'; panel.style.top = '62px';
        }} else {{
          panel.style.bottom = '62px'; panel.style.top = 'auto';
        }}
        if (r.left < 230) {{
          panel.style.right = 'auto'; panel.style.left = '0';
        }} else {{
          panel.style.right = '0'; panel.style.left = 'auto';
        }}
      }}

      /* --- 恢復位置 --- */
      try {{
        var saved = JSON.parse(localStorage.getItem('fb-float-pos'));
        if (saved && saved.l && saved.l !== 'auto') {{
          btn.style.left = saved.l; btn.style.top = saved.t;
          btn.style.right = 'auto'; btn.style.bottom = 'auto';
        }}
      }} catch(x) {{}}
      adjustPanel();
    }})();
    </script>
    """, unsafe_allow_javascript=True)

    # 上半部：面板 + 市場細分析 + 個股排行（輕量，快速刷新）
    st.fragment(data_section_upper, run_every=run_every)()

    # 下半部：趨勢圖（較重，獨立刷新不擋上半部）
    st.fragment(trend_section, run_every=run_every)()

    # 指標定義說明（純文字，不需要隨資料刷新）
    _settings = settings
    t1, t2, t3 = _normalize_bucket_tiers(_settings.get('bucket_tiers'))
    flat_thr = max(0.1, float(_settings.get('flat_threshold', DEFAULT_SETTINGS['flat_threshold'])))
    limit_thr = _settings.get('limit_threshold', 9.5)
    strong_thr = _settings.get('indicator_strong', 3)
    super_strong_thr = _settings.get('indicator_super_strong', 7.5)
    top_n = _settings.get('top_bottom_n', 100)
    abnormal_thr = 10.01
    cont_thr = _settings.get('continuity_threshold', 5)
    vol_filter = _settings.get('volume_filter', 0)

    st.markdown('<div class="section-hdr">📘 指標定義說明</div>', unsafe_allow_html=True)
    st.markdown(
        f"""
        - 統計樣本：一般股票（4 碼、非異常、`漲跌幅%` 有值），且成交量 ≥ {vol_filter} 張。
        - 多空家數情緒指數：`(上漲家數 - 下跌家數) / (上漲家數 + 下跌家數) × 100`。
        - 多空家數比：`上漲家數 / 下跌家數`（下跌家數為 0 時顯示 N/A）。
        - 多空活躍度：`(多方股家數 + 空方股家數) / 總家數 × 100%`，多方股：`漲跌幅% ≥ {strong_thr}`；空方股：`漲跌幅% ≤ -{strong_thr}`。
        - 全市場波動度：`全市場漲跌幅% 的標準差`。
        - 強弱勢漲跌幅指數：將 `漲跌幅% ≥ {strong_thr}` 和 `漲跌幅% ≤ -{strong_thr}` 兩組股票合併後，計算平均漲跌幅。
        - 多方股：`漲跌幅% ≥ {strong_thr}` 的家數；空方股：`漲跌幅% ≤ -{strong_thr}` 的家數。
        - 強勢百：排除 `|漲跌幅%| > {abnormal_thr}%` 後，取前 {top_n} 檔平均漲幅。
        - 弱勢百：排除 `|漲跌幅%| > {abnormal_thr}%` 後，取後 {top_n} 檔平均漲幅。
        - 前日強勢股：前一交易日漲幅 `>{cont_thr}%` 的股票數。
        - 前日強勢股今日均漲跌幅：上述股票在今日的平均漲跌幅。
        - 前日強勢股正報酬率家數：上述股票中 `今日漲跌幅 > 0` 的家數占比。
        - 前日弱勢股：前一交易日跌幅 `<-{cont_thr}%` 的股票數。
        - 前日弱勢股今日均漲跌幅：上述股票在今日的平均漲跌幅。
        - 前日弱勢股負報酬率家數：上述股票中 `今日漲跌幅 < 0` 的家數占比。
        - 超強勢占比：`超強勢家數 / 多方股家數 × 100%`，超強勢：`漲跌幅% ≥ {super_strong_thr}`；多方股：`漲跌幅% ≥ {strong_thr}`。
        - 超弱勢占比：`超弱勢家數 / 空方股家數 × 100%`，超弱勢：`漲跌幅% ≤ -{super_strong_thr}`；空方股：`漲跌幅% ≤ -{strong_thr}`。
        - 近漲停：`漲跌幅% ≥ {limit_thr}`；近跌停：`漲跌幅% ≤ -{limit_thr}`。
        - 持平區間：`-{flat_thr}% ~ +{flat_thr}%`。
        - 漲幅區間：`{flat_thr}~{t1}%`, `{t1}~{t2}%`, `{t2}~{t3}%`, `>{t3}%`。
        - 跌幅對稱區間：`-{t1}~-{flat_thr}%`, `-{t2}~-{t1}%`, `-{t3}~-{t2}%`, `<-{t3}%`。
        """,
        unsafe_allow_html=False,
    )

    st.markdown(
        "<hr><p style='text-align:center; color:#aaa; font-size:0.72rem;'>"
        "盤中情緒監控系統 | 資料來源：富邦 API + 證交所/櫃買中心</p>",
        unsafe_allow_html=True)


def data_section_upper():
    """上半部：面板 + 市場細分析 + 個股排行（輕量快速刷新）"""
    _settings = load_settings()
    _vol_filter = _settings.get('volume_filter', 0)
    try:
        stats = load_latest_stats()
        snapshot_df = load_latest_snapshot(_vol_filter)
    except sqlite3.Error as e:
        st.error(f"讀取市場資料庫失敗：{e}")
        st.caption(f"資料庫路徑：{DB_PATH}")
        return

    if not stats:
        st.markdown(
            "<h2 style='text-align:center; margin:0; color:#c0392b; font-size:32px;'>盤中情緒監控系統</h2>"
            "<div style='text-align:center; padding:80px 20px;'>"
            "<div style='font-size:48px; margin-bottom:16px;'>📊</div>"
            "<div style='font-size:24px; font-weight:600; color:#555; margin-bottom:8px;'>尚未開盤 — 等待盤中資料</div>"
            "<div style='font-size:15px; color:#999; margin-bottom:20px;'>盤中啟動 main.py 後，資料會自動載入</div>"
            "<div id='wait-clock' style='font-size:28px; font-weight:700; color:#c0392b;'></div>"
            "</div>"
            "<script>"
            "(function(){"
            "  var el = document.getElementById('wait-clock');"
            "  if(!el) return;"
            "  function t(){"
            "    var n=new Date();"
            "    el.textContent=String(n.getHours()).padStart(2,'0')+':'+String(n.getMinutes()).padStart(2,'0')+':'+String(n.getSeconds()).padStart(2,'0');"
            "  }"
            "  t(); setInterval(t,1000);"
            "})();"
            "</script>",
            unsafe_allow_html=True,
        )
        return

    stime = stats.get('snapshot_time', '')
    date_str = stime[:10]
    filtered_total = stats.get('filtered_total', 0) or 0
    freshness_warning = get_daily_stocks_freshness_warning(datetime.now())
    try:
        raw_total = load_total_stock_count()
        history_df = load_stats_history(date_str)
    except sqlite3.Error as e:
        st.error(f"讀取市場資料庫失敗：{e}")
        st.caption(f"資料庫路徑：{DB_PATH}")
        return

    st.markdown(
        f"<h2 style='text-align:center; margin:0; color:#c0392b; font-size:32px;'>盤中情緒監控系統</h2>"
        f"<p style='text-align:center; color:#888; font-size:15px; margin:0 0 10px;'>"
        f"最後更新: {stime} | 交易日: {date_str} | "
        f"總資料來源 {raw_total:,} 檔股票 | 目前統計 {filtered_total:,} 檔 (≥{_vol_filter}張)</p>",
        unsafe_allow_html=True
    )
    if freshness_warning:
        st.warning(freshness_warning)
    # 通知浮動按鈕：fragment 已刷新，倒數歸零
    st.html("""<script>
    window.dispatchEvent(new CustomEvent('fb-fragment-refresh'));
    </script>""", unsafe_allow_javascript=True)

    # === 載入量能指標 ===
    vt_df = load_volume_tide(date_str)
    vt_net = vt_up_pct = vt_down_pct = None
    vt_net_arrow = vt_up_arrow = vt_down_arrow = None
    if not vt_df.empty:
        vt_latest = vt_df.iloc[-1]
        vt_net = vt_latest['net_flow']
        vt_up_pct = vt_latest['up_pct']
        vt_down_pct = vt_latest['down_pct']
        if len(vt_df) >= 2:
            vt_prev = vt_df.iloc[-2]
            vt_net_arrow = get_arrow(vt_net, vt_prev['net_flow'])
            vt_up_arrow = get_arrow(vt_up_pct, vt_prev['up_pct'])
            vt_down_arrow = get_arrow(vt_down_pct, vt_prev['down_pct'])

    # === 取前一筆做箭頭比對 ===
    prev_row = history_df.iloc[-2] if len(history_df) >= 2 else None

    # ===== 第一排：3面板 =====
    p1, p2, p3 = st.columns([3.2, 3.0, 4.0])

    # --- 面板1: 市場數據總覽（全部指標）---
    with p1:
        sentiment = stats.get('sentiment_index', 0) or 0
        ad = stats.get('ad_ratio')
        activity = stats.get('activity_rate', 0) or 0
        vol = stats.get('volatility', 0) or 0
        strength = stats.get('strength_index', 0) or 0
        top_avg = stats.get('top_n_avg')
        bottom_avg = stats.get('bottom_n_avg')
        prev_count = stats.get('prev_strong_count')
        prev_avg = stats.get('prev_strong_avg_today')
        prev_rate = stats.get('prev_strong_positive_rate')
        prev_weak_count = stats.get('prev_weak_count')
        prev_weak_avg = stats.get('prev_weak_avg_today')
        prev_weak_rate = stats.get('prev_weak_negative_rate')
        summary_prev_rate_range = format_range_text(history_df, 'prev_strong_positive_rate', decimals=2, suffix='%')
        summary_prev_weak_rate_range = format_range_text(history_df, 'prev_weak_negative_rate', decimals=2, suffix='%')

        s_c = "red" if sentiment > 0 else "green" if sentiment < 0 else "gray"
        ad_c = "red" if ad is not None and ad > 1 else "green" if ad is not None and ad < 1 else "gray"
        str_c = "red" if strength > 0 else "green" if strength < 0 else "gray"
        top_c = "red" if top_avg is not None and top_avg > 0 else "green" if top_avg is not None and top_avg < 0 else "gray"
        bot_c = "green" if bottom_avg is not None and bottom_avg < 0 else "red" if bottom_avg is not None and bottom_avg > 0 else "gray"
        pa_c = "red" if prev_avg is not None and prev_avg > 0 else "green" if prev_avg is not None and prev_avg < 0 else "gray"
        pw_c = "red" if prev_weak_avg is not None and prev_weak_avg > 0 else "green" if prev_weak_avg is not None and prev_weak_avg < 0 else "gray"

        # 箭頭比對
        _pa = lambda col: get_arrow(stats.get(col), prev_row[col] if prev_row is not None and col in prev_row.index else None)

        # 量能指標行 HTML
        vt_html = ""
        if vt_net is not None:
            vt_net_color = "#e74c3c" if vt_net > 0 else "#27ae60"
            vt_up_color = "#e74c3c" if vt_up_pct and vt_up_pct > 50 else "#27ae60"
            vt_html = "".join([
                '<div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:4px; margin-top:4px;">',
                metric_box_html(
                    f"{vt_net:+,.0f} 億",
                    "量能淨流入",
                    "",
                    num_class="metric-num",
                    style=f"color:{vt_net_color};",
                    arrow=vt_net_arrow,
                ),
                metric_box_html(
                    f"{vt_up_pct:.1f}%" if vt_up_pct is not None else "N/A",
                    "上漲股量能佔比",
                    "",
                    num_class="metric-num",
                    style=f"color:{vt_up_color};",
                    arrow=vt_up_arrow,
                ),
                metric_box_html(
                    f"{vt_down_pct:.1f}%" if vt_down_pct is not None else "N/A",
                    "下跌股量能佔比",
                    "",
                    num_class="metric-num",
                    style=f"color:{'#27ae60' if vt_down_pct and vt_down_pct > 50 else '#e74c3c'};",
                    arrow=vt_down_arrow,
                ),
                '</div>',
            ])

        summary_html = "".join([
            '<div style="display:grid; grid-template-columns:1fr 1fr; gap:6px;">',
            metric_box_html(
                format_metric_value(sentiment, decimals=1),
                "多空家數情緒指數",
                format_range_text(history_df, 'sentiment_index', decimals=1),
                num_class="metric-num",
                color_class=s_c,
                arrow=_pa('sentiment_index'),
            ),
            metric_box_html(
                format_metric_value(ad, decimals=2),
                "多空家數比",
                format_range_text(history_df, 'ad_ratio', decimals=2),
                num_class="metric-num",
                color_class=ad_c,
                arrow=_pa('ad_ratio'),
            ),
            metric_box_html(
                format_metric_value(activity, decimals=1, suffix='%'),
                "多空活躍度",
                format_range_text(history_df, 'activity_rate', decimals=1, suffix='%'),
                num_class="metric-num",
                style="color:#27ae60;",
                arrow=_pa('activity_rate'),
            ),
            metric_box_html(
                format_metric_value(vol, decimals=2, suffix='%'),
                "全市場波動度",
                format_range_text(history_df, 'volatility', decimals=2, suffix='%'),
                num_class="metric-num",
                style="color:#e67e22;",
                arrow=_pa('volatility'),
            ),
            '</div>',
            '<div style="display:grid; grid-template-columns:1fr; gap:6px; margin-top:6px;">',
            metric_box_html(
                format_metric_value(strength, decimals=2, suffix='%'),
                "強弱勢漲跌幅指數",
                format_range_text(history_df, 'strength_index', decimals=2, suffix='%'),
                num_class="metric-num",
                color_class=str_c,
                arrow=_pa('strength_index'),
            ),
            '</div>',
            '<div style="display:grid; grid-template-columns:1fr 1fr; gap:4px; margin-top:4px;">',
            metric_box_html(
                format_metric_value(top_avg, decimals=2, suffix='%', signed=True),
                "強勢百",
                format_range_text(history_df, 'top_n_avg', decimals=2, suffix='%'),
                num_class="metric-num",
                color_class=top_c,
                arrow=_pa('top_n_avg'),
            ),
            metric_box_html(
                format_metric_value(bottom_avg, decimals=2, suffix='%', signed=True),
                "弱勢百",
                format_range_text(history_df, 'bottom_n_avg', decimals=2, suffix='%'),
                num_class="metric-num",
                color_class=bot_c,
                arrow=_pa('bottom_n_avg'),
            ),
            '</div>',
            vt_html,
            '<div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:4px; margin-top:4px;">',
            metric_box_html(
                format_metric_value(prev_count, decimals=0),
                "前日強勢股",
                "",
                num_class="metric-num",
                color_class="gray",
            ),
            metric_box_html(
                format_metric_value(prev_avg, decimals=2, suffix='%', signed=True),
                "今日均漲跌幅",
                format_range_text(history_df, 'prev_strong_avg_today', decimals=2, suffix='%'),
                num_class="metric-num",
                color_class=pa_c,
                arrow=_pa('prev_strong_avg_today'),
            ),
            metric_box_html(
                format_metric_value(prev_rate, decimals=0, suffix='%'),
                "正報酬率家數",
                summary_prev_rate_range,
                num_class="metric-num",
                color_class="gray",
            ),
            '</div>',
            '<div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:4px; margin-top:4px;">',
            metric_box_html(
                format_metric_value(prev_weak_count, decimals=0),
                "前日弱勢股",
                "",
                num_class="metric-num",
                color_class="gray",
            ),
            metric_box_html(
                format_metric_value(prev_weak_avg, decimals=2, suffix='%', signed=True),
                "今日均漲跌幅",
                format_range_text(history_df, 'prev_weak_avg_today', decimals=2, suffix='%'),
                num_class="metric-num",
                color_class=pw_c,
                arrow=_pa('prev_weak_avg_today'),
            ),
            metric_box_html(
                format_metric_value(prev_weak_rate, decimals=0, suffix='%'),
                "負報酬率家數",
                summary_prev_weak_rate_range,
                num_class="metric-num",
                color_class="gray",
            ),
            '</div>',
        ])

        # 情緒背景色
        _mood = "panel-bullish" if sentiment > 10 else "panel-bearish" if sentiment < -10 else "panel-neutral"
        st.markdown(
            f"""
            <div class="panel {_mood}">
                <div class="panel-title">市場數據總覽</div>
                {summary_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

    # --- 面板2: 極端值監控（含漲跌停）---
    with p2:
        ss = stats.get('super_strong_count', 0) or 0
        sw = stats.get('super_weak_count', 0) or 0
        sc = stats.get('strong_count', 0) or 0
        wc = stats.get('weak_count', 0) or 0
        nu = stats.get('near_limit_up_count', 0) or 0
        nd = stats.get('near_limit_down_count', 0) or 0
        limit_thr = _settings.get('limit_threshold', 9.5)
        strong_thr = _settings.get('indicator_strong', 3)
        super_strong_thr = _settings.get('indicator_super_strong', 7.5)
        ss_p = f"{ss/sc*100:.0f}%" if sc > 0 else "N/A"
        sw_p = f"{sw/wc*100:.0f}%" if wc > 0 else "N/A"
        extreme_html = "".join([
            '<div style="display:grid; grid-template-columns:1fr 1fr; gap:6px;">',
            metric_box_html(
                format_metric_value(ss, decimals=0),
                f"超強勢(≥{super_strong_thr}%)",
                format_range_text(history_df, 'super_strong_count', decimals=0),
                num_class="metric-num",
                color_class="red",
            ),
            metric_box_html(
                format_metric_value(sw, decimals=0),
                f"超弱勢(≤-{super_strong_thr}%)",
                format_range_text(history_df, 'super_weak_count', decimals=0),
                num_class="metric-num",
                color_class="green",
            ),
            metric_box_html(
                format_metric_value(sc, decimals=0),
                f"多方股(≥{strong_thr}%)",
                format_range_text(history_df, 'strong_count', decimals=0),
                num_class="metric-num",
                color_class="red",
            ),
            metric_box_html(
                format_metric_value(wc, decimals=0),
                f"空方股(≤-{strong_thr}%)",
                format_range_text(history_df, 'weak_count', decimals=0),
                num_class="metric-num",
                color_class="green",
            ),
            metric_box_html(
                ss_p,
                "超強勢占比",
                format_ratio_range_text(history_df, 'super_strong_count', 'strong_count', decimals=2),
                num_class="metric-num",
                color_class="red",
            ),
            metric_box_html(
                sw_p,
                "超弱勢占比",
                format_ratio_range_text(history_df, 'super_weak_count', 'weak_count', decimals=2),
                num_class="metric-num",
                color_class="green",
            ),
            metric_box_html(
                format_metric_value(nu, decimals=0),
                f"近漲停(≥{limit_thr}%)",
                format_range_text(history_df, 'near_limit_up_count', decimals=0),
                num_class="metric-num",
                color_class="red",
            ),
            metric_box_html(
                format_metric_value(nd, decimals=0),
                f"近跌停(≤-{limit_thr}%)",
                format_range_text(history_df, 'near_limit_down_count', decimals=0),
                num_class="metric-num",
                color_class="green",
            ),
            '</div>',
        ])

        st.markdown(
            f"""
            <div class="panel">
                <div class="panel-title">極端值監控</div>
                {extreme_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

    # --- 面板3: 多空家數統計分布圖 ---
    with p3:
        st.plotly_chart(
            make_distribution_chart(snapshot_df, _settings, "多空家數統計分布圖", height=340),
            width="stretch",
        )

    # ===== 第二排：市場細分析 =====
    st.markdown('<div class="section-hdr">🔍 市場細分析</div>', unsafe_allow_html=True)

    # 重新讀取 CSV（可能剛被設定彈窗更新）
    tse_list = load_csv_list(TSE_TOP20_PATH)
    otc_list = load_csv_list(OTC_TOP20_PATH)

    m1, m2 = st.columns(2)
    with m1:
        st.plotly_chart(
            make_top_stocks_chart(snapshot_df, tse_list, "上市市值前20大"),
            width="stretch"
        )
    with m2:
        st.plotly_chart(
            make_top_stocks_chart(snapshot_df, otc_list, "上櫃市值前20大"),
            width="stretch"
        )

    weighted_symbols = list(dict.fromkeys(tse_list + otc_list))
    weighted_df = snapshot_df[snapshot_df['symbol'].isin(weighted_symbols)].copy() if not snapshot_df.empty else pd.DataFrame()
    high_price_threshold = _settings.get('high_price_threshold', 300)
    high_price_df = (
        snapshot_df[pd.to_numeric(snapshot_df['close_price'], errors='coerce') >= high_price_threshold].copy()
        if not snapshot_df.empty else pd.DataFrame()
    )

    m3, m4 = st.columns(2)
    with m3:
        st.plotly_chart(
            make_distribution_chart(weighted_df, _settings, "權值股漲跌分布"),
            width="stretch"
        )
    with m4:
        st.plotly_chart(
            make_distribution_chart(
                high_price_df,
                _settings,
                f"高價股漲跌分布 (>{format_threshold(high_price_threshold)}元)",
                subtitle=f"高價股家數：{len(high_price_df)}家",
            ),
            width="stretch"
        )

    # ===== 個股排行（預設收合，點開才渲染）=====
    with st.expander("📋 個股漲跌排行", expanded=False):
        if not snapshot_df.empty:
            tab1, tab2 = st.tabs(["🔴 漲幅前 30", "🟢 跌幅前 30"])
            cols = ['symbol', 'name', 'market', 'change_percent', 'close_price', 'trade_volume']
            names = ['代號', '名稱', '市場', '漲跌幅%', '收盤價', '成交量(張)']

            with tab1:
                top = snapshot_df.nlargest(30, 'change_percent')[cols].copy()
                top.columns = names
                top['漲跌幅%'] = top['漲跌幅%'].apply(lambda x: f"{x:+.2f}%")
                st.dataframe(top, hide_index=True, width="stretch")
            with tab2:
                bot = snapshot_df.nsmallest(30, 'change_percent')[cols].copy()
                bot.columns = names
                bot['漲跌幅%'] = bot['漲跌幅%'].apply(lambda x: f"{x:+.2f}%")
                st.dataframe(bot, hide_index=True, width="stretch")



def trend_section():
    """下半部：趨勢圖區（獨立 fragment，不擋上半部渲染）"""
    try:
        stats = load_latest_stats()
    except sqlite3.Error as e:
        st.error(f"趨勢資料載入失敗：{e}")
        return

    if not stats:
        return

    stime = stats.get('snapshot_time', '')
    date_str = stime[:10]
    try:
        history_df = load_stats_history(date_str)
    except sqlite3.Error as e:
        st.error(f"趨勢資料載入失敗：{e}")
        return

    if len(history_df) <= 1:
        st.markdown(
            '<div class="section-hdr">📉 即時趨勢監控（當日變化）</div>'
            '<div class="panel" style="text-align:center; padding:25px; margin:10px 0;">'
            '<p class="gray">📊 即時趨勢圖表需要盤中多筆快照資料。'
            '盤中執行 main.py 後此區會自動填充。</p></div>',
            unsafe_allow_html=True)
        return

    # 快取判斷：只在資料真正變動時重新建圖
    chart_font_base = st.session_state.get('chart_font_base', 10)
    chart_height_base = st.session_state.get('chart_height_base', 340)
    latest_history_time = history_df['snapshot_time'].iloc[-1]
    current_key = (date_str, latest_history_time, chart_font_base, chart_height_base)
    cached_key = st.session_state.get('trend_payload_key')
    cached_payload = st.session_state.get('trend_payload')

    if cached_key == current_key and cached_payload and cached_payload.get('date_str') == date_str:
        # 資料沒變，直接用快取（不重新建圖）
        trend_payload = cached_payload
    else:
        # 資料有變，重新建圖
        trend_payload = build_trend_payload(history_df, date_str)
        st.session_state.trend_payload = trend_payload
        st.session_state.trend_payload_key = current_key

    render_trend_section(trend_payload)


if __name__ == "__main__":
    main()
