"""
共用圖表設定模組
讀寫 DB app_settings（key=chart_settings）及 config/chart_settings.json，供設定頁和歷史指標頁共用。
讀取優先順序：DB → 本機 JSON → 預設值；儲存時同時寫 DB + 本機 JSON。
"""
import json
import os

_BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CHART_SETTINGS_PATH = os.path.join(_BASE_DIR, 'config', 'chart_settings.json')

DEFAULT_CHART_SETTINGS = {
    "font": {
        "title_size": 16,
        "axis_tick_size": 11,
        "legend_size": 10,
        "stat_card_value_rem": 1.6,
        "stat_card_label_rem": 0.82,
    },
    "chart": {
        "height": 400,
        "line_width_raw": 1.5,
        "line_width_ma5": 2.5,
        "raw_alpha": 0.4,
        "band_alpha_outer": 0.08,
        "band_alpha_inner": 0.18,
        "show_bands": True,
        "show_median": True,
        "show_p5_p95": True,
        "show_iqr_outlier": True,
    },
    "palette": {
        "primary": "#4A90D9",
        "positive": "#E8756C",
        "negative": "#5BAA8A",
        "sma_colors": ["#F5C26B", "#4A90D9", "#9B8EC4"],
        "median_color": "#BBBBBB",
        "iqr_outlier_color": "#FF6B6B",
        "p25p75_color": "#F59E0B",
    },
    "table": {
        "show_all_columns": False,
        "height": 480,
    },
    "lookback": 1000,
}


def _deep_merge(base: dict, override: dict) -> dict:
    """把 override 的值覆蓋到 base，遞迴處理巢狀 dict，遺漏的 key 保留 base 預設值。"""
    result = dict(base)
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def _normalize(data: dict) -> dict:
    """型別安全轉換，確保所有欄位型別正確。"""
    import copy
    normalized = _deep_merge(copy.deepcopy(DEFAULT_CHART_SETTINGS), data)
    f = normalized['font']
    f['title_size'] = int(f['title_size'])
    f['axis_tick_size'] = int(f['axis_tick_size'])
    f['legend_size'] = int(f['legend_size'])
    f['stat_card_value_rem'] = float(f['stat_card_value_rem'])
    f['stat_card_label_rem'] = float(f['stat_card_label_rem'])
    c = normalized['chart']
    c['height'] = int(c['height'])
    c['line_width_raw'] = float(c['line_width_raw'])
    c['line_width_ma5'] = float(c['line_width_ma5'])
    c['raw_alpha'] = float(c['raw_alpha'])
    c['band_alpha_outer'] = float(c['band_alpha_outer'])
    c['band_alpha_inner'] = float(c['band_alpha_inner'])
    c['show_bands'] = bool(c['show_bands'])
    c['show_median'] = bool(c['show_median'])
    c['show_p5_p95'] = bool(c['show_p5_p95'])
    c['show_iqr_outlier'] = bool(c['show_iqr_outlier'])
    normalized['lookback'] = int(normalized['lookback'])
    normalized['table']['height'] = int(normalized['table']['height'])
    normalized['table']['show_all_columns'] = bool(normalized['table']['show_all_columns'])
    return normalized


def load_chart_settings() -> dict:
    """讀取設定。優先順序：DB app_settings → 本機 JSON → 預設值。"""
    import copy
    raw = None
    # 1. 嘗試從 DB 讀取
    try:
        import sys
        sys.path.insert(0, _BASE_DIR)
        from lib.db import get_connection, load_db_setting
        conn = get_connection()
        try:
            val = load_db_setting(conn, 'chart_settings')
            if val:
                raw = json.loads(val)
        finally:
            conn.close()
    except Exception:
        pass
    # 2. 回退到本機 JSON
    if raw is None:
        try:
            with open(CHART_SETTINGS_PATH, 'r', encoding='utf-8') as fp:
                raw = json.load(fp)
        except (FileNotFoundError, json.JSONDecodeError):
            pass
    # 3. 合併預設值
    if raw:
        return _normalize(raw)
    return copy.deepcopy(DEFAULT_CHART_SETTINGS)


def save_chart_settings(data: dict) -> None:
    """正規化後同時寫入 DB app_settings 和本機 JSON。"""
    normalized = _normalize(data)
    payload = json.dumps(normalized, ensure_ascii=False)
    # 1. 寫入 DB
    try:
        import sys
        sys.path.insert(0, _BASE_DIR)
        from lib.db import get_connection, save_db_setting, init_all_tables
        conn = get_connection()
        try:
            init_all_tables(conn)
            save_db_setting(conn, 'chart_settings', payload)
        finally:
            conn.close()
    except Exception:
        pass
    # 2. 同時寫入本機 JSON（本地開發備用）
    os.makedirs(os.path.dirname(CHART_SETTINGS_PATH), exist_ok=True)
    with open(CHART_SETTINGS_PATH, 'w', encoding='utf-8') as f_out:
        json.dump(normalized, f_out, ensure_ascii=False, indent=4)
