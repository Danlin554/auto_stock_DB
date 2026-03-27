"""
Zeabur IP 連線測試腳本
=====================
此腳本不需要 fubon_neo SDK，直接測試 Zeabur 伺服器能否連線到
富邦 API 的各個端點。

部署到 Zeabur 後查看 Service Logs，即可得知結果。
"""
import socket
import ssl
import time
import urllib.request
import json
import sys

# ── 測試目標 ─────────────────────────────────────────────
TARGETS = [
    ("neoapi.fbs.com.tw", 443, "富邦交易 WebSocket API（主要）"),
    ("api.fugle.tw",       443, "Fugle 行情 API（Fubon SDK 依賴）"),
    ("fubon-api.fugle.tw", 443, "Fugle WebSocket（行情）"),
]


def get_my_ip():
    """取得本機對外 IP"""
    try:
        with urllib.request.urlopen("https://api.ipify.org?format=json", timeout=5) as r:
            data = json.loads(r.read())
            return data.get("ip", "未知")
    except Exception as e:
        return f"無法取得（{e}）"


def tcp_test(host, port, label):
    """TCP + TLS 連線測試"""
    print(f"\n▶ 測試：{label}")
    print(f"  主機：{host}:{port}")

    # DNS 解析
    try:
        resolved_ips = [info[4][0] for info in socket.getaddrinfo(host, port)]
        print(f"  DNS 解析：{', '.join(set(resolved_ips))}")
    except socket.gaierror as e:
        print(f"  ✗ DNS 解析失敗：{e}")
        return False

    # TCP 連線
    start = time.time()
    try:
        sock = socket.create_connection((host, port), timeout=10)
        latency_ms = (time.time() - start) * 1000
        print(f"  ✓ TCP 連線成功（延遲：{latency_ms:.0f}ms）")
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        print(f"  ✗ TCP 連線失敗：{e}")
        return False

    # TLS 握手
    try:
        ctx = ssl.create_default_context()
        tls_sock = ctx.wrap_socket(sock, server_hostname=host)
        cert = tls_sock.getpeercert()
        subj = dict(x[0] for x in cert.get('subject', []))
        print(f"  ✓ TLS 握手成功（憑證主體：{subj.get('commonName', 'N/A')}）")
        tls_sock.close()
    except ssl.SSLError as e:
        print(f"  ✗ TLS 失敗：{e}")
        sock.close()
        return False

    return True


def main():
    print("=" * 55)
    print("  富邦 API 連線測試（Zeabur IP 測試）")
    print("=" * 55)

    my_ip = get_my_ip()
    print(f"\n本機對外 IP：{my_ip}")
    print("（請確認此 IP 是否在台灣 IP 範圍 — 若 Fubon 有 IP 限制）\n")

    results = []
    for host, port, label in TARGETS:
        ok = tcp_test(host, port, label)
        results.append((label, ok))

    print("\n" + "=" * 55)
    print("  測試結果摘要")
    print("=" * 55)
    all_ok = True
    for label, ok in results:
        status = "✓ 可連線" if ok else "✗ 無法連線"
        print(f"  {status}  {label}")
        if not ok:
            all_ok = False

    print()
    if all_ok:
        print("✅ 所有端點均可連線！")
        print("   Zeabur 伺服器 IP 可以到達富邦 API 伺服器。")
        print("   下一步：進行完整 SDK 登入測試以確認身分驗證是否通過。")
    else:
        print("❌ 部分端點無法連線。")
        print("   建議：考慮混合架構（main.py 留本機，儀表板上雲）。")

    print()
    print(f"測試完成時間：{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
