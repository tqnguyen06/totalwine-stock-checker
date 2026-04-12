"""
Total Wine In-Store Stock Checker - Railway Edition

Monitors Total Wine product pages for in-store stock availability and sends
Pushover + Discord alerts when items are found at nearby stores.

No browser required - scrapes product pages with curl_cffi.

Usage:
    python totalwine_railway.py              # Run continuous monitor
    python totalwine_railway.py --once       # Check once and exit
    python totalwine_railway.py --test       # Test notifications
    python totalwine_railway.py --help       # Show help

Environment Variables:
    TW_PRODUCTS              - Product configs (see below for format)
    TW_STORES                - Comma-separated store IDs (default: 907,945)
    TW_CHECK_INTERVAL        - Seconds between checks (default: 600)
    PUSHOVER_APP_TOKEN       - Pushover API token
    DISCORD_WEBHOOK_URL      - Discord webhook URL (optional)
    DISCORD_ROLE_ID          - Discord role ID to ping (optional)
    PROXY                    - Residential proxy (format: host:port:user:pass)
    TIMEZONE                 - Timezone for timestamps (default: America/New_York)

TW_PRODUCTS format (pipe-separated fields, semicolon between products):
    name|productId|url ; name|productId|url

    The productId is the number at the end of the Total Wine URL path.

    Example:
    Jack Daniels 14yr|2126261899|https://www.totalwine.com/spirits/american-whiskey/jack-daniels-14-year-tennessee-whiskey/p/2126261899
"""

import json
import os
import re
import sys
import time
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

from curl_cffi import requests as curl_requests
from zoneinfo import ZoneInfo

# Regular requests for Pushover/Discord (doesn't need TLS fingerprinting)
import requests

# Shared stock cache for the API server
_stock_cache: dict = {"stock": [], "last_updated": None}

# Configuration
TW_STORES = [s.strip() for s in os.getenv("TW_STORES", "907,945").split(",") if s.strip()]
TW_CHECK_INTERVAL = int(os.getenv("TW_CHECK_INTERVAL", "600"))
PUSHOVER_APP_TOKEN = os.getenv("PUSHOVER_APP_TOKEN", "")
PUSHOVER_USER_KEY = "uzmaqrmwawus7dk8smym64rzovrt5p"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
DISCORD_ROLE_ID = os.getenv("DISCORD_ROLE_ID", "")
TIMEZONE = os.getenv("TIMEZONE", "America/New_York")

# Proxy support - format: host:port:user:pass
_proxy_raw = os.getenv("PROXY", "")
PROXY_URL = None
if _proxy_raw:
    parts = _proxy_raw.split(":")
    if len(parts) == 4:
        host, port, user, password = parts
        PROXY_URL = f"http://{user}:{password}@{host}:{port}"
    elif _proxy_raw.startswith("http"):
        PROXY_URL = _proxy_raw
    else:
        print(f"WARNING: Invalid PROXY format. Expected host:port:user:pass")

BASE_URL = "https://www.totalwine.com"
STATE_FILE = os.getenv("STATE_FILE", "/tmp/totalwine_stock_state.json")

# Store name mapping
STORE_NAMES = {
    "907": "Jacksonville",
    "945": "North Jacksonville",
}


def log(msg: str) -> None:
    """Print with timestamp."""
    tz = ZoneInfo(TIMEZONE)
    ts = datetime.now(tz).strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def get_time_str() -> str:
    """Get current time string."""
    tz = ZoneInfo(TIMEZONE)
    return datetime.now(tz).strftime("%I:%M %p %Z on %B %d, %Y")


def store_display(store_id: str) -> str:
    """Get display name for a store."""
    name = STORE_NAMES.get(store_id, "")
    return f"{name} (#{store_id})" if name else f"Store #{store_id}"


# ---------------------------------------------------------------------------
# Product parsing
# ---------------------------------------------------------------------------

def parse_products_env() -> list[dict]:
    """Parse TW_PRODUCTS env var into product list.

    Uses semicolon (;) between products.
    Fields within each product are pipe (|) separated.
    """
    raw = os.getenv("TW_PRODUCTS", "")
    if not raw.strip():
        return []

    products = []
    for entry in raw.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split("|")
        if len(parts) < 2:
            print(f"WARNING: Skipping malformed product entry: {entry}")
            print("  Expected format: name|productId|url")
            continue

        product_id = parts[1].strip()
        url = parts[2].strip() if len(parts) > 2 else ""

        # If no URL provided, we can't scrape
        if not url:
            print(f"WARNING: No URL for {parts[0].strip()}, skipping")
            continue

        products.append({
            "name": parts[0].strip(),
            "productId": product_id,
            "url": url,
        })
    return products


# ---------------------------------------------------------------------------
# Stock checking via page scrape
# ---------------------------------------------------------------------------

def check_stock(product: dict, store_id: str, session) -> dict:
    """
    Check stock for a product at a specific Total Wine store.

    Returns dict with: store_id, store_name, in_stock, stock_message
    """
    # Build URL with store parameter
    url = product["url"]
    # Remove existing query params and add store
    base_url = url.split("?")[0]
    check_url = f"{base_url}?s={store_id}&igrules=true"

    store_name = store_display(store_id)

    try:
        resp = session.get(check_url, timeout=20)

        if resp.status_code == 403:
            log(f"  403 Forbidden - Total Wine may be blocking requests")
            return {"store_id": store_id, "store_name": store_name, "error": "blocked"}

        if resp.status_code != 200:
            log(f"  HTTP {resp.status_code} for store {store_id}")
            return {"store_id": store_id, "store_name": store_name, "error": f"http_{resp.status_code}"}

        text = resp.text

        # Extract stock messages for each shopping method
        stock_msgs = re.findall(
            r'"shoppingMethod":"([^"]+)","stockMessage":"([^"]+)"',
            text,
        )

        # Check INSTORE_PICKUP specifically
        pickup_status = "Unknown"
        for method, msg in stock_msgs:
            if method == "INSTORE_PICKUP":
                pickup_status = msg
                break

        in_stock = pickup_status.lower() not in ("out of stock", "unavailable", "unknown")

        # Extract quantity — use digitalStoreQuantity first (accurate for
        # allocated/in-store-only bottles where "stock" reports 0)
        qty_match = re.search(r'"digitalStoreQuantity":(\d+)', text)
        if not qty_match or int(qty_match.group(1)) == 0:
            qty_match = re.search(r'"stock":(\d+)', text)
        quantity = int(qty_match.group(1)) if qty_match else 0

        # If status says "in stock" but quantity is 0, treat as out of stock.
        # Total Wine's CDN sometimes serves the "limited quantities" message
        # inconsistently for allocated bottles with no actual stock.
        if in_stock and quantity == 0:
            in_stock = False

        # Extract price
        price_match = re.search(r'itemProp="price" content="([^"]+)"', text)
        price = price_match.group(1) if price_match else ""

        # Extract aisle/bay
        aisle_match = re.search(r'(Aisle \d+[^"]*)', text)
        bay_match = re.search(r'"bay":"([^"]+)"', text)
        aisle = aisle_match.group(1) if aisle_match else ""
        bay = bay_match.group(1) if bay_match else ""
        location = ""
        if aisle and bay:
            location = f"{aisle} | {bay}"
        elif aisle:
            location = aisle
        elif bay:
            location = bay

        return {
            "store_id": store_id,
            "store_name": store_name,
            "in_stock": in_stock,
            "stock_message": pickup_status,
            "quantity": quantity,
            "price": price,
            "location": location,
            "all_methods": {method: msg for method, msg in stock_msgs},
        }

    except Exception as e:
        log(f"  Error checking store {store_id}: {e}")
        return {"store_id": store_id, "store_name": store_name, "error": str(e)}


def check_all_stores(products: list[dict], store_ids: list[str]) -> dict:
    """
    Check stock for all products across all stores.

    Returns dict: product_name -> list of store results
    """
    proxy_kwargs = {}
    if PROXY_URL:
        proxy_kwargs["proxies"] = {"http": PROXY_URL, "https": PROXY_URL}
    session = curl_requests.Session(impersonate="chrome", **proxy_kwargs)

    results = {}

    for product in products:
        store_results = []

        for store_id in store_ids:
            result = check_stock(product, store_id, session)
            store_results.append(result)

            in_stock = result.get("in_stock", False)
            msg = result.get("stock_message", result.get("error", "?"))
            qty = result.get("quantity", 0)
            price = result.get("price", "")
            loc = result.get("location", "")
            store_name = result.get("store_name", store_id)
            details = f"Qty: {qty}"
            if price:
                details += f", ${price}"
            if loc:
                details += f", {loc}"
            status = f"IN STOCK ({details})" if in_stock else msg
            log(f"  [{store_name}] {product['name']}: {status}")

            # Small delay between stores
            time.sleep(2)

        results[product["name"]] = store_results

        # Delay between products
        if len(products) > 1:
            time.sleep(2)

    return results


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state() -> dict:
    """Load previous alert state."""
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r") as f:
                return json.load(f)
    except (json.JSONDecodeError, IOError):
        pass
    return {"in_stock_stores": {}}


def save_state(state: dict) -> None:
    """Save alert state."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except IOError as e:
        log(f"Could not save state: {e}")


# ---------------------------------------------------------------------------
# Discord notifications
# ---------------------------------------------------------------------------

def send_discord_alert(product_name: str, stores: list[dict], product_url: str) -> bool:
    """Send Discord embed for in-stock product."""
    if not DISCORD_WEBHOOK_URL:
        return False

    store_lines = []
    for s in stores[:10]:
        details = []
        qty = s.get("quantity", 0)
        price = s.get("price", "")
        loc = s.get("location", "")
        if qty:
            details.append(f"Qty: {qty}")
        if price:
            details.append(f"${price}")
        if loc:
            details.append(loc)
        detail_str = f" ({', '.join(details)})" if details else ""
        store_lines.append(
            f"**{s['store_name']}** — {s.get('stock_message', 'In stock')}{detail_str}"
        )

    description = "\n".join(store_lines)

    embed = {
        "title": f"TOTAL WINE IN STOCK: {product_name}",
        "description": description,
        "color": 0x00FF00,
        "footer": {"text": f"Stores: {', '.join(TW_STORES)} | {get_time_str()}"},
    }
    if product_url:
        embed["url"] = product_url

    payload: dict = {"embeds": [embed]}
    if DISCORD_ROLE_ID:
        payload["content"] = f"<@&{DISCORD_ROLE_ID}>"

    try:
        resp = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        log(f"Discord alert sent for {product_name}")
        return True
    except requests.RequestException as e:
        log(f"Discord error: {e}")
        return False


# ---------------------------------------------------------------------------
# Pushover notifications
# ---------------------------------------------------------------------------

def send_pushover_alert(product_name: str, stores: list[dict], product_url: str) -> bool:
    """Send Pushover emergency alert for in-stock product."""
    if not PUSHOVER_APP_TOKEN:
        return False

    store_lines = []
    for s in stores[:3]:
        details = []
        qty = s.get("quantity", 0)
        price = s.get("price", "")
        loc = s.get("location", "")
        if qty:
            details.append(f"Qty: {qty}")
        if price:
            details.append(f"${price}")
        if loc:
            details.append(loc)
        detail_str = f" ({', '.join(details)})" if details else ""
        store_lines.append(f"{s['store_name']}: {s.get('stock_message', 'In stock')}{detail_str}")

    message = f"{product_name}\n\n" + "\n".join(store_lines)
    if len(stores) > 3:
        message += f"\n+{len(stores) - 3} more stores"

    data = {
        "token": PUSHOVER_APP_TOKEN,
        "user": PUSHOVER_USER_KEY,
        "title": "TOTAL WINE IN STOCK",
        "message": message,
        "priority": 2,
        "retry": 30,
        "expire": 300,
        "sound": "siren",
        "url": product_url,
        "url_title": "Open Total Wine",
        "timestamp": int(time.time()),
    }
    try:
        resp = requests.post("https://api.pushover.net/1/messages.json", data=data, timeout=10)
        if resp.status_code == 200:
            log("Pushover alert sent")
            return True
        else:
            log(f"Pushover failed: HTTP {resp.status_code}")
            return False
    except requests.RequestException as e:
        log(f"Pushover error: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_once(products: list[dict], silent: bool = False) -> bool:
    """Run a single check across all stores. Returns True if any product is in stock.
    If silent=True, records state but skips sending alerts (used for baseline scan)."""
    stores_str = ", ".join(store_display(s) for s in TW_STORES)
    log(f"Checking {len(products)} product(s) at {len(TW_STORES)} store(s): {stores_str}")

    results = check_all_stores(products, TW_STORES)

    # Update stock cache for API server
    update_stock_cache(products, results)

    state = load_state()
    any_in_stock = False
    state_changed = False

    for product in products:
        name = product["name"]
        store_results = results.get(name, [])

        in_stock_stores = [s for s in store_results if s.get("in_stock")]
        errored_stores = [s for s in store_results if s.get("error")]
        current_store_ids = {s["store_id"] for s in in_stock_stores}
        known_stores = set(state.get("in_stock_stores", {}).get(name, []))
        oos_counts = state.get("oos_counts", {}).get(name, {})

        # Don't count errored stores as "gone out of stock"
        errored_ids = {s["store_id"] for s in errored_stores}
        current_store_ids |= (known_stores & errored_ids)

        # Require 3 consecutive out-of-stock checks before removing a store.
        # Prevents CDN flip-flop from wiping state and re-alerting.
        stores_to_remove = set()
        for sid in known_stores - current_store_ids:
            count = oos_counts.get(sid, 0) + 1
            oos_counts[sid] = count
            if count >= 3:
                stores_to_remove.add(sid)
                log(f"{name}: Store {store_display(sid)} confirmed out of stock ({count} consecutive)")
            else:
                # Keep in state until confirmed
                current_store_ids.add(sid)
                log(f"{name}: Store {store_display(sid)} OOS check {count}/3 — keeping in state")

        # Clear OOS counters for stores that are in stock
        for sid in current_store_ids:
            oos_counts.pop(sid, None)

        # Remove confirmed OOS stores
        current_store_ids -= stores_to_remove

        new_stores = [s for s in in_stock_stores if s["store_id"] not in known_stores]

        if in_stock_stores:
            any_in_stock = True

        if new_stores:
            if silent:
                log(f"BASELINE: {name} at {len(new_stores)} store(s) (no alert)")
            else:
                log(f"NEW STOCK: {name} at {len(new_stores)} new store(s)!")
                send_pushover_alert(name, new_stores, product.get("url", ""))
                send_discord_alert(name, new_stores, product.get("url", ""))

        if stores_to_remove:
            log(f"{name}: {len(stores_to_remove)} store(s) confirmed out of stock")

        if in_stock_stores:
            log(f"{name}: In stock at {len(in_stock_stores)} store(s) ({len(new_stores)} new)")
        else:
            log(f"{name}: Out of stock everywhere")

        if current_store_ids != known_stores or oos_counts:
            state.setdefault("in_stock_stores", {})[name] = list(current_store_ids)
            state.setdefault("oos_counts", {})[name] = oos_counts
            state_changed = True

    if state_changed:
        save_state(state)

    return any_in_stock


def run_continuous(products: list[dict]) -> None:
    """Run continuous monitoring loop."""
    log(f"Starting continuous monitor")
    log(f"Products: {len(products)}")
    log(f"Stores: {', '.join(store_display(s) for s in TW_STORES)}")
    log(f"Check interval: {TW_CHECK_INTERVAL}s ({TW_CHECK_INTERVAL // 60}min)")
    log(f"Pushover: {'Yes' if PUSHOVER_APP_TOKEN else 'No'}")
    log(f"Discord: {'Yes' if DISCORD_WEBHOOK_URL else 'No'}")
    log(f"Proxy: {'Yes' if PROXY_URL else 'No'}")

    for p in products:
        log(f"  - {p['name']}")

    # Start API server in background thread
    api_thread = threading.Thread(target=start_api_server, daemon=True)
    api_thread.start()

    check_count = 0

    while True:
        check_count += 1
        log(f"--- Check #{check_count} ---")

        try:
            run_once(products)
        except Exception as e:
            log(f"Error during check: {e}")

        log(f"Next check in {TW_CHECK_INTERVAL}s")
        time.sleep(TW_CHECK_INTERVAL)


def test_notifications() -> None:
    """Test Pushover and Discord notifications."""
    print("\n--- Testing Notifications ---\n")

    fake_stores = [{
        "store_id": "907",
        "store_name": "Jacksonville (#907)",
        "in_stock": True,
        "stock_message": "In stock",
    }]

    if PUSHOVER_APP_TOKEN:
        ok = send_pushover_alert("Test Product", fake_stores, "https://www.totalwine.com")
        print(f"Pushover: {'OK' if ok else 'FAILED'}")
    else:
        print("Pushover: Not configured (set PUSHOVER_APP_TOKEN)")

    if DISCORD_WEBHOOK_URL:
        ok = send_discord_alert("Test Product", fake_stores, "https://www.totalwine.com")
        print(f"Discord: {'OK' if ok else 'FAILED'}")
    else:
        print("Discord: Not configured (set DISCORD_WEBHOOK_URL)")


def show_help() -> None:
    print("""
Total Wine In-Store Stock Checker - Railway Edition
=====================================================

Monitors Total Wine product pages and alerts via Pushover + Discord.

Commands:
    python totalwine_railway.py              Continuous monitoring
    python totalwine_railway.py --once       Single check, then exit
    python totalwine_railway.py --test       Test notifications
    python totalwine_railway.py --help       Show this help

TW_PRODUCTS format:
    name|productId|url ; name|productId|url

Example:
    Jack Daniels 14yr|2126261899|https://www.totalwine.com/spirits/american-whiskey/jack-daniels-14-year-tennessee-whiskey/p/2126261899

Known Jacksonville area stores:
    907 = Jacksonville (Town Center Parkway)
    945 = North Jacksonville

Environment Variables:
    TW_PRODUCTS              Product list (required, see format above)
    TW_STORES                Store IDs, comma-separated (default: 907,945)
    TW_CHECK_INTERVAL        Seconds between checks (default: 600)
    PUSHOVER_APP_TOKEN       Pushover API token
    DISCORD_WEBHOOK_URL      Discord webhook URL (optional)
    DISCORD_ROLE_ID          Discord role ID to ping (optional)
    PROXY                    Residential proxy (host:port:user:pass)
    TIMEZONE                 Timezone (default: America/New_York)
""")


# ---------------------------------------------------------------------------
# HTTP API server (runs in background thread)
# ---------------------------------------------------------------------------

API_PORT = int(os.getenv("PORT", "8080"))
API_TOKEN = os.getenv("STOCK_API_TOKEN", "")

class StockAPIHandler(BaseHTTPRequestHandler):
    """Serves cached stock data as JSON."""

    def do_GET(self):
        # Optional token auth
        if API_TOKEN:
            auth = self.headers.get("Authorization", "")
            if auth != f"Bearer {API_TOKEN}":
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"Unauthorized"}')
                return

        if self.path == "/api/stock":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(_stock_cache).encode())
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok":true}')
        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization, Content-Type")
        self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress HTTP logs


def start_api_server():
    """Start HTTP server in background thread."""
    server = HTTPServer(("0.0.0.0", API_PORT), StockAPIHandler)
    log(f"Stock API server listening on port {API_PORT}")
    server.serve_forever()


def update_stock_cache(products: list[dict], results: dict):
    """Update the shared stock cache with latest check results."""
    stock_list = []
    for product in products:
        name = product["name"]
        store_results = results.get(name, [])
        stores = []
        for s in store_results:
            if s.get("error"):
                stores.append({
                    "storeId": s.get("store_id", ""),
                    "storeName": s.get("store_name", ""),
                    "inStock": False,
                    "stockMessage": s.get("error", "Error"),
                    "quantity": 0,
                    "price": "",
                    "location": "",
                })
            else:
                stores.append({
                    "storeId": s.get("store_id", ""),
                    "storeName": s.get("store_name", ""),
                    "inStock": s.get("in_stock", False),
                    "stockMessage": s.get("stock_message", "Unknown"),
                    "quantity": s.get("quantity", 0),
                    "price": s.get("price", ""),
                    "location": s.get("location", ""),
                })
        stock_list.append({
            "name": name,
            "productId": product["productId"],
            "stores": stores,
        })

    _stock_cache["stock"] = stock_list
    _stock_cache["last_updated"] = datetime.now(ZoneInfo(TIMEZONE)).isoformat()


def main() -> None:
    if "--help" in sys.argv or "-h" in sys.argv:
        show_help()
        return

    if "--test" in sys.argv:
        test_notifications()
        return

    products = parse_products_env()
    if not products:
        print("ERROR: No products configured.")
        print("Set TW_PRODUCTS env var. Run with --help for format.")
        sys.exit(1)

    if "--once" in sys.argv:
        run_once(products)
    else:
        run_continuous(products)


if __name__ == "__main__":
    main()
