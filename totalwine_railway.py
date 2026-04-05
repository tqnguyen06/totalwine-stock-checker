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
from datetime import datetime
from typing import Optional

from curl_cffi import requests as curl_requests
from zoneinfo import ZoneInfo

# Regular requests for Pushover/Discord (doesn't need TLS fingerprinting)
import requests

# Configuration
TW_STORES = [s.strip() for s in os.getenv("TW_STORES", "907,945").split(",") if s.strip()]
TW_CHECK_INTERVAL = int(os.getenv("TW_CHECK_INTERVAL", "600"))
PUSHOVER_APP_TOKEN = os.getenv("PUSHOVER_APP_TOKEN", "")
PUSHOVER_USER_KEY = "uzmaqrmwawus7dk8smym64rzovrt5p"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
DISCORD_ROLE_ID = os.getenv("DISCORD_ROLE_ID", "")
TIMEZONE = os.getenv("TIMEZONE", "America/New_York")

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

        return {
            "store_id": store_id,
            "store_name": store_name,
            "in_stock": in_stock,
            "stock_message": pickup_status,
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
    session = curl_requests.Session(impersonate="chrome")

    results = {}

    for product in products:
        store_results = []

        for store_id in store_ids:
            result = check_stock(product, store_id, session)
            store_results.append(result)

            in_stock = result.get("in_stock", False)
            msg = result.get("stock_message", result.get("error", "?"))
            store_name = result.get("store_name", store_id)
            status = f"IN STOCK ({msg})" if in_stock else msg
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
        store_lines.append(
            f"**{s['store_name']}** — {s.get('stock_message', 'In stock')}"
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
        store_lines.append(f"{s['store_name']}: {s.get('stock_message', 'In stock')}")

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

def run_once(products: list[dict]) -> bool:
    """Run a single check across all stores. Returns True if any product is in stock."""
    stores_str = ", ".join(store_display(s) for s in TW_STORES)
    log(f"Checking {len(products)} product(s) at {len(TW_STORES)} store(s): {stores_str}")

    results = check_all_stores(products, TW_STORES)

    state = load_state()
    any_in_stock = False
    state_changed = False

    for product in products:
        name = product["name"]
        store_results = results.get(name, [])

        in_stock_stores = [s for s in store_results if s.get("in_stock")]
        current_store_ids = {s["store_id"] for s in in_stock_stores}
        known_stores = set(state.get("in_stock_stores", {}).get(name, []))

        new_stores = [s for s in in_stock_stores if s["store_id"] not in known_stores]
        gone_stores = known_stores - current_store_ids

        if in_stock_stores:
            any_in_stock = True

        if new_stores:
            log(f"NEW STOCK: {name} at {len(new_stores)} new store(s)!")
            send_pushover_alert(name, new_stores, product.get("url", ""))
            send_discord_alert(name, new_stores, product.get("url", ""))

        if gone_stores:
            log(f"{name}: {len(gone_stores)} store(s) went out of stock")

        if in_stock_stores:
            log(f"{name}: In stock at {len(in_stock_stores)} store(s) ({len(new_stores)} new)")
        else:
            log(f"{name}: Out of stock everywhere")

        if current_store_ids != known_stores:
            state.setdefault("in_stock_stores", {})[name] = list(current_store_ids)
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

    for p in products:
        log(f"  - {p['name']}")

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
    TIMEZONE                 Timezone (default: America/New_York)
""")


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
