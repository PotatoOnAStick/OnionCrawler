# onion_crawler/config.py
import threading
import time

# --- Tor Configuration ---
TOR_PROXY = {
    'http': 'socks5h://127.0.0.1:9050',
    'https': 'socks5h://127.0.0.1:9050'
}

# --- Timeouts (seconds) ---
MAX_STALLED_TIME = 60  # Max time without progress before considering stall
MAX_CRAWL_TIME = 600   # Max total crawl time per site (10 minutes)
WORKER_TIMEOUT = 60    # Max time a worker can be inactive before watchdog intervention
REQUEST_TIMEOUT_BASE = 15 # Base timeout for requests
REQUEST_TIMEOUT_JITTER = 5  # Random jitter added to request timeout
MONITOR_INTERVAL = 5    # How often the pool monitor checks queue/workers (seconds)
WATCHDOG_INTERVAL = 15  # How often the watchdog checks worker health (seconds)

# --- Concurrency ---
DEFAULT_MAX_WORKERS_PER_SITE = 10
DEFAULT_PARALLEL_SITES = 5
DEFAULT_MAX_TOR_CONNECTIONS = 200
QUEUE_PER_WORKER_RATIO = 5 # How many items in queue trigger adding a new worker (adjust as needed)

# --- Database ---
DATABASE_FILE = 'crawl_data.db'
LOG_INDIVIDUAL_ITEMS = False # <<< New Flag: Default to False

# --- Output Control ---
SILENT_MODE = False
CONSOLE_LOCK = threading.Lock()

# --- Global Control ---
GLOBAL_STOP_EVENT = threading.Event()
TOR_RATE_LIMITER = threading.Semaphore(DEFAULT_MAX_TOR_CONNECTIONS) # Default, can be overridden by args

# --- Other Constants ---
MAX_RESETS_PER_SITE = 1 # Max worker pool resets before giving up on a site

# --- Helper to update rate limiter ---
def set_tor_rate_limit(limit: int):
    global TOR_RATE_LIMITER
    TOR_RATE_LIMITER = threading.Semaphore(limit)

# --- Helper to update silent mode ---
def set_silent_mode(silent: bool):
    global SILENT_MODE
    SILENT_MODE = silent

# --- Helper to update item logging mode ---
def set_log_items_mode(log_items: bool): # <<< New Helper
    global LOG_INDIVIDUAL_ITEMS
    LOG_INDIVIDUAL_ITEMS = log_items

# --- Helper to get current time ---
def get_current_timestamp():
    return time.strftime("%Y-%m-%d %H:%M:%S")
