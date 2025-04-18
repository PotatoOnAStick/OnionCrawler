# onion_crawler/database.py
import sqlite3
import threading
from contextlib import contextmanager
from . import config
from . import utils

# Use a thread-local connection pattern or a single connection with locking
# For simplicity with ThreadPoolExecutor, locking a single connection is often easier.
db_lock = threading.Lock()
_connection = None

def get_db_connection():
    """Gets the singleton database connection, creating it if necessary."""
    global _connection
    if _connection is None:
        try:
            # isolation_level=None enables autocommit mode, simplifying transactions
            # for single statements. Use explicit transactions for multi-statement operations.
            # Set timeout to handle potential lock contention
            _connection = sqlite3.connect(config.DATABASE_FILE, check_same_thread=False, timeout=10.0) # Allow sharing across threads
            _connection.row_factory = sqlite3.Row # Access columns by name
            utils.print_if_allowed(f"Database connection established to {config.DATABASE_FILE}")
        except sqlite3.Error as e:
            utils.print_if_allowed(f"Error connecting to database {config.DATABASE_FILE}: {e}")
            raise # Propagate error if connection fails
    return _connection

@contextmanager
def db_cursor(commit=False):
    """Provides a database cursor and handles commit/rollback and closing."""
    conn = get_db_connection()
    # Acquire lock before getting cursor to ensure thread safety for the operation
    with db_lock:
        cursor = conn.cursor()
        try:
            yield cursor
            if commit:
                conn.commit()
        except sqlite3.Error as e:
            utils.print_if_allowed(f"Database Error: {e}. Rolling back transaction.")
            conn.rollback()
            raise # Re-raise the exception after rollback
        finally:
            cursor.close() # Ensure cursor is closed

def initialize_database():
    """Creates the necessary tables if they don't exist."""
    utils.print_if_allowed("Initializing database schema...")
    try:
        with db_cursor(commit=True) as cursor:
            # Crawl summary table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crawls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT NOT NULL UNIQUE,
                    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    end_time TIMESTAMP,
                    status TEXT CHECK(status IN ('Starting', 'Completed', 'Interrupted', 'Timeout', 'Stalled', 'Error', 'NoListing', 'NoData')) NOT NULL,
                    total_files INTEGER,
                    total_dirs INTEGER,
                    error_message TEXT
                )
            """)
            # Index on URL for faster lookups
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_crawls_url ON crawls(url)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_crawls_status ON crawls(status)")

            # File extensions table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS extensions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    crawl_id INTEGER NOT NULL,
                    extension TEXT NOT NULL,
                    count INTEGER NOT NULL,
                    percentage REAL NOT NULL,
                    FOREIGN KEY(crawl_id) REFERENCES crawls(id) ON DELETE CASCADE
                )
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_extensions_crawl_id ON extensions(crawl_id)")

            # Discovered items table
            cursor.execute("""
                 CREATE TABLE IF NOT EXISTS discovered_items (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     crawl_id INTEGER NOT NULL,
                     item_type TEXT CHECK(item_type IN ('file', 'directory')) NOT NULL,
                     item_path TEXT NOT NULL, -- Store full path relative to base URL
                     FOREIGN KEY(crawl_id) REFERENCES crawls(id) ON DELETE CASCADE
                 )
            """)
            # Add index for faster querying by crawl_id
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_items_crawl_id ON discovered_items(crawl_id)")
            # Add UNIQUE constraint to prevent duplicates per crawl
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_unique ON discovered_items(crawl_id, item_path)")


        utils.print_if_allowed("Database schema initialized successfully.")
    except sqlite3.Error as e:
        utils.print_if_allowed(f"Database initialization failed: {e}")
        raise

def start_crawl_record(url: str) -> tuple[int | None, str]:
    """
    Records the start of a crawl attempt or updates the start time if retrying.
    Checks if a crawl for the URL already exists and is in a state that should not be restarted.

    Returns:
        tuple[int | None, str]:
            - crawl_id (int): The ID of the crawl record, or None if an error occurred.
            - initial_status (str): 'New' if inserted, 'Restarted' if updated from a non-terminal state,
                                    'Exists' if found in a terminal state (e.g., Completed), 'Error' on failure.
    """
    normalized_url = utils.normalize_url(url)
    start_timestamp = config.get_current_timestamp()
    crawl_id = None
    initial_status = "Error" # Default status

    try:
        with db_cursor(commit=True) as cursor:
            # --- Check existing status first ---
            cursor.execute("SELECT id, status FROM crawls WHERE url = ?", (normalized_url,))
            existing_record = cursor.fetchone()

            if existing_record:
                existing_id = existing_record['id']
                existing_status = existing_record['status']
                # Define statuses that mean we should NOT restart the crawl
                # Add 'Starting' here too, to prevent race conditions if another process just started it.
                terminal_statuses = {'Completed', 'Starting'} # Add others? 'Timeout', 'Stalled'? Maybe allow restarting these?
                                                              # Let's keep it simple for now: only skip 'Completed' and 'Starting'.
                if existing_status in terminal_statuses:
                    utils.print_if_allowed(f"Crawl record for {normalized_url} exists with status '{existing_status}'. Skipping restart.")
                    return existing_id, "Exists"
                else:
                    # Record exists but in a state we can restart (e.g., Error, Interrupted, NoData, Timeout, Stalled)
                    initial_status = "Restarted"
            else:
                # Record does not exist yet
                initial_status = "New"

            # --- Perform INSERT or UPDATE (Resetting to 'Starting') ---
            # This query will either insert a new record or update an existing one,
            # ensuring the status is 'Starting' and timestamps/counts are reset.
            cursor.execute("""
                INSERT INTO crawls (url, start_time, status)
                VALUES (?, ?, 'Starting')
                ON CONFLICT(url) DO UPDATE SET
                    start_time = excluded.start_time,
                    status = 'Starting',
                    end_time = NULL,
                    total_files = NULL,
                    total_dirs = NULL,
                    error_message = NULL
                WHERE crawls.url = excluded.url -- Ensure we update the correct row
            """, (normalized_url, start_timestamp))

            # --- Get the ID ---
            # If it was an update, existing_id holds the value. If new, get lastrowid.
            if existing_record:
                 crawl_id = existing_record['id']
            else:
                 # Fetch the ID reliably after insert
                 cursor.execute("SELECT id FROM crawls WHERE url = ?", (normalized_url,))
                 result = cursor.fetchone()
                 if result:
                     crawl_id = result['id']
                 else:
                      # This path should be unlikely now, but handle defensively
                      raise sqlite3.Error(f"Failed to retrieve crawl ID after insert for {normalized_url}")

            return crawl_id, initial_status # Return ID and the determined initial status

    except sqlite3.Error as e:
        utils.print_if_allowed(f"Database error starting crawl record for {url}: {e}")
        return None, "Error" # Return None for ID and 'Error' status on DB exception
    except Exception as e: # Catch unexpected errors too
        utils.print_if_allowed(f"Unexpected error in start_crawl_record for {url}: {e}")
        return None, "Error"

def finish_crawl_record(crawl_id: int, status: str, files: int, dirs: int, extensions_summary: dict, error_msg: str | None = None):
    """Updates the crawl record with final status, counts, and extensions."""
    end_timestamp = config.get_current_timestamp()
    try:
        with db_cursor(commit=True) as cursor: # Commit happens after all operations succeed
            # Update the main crawl record
            cursor.execute("""
                UPDATE crawls
                SET end_time = ?, status = ?, total_files = ?, total_dirs = ?, error_message = ?
                WHERE id = ?
            """, (end_timestamp, status, files, dirs, error_msg, crawl_id))

            # Clear previous extension data for this crawl_id before inserting new ones
            cursor.execute("DELETE FROM extensions WHERE crawl_id = ?", (crawl_id,))

            # Insert new extension data if the crawl was successful/partially successful
            if status in ('Completed', 'Interrupted', 'Timeout', 'Stalled') and extensions_summary:
                ext_data = [
                    (crawl_id, ext, count, percentage)
                    for ext, (count, percentage) in extensions_summary.items()
                ]
                if ext_data:
                    cursor.executemany("""
                        INSERT INTO extensions (crawl_id, extension, count, percentage)
                        VALUES (?, ?, ?, ?)
                    """, ext_data)

            # --- Placeholder for future individual item logging ---
            # if config.LOG_INDIVIDUAL_ITEMS:
            #     # Clear previous items
            #     cursor.execute("DELETE FROM discovered_items WHERE crawl_id = ?", (crawl_id,))
            #     # Insert new items (example)
            #     # file_items = [(crawl_id, 'file', path) for path in discovered_files]
            #     # dir_items = [(crawl_id, 'directory', path) for path in discovered_dirs]
            #     # if file_items: cursor.executemany("...", file_items)
            #     # if dir_items: cursor.executemany("...", dir_items)
            # --- End Placeholder ---

    except sqlite3.Error as e:
        utils.print_if_allowed(f"Database error finishing crawl record ID {crawl_id}: {e}")
        # Don't re-raise here, as the crawl itself finished, just logging failed.
        # Consider logging this failure to a separate fallback log if critical.

def log_discovered_items(crawl_id: int, items: list[tuple[str, str]]):
    """
    Logs discovered files and directories for a specific crawl.

    Args:
        crawl_id: The ID of the crawl.
        items: A list of tuples, where each tuple is (item_type, item_path).
               item_type is 'file' or 'directory'.
               item_path is the relative path found.
    """
    if not items:
        return

    # Prepare data with crawl_id added
    data_to_insert = [(crawl_id, item_type, item_path) for item_type, item_path in items]

    try:
        with db_cursor(commit=True) as cursor:
            # Use INSERT OR IGNORE to gracefully handle potential duplicate paths
            # due to the UNIQUE constraint (idx_items_unique)
            cursor.executemany("""
                INSERT OR IGNORE INTO discovered_items (crawl_id, item_type, item_path)
                VALUES (?, ?, ?)
            """, data_to_insert)
    except sqlite3.Error as e:
        # Log the error but don't stop the crawl
        utils.print_if_allowed(f"Database error logging discovered items for crawl ID {crawl_id}: {e}")

def get_crawls(status_filter=None, limit=None, sort_by='start_time', descending=True, url_pattern=None):
    """
    Retrieves crawl records with filtering, sorting, and limiting.

    Args:
        status_filter (list[str], optional): List of statuses to include. Defaults to None (all statuses).
        limit (int, optional): Maximum number of records to return. Defaults to None (no limit).
        sort_by (str, optional): Column to sort by (e.g., 'start_time', 'end_time', 'total_files'). Defaults to 'start_time'.
        descending (bool, optional): Sort in descending order. Defaults to True.
        url_pattern (str, optional): SQL LIKE pattern for URL matching. Defaults to None.

    Returns:
        list[sqlite3.Row]: A list of crawl records (as Row objects).
    """
    valid_sort_columns = ['id', 'url', 'start_time', 'end_time', 'status', 'total_files', 'total_dirs']
    if sort_by not in valid_sort_columns:
        sort_by = 'start_time' # Default to safe sort column

    query = f"SELECT id, url, status, start_time, end_time, total_files, total_dirs FROM crawls"
    params = []
    conditions = []

    if status_filter:
        # Create placeholders for each status in the list
        placeholders = ', '.join('?' * len(status_filter))
        conditions.append(f"status IN ({placeholders})")
        params.extend(status_filter)

    if url_pattern:
        conditions.append("url LIKE ?")
        params.append(f"%{url_pattern}%") # Add wildcards for LIKE

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += f" ORDER BY {sort_by} {'DESC' if descending else 'ASC'}"

    if limit:
        query += " LIMIT ?"
        params.append(limit)

    try:
        with db_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    except sqlite3.Error as e:
        utils.print_if_allowed(f"Database error fetching crawls: {e}")
        return []

def get_crawl_details(crawl_id=None, url=None):
    """
    Retrieves full details for a single crawl by ID or URL.

    Args:
        crawl_id (int, optional): The ID of the crawl.
        url (str, optional): The URL of the crawl (will be normalized).

    Returns:
        tuple(sqlite3.Row | None, list[sqlite3.Row]):
            - The crawl record (or None if not found).
            - A list of extension records for that crawl.
    """
    if not crawl_id and not url:
        return None, []

    crawl_record = None
    extensions = []

    try:
        with db_cursor() as cursor:
            if crawl_id:
                cursor.execute("SELECT * FROM crawls WHERE id = ?", (crawl_id,))
            elif url:
                normalized_url = utils.normalize_url(url)
                cursor.execute("SELECT * FROM crawls WHERE url = ?", (normalized_url,))

            crawl_record = cursor.fetchone()

            if crawl_record:
                # Fetch extensions if crawl record was found
                cursor.execute("""
                    SELECT extension, count, percentage
                    FROM extensions
                    WHERE crawl_id = ?
                    ORDER BY count DESC
                """, (crawl_record['id'],))
                extensions = cursor.fetchall()

        return crawl_record, extensions

    except sqlite3.Error as e:
        utils.print_if_allowed(f"Database error fetching crawl details: {e}")
        return None, []

def get_aggregated_extensions(top_n=20, min_count=1):
    """
    Retrieves aggregated extension counts across all successful/partially successful crawls.

    Args:
        top_n (int, optional): Return the top N extensions. Defaults to 20.
        min_count (int, optional): Minimum total count for an extension to be included. Defaults to 1.

    Returns:
        list[tuple[str, int]]: List of (extension, total_count) tuples.
    """
    query = """
        SELECT extension, SUM(count) as total_count
        FROM extensions
        GROUP BY extension
        HAVING total_count >= ?
        ORDER BY total_count DESC
        LIMIT ?
    """
    params = (min_count, top_n)
    try:
        with db_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    except sqlite3.Error as e:
        utils.print_if_allowed(f"Database error fetching aggregated extensions: {e}")
        return []

def get_crawl_summary_stats():
    """Calculates overall statistics from the crawls table."""
    stats = {
        'total_crawls': 0,
        'status_counts': {},
        'total_files_sum': 0,
        'total_dirs_sum': 0,
        'avg_duration_seconds': None,
        'completed_count': 0,
    }
    try:
        with db_cursor() as cursor:
            # Total crawls and status counts
            cursor.execute("SELECT status, COUNT(*) as count FROM crawls GROUP BY status")
            status_rows = cursor.fetchall()
            for row in status_rows:
                stats['status_counts'][row['status']] = row['count']
                stats['total_crawls'] += row['count']
                if row['status'] == 'Completed':
                    stats['completed_count'] = row['count']


            # Sum of files/dirs (only for crawls where they are not NULL)
            cursor.execute("SELECT SUM(total_files), SUM(total_dirs) FROM crawls WHERE total_files IS NOT NULL AND total_dirs IS NOT NULL")
            sums = cursor.fetchone()
            if sums:
                stats['total_files_sum'] = sums[0] if sums[0] is not None else 0
                stats['total_dirs_sum'] = sums[1] if sums[1] is not None else 0

            # Average duration for completed crawls
            # Use julianday for potentially more accurate time differences in SQLite
            cursor.execute("""
                SELECT AVG(CAST((julianday(end_time) - julianday(start_time)) * 86400.0 AS REAL)) as avg_duration
                FROM crawls
                WHERE status = 'Completed' AND start_time IS NOT NULL AND end_time IS NOT NULL
            """)
            avg_row = cursor.fetchone()
            if avg_row and avg_row['avg_duration'] is not None:
                 # Round the average duration to 2 decimal places
                 stats['avg_duration_seconds'] = round(avg_row['avg_duration'], 2)


        return stats
    except sqlite3.Error as e:
        utils.print_if_allowed(f"Database error fetching summary stats: {e}")
        return stats # Return partially filled stats if possible

def get_discovered_items(crawl_id: int, item_type_filter: str | None = None, limit: int | None = None):
    """
    Retrieves discovered items for a specific crawl.

    Args:
        crawl_id (int): The ID of the crawl.
        item_type_filter (str, optional): Filter by 'file' or 'directory'. Defaults to None (both).
        limit (int, optional): Maximum number of items to return. Defaults to None.

    Returns:
        list[sqlite3.Row]: A list of discovered item records.
    """
    query = "SELECT item_type, item_path FROM discovered_items WHERE crawl_id = ?"
    params = [crawl_id]

    if item_type_filter and item_type_filter in ('file', 'directory'):
        query += " AND item_type = ?"
        params.append(item_type_filter)

    query += " ORDER BY item_path" # Sort alphabetically for consistent output

    if limit:
        query += " LIMIT ?"
        params.append(limit)

    try:
        with db_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    except sqlite3.Error as e:
        utils.print_if_allowed(f"Database error fetching discovered items for crawl ID {crawl_id}: {e}")
        return []

def close_db_connection():
    """Closes the database connection if it's open."""
    global _connection
    if _connection:
        with db_lock: # Ensure no other thread is using it
            try:
                _connection.close()
                _connection = None
                utils.print_if_allowed("Database connection closed.")
            except sqlite3.Error as e:
                 utils.print_if_allowed(f"Error closing database connection: {e}")

