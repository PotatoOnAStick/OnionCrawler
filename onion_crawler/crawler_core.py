# onion_crawler/crawler_core.py
import threading
import queue
import time
import random
import traceback
from urllib.parse import urljoin, urlparse
import requests 

from bs4 import BeautifulSoup
from . import config
from . import utils
from . import tor_utils
from . import data_structures
from . import database

def process_url(url: str, crawl_id: int, # <<< Added crawl_id
                visited: data_structures.SafeSet,
                files_counter: data_structures.SafeCounter,
                dirs_counter: data_structures.SafeCounter,
                extension_counter: data_structures.SafeExtensionCounter,
                max_depth: int, current_depth: int,
                work_queue: queue.Queue):
    """
    Processes a single URL: fetches content, parses for links, updates counters,
    optionally logs items to DB, and adds subdirectories to the work queue.

    Returns:
        tuple: (status_code, message, num_dirs_added)
               status_code: HTTP status or 0 for internal errors/skips
               message: Description of outcome (e.g., "Success", "HTTP 404", "Timeout")
               num_dirs_added: Number of new directories added to the queue
    """
    if config.GLOBAL_STOP_EVENT.is_set():
        return 0, "Skipped (Shutdown)", 0

    if current_depth > max_depth:
        return 0, f"Skipped (Depth {current_depth} > {max_depth})", 0

    # Normalize URL before adding to visited set
    normalized_url = utils.normalize_url(url)
    if not normalized_url:
         return 0, "Skipped (Invalid URL)", 0

    # Check if already visited (use normalized URL)
    if not visited.add(normalized_url):
        return 0, "Skipped (Already Visited)", 0

    dirs_added_to_queue = 0
    discovered_items_on_page = [] # <<< List to hold items found on this page

    try:
        response = tor_utils.make_tor_request(normalized_url) # Handles rate limiting and basic errors

        # Check if it's likely a directory listing page *before* parsing links
        if not utils.is_directory_listing(response.text):
            return response.status_code, "Not a directory listing", 0

        soup = BeautifulSoup(response.text, 'html.parser')
        links_found = 0
        files_found_on_page = 0
        dirs_found_on_page = 0

        # Get the base path of the current URL to store relative paths correctly
        base_path = urlparse(normalized_url).path

        for a_tag in soup.find_all('a', href=True):
            if config.GLOBAL_STOP_EVENT.is_set():
                break

            href = a_tag.get('href')
            # Basic filtering: ignore empty, anchors, javascript, mailto, absolute paths starting with /
            # Also ignore query parameters for now
            if not href or href.startswith(('#', 'javascript:', 'mailto:', '?', '/')):
                continue
            # Ignore parent directory links more robustly
            if href in {"../", "..", "./", "."}:
                 continue

            links_found += 1
            # Construct the full URL for adding to the queue later
            full_url_for_queue = urljoin(normalized_url, href)

            # Determine the relative path for storage
            # urljoin handles resolving relative paths like ../ correctly for the *full URL*
            # For storage, we often want the path relative to the *site root*.
            # Let's store the href as found for simplicity, assuming listings use relative links.
            # If absolute paths within the site are found, urlparse(full_url_for_queue).path could be used.
            item_path_to_store = href # Store the relative link as found

            # Check if it's a directory (ends with /)
            if href.endswith('/'):
                dirs_found_on_page += 1
                # Add to list for DB logging if enabled
                if config.LOG_INDIVIDUAL_ITEMS:
                    discovered_items_on_page.append(('directory', item_path_to_store))

                # Only add to queue if within depth limits and not already visited
                if current_depth < max_depth:
                    normalized_sub_url = utils.normalize_url(full_url_for_queue)
                    if normalized_sub_url and not visited.contains(normalized_sub_url):
                         work_queue.put((normalized_sub_url, current_depth + 1))
                         dirs_added_to_queue += 1
            else:
                # It's likely a file
                files_found_on_page += 1
                extension = utils.get_file_extension(href)
                if extension:
                    extension_counter.increment(extension)
                else:
                    extension_counter.increment("no_extension")

                # Add to list for DB logging if enabled
                if config.LOG_INDIVIDUAL_ITEMS:
                    discovered_items_on_page.append(('file', item_path_to_store))


        # Increment global counters after processing the page
        if files_found_on_page > 0:
            files_counter.increment(files_found_on_page)
        if dirs_found_on_page > 0:
            dirs_counter.increment(dirs_found_on_page)

        # --- Log discovered items to DB if enabled ---
        if config.LOG_INDIVIDUAL_ITEMS and discovered_items_on_page:
            database.log_discovered_items(crawl_id, discovered_items_on_page)
        # ---

        return response.status_code, f"Success ({files_found_on_page} files, {dirs_found_on_page} dirs)", dirs_added_to_queue

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code
        msg = f"HTTP Error {status}"
        return status, msg, 0
    except requests.exceptions.Timeout:
        return 0, "Request Timeout", 0
    except requests.exceptions.RequestException as e:
        msg = f"Request Error: {type(e).__name__}"
        return 0, msg, 0
    except Exception as e:
        utils.print_if_allowed(f"Error processing {normalized_url}: {e}\n{traceback.format_exc()}")
        msg = f"Processing Error: {type(e).__name__}"
        return 0, msg, 0


class WorkerPool:
    """Manages a pool of worker threads for crawling a single site."""

    # <<< Add crawl_id to __init__ and store it >>>
    def __init__(self, max_workers: int, min_workers: int, target_func, worker_args: tuple, site_url: str, crawl_id: int):
        self.max_workers = max(1, max_workers) # Ensure at least 1
        self.min_workers = max(0, min_workers) # Ensure at least 0
        self.target_func = target_func
        self.worker_args = worker_args # Should include queue, counters, etc.
        self.site_url = site_url
        self.crawl_id = crawl_id # <<< Store crawl_id

        self.workers = []       # List of Thread objects
        self.worker_map = {}    # worker_id -> Thread object
        self.worker_health = {} # worker_id -> {'last_active': timestamp, 'status': str, 'url': str}
        self.active_workers_count = 0

        self.pool_lock = threading.Lock()
        self.stop_event = threading.Event() # Local stop for this pool
        self.adjust_event = threading.Event() # Signal for monitor thread
        self.last_progress_time = time.time()
        self.monitor_thread = None
        self.watchdog_thread = None

    def _worker_wrapper(self, worker_id: int):
        """Wraps the target function to update health and handle stop events."""
        try:
            # <<< Pass crawl_id to the target function >>>
            self.target_func(self, worker_id, self.crawl_id, *self.worker_args, stop_event=self.stop_event)
        except Exception as e:
            utils.print_if_allowed(f"[{self.site_url}] Worker {worker_id} encountered unhandled error: {e}\n{traceback.format_exc()}")
            self.update_worker_health(worker_id, status="Error (Unhandled Exception)")
        finally:
            self.signal_adjustment_needed()

    def _add_worker(self):
        """Adds a new worker thread to the pool. Assumes lock is held."""
        # Check limit based on current count derived from worker_health keys
        if len(self.worker_health) >= self.max_workers:
             # utils.print_if_allowed(f"[{self.site_url}] Add worker skipped: Limit ({self.max_workers}) reached based on health dict.")
             return None

        # Find the next available worker ID
        current_ids = set(self.worker_health.keys())
        worker_id = 0
        while worker_id in current_ids:
            worker_id += 1

        # Use a more descriptive thread name
        thread_name = f"Worker-{worker_id}-{self.site_url[:15]}" # Shorten site_url if needed
        thread = threading.Thread(target=self._worker_wrapper, args=(worker_id,), daemon=True, name=thread_name)

        # Initialize health with URL set to N/A
        self.worker_health[worker_id] = {'last_active': time.time(), 'status': 'Initializing', 'url': 'N/A'}
        self.worker_map[worker_id] = thread # Add to map
        self.workers.append(thread) # Keep track of thread objects too
        self.active_workers_count = len(self.worker_health) # Update count based on health dict size

        thread.start()
        # utils.print_if_allowed(f"[{self.site_url}] Added worker {worker_id}. Total active: {self.active_workers_count}")
        return thread

    def start(self, initial_workers=1):
        """Starts the worker pool and monitoring threads."""
        utils.print_if_allowed(f"[{self.site_url}] Starting worker pool (Min={self.min_workers}, Max={self.max_workers})...")
        self.last_progress_time = time.time()
        with self.pool_lock:
            # Start with min_workers or initial_workers, whichever is larger, up to max_workers
            start_count = min(max(self.min_workers, initial_workers), self.max_workers)
            for _ in range(start_count):
                self._add_worker()

        # Start monitoring thread
        self.monitor_thread = threading.Thread(target=self._monitor_pool, daemon=True, name=f"Monitor-{self.site_url[:15]}")
        self.monitor_thread.start()

        # Start watchdog thread
        self.watchdog_thread = threading.Thread(target=self._watchdog, daemon=True, name=f"Watchdog-{self.site_url[:15]}")
        self.watchdog_thread.start()

    def update_worker_health(self, worker_id: int, status: str | None = None, url: str | None = None):
        """Updates the health status and last active time for a worker."""
        with self.pool_lock:
            if worker_id in self.worker_health:
                health_entry = self.worker_health[worker_id]
                now = time.time()
                health_entry['last_active'] = now # Always update activity time

                if status:
                    health_entry['status'] = status

                # --- URL Logic ---
                if url is not None:
                    # If a specific URL is provided (usually when starting processing), set it.
                    health_entry['url'] = url
                elif status and ("Idle" in status or "Initializing" in status or "Finished" in status):
                    # If the new status indicates the worker is definitely not processing a specific URL,
                    # explicitly set the URL field to N/A.
                    health_entry['url'] = "N/A"
                # --- If status is 'Error' or 'Processing' without a new URL, LEAVE the existing URL ---

                # Also update overall pool progress time if worker is active/just finished
                # Avoid updating if just idle waiting for queue
                if status and "Idle (Queue Empty)" not in status:
                    self.last_progress_time = now

            # else: # Optional: Log if worker_id not found
            #    utils.print_if_allowed(f"[{self.site_url}] Watchdog Warning: Attempted to update health for unknown worker_id {worker_id}")

    def _watchdog(self):
        """Monitors worker threads for inactivity (potential stalls)."""
        utils.print_if_allowed(f"[{self.site_url}] Watchdog started (Timeout: {config.WORKER_TIMEOUT}s).")
        while not self.stop_event.is_set() and not config.GLOBAL_STOP_EVENT.is_set():
            # Use a configurable check interval
            time.sleep(config.WATCHDOG_INTERVAL)
            if self.stop_event.is_set() or config.GLOBAL_STOP_EVENT.is_set(): break

            now = time.time()
            stuck_workers_details = []

            with self.pool_lock:
                if not self.worker_health: # Skip check if no workers are supposed to be active
                    continue

                # Iterate over a copy of keys in case monitor modifies dict
                for worker_id in list(self.worker_health.keys()):
                    health_info = self.worker_health.get(worker_id)
                    if not health_info: continue # Worker might have been removed by monitor

                    last_active = health_info.get('last_active', now)
                    inactive_time = now - last_active

                    # Check if inactive beyond timeout
                    if inactive_time > config.WORKER_TIMEOUT:
                        status = health_info.get('status', 'Unknown')
                        url = health_info.get('url', 'N/A') # Get URL from health info

                        stuck_workers_details.append(
                            f"Worker {worker_id} (Status: {status}, URL: {url}, Inactive: {inactive_time:.0f}s)"
                        )

            if stuck_workers_details:
                 # Log all stuck workers found in this check
                 utils.print_if_allowed(f"[{self.site_url}] Watchdog: Potential stall detected. Stuck workers:")
                 for detail in stuck_workers_details:
                     utils.print_if_allowed(f"  - {detail}")
                 # Signal the main crawl loop (via DynamicCrawler) to handle the stall/reset
                 # The main loop checks time_since_progress which is updated by update_worker_health
                 # The watchdog's role is primarily logging the details of potentially stuck workers.
                 # It can also signal the monitor if needed, but stall handling is in DynamicCrawler.
                 self.signal_adjustment_needed() # Wake up monitor to potentially clean up dead threads sooner

        utils.print_if_allowed(f"[{self.site_url}] Watchdog stopped.")

    def _monitor_pool(self):
        """Dynamically adjusts the number of workers based on queue size and activity."""
        utils.print_if_allowed(f"[{self.site_url}] Pool monitor started.")
        while not self.stop_event.is_set() and not config.GLOBAL_STOP_EVENT.is_set():
            # Wait for signal or check periodically
            self.adjust_event.wait(timeout=config.MONITOR_INTERVAL)
            if self.stop_event.is_set() or config.GLOBAL_STOP_EVENT.is_set(): break
            self.adjust_event.clear() # Reset the event if it was set

            try:
                work_queue = self.worker_args[0] # Assumes queue is the first arg
                queue_size = work_queue.qsize() if hasattr(work_queue, 'qsize') else -1
            except Exception as e:
                 utils.print_if_allowed(f"[{self.site_url}] Monitor Error getting queue size: {e}")
                 queue_size = -1

            with self.pool_lock:
                # --- Worker Cleanup (using worker_map and health dict) ---
                dead_worker_ids = []
                # Iterate over a copy of keys in case we modify the dict
                for worker_id in list(self.worker_health.keys()):
                     thread = self.worker_map.get(worker_id)
                     # Thread might be gone from map already, or thread object exists but isn't alive
                     if not thread or not thread.is_alive():
                         dead_worker_ids.append(worker_id)

                if dead_worker_ids:
                     # utils.print_if_allowed(f"[{self.site_url}] Monitor: Cleaning up {len(dead_worker_ids)} dead worker(s): {dead_worker_ids}")
                     for worker_id in dead_worker_ids:
                         thread_to_remove = self.worker_map.pop(worker_id, None)
                         self.worker_health.pop(worker_id, None)
                         if thread_to_remove:
                             try:
                                 self.workers.remove(thread_to_remove) # Remove from list too
                             except ValueError:
                                 pass # Already removed

                # Recalculate active count based on remaining health entries
                current_active_count = len(self.worker_health)
                self.active_workers_count = current_active_count # Keep instance var in sync

                # --- Worker Adjustment Logic ---
                should_add = False
                should_remove = False

                # Decide whether to add workers
                if queue_size > 0 and current_active_count < self.max_workers:
                    # Add if queue is large relative to workers, or if below min workers
                    if queue_size > current_active_count * config.QUEUE_PER_WORKER_RATIO or current_active_count < self.min_workers:
                         should_add = True

                # Decide whether to remove workers (only if above min_workers)
                # Example: Remove if queue is empty and idle for a while
                # time_since_progress = self.time_since_progress() # Needs access outside lock? Or pass time?
                # if queue_size == 0 and current_active_count > self.min_workers and time_since_progress > 30:
                #     should_remove = True # More complex logic needed here

                # Add workers if needed
                if should_add:
                    workers_to_add = min(
                        max(1, queue_size // config.QUEUE_PER_WORKER_RATIO if config.QUEUE_PER_WORKER_RATIO > 0 else 1),
                        self.max_workers - current_active_count
                    )
                    if workers_to_add > 0:
                        # utils.print_if_allowed(f"[{self.site_url}] Monitor: Queue({queue_size}), Active({current_active_count}). Adding {workers_to_add} worker(s).")
                        added_count = 0
                        for _ in range(workers_to_add):
                            if self._add_worker(): # _add_worker handles count increment
                                added_count += 1
                        # if added_count > 0: utils.print_if_allowed(f"[{self.site_url}] Monitor: Added {added_count} workers.")

                # Remove workers if needed (Placeholder - requires careful implementation)
                # if should_remove:
                #    workers_to_remove = current_active_count - self.min_workers
                #    if workers_to_remove > 0:
                #        # Implement logic to signal specific idle workers to stop
                #        pass

            # Prevent busy-looping if adjust_event is triggered frequently without timeout
            # time.sleep(0.1) # Short sleep if needed, but wait() timeout handles this mostly

        utils.print_if_allowed(f"[{self.site_url}] Pool monitor stopped.")

    def signal_adjustment_needed(self):
        """Signals the monitor thread to check the pool state."""
        self.adjust_event.set()

    def record_progress(self):
        """Records that some progress was made (e.g., a URL was processed)."""
        # This method might be redundant if update_worker_health handles it
        with self.pool_lock:
            self.last_progress_time = time.time()

    def time_since_progress(self) -> float:
        """Returns the time in seconds since progress was last recorded."""
        with self.pool_lock:
            # Ensure read is atomic with potential updates
            last_prog = self.last_progress_time
        return time.time() - last_prog

    def get_active_count(self) -> int:
        """Returns the current count of active workers based on health tracking."""
        with self.pool_lock:
            # The number of entries in worker_health should be the most reliable count
            # as it's maintained by the monitor's cleanup logic.
            current_health_count = len(self.worker_health)
            # Optional: reconcile with self.active_workers_count if needed
            # if current_health_count != self.active_workers_count:
            #     self.active_workers_count = current_health_count
            return current_health_count

    def reset_workers(self):
        """Stops all current workers and starts a few fresh ones."""
        utils.print_if_allowed(f"[{self.site_url}] Resetting worker pool...")
        dead_threads = []
        with self.pool_lock:
            # Signal existing workers to stop via the shared event
            self.stop_event.set()

            # Clear tracking structures immediately
            current_worker_ids = list(self.worker_map.keys())
            dead_threads = [self.worker_map.pop(w_id) for w_id in current_worker_ids if w_id in self.worker_map]
            self.worker_health.clear()
            self.workers.clear() # Clear the list of threads too
            self.active_workers_count = 0

        # Wait briefly for threads to exit outside the lock
        for thread in dead_threads:
            if thread and thread.is_alive():
                thread.join(timeout=1.0) # Give 1 sec to exit

        # Clear the stop event for new workers
        self.stop_event.clear()
        self.last_progress_time = time.time() # Reset progress timer

        # Start a small number of new workers
        with self.pool_lock:
            initial_reset_workers = min(max(self.min_workers, 1), self.max_workers) # Start at least 1, up to max
            utils.print_if_allowed(f"[{self.site_url}] Starting {initial_reset_workers} fresh workers after reset.")
            for _ in range(initial_reset_workers):
                self._add_worker()
        self.signal_adjustment_needed() # Tell monitor to check state

    def shutdown(self, wait=True):
        """Stops all worker threads and monitoring."""
        utils.print_if_allowed(f"[{self.site_url}] Shutting down worker pool...")
        self.stop_event.set() # Signal workers and monitor/watchdog
        self.signal_adjustment_needed() # Wake up monitor if waiting

        # Wait for monitor and watchdog
        threads_to_join = []
        if self.monitor_thread and self.monitor_thread.is_alive():
            threads_to_join.append(self.monitor_thread)
        if self.watchdog_thread and self.watchdog_thread.is_alive():
            threads_to_join.append(self.watchdog_thread)

        # Wait for worker threads if requested
        if wait:
            with self.pool_lock: # Access worker_map safely
                threads_to_join.extend([t for t in self.worker_map.values() if t and t.is_alive()])

        for thread in threads_to_join:
            try:
                thread.join(timeout=2.0) # Give threads time to finish
            except Exception as e:
                 utils.print_if_allowed(f"[{self.site_url}] Error joining thread {thread.name}: {e}")


        # Final cleanup
        with self.pool_lock:
            self.active_workers_count = 0
            self.workers = []
            self.worker_health = {}
            self.worker_map = {}
        utils.print_if_allowed(f"[{self.site_url}] Worker pool shut down.")


def worker_function(pool: WorkerPool, # Pass pool instance
                    worker_id: int,
                    crawl_id: int, # <<< Added crawl_id
                    # Unpack other args passed during pool creation
                    work_queue: queue.Queue,
                    visited: data_structures.SafeSet,
                    files_counter: data_structures.SafeCounter,
                    dirs_counter: data_structures.SafeCounter,
                    extension_counter: data_structures.SafeExtensionCounter,
                    max_depth: int,
                    stop_event: threading.Event): # Pool's stop event
    """Target function for worker threads."""
    if not pool:
        utils.print_if_allowed(f"Worker {worker_id}: Could not get pool reference. Exiting.")
        return

    pool.update_worker_health(worker_id, status="Idle (Started)")

    while not stop_event.is_set() and not config.GLOBAL_STOP_EVENT.is_set():
        url = None
        depth = -1
        item_retrieved = False
        try:
            try:
                url, depth = work_queue.get(timeout=1.0)
                item_retrieved = True
            except queue.Empty:
                pool.update_worker_health(worker_id, status="Idle (Queue Empty)")
                time.sleep(0.5)
                continue

            pool.update_worker_health(worker_id, status="Processing", url=url)

            # <<< Pass crawl_id to process_url >>>
            status_code, message, dirs_added = process_url(
                url, crawl_id, visited, files_counter, dirs_counter,
                extension_counter, max_depth, depth, work_queue
            )

            if dirs_added > 0:
                pool.signal_adjustment_needed()

            pool.update_worker_health(worker_id, status="Idle (Task Done)")

        except Exception as e:
            utils.print_if_allowed(f"Worker {worker_id} error in main loop (URL: {url}): {e}\n{traceback.format_exc()}")
            pool.update_worker_health(worker_id, status="Error")
            time.sleep(1)
        finally:
            if item_retrieved:
                try:
                    work_queue.task_done()
                except ValueError:
                    pass
                except Exception as td_err:
                    utils.print_if_allowed(f"Worker {worker_id}: Error calling task_done for {url}: {td_err}")

    pool.update_worker_health(worker_id, status="Finished")

class DynamicCrawler:
    """Orchestrates the crawl for a single website using a WorkerPool."""
    def __init__(self, max_workers: int, max_depth: int):
        self.max_workers = max_workers if max_workers > 0 else config.DEFAULT_MAX_WORKERS_PER_SITE
        self.min_workers = 1
        self.max_depth = max_depth
        self.pool = None

    def crawl_site(self, start_url: str, crawl_id: int):
        """
        Crawls a single site starting from start_url.
        Args: start_url, crawl_id
        Returns: status, files_count, dirs_count, extensions_summary
        """
        normalized_start_url = utils.normalize_url(start_url)
        utils.print_if_allowed(f"[{normalized_start_url}] Starting crawl (ID: {crawl_id})...")

        visited = data_structures.SafeSet()
        files_counter = data_structures.SafeCounter(0)
        dirs_counter = data_structures.SafeCounter(0)
        extension_counter = data_structures.SafeExtensionCounter()
        work_queue = queue.Queue()
        work_queue.put((normalized_start_url, 0))

        # <<< Pass crawl_id when creating WorkerPool >>>
        self.pool = WorkerPool(
            max_workers=self.max_workers,
            min_workers=self.min_workers,
            target_func=worker_function,
            worker_args=(work_queue, visited, files_counter, dirs_counter,
                         extension_counter, self.max_depth),
            site_url=normalized_start_url,
            crawl_id=crawl_id # <<< Pass crawl_id here
        )
        self.pool.start(initial_workers=self.min_workers)

        start_time = time.time()
        last_status_print_time = start_time
        status = "Error"
        reset_count = 0

        try:
            # --- Main crawl loop ---
            while not config.GLOBAL_STOP_EVENT.is_set():
                current_time = time.time()
                elapsed_time = current_time - start_time
                
                # --- Check for completion ---
                active_workers = self.pool.get_active_count()
                try:
                    is_queue_empty = work_queue.empty()
                    q_size = work_queue.qsize() if hasattr(work_queue, 'qsize') else -1
                    if is_queue_empty:
                        self.pool.signal_adjustment_needed()
                except Exception as q_err:
                    utils.print_if_allowed(f"[{normalized_start_url}] Error checking queue: {q_err}")
                    is_queue_empty = False
                    q_size = -1

                if is_queue_empty and active_workers == 0:
                     time.sleep(0.5)
                     if work_queue.empty() and self.pool.get_active_count() == 0:
                         if self.pool.time_since_progress() > 3.0:
                             status = "Completed"
                             utils.print_if_allowed(f"[{normalized_start_url}] Queue empty, no active workers, no recent progress. Crawl complete.")
                             break
                         else:
                              utils.print_if_allowed(f"[{normalized_start_url}] Queue empty, no active workers, but recent progress detected. Re-checking...")
                     else:
                         utils.print_if_allowed(f"[{normalized_start_url}] Completion check: Queue empty, but active workers > 0 or refilled. Continuing...")
                         self.pool.signal_adjustment_needed()

                # --- Check for Stalls ---
                time_since_progress = self.pool.time_since_progress()
                if time_since_progress > config.MAX_STALLED_TIME:
                    # --- Reset/Stall Logic ---
                    if reset_count < config.MAX_RESETS_PER_SITE:
                         utils.print_if_allowed(f"[{normalized_start_url}] No progress for {time_since_progress:.0f}s. Resetting worker pool (Attempt {reset_count + 1}/{config.MAX_RESETS_PER_SITE}).")
                         self.pool.reset_workers()
                         reset_count += 1
                    else:
                         status = "Stalled"
                         utils.print_if_allowed(f"[{normalized_start_url}] Stalled for {time_since_progress:.0f}s after {reset_count} resets. Terminating crawl.")
                         break

                # --- Status Reporting ---
                if current_time - last_status_print_time >= 10.0:
                    files = files_counter.get()
                    dirs = dirs_counter.get()
                    q_size_str = str(q_size) if q_size != -1 else "?"
                    progress_time_str = f"{time_since_progress:.0f}s" if time_since_progress > 1 else "<1s"
                    status_line = (f"[{normalized_start_url}] "
                                   f"Time: {elapsed_time:.0f}s | "
                                   f"Workers: {active_workers}/{self.max_workers} | "
                                   f"Queue: {q_size_str} | "
                                   f"Files: {files} | "
                                   f"Dirs: {dirs} | "
                                   f"Idle: {progress_time_str}")
                    utils.print_if_allowed(status_line)
                    last_status_print_time = current_time

                time.sleep(1.0)

        except KeyboardInterrupt:
            utils.print_if_allowed(f"\n[{normalized_start_url}] KeyboardInterrupt received.")
            config.GLOBAL_STOP_EVENT.set()
            status = "Interrupted"
        except Exception as e:
            utils.print_if_allowed(f"[{normalized_start_url}] Unexpected error during crawl loop: {e}\n{traceback.format_exc()}")
            status = "Error"
        finally:
            if self.pool:
                self.pool.shutdown(wait=False)

            final_files = files_counter.get()
            final_dirs = dirs_counter.get()
            extensions_summary = extension_counter.get_summary(top_n=20)

            # Refine final status based on results
            if status == "Completed" and final_files == 0 and final_dirs == 0:
                 # If it completed normally but found nothing, check if start URL was valid listing
                 # This might require checking the result of the *first* process_url call
                 # For now, let's use a specific status like 'NoData' or 'NoListing'
                 # We need a way to know if the start URL itself was not a listing.
                 # Let's assume 'NoData' for now if completed with zero results.
                 status = "NoData"
            elif status == "Error" and final_files == 0 and final_dirs == 0:
                 pass

            utils.print_if_allowed(f"[{normalized_start_url}] Crawl finished with status: {status}")

            return status, final_files, final_dirs, extensions_summary