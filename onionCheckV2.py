import sys
import os
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import argparse
from datetime import datetime
import threading
import queue
import time
import random
import signal
import concurrent.futures
from collections import Counter
import json
import csv
import traceback

# Configure the proxy for Tor
TOR_PROXY = {
    'http': 'socks5h://127.0.0.1:9050',
    'https': 'socks5h://127.0.0.1:9050'
}

# Global stop event
GLOBAL_STOP_EVENT = threading.Event()

# Global silent mode flag
SILENT_MODE = False

# Global console lock to prevent output overlap
CONSOLE_LOCK = threading.Lock()

# Constants for timeouts
MAX_STALLED_TIME = 60  # Maximum time (seconds) to wait when no progress is made
MAX_CRAWL_TIME = 600   # Maximum total crawl time (10 minutes)
WORKER_TIMEOUT = 60    # Maximum time a worker can be running without making progress


def print_if_allowed(message, end="\n"):
    """Print message only if not in silent mode"""
    if not SILENT_MODE:
        with CONSOLE_LOCK:  # Use lock to prevent output overlap
            print(message, end=end)
            sys.stdout.flush()  # Ensure output is flushed immediately


class SafeCounter:
    def __init__(self, initial=0):
        self.value = initial
        self.lock = threading.Lock()
        self.last_update_time = time.time()
        
    def increment(self, amount=1):
        with self.lock:
            self.value += amount
            self.last_update_time = time.time()
            return self.value
            
    def get(self):
        with self.lock:
            return self.value
            
    def get_time_since_update(self):
        """Return seconds since last update"""
        with self.lock:
            return time.time() - self.last_update_time


class SafeExtensionCounter:
    def __init__(self):
        self.extensions = {}
        self.lock = threading.Lock()
        
    def increment(self, extension):
        if not extension or extension == "no_extension":
            return
            
        with self.lock:
            if extension in self.extensions:
                self.extensions[extension] += 1
            else:
                self.extensions[extension] = 1
    
    def get_all(self):
        with self.lock:
            return dict(self.extensions)
    
    def get_summary(self, top_n=10):
        with self.lock:
            total = sum(self.extensions.values())
            if total == 0:
                return {}
                
            # Get most common extensions
            extensions_counter = Counter(self.extensions)
            most_common = extensions_counter.most_common(top_n)
            
            # Calculate percentages
            result = {}
            for ext, count in most_common:
                percentage = (count / total) * 100
                result[ext] = (count, round(percentage, 2))
                
            return result


class SafeSet:
    def __init__(self):
        self.items = set()
        self.lock = threading.Lock()
        
    def add(self, item):
        with self.lock:
            if item in self.items:
                return False
            self.items.add(item)
            return True
            
    def size(self):
        with self.lock:
            return len(self.items)


class AtomicLogger:
    def __init__(self, success_file, error_file, suppress_errors=False, export_format=None):
        self.success_file = success_file
        self.error_file = error_file
        self.suppress_errors = suppress_errors
        self.export_format = export_format
        self.file_lock = threading.Lock()
        self.results_data = []  # Store structured data for export
        
    def log_success(self, url, files, dirs, extensions_summary, status):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # If no files and no directories were found, log as error instead
        if files == 0 and dirs == 0:
            self.log_error(url, "No files or directories found")
            return
            
        # Store structured data for export
        result_data = {
            "timestamp": timestamp,
            "url": url,
            "files": files,
            "directories": dirs,
            "extensions": extensions_summary,
            "status": status
        }
        
        # Add to results data for potential export
        with self.file_lock:
            self.results_data.append(result_data)
        
        # Log to text file
        log_line = f"{timestamp} | {url} | Files: {files} | Directories: {dirs} | Extensions: {extensions_summary} | {status}\n"
        with self.file_lock:
            with open(self.success_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
                f.flush()
                
    def log_error(self, url, error):
        if self.suppress_errors:
            return
            
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"{timestamp} | {url} | Error: {error}\n"
        with self.file_lock:
            with open(self.error_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
                f.flush()
                
    def log_exception(self, url, exception, include_traceback=True):
        """Log exception with optional traceback for better debugging"""
        if self.suppress_errors:
            return
            
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Format exception with traceback if requested
        if include_traceback:
            tb = traceback.format_exc()
            error_msg = f"Exception: {str(exception)}\n{tb}"
        else:
            error_msg = f"Exception: {str(exception)}"
            
        log_line = f"{timestamp} | {url} | {error_msg}\n"
        
        with self.file_lock:
            with open(self.error_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
                f.flush()
    
    def export_results(self):
        """Export results to the specified format."""
        if not self.export_format or not self.results_data:
            return
            
        base_filename = os.path.splitext(self.success_file)[0]
        
        with self.file_lock:
            if self.export_format == 'json':
                json_file = f"{base_filename}.json"
                with open(json_file, 'w', encoding='utf-8') as f:
                    json.dump(self.results_data, f, indent=2)
                print_if_allowed(f"Results exported to JSON: {json_file}")
                
            elif self.export_format == 'csv':
                csv_file = f"{base_filename}.csv"
                with open(csv_file, 'w', encoding='utf-8', newline='') as f:
                    # Extract all possible extension keys from results
                    extension_keys = set()
                    for result in self.results_data:
                        if isinstance(result['extensions'], dict):
                            for ext in result['extensions']:
                                extension_keys.add(ext)
                        
                    # Create flattened data for CSV
                    flat_data = []
                    for result in self.results_data:
                        flat_row = {
                            'timestamp': result['timestamp'],
                            'url': result['url'],
                            'files': result['files'],
                            'directories': result['directories'],
                            'status': result['status']
                        }
                        
                        # Add extensions data
                        if isinstance(result['extensions'], dict):
                            for ext in extension_keys:
                                if ext in result['extensions']:
                                    count, percentage = result['extensions'][ext]
                                    flat_row[f"ext_{ext}_count"] = count
                                    flat_row[f"ext_{ext}_percent"] = percentage
                                else:
                                    flat_row[f"ext_{ext}_count"] = 0
                                    flat_row[f"ext_{ext}_percent"] = 0
                        
                        flat_data.append(flat_row)
                    
                    # Write to CSV
                    if flat_data:
                        writer = csv.DictWriter(f, fieldnames=flat_data[0].keys())
                        writer.writeheader()
                        writer.writerows(flat_data)
                    
                print_if_allowed(f"Results exported to CSV: {csv_file}")


def is_directory_listing(html_content):
    """Check if the HTML is a directory listing page (Index of)."""
    soup = BeautifulSoup(html_content, 'html.parser')
    title = soup.title.text.strip() if soup.title else ""
    h1_text = soup.h1.text.strip() if soup.h1 else ""
    
    return ("Index of" in title or "Index of" in h1_text)


def normalize_url(url):
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    if not url.endswith('/'):
        url += '/'

    return url


def get_file_extension(filename):
    """Extract file extension from a filename."""
    if not filename or filename.endswith('/'):
        return None
    
    # Handle files without extensions
    if '.' not in filename:
        return "no_extension"
        
    # Get the extension (lowercase for consistency)
    extension = filename.split('.')[-1].lower()
    
    # If the extension is too long, it might not be a real extension
    if len(extension) > 10:
        return "no_extension"
        
    return extension


def process_url(url, visited, files_counter, dirs_counter, extension_counter, max_depth, current_depth, work_queue, results, logger):
    """Process a single URL, counting files and adding directories to queue"""
    if GLOBAL_STOP_EVENT.is_set():
        return 0
    
    if current_depth > max_depth:
        return 0
    
    url = normalize_url(url)
        
    # Skip if already visited
    if not visited.add(url):
        return 0

    try:
        # Acquire semaphore
        with TOR_RATE_LIMITER:
            #  Add jitter to prevent THUNDERING HERD
            time.sleep(random.uniform(0.1, 0.5))
            
            timeout = 15 + random.uniform(0, 5)
            response = requests.get(url, proxies=TOR_PROXY, timeout=timeout)
        
        if response.status_code != 200:
            results[url] = f"HTTP error: {response.status_code}"
            return 0
            
        if not is_directory_listing(response.text):
            results[url] = "Not a directory listing page"
            return 0
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # find all links
        links = []
        for a_tag in soup.find_all('a'):
            href = a_tag.get('href')
            if href and not href.startswith(('?', '/')):
                links.append(href)
        
        local_files = 0
        local_dirs = 0
        
        for link in links:
            if GLOBAL_STOP_EVENT.is_set():
                break
                
            # Skip parent directory links
            if link in {"../", "..", ".", "./"}:
                continue
                
            # Check again
            if link.endswith('/'):
                local_dirs += 1
                dirs_counter.increment()  # Increment global counter
                
                # Add subdir to queue
                if current_depth < max_depth:
                    full_url = urljoin(url, link)
                    work_queue.put((full_url, current_depth + 1))
            else:
                local_files += 1
                files_counter.increment()  # Increment global counter
                
                # Extract and count file extension
                extension = get_file_extension(link)
                if extension:
                    extension_counter.increment(extension)
        
        # Store success result
        results[url] = f"Success (found {local_files} files, {local_dirs} dirs at depth {current_depth})"
        
        # Return number of directories found at this level
        return local_dirs
        
    except requests.exceptions.RequestException as e:
        results[url] = f"Request error: {str(e)}"
    except Exception as e:
        results[url] = f"Error: {str(e)}"
        # Log exception with traceback for better debugging
        logger.log_exception(url, e)
    
    return 0


class WorkerPool:
    """A pool of worker threads that can be dynamically adjusted with improved deadlock detection"""
    
    def __init__(self, max_workers, target_func, worker_args, site_url="", logger=None):
        self.max_workers = max_workers
        self.target_func = target_func
        self.worker_args = worker_args
        self.workers = []
        self.active_workers = 0
        self.stop_event = threading.Event()
        self.pool_lock = threading.Lock()
        self.adjust_event = threading.Event()
        self.site_url = site_url
        self.logger = logger
        self.last_progress_time = time.time()
        self.worker_health = {}  # Track workers health
        self.worker_watchdog = None
        
    def start(self, initial_workers=1):
        """Start the worker pool with initial_workers"""
        with self.pool_lock:
            for i in range(initial_workers):
                self._add_worker()
                
            # Start monitoring thread
            self.monitor_thread = threading.Thread(target=self._monitor_workers)
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
            
            # Start watchdog thread
            self.worker_watchdog = threading.Thread(target=self._watchdog)
            self.worker_watchdog.daemon = True
            self.worker_watchdog.start()
    
    def _add_worker(self):
        """Add a new worker to the pool"""
        worker_id = len(self.workers)
        thread = threading.Thread(
            target=self.target_func,
            args=self.worker_args + (self.stop_event, worker_id)
        )
        thread.daemon = True
        thread.start()
        self.workers.append(thread)
        self.active_workers += 1
        
        # Add worker to health tracking
        self.worker_health[worker_id] = {
            'last_active': time.time(),
            'url': 'initializing'
        }
        
        return thread
    
    def update_worker_health(self, worker_id, url=None):
        """Update worker health status"""
        with self.pool_lock:
            if worker_id in self.worker_health:
                self.worker_health[worker_id]['last_active'] = time.time()
                if url:
                    self.worker_health[worker_id]['url'] = url
                    
    def _watchdog(self):
        """Watch for workers that may be stuck"""
        while not self.stop_event.is_set() and not GLOBAL_STOP_EVENT.is_set():
            # Check if any worker is stuck
            current_time = time.time()
            restart_needed = False
            
            with self.pool_lock:
                for worker_id, status in list(self.worker_health.items()):
                    if current_time - status['last_active'] > WORKER_TIMEOUT:
                        # Worker is stuck
                        if self.logger:
                            self.logger.log_error(
                                f"{self.site_url} - Worker {worker_id}", 
                                f"Worker appears to be stuck on {status['url']} for {int(current_time - status['last_active'])}s"
                            )
                        restart_needed = True
                        
                        # Consider worker dead
                        self.worker_health.pop(worker_id, None)
            
            # If worker is stuck and we need more workers, signal adjustment
            if restart_needed:
                self.signal_adjustment_needed()
                
            time.sleep(5)  # Check every 5 seconds
                
    def _monitor_workers(self):
        """Monitor thread that maintains the pool at optimal size"""
        while not self.stop_event.is_set() and not GLOBAL_STOP_EVENT.is_set():
            # Wait for adjust signal or timeout
            self.adjust_event.wait(timeout=2)
            self.adjust_event.clear()
            
            with self.pool_lock:
                # Count active workers
                active_count = sum(1 for w in self.workers if w.is_alive())
                
                # If we have fewer active workers than our count suggests, adjust
                if active_count < self.active_workers:
                    dead_workers = self.active_workers - active_count
                    self.active_workers = active_count
                    
                    # Log dead workers
                    if self.logger and dead_workers > 0:
                        self.logger.log_error(
                            self.site_url,
                            f"Detected {dead_workers} dead worker threads"
                        )
                
                # Add new workers if we're below max and have work to do
                work_queue = self.worker_args[0]  # Assuming work_queue is first arg
                if (self.active_workers < self.max_workers and 
                        not work_queue.empty()):
                    try:
                        # Try to get queue size (may not be available in all queue implementations)
                        try:
                            queue_size = work_queue.qsize()
                        except Exception:
                            queue_size = 1  # Assume at least one item
                        
                        # Calculate how many workers to add
                        workers_to_add = min(
                            max(1, min(queue_size // 2, 3)),  # Add 1-3 based on queue size
                            self.max_workers - self.active_workers  # Don't exceed max
                        )
                        
                        # Add workers
                        for _ in range(workers_to_add):
                            self._add_worker()
                            
                        if workers_to_add > 0:
                            print_if_allowed(f"Site {self.site_url}: Added {workers_to_add} workers (total active: {self.active_workers})")
                    except Exception as e:
                        print_if_allowed(f"Error adding workers: {e}")
                        if self.logger:
                            self.logger.log_exception(self.site_url, e)
            
            # Sleep to prevent excessive checking
            time.sleep(0.5)
    
    def signal_adjustment_needed(self):
        """Signal that pool adjustment might be needed"""
        self.adjust_event.set()
    
    def record_progress(self):
        """Record that progress is being made"""
        self.last_progress_time = time.time()
    
    def time_since_progress(self):
        """Return seconds since last progress"""
        return time.time() - self.last_progress_time
        
    def get_active_count(self):
        """Get count of active workers"""
        with self.pool_lock:
            return self.active_workers
    
    def reset_workers(self):
        """Kill all workers and create fresh ones"""
        with self.pool_lock:
            # Signal all workers to stop
            self.stop_event.set()
            
            # Wait briefly for them to finish
            for worker in self.workers:
                worker.join(timeout=0.5)
                
            # Reset state
            self.workers = []
            self.active_workers = 0
            self.worker_health = {}
            self.stop_event.clear()
            
            # Add new workers
            for i in range(min(2, self.max_workers)):
                self._add_worker()
                
            print_if_allowed(f"Site {self.site_url}: Reset worker pool with {self.active_workers} fresh workers")
            
    def shutdown(self, wait=True):
        """Shutdown the worker pool"""
        self.stop_event.set()
        if wait:
            for worker in self.workers:
                worker.join(timeout=2)
        self.active_workers = 0


def worker_function(work_queue, visited, files_counter, dirs_counter, extension_counter, max_depth, results, logger, stop_event, worker_id):
    """Worker thread that processes URLs from the queue."""
    while not stop_event.is_set() and not GLOBAL_STOP_EVENT.is_set():
        try:
            # Get a URL from the queue with timeout
            try:
                url, depth = work_queue.get(timeout=1)
            except queue.Empty:
                # No work available, signal potential adjustment
                if 'pool' in globals():
                    pool.signal_adjustment_needed()
                continue
            
            # Update worker health
            if 'pool' in globals():
                pool.update_worker_health(worker_id, url)
                
            try:
                dirs_found = process_url(url, visited, files_counter, dirs_counter, 
                                       extension_counter, max_depth, depth, 
                                       work_queue, results, logger)
                                       
                # Mark task as done
                work_queue.task_done()
                
                # Signal progress was made
                if 'pool' in globals():
                    pool.record_progress()
                    
                # If we found directories, might need more workers
                if dirs_found > 0 and 'pool' in globals():
                    pool.signal_adjustment_needed()
                    
            except Exception as e:
                # Ensure task is marked complete even if error occurred
                work_queue.task_done()
                
                # Log exception
                logger.log_exception(url, e)
                print_if_allowed(f"Error processing {url}: {e}")
                
        except Exception as e:
            # Log any unexpected errors
            logger.log_exception(f"Worker {worker_id}", e)
            print_if_allowed(f"Worker {worker_id} error: {e}")
            
            # Update health status to show worker is still alive
            if 'pool' in globals():
                pool.update_worker_health(worker_id, "error_recovery")


class DynamicCrawler:
    """Crawler class that dynamically adjusts the number of worker threads with improved termination."""
    def __init__(self, max_workers, max_depth):
        self.max_workers = max_workers
        self.max_depth = max_depth
        
    def crawl_site(self, start_url, logger):
        global pool  # Make pool accessible to worker functions
        
        start_url = normalize_url(start_url)
        print_if_allowed(f"Starting crawl of {start_url}...")
        
        # Initialize data
        visited = SafeSet()
        files_counter = SafeCounter(0)
        dirs_counter = SafeCounter(0)
        extension_counter = SafeExtensionCounter()
        results = {}  # Store status msg for each URL
        
        work_queue = queue.Queue()
        
        # Add the initial URL to the queue
        work_queue.put((start_url, 0))  # (url, depth)
        
        # Create and start the worker pool
        pool = WorkerPool(
            max_workers=self.max_workers,
            target_func=worker_function,
            worker_args=(work_queue, visited, files_counter, dirs_counter, 
                        extension_counter, self.max_depth, results, logger),
            site_url=start_url,
            logger=logger
        )
        pool.start(initial_workers=1)
        
        try:
            start_time = time.time()
            last_update_time = start_time
            last_files = 0
            last_dirs = 0
            stalled_count = 0
            reset_count = 0
            max_resets = 2  # Maximum number of pool resets before giving up
            
            while ((not work_queue.empty() or pool.get_active_count() > 0) 
                  and (time.time() - start_time) < MAX_CRAWL_TIME
                  and not GLOBAL_STOP_EVENT.is_set()):
                
                # Status reporting 
                active_workers = pool.get_active_count()
                queue_size = work_queue.qsize() if hasattr(work_queue, 'qsize') else "unknown"
                files = files_counter.get()
                dirs = dirs_counter.get()
                
                # Use proper status line with newline
                status_line = f"{start_url}: Workers: {active_workers}/{self.max_workers}, Queue: {queue_size}, Files: {files}, Dirs: {dirs}"
                print_if_allowed(status_line)
                
                # Detect deadlock: no workers but queue has items
                if active_workers == 0 and queue_size > 0:
                    print_if_allowed(f"Detected potential deadlock on {start_url}: Queue has {queue_size} items but no active workers")
                    
                    # Reset the worker pool if we haven't exceeded max resets
                    if reset_count < max_resets:
                        print_if_allowed(f"Resetting worker pool for {start_url}")
                        pool.reset_workers()
                        reset_count += 1
                    else:
                        print_if_allowed(f"Exceeded maximum resets ({max_resets}) for {start_url}, terminating crawl")
                        break
                
                # Check if we're making progress
                current_time = time.time()
                if current_time - last_update_time >= 10:  # Check every 10 seconds
                    if files == last_files and dirs == last_dirs:
                        stalled_count += 1
                        print_if_allowed(f"No progress detected for {stalled_count * 10}s on {start_url}")
                    else:
                        stalled_count = 0
                    
                    # If stalled for too long, terminate or reset
                    if stalled_count >= MAX_STALLED_TIME // 10:  # Convert MAX_STALLED_TIME to cycles
                        if queue_size == 0:
                            print_if_allowed(f"Crawl of {start_url} has stalled with empty queue. Finishing...")
                            break
                        elif reset_count < max_resets:
                            print_if_allowed(f"Crawl of {start_url} stalled for {stalled_count * 10}s. Resetting worker pool.")
                            pool.reset_workers()
                            reset_count += 1
                            stalled_count = 0
                        else:
                            print_if_allowed(f"Crawl of {start_url} stalled for {stalled_count * 10}s and exceeded max resets. Terminating.")
                            break
                    
                    last_files = files
                    last_dirs = dirs
                    last_update_time = current_time
                
                # Additional termination condition: if no workers and empty queue, we're done
                if active_workers == 0 and queue_size == 0:
                    print_if_allowed(f"No more workers or work for {start_url}. Crawl complete.")
                    break
                
                # Shorter sleep to be more responsive
                time.sleep(5)
                    
        except KeyboardInterrupt:
            print_if_allowed("\nCrawling interrupted by user. Cleaning up...")
            GLOBAL_STOP_EVENT.set()
        except Exception as e:
            # Log any unexpected exceptions
            logger.log_exception(start_url, e)
            print_if_allowed(f"Unexpected error during crawl of {start_url}: {e}")
        finally:
            # Shutdown the worker pool
            pool.shutdown()
            
            # Determine final status
            if GLOBAL_STOP_EVENT.is_set():
                status = "Interrupted"
            elif time.time() - start_time >= MAX_CRAWL_TIME:
                status = "Timeout"
            elif stalled_count >= MAX_STALLED_TIME // 10:
                status = "Stalled"
            else:
                status = "Completed"
                
            print_if_allowed(f"\nCrawl of {start_url} {status.lower()}.")
        
        # Get final results
        final_files = files_counter.get()
        final_dirs = dirs_counter.get()
        extensions_summary = extension_counter.get_summary(top_n=10)
        
        # Format extensions summary for logging
        ext_log_str = format_extensions_summary(extensions_summary)
        
        # Log results - if we have no files and no directories, log as error
        if final_files == 0 and final_dirs == 0:
            logger.log_error(start_url, "No files or directories found")
        else:
            logger.log_success(start_url, final_files, final_dirs, ext_log_str, status)

        return final_files, final_dirs, extensions_summary, status


def format_extensions_summary(extensions_summary):
    """Format extensions summary for logging and display."""
    if not extensions_summary:
        return "No files with extensions found"
    
    parts = []
    for ext, (count, percentage) in extensions_summary.items():
        parts.append(f"{ext}:{count}({percentage}%)")
    
    return ", ".join(parts)


def process_site(url, max_depth, max_workers, logger):
    """Process a single onion site and log results."""
    try:
        url = normalize_url(url)
        parsed = urlparse(url)
        if not parsed.netloc or not parsed.netloc.endswith('.onion'):
            logger.log_error(url, "Invalid onion address")
            return False

        crawler = DynamicCrawler(max_workers, max_depth)
        
        # Crawl the site
        files, dirs, extensions_summary, status = crawler.crawl_site(url, logger)
        
        # Calculate files without extensions
        extensions_total = sum(count for _, (count, _) in extensions_summary.items())
        files_without_ext = files - extensions_total
        
        # Print summary
        print_if_allowed(f"\nCrawl summary for {url}:")
        print_if_allowed(f"Total files: {files}")
        print_if_allowed(f"Files without extensions: {files_without_ext}")
        print_if_allowed(f"Total directories: {dirs}")
        print_if_allowed(f"Status: {status}")
        
        # Print extension statistics
        print_if_allowed("File extensions breakdown:")
        if extensions_summary:
            for ext, (count, percentage) in extensions_summary.items():
                print_if_allowed(f"  {ext}: {count} files ({percentage}%)")
        else:
            print_if_allowed("  No files with extensions found")
        
        return True
    except Exception as e:
        logger.log_exception(url, e, include_traceback=True)
        print_if_allowed(f"Fatal error processing {url}: {e}")
        return False


def process_site_wrapper(args):
    """Wrapper function for processing a site to be used with ThreadPoolExecutor."""
    url, max_depth, max_workers, logger = args
    return process_site(url, max_depth, max_workers, logger)


def signal_handler(sig, frame):
    print_if_allowed("\nReceived shutdown signal, cleaning up...")
    GLOBAL_STOP_EVENT.set()
    time.sleep(2)  


def get_unique_filename(base_filename):
    """Generate a unique filename if base filename doesn't exists"""
    # Use os.path for cross-platform path handling
    base_filename = os.path.abspath(base_filename)
    
    # Make sure directory exists
    directory = os.path.dirname(base_filename)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)
        
    if not os.path.exists(base_filename):
        # Create the file with headers
        with open(base_filename, 'w', encoding='utf-8') as f:
            f.write("Timestamp | URL | Files | Directories | Extensions | Status\n")
            f.write("-" * 100 + "\n")
    return base_filename


def check_tor_connection():
    """Check if the Tor connection is working."""
    try:
        with TOR_RATE_LIMITER:
            response = requests.get('https://check.torproject.org', 
                                    proxies=TOR_PROXY, 
                                    timeout=30)
        if "Congratulations" in response.text:
            return True
        return False
    except Exception as e:
        print_if_allowed(f"Error connecting to Tor: {e}")
        return False


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description='Iteratively count files and directories from an onion link', 
                               usage='%(prog)s [-h] [-i INPUT_FILE] [-d MAX_DEPTH] [-w MAX_WORKERS] [-p PARALLEL_SITES] [-c CONNECTIONS] [-o OUTPUT] [-e ERRORS] [-s] [--silent] [-f {txt,json,csv}] urls ...')
    parser.add_argument('urls', nargs='*', help='Onion URLs to crawl')
    parser.add_argument('-i', '--input-file', help='File containing URLs')
    parser.add_argument('-d', '--max-depth', type=int, default=5, help='Maximum recursion depth (default: 5)')
    parser.add_argument('-w', '--max-workers', type=int, default=10, help='Maximum worker threads per site (default: 10)')
    parser.add_argument('-p', '--parallel-sites', type=int, default=5, help='Number of sites to crawl in parallel (default: 5)')
    parser.add_argument('-c', '--connections', type=int, default=200, help='Maximum concurrent Tor connections (default: 200)')
    parser.add_argument('-o', '--output', default='results.txt', help='Success output file')
    parser.add_argument('-e', '--errors', default='errors.txt', help='Error output file')
    parser.add_argument('-s', '--suppress-errors', action='store_true', help='Suppress error output')
    parser.add_argument('--silent', action='store_true', help='Silent mode: suppress all console output')
    parser.add_argument('-f', '--format', choices=['txt', 'json', 'csv'], default='txt', 
                        help='Output format: txt (default), json, csv')

    args = parser.parse_args()

    # Set global silent mode flag
    global SILENT_MODE
    SILENT_MODE = args.silent
    
    # If silent mode is enabled, automatically enable suppress_errors
    if args.silent:
        args.suppress_errors = True

    # Set global rate limiter based on user parameter
    global TOR_RATE_LIMITER
    TOR_RATE_LIMITER = threading.Semaphore(args.connections)

    # Get output format - txt is default
    export_format = None
    if args.format == 'json':
        export_format = 'json'
    elif args.format == 'csv':
        export_format = 'csv'

    # Initialize logs with platform-safe paths
    success_file = get_unique_filename(os.path.normpath(args.output))
    error_file = get_unique_filename(os.path.normpath(args.errors))
    logger = AtomicLogger(success_file, error_file, args.suppress_errors, export_format)

    # Load URLs
    urls = set(args.urls)
    if args.input_file:
        try:
            with open(args.input_file, 'r', encoding='utf-8') as f:
                urls.update(line.strip() for line in f if line.strip())
            print_if_allowed(f"Loaded {len(urls) - len(args.urls)} URLs from {args.input_file}")
        except Exception as e:
            logger.log_error("system", f"Failed to read input file: {str(e)}")
            sys.exit(1)

    if not urls:
        print_if_allowed("No URLs provided. Use -i option or provide URLs as arguments.")
        if not SILENT_MODE:
            parser.print_help()
        sys.exit(1)

    # Validate Tor connection
    print_if_allowed("Testing connection to Tor network...")
    if not check_tor_connection():
        print_if_allowed("Failed to connect to Tor. Make sure Tor is running and properly configured.")
        sys.exit(1)
    print_if_allowed("Tor connection successful")

    # Process URLs in parallel
    print_if_allowed(f"Starting to crawl {len(urls)} sites with {args.parallel_sites} parallel crawlers")
    
    # Create arguments for the thread pool
    site_args = [(url, args.max_depth, args.max_workers, logger) for url in urls]
    
    # Process sites in parallel with a ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel_sites) as executor:
        # Submit all tasks
        future_to_url = {executor.submit(process_site_wrapper, arg): arg[0] for arg in site_args}
        
        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                print_if_allowed(f"Completed crawling {url}: {'Success' if result else 'Failed'}")
            except Exception as e:
                print_if_allowed(f"Exception while crawling {url}: {e}")

    # Export results in the specified format
    logger.export_results()

    print_if_allowed(f"\nOperation completed. Results in {success_file}, errors in {error_file}")


if __name__ == "__main__":
    main()
