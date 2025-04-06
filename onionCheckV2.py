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

# Configure the proxy for Tor
TOR_PROXY = {
    'http': 'socks5h://127.0.0.1:9050',
    'https': 'socks5h://127.0.0.1:9050'
}

# Global stop event
GLOBAL_STOP_EVENT = threading.Event()


class SafeCounter:
    def __init__(self, initial=0):
        self.value = initial
        self.lock = threading.Lock()
        
    def increment(self, amount=1):
        with self.lock:
            self.value += amount
            return self.value
            
    def get(self):
        with self.lock:
            return self.value


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


class AtomicLogger:
    def __init__(self, success_file, error_file):
        self.success_file = success_file
        self.error_file = error_file
        self.file_lock = threading.Lock()
        
    def log_success(self, url, files, dirs, extensions_summary, status):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"{timestamp} | {url} | Files: {files} | Directories: {dirs} | Extensions: {extensions_summary} | {status}\n"
        with self.file_lock:
            with open(self.success_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
                f.flush()
                
    def log_error(self, url, error):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"{timestamp} | {url} | Error: {error}\n"
        with self.file_lock:
            with open(self.error_file, 'a', encoding='utf-8') as f:
                f.write(log_line)
                f.flush()


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


def process_url(url, visited, files_counter, dirs_counter, extension_counter, max_depth, current_depth, work_queue, results):
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
    
    return 0


class WorkerPool:
    """A pool of worker threads that can be dynamically adjusted"""
    
    def __init__(self, max_workers, target_func, worker_args):
        self.max_workers = max_workers
        self.target_func = target_func
        self.worker_args = worker_args
        self.workers = []
        self.active_workers = 0
        self.stop_event = threading.Event()
        self.pool_lock = threading.Lock()
        self.adjust_event = threading.Event()
        
    def start(self, initial_workers=1):
        """Start the worker pool with initial_workers"""
        with self.pool_lock:
            for i in range(initial_workers):
                self._add_worker()
                
            # Start monitoring thread
            self.monitor_thread = threading.Thread(target=self._monitor_workers)
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
    
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
        return thread
        
    def _monitor_workers(self):
        """Monitor thread that maintains the pool at optimal size"""
        while not self.stop_event.is_set() and not GLOBAL_STOP_EVENT.is_set():
            # Wait for adjust signal or timeout
            self.adjust_event.wait(timeout=2)
            self.adjust_event.clear()
            
            with self.pool_lock:
                # Remove dead workers from the list
                active_count = sum(1 for w in self.workers if w.is_alive())
                
                # If we have fewer active workers than our count suggests, adjust
                if active_count < self.active_workers:
                    self.active_workers = active_count
                
                # Add new workers if we're below max and have work to do
                work_queue = self.worker_args[0]  # Assuming work_queue is first arg
                if (self.active_workers < self.max_workers and 
                        not work_queue.empty()):
                    # Add workers based on queue size
                    try:
                        queue_size = work_queue.qsize()
                        workers_to_add = min(
                            max(1, min(queue_size // 2, 3)),  # Add 1-3 based on queue size
                            self.max_workers - self.active_workers  # Don't exceed max
                        )
                        
                        for _ in range(workers_to_add):
                            self._add_worker()
                            
                        if workers_to_add > 0:
                            print(f"Added {workers_to_add} workers (total active: {self.active_workers})")
                    except Exception as e:
                        print(f"Error adding workers: {e}")
            
            # Sleep to prevent excessive checking
            time.sleep(0.5)
    
    def signal_adjustment_needed(self):
        """Signal that pool adjustment might be needed"""
        self.adjust_event.set()
        
    def get_active_count(self):
        """Get count of active workers"""
        with self.pool_lock:
            return self.active_workers
            
    def shutdown(self, wait=True):
        """Shutdown the worker pool"""
        self.stop_event.set()
        if wait:
            for worker in self.workers:
                worker.join(timeout=2)
        self.active_workers = 0


def worker_function(work_queue, visited, files_counter, dirs_counter, extension_counter, max_depth, results, stop_event, worker_id):
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
                
            try:
                dirs_found = process_url(url, visited, files_counter, dirs_counter, 
                                       extension_counter, max_depth, depth, 
                                       work_queue, results)
                                       
                # If we found directories, might need more workers
                if dirs_found > 0 and 'pool' in globals():
                    pool.signal_adjustment_needed()
                    
            except Exception as e:
                print(f"Error processing {url}: {e}")
            finally:
                # Always mark task as done
                work_queue.task_done()
                
        except Exception as e:
            print(f"Worker {worker_id} error: {e}")


class DynamicCrawler:
    """Crawler class that dynamically adjusts the number of worker threads."""
    def __init__(self, max_workers, max_depth):
        self.max_workers = max_workers
        self.max_depth = max_depth
        
    def crawl_site(self, start_url, logger):
        global pool  # Make pool accessible to worker functions
        
        start_url = normalize_url(start_url)
        print(f"Starting crawl of {start_url}...")
        
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
                        extension_counter, self.max_depth, results)
        )
        pool.start(initial_workers=1)
        
        try:
            start_time = time.time()
            last_update_time = start_time
            last_files = 0
            last_dirs = 0
            stalled_count = 0
            
            while ((not work_queue.empty() or pool.get_active_count() > 0) 
                  and (time.time() - start_time) < 600  # 10-minute timeout
                  and not GLOBAL_STOP_EVENT.is_set()):
                
                # Status reporting 
                active_workers = pool.get_active_count()
                queue_size = work_queue.qsize() if hasattr(work_queue, 'qsize') else "unknown"
                files = files_counter.get()
                dirs = dirs_counter.get()
                
                print(f"\r{start_url}: Workers: {active_workers}/{self.max_workers}, Queue: {queue_size}, Files: {files}, Dirs: {dirs}", end="")
                
                # Check if we're making progress
                current_time = time.time()
                if current_time - last_update_time >= 10:  # Check every 10 seconds
                    if files == last_files and dirs == last_dirs:
                        stalled_count += 1
                    else:
                        stalled_count = 0
                    
                    # If stalled for too long (30 seconds), consider finishing
                    if stalled_count >= 3 and queue_size == 0:
                        print("\nCrawl appears to be stalled. Finishing...")
                        break
                    
                    last_files = files
                    last_dirs = dirs
                    last_update_time = current_time
                
                time.sleep(1)
                
                # If the queue is empty and no active threads, we're likely done
                if work_queue.empty() and pool.get_active_count() == 0:
                    break
                    
        except KeyboardInterrupt:
            print("\nCrawling interrupted by user. Cleaning up...")
            GLOBAL_STOP_EVENT.set()
        finally:
            # Shutdown the worker pool
            pool.shutdown()
            
            print("\nCrawl completed.")
        
        # Get final results
        final_files = files_counter.get()
        final_dirs = dirs_counter.get()
        extensions_summary = extension_counter.get_summary(top_n=10)
        status = "Interrupted" if GLOBAL_STOP_EVENT.is_set() else "Completed"
        
        # Format extensions summary for logging
        ext_log_str = format_extensions_summary(extensions_summary)
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
        print(f"\nCrawl summary for {url}:")
        print(f"Total files: {files}")
        print(f"Files without extensions: {files_without_ext}")
        print(f"Total directories: {dirs}")
        print(f"Status: {status}")
        
        # Print extension statistics
        print("File extensions breakdown:")
        if extensions_summary:
            for ext, (count, percentage) in extensions_summary.items():
                print(f"  {ext}: {count} files ({percentage}%)")
        else:
            print("  No files with extensions found")
        
        return True
    except Exception as e:
        logger.log_error(url, f"Fatal error: {str(e)}")
        return False


def process_site_wrapper(args):
    """Wrapper function for processing a site to be used with ThreadPoolExecutor."""
    url, max_depth, max_workers, logger = args
    return process_site(url, max_depth, max_workers, logger)


def signal_handler(sig, frame):
    print("\nReceived shutdown signal, cleaning up...")
    GLOBAL_STOP_EVENT.set()
    sys.exit(0)


def get_unique_filename(base_filename):
    """Generate a unique filename if base filename doesn't exists"""
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
        print(f"Error connecting to Tor: {e}")
        return False


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    parser = argparse.ArgumentParser(description='Iteravely count files and directories from an onion link', 
                               usage='%(prog)s [-h] [-i INPUT_FILE] [-d MAX_DEPTH] [-w MAX_WORKERS] [-p PARALLEL_SITES] [-c CONNECTIONS] [-o OUTPUT] [-e ERRORS] urls ...')
    parser.add_argument('urls', nargs='*', help='Onion URLs to crawl')
    parser.add_argument('-i', '--input-file', help='File containing URLs')
    parser.add_argument('-d', '--max-depth', type=int, default=5, help='Maximum recursion depth (default: 5)')
    parser.add_argument('-w', '--max-workers', type=int, default=10, help='Maximum worker threads per site (default: 10)')
    parser.add_argument('-p', '--parallel-sites', type=int, default=5, help='Number of sites to crawl in parallel (default: 5)')
    parser.add_argument('-c', '--connections', type=int, default=100, help='Maximum concurrent Tor connections (default: 100)')
    parser.add_argument('-o', '--output', default='results.txt', help='Success output file')
    parser.add_argument('-e', '--errors', default='errors.txt', help='Error output file')

    args = parser.parse_args()

    # Set global rate limiter based on user parameter
    global TOR_RATE_LIMITER
    TOR_RATE_LIMITER = threading.Semaphore(args.connections)

    # Initialize logs
    success_file = get_unique_filename(args.output)
    error_file = get_unique_filename(args.errors)
    logger = AtomicLogger(success_file, error_file)

    # Load URLs
    urls = set(args.urls)
    if args.input_file:
        try:
            with open(args.input_file) as f:
                urls.update(line.strip() for line in f if line.strip())
            print(f"Loaded {len(urls) - len(args.urls)} URLs from {args.input_file}")
        except Exception as e:
            logger.log_error("system", f"Failed to read input file: {str(e)}")
            sys.exit(1)

    if not urls:
        print("No URLs provided. Use -i option or provide URLs as arguments.")
        parser.print_help()
        sys.exit(1)

    # Validate Tor connection
    print("Testing connection to Tor network...")
    if not check_tor_connection():
        print("Failed to connect to Tor. Make sure Tor is running and properly configured.")
        sys.exit(1)
    print("Tor connection successful")

    # Process URLs in parallel
    print(f"Starting to crawl {len(urls)} sites with {args.parallel_sites} parallel crawlers")
    
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
                print(f"Completed crawling {url}: {'Success' if result else 'Failed'}")
            except Exception as e:
                print(f"Exception while crawling {url}: {e}")

    print(f"\nOperation completed. Results in {success_file}, errors in {error_file}")


if __name__ == "__main__":
    main()
