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
        
    def log_success(self, url, files, dirs, status):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_line = f"{timestamp} | {url} | Files: {files} | Directories: {dirs} | {status}\n"
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

def process_url(url, visited, files_counter, dirs_counter, max_depth, current_depth, work_queue, results):
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
            #  Add jitter to prevent THUNDERING HERD which was also my nickname in college
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
        
        # Store success result
        results[url] = f"Success (found {local_files} files, {local_dirs} dirs at depth {current_depth})"
        
        # Return number of directories found at this level
        return local_dirs
        
    except requests.exceptions.RequestException as e:
        results[url] = f"Request error: {str(e)}"
    except Exception as e:
        results[url] = f"Error: {str(e)}"
    
    return 0 

def worker(work_queue, visited, files_counter, dirs_counter, max_depth, results, stop_event, worker_id):
    """Worker thread that processes URLs from the queue."""
    while not stop_event.is_set() and not GLOBAL_STOP_EVENT.is_set():
        try:
            # Get a URL from the queue with timeout
            try:
                url, depth = work_queue.get(timeout=2)
            except queue.Empty:
                # take a
                break
                
            try:
                process_url(url, visited, files_counter, dirs_counter, max_depth, depth, work_queue, results)
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
        self.active_workers = 0
        self.worker_lock = threading.Lock()
        
    def crawl_site(self, start_url, logger):
        start_url = normalize_url(start_url)
        
        print(f"Starting crawl of {start_url}...")
        
        # Initialize data
        visited = SafeSet()
        files_counter = SafeCounter(0)
        dirs_counter = SafeCounter(0)
        results = {}  # Store status msg for each URL
        
        work_queue = queue.Queue()
        
        # Add the initial URL to the queue
        work_queue.put((start_url, 0))  # (url, depth)
        
        # Event for coordination
        dirs_found_event = threading.Event()
        stop_event = threading.Event()
        
        # Start with the initial worker
        initial_worker_thread = threading.Thread(
            target=self.initial_worker,
            args=(work_queue, visited, files_counter, dirs_counter, 
                 self.max_depth, results, stop_event, dirs_found_event, 0)
        )
        initial_worker_thread.daemon = True
        initial_worker_thread.start()
        
        active_workers = [initial_worker_thread]
        self.active_workers = 1
        
        try:
            start_time = time.time()
            last_update_time = start_time
            last_files = 0
            last_dirs = 0
            stalled_count = 0
            
            while ((not work_queue.empty() or any(t.is_alive() for t in active_workers)) 
                  and (time.time() - start_time) < 600  # 10-minute timeout
                  and not GLOBAL_STOP_EVENT.is_set()):
                
                # Check if we need to add more workers
                if dirs_found_event.is_set() and len(active_workers) < self.max_workers:
                    dirs_found_event.clear()  # Reset the event
                    
                    # Calculate how many more workers to add
                    with self.worker_lock:
                        queue_size = work_queue.qsize() if hasattr(work_queue, 'qsize') else 3
                        # Add workers proportionally to queue size, don't exceed max_workers
                        workers_to_add = min(
                            max(1, queue_size // 3),  # At least one, more for bigger queues
                            self.max_workers - len(active_workers)  # Don't exceed max
                        )
                        
                        # Add new workers
                        for i in range(workers_to_add):
                            worker_id = len(active_workers)
                            worker_thread = threading.Thread(
                                target=worker,
                                args=(work_queue, visited, files_counter, 
                                     dirs_counter, self.max_depth, results, 
                                     stop_event, worker_id)
                            )
                            worker_thread.daemon = True
                            worker_thread.start()
                            active_workers.append(worker_thread)
                            self.active_workers += 1
                            
                        print(f"Added {workers_to_add} workers (total: {len(active_workers)})")
                
                # Status reporting
                curr_workers = sum(1 for t in active_workers if t.is_alive())
                queue_size = work_queue.qsize() if hasattr(work_queue, 'qsize') else "unknown"
                files = files_counter.get()
                dirs = dirs_counter.get()
                
                print(f"\r{start_url}: Workers: {curr_workers}/{self.max_workers}, Queue: {queue_size}, Files: {files}, Dirs: {dirs}", end="")
                
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
                if work_queue.empty() and not any(t.is_alive() for t in active_workers):
                    break
                    
        except KeyboardInterrupt:
            print("\nCrawling interrupted by user. Cleaning up...")
            GLOBAL_STOP_EVENT.set()
            stop_event.set()
        finally:
            # Signal workers to stop
            stop_event.set()
            
            # Wait for threads 
            for t in active_workers:
                t.join(timeout=2)
            
            # Update active workers count
            with self.worker_lock:
                self.active_workers = 0
            
            print("\nCrawl completed.")
        
        # Get final results
        final_files = files_counter.get()
        final_dirs = dirs_counter.get()
        status = "Interrupted" if GLOBAL_STOP_EVENT.is_set() else "Completed"
        logger.log_success(start_url, final_files, final_dirs, status)

        return final_files, final_dirs, status
    
    def initial_worker(self, work_queue, visited, files_counter, dirs_counter, max_depth, results, stop_event, dirs_found_event, worker_id):
        """First worker thread that signals when directories are found."""
        while not stop_event.is_set() and not GLOBAL_STOP_EVENT.is_set():
            try:
                # Get a URL from queue
                try:
                    url, depth = work_queue.get(timeout=2)
                except queue.Empty:
                    break
                    
                try:
                    # Process the URL and check if it had directories
                    local_dirs = process_url(url, visited, files_counter, dirs_counter, max_depth, depth, work_queue, results)
                    
                    # Signal findings (needs more workers)
                    if local_dirs and local_dirs > 0:
                        dirs_found_event.set()
                        
                except Exception as e:
                    print(f"Error in initial worker: {e}")
                finally:
                    # Always mark task as done
                    work_queue.task_done()
                    
            except Exception as e:
                print(f"Initial worker {worker_id} error: {e}")

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
        files, dirs, status = crawler.crawl_site(url, logger)
        
        # Print summary
        print(f"\nCrawl summary for {url}:")
        print(f"Total files: {files}")
        print(f"Total directories: {dirs}")
        print(f"Status: {status}")
        
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
            f.write("Timestamp | URL | Files | Directories | Status\n")
            f.write("-" * 80 + "\n")
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
                               usage='%(prog)s [-h] [-i INPUT_FILE] [-d MAX_DEPTH] [-w MAX_WORKERS] [-p PARALLEL_SITES] [-c CONNECTIONS] [-o OUTPUT] [-e ERRORS] [urls ...]')
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