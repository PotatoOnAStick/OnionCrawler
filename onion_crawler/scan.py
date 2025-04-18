# onion_crawler/main.py
import sys
import os
import argparse
import time
import concurrent.futures
import traceback

# Use absolute imports for modules within the package
from . import config
from . import utils
from . import tor_utils
from . import database
from . import crawler_core
from . import data_structures # May not be needed directly here, but good practice

def process_site(url: str, max_depth: int, max_workers: int) -> str: # <<< Change return type annotation
    """Processes a single site and returns its final status string."""
    crawl_id = -1
    error_message = None
    status = "Error" # Default status if something goes wrong early
    try:
        # 1. Start/Get Crawl Record
        crawl_id, initial_status = database.start_crawl_record(url)
        if crawl_id is None:
            utils.print_if_allowed(f"Failed to initialize database record for {url}. Skipping.")
            return "Error (DB Init)" # <<< Return specific status

        if initial_status == 'Exists':
            utils.print_if_allowed(f"Skipping {url} - already processed or currently processing (status in DB).")
            # You might want a different status here, e.g., "Skipped (Exists)"
            # For now, let's treat it as an early exit, maybe "Skipped" is better?
            return "Skipped (Exists)" # <<< Return specific status

        # 2. Initialize Crawler
        crawler = crawler_core.DynamicCrawler(max_workers=max_workers, max_depth=max_depth)

        # 3. Run Crawl
        status, files_count, dirs_count, extensions_summary = crawler.crawl_site(url, crawl_id)

        # 4. Finish Crawl Record (moved inside try block)
        database.finish_crawl_record(crawl_id, status, files_count, dirs_count, extensions_summary, error_message)
        return status # <<< Return the final status from the crawler

    except Exception as e:
        error_message = f"Unhandled exception in process_site: {type(e).__name__}: {e}"
        utils.print_if_allowed(f"Error processing site {url}: {error_message}\n{traceback.format_exc()}")
        status = "Error" # Ensure status is Error if exception occurs
        # Attempt to update DB even on error, if crawl_id was obtained
        if crawl_id > 0:
            try:
                # Try to record the error state
                database.finish_crawl_record(crawl_id, status, None, None, [], error_message)
            except Exception as db_err:
                utils.print_if_allowed(f"Failed to update database record for {url} after error: {db_err}")
        return status # <<< Return Error status

def process_site_wrapper(args) -> tuple[str, str]: # <<< Return tuple (url, status)
    """Wrapper to catch exceptions from process_site and handle shutdown."""
    url, max_depth, max_workers = args
    final_status = "Error (Wrapper)" # Default if something goes wrong here
    try:
        if config.GLOBAL_STOP_EVENT.is_set():
            return url, "Skipped (Shutdown)" # <<< Return status

        # Call process_site and get the actual status
        final_status = process_site(url, max_depth, max_workers)
        # Print the more informative message using the actual status
        utils.print_if_allowed(f"Completed processing: {url} (Final Status: {final_status})")
        return url, final_status # <<< Return actual status

    except Exception as e:
        # This catches errors within process_site *if they weren't handled internally*
        # or errors in the wrapper itself. process_site should ideally handle its own errors.
        utils.print_if_allowed(f"Unhandled exception in process_site_wrapper for {url}: {e}\n{traceback.format_exc()}")
        # Attempt to update DB if possible (might be redundant if process_site already did)
        crawl_id, _ = database.start_crawl_record(url) # Check if record exists
        if crawl_id and crawl_id > 0:
             database.finish_crawl_record(crawl_id, "Error", None, None, [], f"Wrapper Exception: {e}")
        return url, "Error (Wrapper Exception)" # <<< Return error status
    except KeyboardInterrupt:
        config.GLOBAL_STOP_EVENT.set()
        utils.print_if_allowed(f"\nKeyboardInterrupt caught in wrapper for {url}. Signaling stop.")
        # Try to mark as interrupted in DB
        crawl_id, _ = database.start_crawl_record(url)
        if crawl_id and crawl_id > 0:
             database.finish_crawl_record(crawl_id, "Interrupted", None, None, [], "KeyboardInterrupt")
        return url, "Interrupted" # <<< Return interrupted status


def main():
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(
        description='Crawls .onion sites for directory listings and file counts, storing results in a database.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('urls', nargs='*', help='One or more .onion URLs to crawl.')
    parser.add_argument('-i', '--input-file', help='File containing .onion URLs (one per line).')
    parser.add_argument('-d', '--max-depth', type=int, default=5, help='Maximum recursion depth for crawling.')
    parser.add_argument('-w', '--max-workers', type=int, default=config.DEFAULT_MAX_WORKERS_PER_SITE,
                        help='Maximum worker threads per site.')
    parser.add_argument('-p', '--parallel-sites', type=int, default=config.DEFAULT_PARALLEL_SITES,
                        help='Number of sites to crawl concurrently.')
    parser.add_argument('-c', '--connections', type=int, default=config.DEFAULT_MAX_TOR_CONNECTIONS,
                        help='Maximum concurrent Tor connections (global limit).')
    parser.add_argument('--db-file', default=config.DATABASE_FILE, help='Path to the SQLite database file.')
    parser.add_argument('--silent', action='store_true', help='Suppress all console output except fatal errors.')
    # <<< New Argument >>>
    parser.add_argument('--log-items', action='store_true',
                        help='Log individual file and directory paths found to the database.')

    args = parser.parse_args()

    # --- Configuration Setup ---
    config.set_silent_mode(args.silent)
    config.set_tor_rate_limit(args.connections)
    config.DATABASE_FILE = args.db_file
    config.set_log_items_mode(args.log_items) # <<< Set item logging mode

    utils.print_if_allowed("--- Onion Directory Crawler ---")
    utils.print_if_allowed(f"Max Depth: {args.max_depth}, Workers/Site: {args.max_workers}, Parallel Sites: {args.parallel_sites}, Tor Connections: {args.connections}")
    utils.print_if_allowed(f"Database File: {config.DATABASE_FILE}")
    utils.print_if_allowed(f"Log Individual Items: {config.LOG_INDIVIDUAL_ITEMS}") # <<< Print status

    # --- Signal Handling ---
    utils.setup_signal_handlers()

    # --- Database Initialization ---
    try:
        database.initialize_database()
    except Exception as e:
        utils.print_if_allowed(f"FATAL: Could not initialize database: {e}")
        sys.exit(1)

    # --- Load URLs ---
    urls_to_crawl = set()
    if args.urls:
        urls_to_crawl.update(args.urls)
    if args.input_file:
        try:
            with open(args.input_file, 'r', encoding='utf-8') as f:
                count_before = len(urls_to_crawl)
                for line in f:
                    stripped = line.strip()
                    if stripped and not stripped.startswith('#'): # Ignore empty lines and comments
                        urls_to_crawl.add(stripped)
                count_after = len(urls_to_crawl)
                utils.print_if_allowed(f"Loaded {count_after - count_before} URLs from {args.input_file}")
        except FileNotFoundError:
            utils.print_if_allowed(f"Error: Input file not found: {args.input_file}")
            sys.exit(1)
        except Exception as e:
            utils.print_if_allowed(f"Error reading input file {args.input_file}: {e}")
            sys.exit(1)

    if not urls_to_crawl:
        utils.print_if_allowed("No valid URLs provided. Use positional arguments or the -i option.")
        parser.print_help()
        sys.exit(1)

    # --- Validate URLs (Basic Check) ---
    valid_urls = set()
    normalized_map = {} # Keep track of original -> normalized for potential later use if needed
    for url in urls_to_crawl:
        normalized = utils.normalize_url(url) # Normalize first
        if utils.validate_onion_url(normalized): # Validate the normalized version
            valid_urls.add(normalized) # Store the normalized, valid URL
    invalid_urls = urls_to_crawl - valid_urls
    if invalid_urls:
        utils.print_if_allowed(f"\nWarning: Ignoring {len(invalid_urls)} invalid or non-onion URLs:")
        for url in list(invalid_urls)[:5]: # Show a few examples
            utils.print_if_allowed(f"  - {url}")
        if len(invalid_urls) > 5: utils.print_if_allowed("  ...")
    if not valid_urls:
        utils.print_if_allowed("No valid .onion URLs found to crawl.")
        sys.exit(1)

    utils.print_if_allowed(f"\nFound {len(valid_urls)} valid .onion URLs to process.")

    # --- Tor Connection Check ---
    if not tor_utils.check_tor_connection():
        utils.print_if_allowed("FATAL: Failed to establish Tor connection. Ensure Tor service is running (usually on 127.0.0.1:9050).")
        sys.exit(1)


    # --- Parallel Crawling ---
    utils.print_if_allowed(f"\nStarting crawl with {args.parallel_sites} parallel site processor(s)...")
    start_run_time = time.time()
    # Redefine results dict slightly for clarity
    results = {
        'Completed': 0,
        'NoData': 0,
        'Timeout': 0,
        'Stalled': 0,
        'Error': 0,
        'Interrupted': 0,
        'Skipped': 0, # For already existing or shutdown
        'Other': 0 # Catch-all for unexpected statuses
    }
    site_args = [(url, args.max_depth, args.max_workers) for url in valid_urls]

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.parallel_sites) as executor:
            # Submit jobs and store future -> url mapping
            future_to_url = {executor.submit(process_site_wrapper, arg): arg[0] for arg in site_args}

            for future in concurrent.futures.as_completed(future_to_url):
                url_processed = future_to_url[future] # Get URL associated with this future

                if config.GLOBAL_STOP_EVENT.is_set():
                    # If global stop is set, we might not get a result, or the result might be 'Interrupted'/'Skipped'
                    # Let's ensure skipped count is incremented if we break early
                    # Note: The wrapper already tries to handle this, but this is a safety net
                    if future.done() and not future.cancelled():
                        try:
                            _, status_result = future.result()
                            if status_result.startswith("Skipped"):
                                results['Skipped'] += 1
                            elif status_result == "Interrupted":
                                results['Interrupted'] += 1
                            else: # If it finished before stop fully propagated
                                if status_result in results:
                                    results[status_result] += 1
                                else:
                                    results['Other'] += 1
                        except Exception:
                             results['Error'] += 1 # Count as error if result retrieval fails post-stop
                    else:
                         results['Skipped'] += 1 # Count as skipped if not done/cancelled
                    continue # Continue to next future to allow cleanup/status recording

                try:
                    # Get the result (url, status) from the wrapper
                    returned_url, final_status = future.result()
                    # Increment the counter for the specific status
                    if final_status in results:
                        results[final_status] += 1
                    elif final_status.startswith("Skipped"): # Handle variations like "Skipped (Exists)"
                        results['Skipped'] += 1
                    elif final_status.startswith("Error"): # Handle variations like "Error (DB Init)"
                        results['Error'] += 1
                    else:
                        results['Other'] += 1
                        utils.print_if_allowed(f"Note: Received unexpected final status '{final_status}' for {returned_url}")

                except Exception as exc:
                    # This catches exceptions during future.result() itself
                    results['Error'] += 1
                    utils.print_if_allowed(f"Site {url_processed} generated an exception during result retrieval: {exc}")
                    # Attempt to mark DB record as Error if possible
                    crawl_id, _ = database.start_crawl_record(url_processed)
                    if crawl_id and crawl_id > 0:
                        database.finish_crawl_record(crawl_id, "Error", None, None, [], f"Result Retrieval Exception: {exc}")


    except KeyboardInterrupt:
        utils.print_if_allowed("\n--- KeyboardInterrupt received in main loop. Waiting for tasks to attempt shutdown... ---")
        config.GLOBAL_STOP_EVENT.set()
        # Give workers a moment to notice the stop event
        time.sleep(2)
    except Exception as e:
        utils.print_if_allowed(f"\n--- An unexpected error occurred in the main execution: {e} ---")
        traceback.print_exc()
        config.GLOBAL_STOP_EVENT.set() # Signal stop on major error
    finally:
        end_run_time = time.time()
        utils.print_if_allowed("\n--- Crawl Run Finished ---")
        utils.print_if_allowed(f"Total execution time: {end_run_time - start_run_time:.2f} seconds")
        # Print detailed results
        total_processed = 0
        for status, count in results.items():
            if count > 0:
                utils.print_if_allowed(f"  {status}: {count}")
                total_processed += count
        utils.print_if_allowed(f"Total sites attempted: {len(site_args)}")
        # utils.print_if_allowed(f"Sites processed successfully: {results['Completed']}") # Old summary
        # utils.print_if_allowed(f"Sites failed/errored: {results['Error']}") # Old summary
        # utils.print_if_allowed(f"Sites skipped (shutdown/exists): {results['Skipped']}") # Old summary

        utils.print_if_allowed(f"Results stored in database: {config.DATABASE_FILE}")
        database.close_db_connection() # Ensure connection is closed
        # utils.print_if_allowed("Database connection closed.") # Already printed by close_db_connection if not silent
        utils.print_if_allowed("Exiting.")
if __name__ == "__main__":
    # For direct execution: python -m onion_crawler.main [args]
    main()

