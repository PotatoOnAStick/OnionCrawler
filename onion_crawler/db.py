# onion_crawler/db.py
import argparse
import sys
import os
from datetime import datetime

# Use absolute imports assuming running as part of the package
from . import config
from . import utils
from . import database

# --- Formatting Helpers ---

def format_timestamp(ts_str):
    """Format timestamp string or return 'N/A'."""
    if not ts_str:
        return "N/A"
    try:
        # Assuming format 'YYYY-MM-DD HH:MM:SS'
        dt_obj = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        return dt_obj.strftime("%Y-%m-%d %H:%M") # Shorter format for display
    except (ValueError, TypeError):
        return ts_str # Return original if parsing fails

def format_count(count):
    """Format count or return 'N/A'."""
    return str(count) if count is not None else "N/A"

def print_crawl_list(crawls):
    """Prints a formatted list of crawl records."""
    if not crawls:
        utils.print_if_allowed("No crawl records found matching criteria.")
        return

    # Header
    header = f"{'ID':<5} {'Status':<12} {'Files':<7} {'Dirs':<7} {'Start Time':<17} {'End Time':<17} {'URL':<60}"
    utils.print_if_allowed(header)
    utils.print_if_allowed("-" * len(header))

    # Rows
    for crawl in crawls:
        url_display = crawl['url']
        if len(url_display) > 57:
            url_display = url_display[:57] + "..."

        row = (f"{crawl['id']:<5} "
               f"{crawl['status']:<12} "
               f"{format_count(crawl['total_files']):<7} "
               f"{format_count(crawl['total_dirs']):<7} "
               f"{format_timestamp(crawl['start_time']):<17} "
               f"{format_timestamp(crawl['end_time']):<17} "
               f"{url_display:<60}")
        utils.print_if_allowed(row)

def print_crawl_details(crawl, extensions):
    """Prints detailed information for a single crawl."""
    if not crawl:
        utils.print_if_allowed("Crawl record not found.")
        return

    utils.print_if_allowed("\n--- Crawl Details ---")
    utils.print_if_allowed(f"ID         : {crawl['id']}")
    utils.print_if_allowed(f"URL        : {crawl['url']}")
    utils.print_if_allowed(f"Status     : {crawl['status']}")
    utils.print_if_allowed(f"Start Time : {format_timestamp(crawl['start_time'])}")
    utils.print_if_allowed(f"End Time   : {format_timestamp(crawl['end_time'])}")
    utils.print_if_allowed(f"Files Found: {format_count(crawl['total_files'])}")
    utils.print_if_allowed(f"Dirs Found : {format_count(crawl['total_dirs'])}")
    utils.print_if_allowed(f"Error Msg  : {crawl['error_message'] if crawl['error_message'] else 'None'}")

    if extensions:
        utils.print_if_allowed("\n--- Extensions Found ---")
        ext_header = f"{'Extension':<15} {'Count':<10} {'Percentage':<10}"
        utils.print_if_allowed(ext_header)
        utils.print_if_allowed("-" * len(ext_header))
        for ext in extensions:
            utils.print_if_allowed(f"{ext['extension']:<15} {ext['count']:<10} {ext['percentage']:.1f}%")
    else:
        utils.print_if_allowed("\n--- Extensions Found: None ---")
    utils.print_if_allowed("-" * 40)

def print_aggregated_extensions(agg_extensions):
    """Prints aggregated extension counts."""
    if not agg_extensions:
        utils.print_if_allowed("No extension data found.")
        return

    utils.print_if_allowed("\n--- Top Extensions (Aggregated Across Crawls) ---")
    header = f"{'Extension':<15} {'Total Count':<15}"
    utils.print_if_allowed(header)
    utils.print_if_allowed("-" * len(header))
    for ext, total_count in agg_extensions:
        utils.print_if_allowed(f"{ext:<15} {total_count:<15}")
    utils.print_if_allowed("-" * len(header))

def print_summary_stats(stats):
    """Prints the summary statistics."""
    utils.print_if_allowed("\n--- Crawl Database Summary ---")
    utils.print_if_allowed(f"Total Crawl Records : {stats['total_crawls']}")

    utils.print_if_allowed("\nStatus Breakdown:")
    if stats['status_counts']:
        for status, count in sorted(stats['status_counts'].items()):
            utils.print_if_allowed(f"  - {status:<15}: {count}")
    else:
        utils.print_if_allowed("  No status data available.")

    utils.print_if_allowed("\nOverall Counts:")
    utils.print_if_allowed(f"  Total Files Found   : {format_count(stats['total_files_sum'])}")
    utils.print_if_allowed(f"  Total Dirs Found    : {format_count(stats['total_dirs_sum'])}")

    utils.print_if_allowed("\nPerformance:")
    avg_dur = f"{stats['avg_duration_seconds']:.2f}s" if stats['avg_duration_seconds'] is not None else "N/A"
    utils.print_if_allowed(f"  Avg. Duration (Completed): {avg_dur} (based on {stats['completed_count']} crawls)")
    utils.print_if_allowed("-" * 40)

def print_discovered_items(items, crawl_id):
    """Prints a formatted list of discovered items."""
    if not items:
        utils.print_if_allowed(f"No discovered items found for crawl ID {crawl_id}.")
        return

    utils.print_if_allowed(f"\n--- Discovered Items for Crawl ID {crawl_id} ---")
    header = f"{'Type':<12} {'Path'}"
    utils.print_if_allowed(header)
    utils.print_if_allowed("-" * 40)

    # Group by type for potentially clearer output
    files = []
    dirs = []
    for item in items:
        if item['item_type'] == 'file':
            files.append(item['item_path'])
        elif item['item_type'] == 'directory':
            dirs.append(item['item_path'])

    if dirs:
        utils.print_if_allowed("\nDirectories:")
        for path in sorted(dirs): # Sort alphabetically
            utils.print_if_allowed(f"  - {path}")

    if files:
        utils.print_if_allowed("\nFiles:")
        for path in sorted(files): # Sort alphabetically
            utils.print_if_allowed(f"  - {path}")

    utils.print_if_allowed("-" * 40)


# --- Command Handlers ---

def handle_list_command(args):
    """Handles the 'list' command."""
    status_list = args.status.split(',') if args.status else None
    crawls = database.get_crawls(
        status_filter=status_list,
        limit=args.limit,
        sort_by=args.sort,
        descending=not args.asc,
        url_pattern=args.url_like
    )
    print_crawl_list(crawls)

def handle_show_command(args):
    """Handles the 'show' command."""
    if not args.id and not args.url:
        utils.print_if_allowed("Error: Please provide either --id or --url.")
        sys.exit(1)
    if args.id and args.url:
        utils.print_if_allowed("Error: Please provide only one of --id or --url.")
        sys.exit(1)

    crawl, extensions = database.get_crawl_details(crawl_id=args.id, url=args.url)
    print_crawl_details(crawl, extensions)

def handle_summary_command(args):
    """Handles the 'summary' command."""
    stats = database.get_crawl_summary_stats()
    print_summary_stats(stats)

def handle_top_extensions_command(args):
    """Handles the 'top-extensions' command."""
    agg_extensions = database.get_aggregated_extensions(top_n=args.top, min_count=args.min_count)
    print_aggregated_extensions(agg_extensions)

def handle_show_items_command(args):
    """Handles the 'show-items' command."""
    crawl_id = args.id
    # If URL is provided, we need to get the ID first
    if args.url:
        normalized_url = utils.normalize_url(args.url)
        # Use get_crawl_details just to fetch the ID efficiently
        crawl_record, _ = database.get_crawl_details(url=normalized_url)
        if not crawl_record:
            utils.print_if_allowed(f"Error: Crawl record not found for URL: {normalized_url}")
            sys.exit(1)
        crawl_id = crawl_record['id']
        utils.print_if_allowed(f"Found Crawl ID {crawl_id} for URL {normalized_url}")

    # Now fetch the items using the determined crawl_id
    items = database.get_discovered_items(
        crawl_id=crawl_id,
        item_type_filter=args.type,
        limit=args.limit
    )
    print_discovered_items(items, crawl_id)

# --- Main Execution ---

def run_tool():
    parser = argparse.ArgumentParser(
        description="Tool to interact with the Onion Crawler database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--db-file', default=config.DATABASE_FILE,
                        help='Path to the SQLite database file.')
    parser.add_argument('--silent', action='store_true', help='Suppress console output.')

    subparsers = parser.add_subparsers(dest='command', required=True, help='Available commands')

    # --- List Command ---
    parser_list = subparsers.add_parser('list', help='List crawl records with filters.')
    parser_list.add_argument('--status', type=str,
                             help='Filter by status (comma-separated, e.g., "Completed,Timeout")')
    parser_list.add_argument('--limit', type=int, default=50, help='Limit number of results.')
    parser_list.add_argument('--sort', type=str, default='start_time',
                             choices=['id', 'url', 'start_time', 'end_time', 'status', 'total_files', 'total_dirs'],
                             help='Column to sort by.')
    parser_list.add_argument('--asc', action='store_true', help='Sort in ascending order (default is descending).')
    parser_list.add_argument('--url-like', type=str, help='Filter URLs containing this pattern (case-insensitive).')
    parser_list.set_defaults(func=handle_list_command)

    # --- Show Command ---
    parser_show = subparsers.add_parser('show', help='Show details for a specific crawl.')
    show_group = parser_show.add_mutually_exclusive_group(required=True)
    show_group.add_argument('--id', type=int, help='ID of the crawl record.')
    show_group.add_argument('--url', type=str, help='URL of the crawl record.')
    # parser_show.add_argument('--show-extensions', action='store_true', help='Also display found extensions.') # Included by default now
    parser_show.set_defaults(func=handle_show_command)

    # --- Summary Command ---
    parser_summary = subparsers.add_parser('summary', help='Show overall statistics from the database.')
    parser_summary.set_defaults(func=handle_summary_command)

    # --- Top Extensions Command ---
    parser_top = subparsers.add_parser('top-extensions', help='Show aggregated top file extensions.')
    parser_top.add_argument('--top', type=int, default=20, help='Number of top extensions to show.')
    parser_top.add_argument('--min-count', type=int, default=1, help='Minimum total count for an extension.')
    parser_top.set_defaults(func=handle_top_extensions_command)

    # --- New Show Items Command ---
    parser_items = subparsers.add_parser('show-items', help='Show discovered file/directory paths for a crawl.')
    items_group = parser_items.add_mutually_exclusive_group(required=True)
    items_group.add_argument('--id', type=int, help='ID of the crawl record.')
    items_group.add_argument('--url', type=str, help='URL of the crawl record.')
    parser_items.add_argument('--type', choices=['file', 'directory'], help='Filter by item type.')
    parser_items.add_argument('--limit', type=int, help='Limit number of items shown.')
    parser_items.set_defaults(func=handle_show_items_command)

    args = parser.parse_args()

    # --- Configuration & Initialization ---
    config.set_silent_mode(args.silent)
    config.DATABASE_FILE = args.db_file # Use the specified DB file

    if not os.path.exists(config.DATABASE_FILE):
         utils.print_if_allowed(f"Error: Database file not found: {config.DATABASE_FILE}")
         sys.exit(1)

    try:
        args.func(args)
    except Exception as e:
        utils.print_if_allowed(f"\nAn unexpected error occurred: {e}")
        # Optionally print traceback if not silent
        # if not args.silent:
        #     import traceback
        #     traceback.print_exc()
        sys.exit(1)
    finally:
        database.close_db_connection()

if __name__ == "__main__":
    # Allows running the tool directly: python -m onion_crawler.db [command] [options]
    run_tool()
