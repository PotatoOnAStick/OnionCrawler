# onion_crawler/utils.py
import sys
import os
import re
import signal
import time
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from . import config # Use relative import

def print_if_allowed(message, end="\n"):
    """Print message only if not in silent mode and acquire lock."""
    if not config.SILENT_MODE:
        with config.CONSOLE_LOCK:
            print(message, end=end)
            sys.stdout.flush() # Ensure output is flushed immediately

def normalize_url(url: str) -> str:
    """Ensure URL starts with http:// and ends with /."""
    if not url:
        return ""
    if not url.startswith(('http://', 'https://')):
        url = 'http://' + url
    # Ensure it ends with a slash for consistency with directory crawling
    if not urlparse(url).path: # If no path component, add /
         url += '/'
    elif not url.endswith('/'):
        # Check if the last part looks like a file (has an extension)
        path_part = urlparse(url).path
        if '.' in os.path.basename(path_part):
             # It looks like a file, don't add slash
             pass
        else:
            url += '/'
    return url

def get_file_extension(filename: str) -> str | None:
    """Extract file extension from a filename."""
    if not filename or filename.endswith('/'):
        return None

    base_name = os.path.basename(filename) # Handle potential paths

    # Handle files starting with '.' (like .bashrc) - consider them extensionless
    if base_name.startswith('.'):
         return "no_extension" # Or return None, depending on desired behavior

    # Handle files without extensions
    if '.' not in base_name:
        return "no_extension"

    # Get the part after the last dot
    extension = base_name.split('.')[-1].lower()

    # Basic validation: not empty and reasonable length
    if not extension or len(extension) > 10:
        return "no_extension"

    # Optional: Filter out purely numeric extensions if desired
    # if extension.isdigit():
    #    return "no_extension"

    return extension


def is_directory_listing(html_content: str) -> bool:
    """
    Check if the HTML likely represents a directory listing using various heuristics.
    """
    if not html_content:
        return False

    # --- Heuristic 1: Common Title/Header Phrases (Case-Insensitive) ---
    # Check common phrases early for performance
    lower_content_sample = html_content[:1024].lower() # Check only beginning for speed
    common_phrases = ["index of /", "<h1>index of</h1>", "directory listing for /"]
    if any(phrase in lower_content_sample for phrase in common_phrases):
        return True

    # --- Heuristic 2: BeautifulSoup Parsing (More Robust) ---
    try:
        soup = BeautifulSoup(html_content, 'html.parser')

        # Check Title and H1 again, more reliably and case-insensitive
        title = soup.title.text.strip().lower() if soup.title else ""
        h1_text = soup.h1.text.strip().lower() if soup.h1 else ""
        if "index of" in title or "index of" in h1_text:
            return True

        # --- Heuristic 3: Presence of "Parent Directory" Link ---
        # Very common indicator
        parent_dir_links = soup.find_all('a', string=re.compile(r"^\s*Parent Directory\s*$", re.IGNORECASE))
        if parent_dir_links:
            return True
        # Also check common href values for parent links
        parent_hrefs = soup.find_all('a', href=re.compile(r"^\.{1,2}/?$")) # Matches ./ ../ . ..
        if parent_hrefs:
             # Check if it's not the *only* link (e.g., simple page with just a back link)
             all_links = soup.find_all('a', href=True)
             if len(all_links) > 1:
                 return True


        # --- Heuristic 4: Structure - <pre> tag with links (Apache style) ---
        pre_tags = soup.find_all('pre')
        for pre in pre_tags:
            # Check if it contains multiple links, potentially indicating a listing
            if len(pre.find_all('a', href=True)) > 2: # Threshold > 2 to avoid simple footers
                return True

        # --- Heuristic 5: Structure - Common table structures (Nginx/Lighttpd style) ---
        # Example: Look for tables where rows contain links and size/date info
        # This is more complex and might need refinement based on observed patterns
        tables = soup.find_all('table')
        for table in tables:
             rows = table.find_all('tr')
             if len(rows) > 2: # Need more than just a header row potentially
                 # Check if multiple rows contain an 'a' tag
                 link_rows = [r for r in rows if r.find('a', href=True)]
                 if len(link_rows) > 2: # If several rows have links
                     # Optional: Add checks for size/date columns if needed
                     return True

        # --- Add more heuristics as needed based on sites that are missed ---

    except Exception as e:
        # Handle potential parsing errors gracefully
        # print_if_allowed(f"Debug: Error parsing HTML for directory check: {e}") # Optional debug
        return False # Assume not a directory listing if parsing fails

    # If none of the heuristics matched
    return False



def format_extensions_summary(extensions_summary: dict) -> str:
    """Format extensions summary for display."""
    if not extensions_summary:
        return "N/A"

    parts = []
    # Sort by count descending for display
    sorted_items = sorted(extensions_summary.items(), key=lambda item: item[1][0], reverse=True)

    for ext, (count, percentage) in sorted_items:
        parts.append(f"{ext}:{count}({percentage:.1f}%)")

    return ", ".join(parts)

def signal_handler(sig, frame):
    """Sets the global stop event upon receiving SIGINT or SIGTERM."""
    print_if_allowed("\nReceived shutdown signal, initiating graceful shutdown...")
    config.GLOBAL_STOP_EVENT.set()
    # Give threads a moment to notice the event
    time.sleep(1)

def setup_signal_handlers():
    """Sets up signal handlers for SIGINT and SIGTERM."""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def validate_onion_url(url: str) -> bool:
    """Checks if a URL is a syntactically valid .onion address."""
    if not url:
        return False
    try:
        parsed = urlparse(url)
        # Check scheme (optional, normalization adds http://)
        # if parsed.scheme not in ('http', 'https'): return False
        # Check netloc exists and ends with .onion
        if not parsed.netloc or not parsed.netloc.endswith('.onion'):
            return False
        # Basic check for v3 onion address length (56 chars + .onion)
        # This isn't foolproof but catches obvious errors
        domain_part = parsed.netloc[:-6] # Remove .onion
        if len(domain_part) != 56:
             # Could be a v2 address (16 chars), but they are deprecated
             if len(domain_part) != 16:
                 print_if_allowed(f"Warning: Unusual onion address length for {parsed.netloc}")
                 # Allow it for now, Tor will handle validity
        return True
    except Exception:
        return False

