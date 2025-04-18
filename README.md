# Onion Directory Crawler

This tool crawls `.onion` websites accessible via the Tor network, specifically looking for open directory listings. It identifies files and subdirectories, counts them, analyzes file extensions, and stores the results in a local SQLite database. A separate command-line tool allows interaction with the stored data.

This was created mostly as a programming exercise that went a bit out of hand. Hopefully it will make some security researchers days just a little bit easier.

## Features

* **Concurrent Crawling:** Crawls multiple sites simultaneously and uses multiple threads per site for faster processing.
* **Rate Limiting:** Configurable global limit on concurrent Tor connections.
* **Database Storage:** Crawl summaries, status, file/directory counts, and extension statistics are stored in an SQLite database (`crawl_data.db` by default).
* **Optional Item Logging:** Can store the full paths of discovered files and directories in the database (`--log-items`).

## Requirements

* Python 3.x
* Tor service running (usually listening on `127.0.0.1:9050`)
* Python libraries: `requests`, `beautifulsoup4`, `pysocks` (for SOCKS proxy support in `requests`)

## Installation

1. **Clone or Download:** Get the project files.
2. **Install Dependencies:**
   ```bash
   pip install requests beautifulsoup4 pysocks
   ```
3. **Ensure Tor is Running:** Start your Tor service.

## Usage

The project consists of two main parts: the crawler and the database tool. Run commands from the directory *above* the `onion_crawler` package directory.

### 1. Running the Crawler (`scan.py`)

Use `python -m onion_crawler.scan` to start crawling.

**Basic Examples:**

* Crawl a single URL:
  ```bash
  python -m onion_crawler.scan http://example.onion
  ```
* Crawl multiple URLs:
  ```bash
  python -m onion_crawler.scan http://onion1.onion http://onion2.onion
  ```
* Crawl URLs from a file (one URL per line, '#' comments ignored):
  ```bash
  python -m onion_crawler.scan -i urls.txt
  ```

**Common Options:**

* `-d, --max-depth N`: Set maximum crawl depth (default: 5).
* `-w, --max-workers N`: Max worker threads per site (default: 10).
* `-p, --parallel-sites N`: Number of sites to crawl concurrently (default: 5).
* `-c, --connections N`: Max concurrent Tor connections (global limit, default: 200).
* `--log-items`: Store individual file/directory paths found in the database. (Increases DB size significantly).
* `--db-file PATH`: Specify a different database file path.
* `--silent`: Suppress most console output.

Example with options:
```bash
python -m onion_crawler.scan -i sites_to_scan.txt -p 10 -w 15 --log-items
```

### 2. Using the Database Tool (`db.py`)

Use `python -m onion_crawler.db` to interact with the data stored by the crawler.

**Commands:**

* **list**: Show a summary list of crawl records.
  * `--status STATUS[,STATUS...]`: Filter by status (e.g., Completed, Timeout).
  * `--limit N`: Limit number of results.
  * `--sort COLUMN`: Sort by id, url, start_time, etc.
  * `--asc`: Sort ascending.
  * `--url-like PATTERN`: Filter URLs containing pattern.
  ```bash
  python -m onion_crawler.db list --status Completed --limit 20 --sort total_files
  ```

* **show**: Display detailed information for a single crawl.
  * `--id ID`: Show details for a specific crawl ID.
  * `--url URL`: Show details for a specific URL.
  ```bash
  python -m onion_crawler.db show --id 42
  python -m onion_crawler.db show --url http://example.onion/
  ```

* **summary**: Show overall statistics from the database (total crawls, status breakdown, average duration, etc.).
  ```bash
  python -m onion_crawler.db summary
  ```

* **top-extensions**: Show aggregated file extension counts across crawls.
  * `--top N`: Show top N extensions.
  * `--min-count N`: Minimum count for an extension to be included.
  ```bash
  python -m onion_crawler.db top-extensions --top 15
  ```

* **show-items**: List individual files/directories logged for a crawl (requires `--log-items` during crawl).
  * `--id ID` or `--url URL`: Specify the crawl.
  * `--type [file|directory]`: Filter by item type.
  * `--limit N`: Limit number of items shown.
  ```bash
  python -m onion_crawler.db show-items --id 42 --type file --limit 100
  ```

**Common Options (for DB Tool):**

* `--db-file PATH`: Specify the database file to query.
* `--silent`: Suppress output.

## Database

Crawl results are stored in an SQLite database file (default: `crawl_data.db`).

* **crawls**: Main table storing summary information for each site crawled (URL, status, start/end times, counts, errors).
* **extensions**: Stores counts and percentages of file extensions found per crawl.
* **discovered_items**: (Optional) Stores individual file and directory paths found during crawls if `--log-items` was used.

You can inspect this file using standard SQLite tools if needed.
