# Tor Directory Listing Crawler

A multi-threaded directory indexing tool for scanning and cataloging open directory listings on Tor onion services. This tool enables efficient discovery and analysis of content across the Tor network by identifying open directories and creating detailed reports of their contents.

## Features
- **Multi-level Directory Scanning**: Recursively indexes discovered directories to a configurable depth
- **Adaptive Threading**: Dynamically adjusts worker threads based on workload
- **Parallel Site Processing**: Crawl multiple onion sites simultaneously
- **Comprehensive Reporting**: Create an automated overview of the filetypes that can be found on a server
- **Connection Management**: Configurable rate limiting to prevent Tor network overload

## Requirements

- Python 3.6+
- Tor service running locally (typically on port 9050)
- Required Python packages:
  - requests
  - beautifulsoup4
  - PySocks

## Installation

```bash
# Install required packages
pip install requests beautifulsoup4 pysocks

# Make sure Tor is running
# On Linux/Mac: service tor start
# On Windows: Start the Tor Browser to run the Tor service
```

## Usage

```bash
python tor_crawler.py [-h] [-i INPUT_FILE] [-d MAX_DEPTH] [-w MAX_WORKERS] 
                      [-p PARALLEL_SITES] [-c CONNECTIONS] [-o OUTPUT] 
                      [-e ERRORS] urls ...
```

### Arguments

- `urls`: Space-separated list of onion URLs to crawl
- `-i, --input-file`: File containing URLs to crawl (one per line)
- `-d, --max-depth`: Maximum recursion depth (default: 5)
- `-w, --max-workers`: Maximum worker threads per site (default: 10)
- `-p, --parallel-sites`: Number of sites to crawl in parallel (default: 5)
- `-c, --connections`: Maximum concurrent Tor connections (default: 100)
- `-o, --output`: Success output file (default: results.txt)
- `-e, --errors`: Error output file (default: errors.txt)

### Examples

```bash
# Crawl a single onion site
python tor_crawler.py http://abcdefghijklmnop.onion/

# Crawl multiple sites from a file with custom settings
python tor_crawler.py -i onion_list.txt -d 3 -w 15 -p 3 -c 50
```

## Output

The tool generates two log files:

1. **Success log** (default: results.txt): Records successful crawls with:
   - Timestamp
   - URL
   - Number of files found
   - Number of directories found
   - File extension statistics
   - Crawl status

2. **Error log** (default: errors.txt): Records failed crawls with:
   - Timestamp
   - URL
   - Error details

## Safety and Ethics

This tool is designed for research, archival, and legitimate security assessment purposes. Please use responsibly and in accordance with applicable laws and regulations. Always:

- Respect website terms of service
- Consider the load your crawling places on services.
- Just don't do illegal shit

MIT License or whatever
