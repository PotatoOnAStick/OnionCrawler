import requests
from bs4 import BeautifulSoup
import re
import sys
import os
from urllib.parse import urlparse, unquote

def extract_onion_domains(html_content):
    """Extract and normalize unique onion domains from Ahmia search results."""
    soup = BeautifulSoup(html_content, 'html.parser')
    domains = set()
    
    for result in soup.select('ol.searchResults li.result'):
        link = result.select_one('h4 a')
        if not link or 'href' not in link.attrs:
            continue
            
        match = re.search(r'redirect_url=([^&]+)', link['href'])
        if not match:
            continue
            
        try:
            url = unquote(match.group(1))
            if '.onion' in url:
                # Extract just the domain part
                domain = urlparse(url).netloc
                domains.add(domain)
        except Exception as e:
            print(f"Error processing URL: {e}")
    
    return list(domains)

def get_unique_filename(base_filename):
    """Generate a unique filename if the base filename already exists."""
    if not os.path.exists(base_filename):
        return base_filename
    
    name, ext = os.path.splitext(base_filename)
    counter = 1
    
    while True:
        new_filename = f"{name}_{counter}{ext}"
        if not os.path.exists(new_filename):
            return new_filename
        counter += 1

def save_domains_to_file(domains, filename="onion_domains.txt"):
    """Save domains to a text file with UTF-8 encoding and fallback to ASCII."""
    unique_filename = get_unique_filename(filename)
    
    try:
        with open(unique_filename, 'w', encoding='utf-8') as f:
            for domain in domains:
                f.write(f"{domain}\n")
        print(f"Saved {len(domains)} unique domains to {unique_filename}")
        return True
    except UnicodeEncodeError:
        try:
            ascii_filename = get_unique_filename(filename.replace('.txt', '_ascii.txt'))
            with open(ascii_filename, 'w', encoding='ascii', errors='ignore') as f:
                for domain in domains:
                    f.write(f"{domain}\n")
            print(f"Saved domains to {ascii_filename} with some characters removed")
            return True
        except Exception as e:
            print(f"Failed to save domains: {e}")
            return False

def main():
    # Check if search query is provided as command line argument
    if len(sys.argv) < 2:
        print("Usage: python script.py <search_query>")
        sys.exit(1)
    
    search_query = ' '.join(sys.argv[1:])
    print(f"Searching for: {search_query}")
    
    try:
        # Prepare headers and make request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        url = f"https://ahmia.fi/search/?q={search_query}"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            print(f"Failed to retrieve search results: HTTP {response.status_code}")
            sys.exit(1)
        
        # Extract and save domains
        domains = extract_onion_domains(response.text)
        print(f"Found {len(domains)} unique .onion domains")
        
        if domains:
            save_domains_to_file(domains)
            
            print("\nExtracted Domains:")
            for i, domain in enumerate(domains, 1):
                print(f"{i}. {domain}")
        else:
            print("No onion domains found in search results.")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()