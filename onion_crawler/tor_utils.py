# onion_crawler/tor_utils.py
import requests
import time
import random
# *** Import specific exceptions for retry logic ***
from requests.exceptions import Timeout, RequestException, ConnectionError
# socks proxy errors might be under requests.exceptions or urllib3 depending on setup
# Let's catch broadly first and refine if needed. ConnectionAbortedError is built-in.

from . import config
from . import utils

# Define errors that might warrant a retry
RETRYABLE_ERRORS = (
    Timeout,
    ConnectionError, # Includes SOCKS errors, ConnectionAbortedError etc.
    ConnectionAbortedError, # Explicitly catch this Windows error
    # Add other specific exceptions if observed, e.g., from urllib3 or socks library if needed
)

def make_tor_request(url: str, method: str = 'GET', retries: int = 2, initial_retry_delay: float = 1.0, **kwargs):
    """
    Makes an HTTP request through the configured Tor proxy with rate limiting and retries.

    Args:
        url (str): The URL to request.
        method (str): HTTP method (default: 'GET').
        retries (int): Number of retries on failure (default: 2).
        initial_retry_delay (float): Initial delay before the first retry (seconds, default: 1.0).
        **kwargs: Additional arguments passed to requests.request.

    Returns:
        requests.Response: The response object on success.

    Raises:
        TimeoutError: If the rate limit semaphore cannot be acquired.
        requests.exceptions.RequestException: If the request fails after all retries.
    """
    last_exception = None
    current_retry_delay = initial_retry_delay

    for attempt in range(retries + 1):
        # Ensure rate limiter is acquired before making the request
        acquired = config.TOR_RATE_LIMITER.acquire(timeout=config.WORKER_TIMEOUT) # Prevent indefinite blocking
        if not acquired:
            # If semaphore times out, it's a different kind of failure, don't retry here.
            raise TimeoutError(f"Could not acquire Tor rate limit semaphore for {url}")

        try:
            # Add some random delay (jitter) before the request
            time.sleep(random.uniform(0.1, 0.5))

            # Calculate timeout
            timeout = config.REQUEST_TIMEOUT_BASE + random.uniform(0, config.REQUEST_TIMEOUT_JITTER)

            # Default arguments for requests
            request_args = {
                'proxies': config.TOR_PROXY,
                'timeout': timeout,
            }
            request_args.update(kwargs) # Update with any additional kwargs provided

            # utils.print_if_allowed(f"Attempt {attempt + 1}: Requesting {url}...") # Debugging
            response = requests.request(method, url, **request_args)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            return response # Success!

        # Catch specific retryable errors first
        except RETRYABLE_ERRORS as e:
            last_exception = e
            utils.print_if_allowed(f"Retryable error on attempt {attempt + 1}/{retries + 1} for {url}: {type(e).__name__}")
            if attempt < retries:
                utils.print_if_allowed(f"Retrying in {current_retry_delay:.1f}s...")
                time.sleep(current_retry_delay)
                current_retry_delay *= 2 # Exponential backoff (optional)
            # Continue to next attempt

        # Catch other non-retryable request exceptions (like HTTPError from raise_for_status)
        except RequestException as e:
            utils.print_if_allowed(f"Non-retryable request failed for {url}: {e}")
            last_exception = e
            break # Don't retry on non-retryable errors (like 404 Not Found)

        # Catch unexpected errors
        except Exception as e:
             utils.print_if_allowed(f"Unexpected error during request for {url}: {e}")
             last_exception = e
             break # Don't retry unexpected errors

        finally:
            # IMPORTANT: Release the semaphore whether the request succeeded or failed
            config.TOR_RATE_LIMITER.release()

    # If loop finished without returning (i.e., all retries failed or non-retryable error)
    utils.print_if_allowed(f"Request failed for {url} after {retries + 1} attempts.")
    if last_exception:
        raise last_exception # Re-raise the last captured exception
    else:
        # Should not happen, but raise a generic error if no exception was captured
        raise RequestException(f"Request failed for {url} after retries with unknown error.")


def check_tor_connection() -> bool:
    """Check if the Tor connection is working via check.torproject.org."""
    check_url = 'https://check.torproject.org/'
    utils.print_if_allowed(f"Testing Tor connection via {check_url}...")
    try:
        # Use make_tor_request which now includes retries
        response = make_tor_request(check_url, timeout=30, retries=1) # Use a longer timeout, maybe 1 retry
        if response and "Congratulations. This browser is configured to use Tor." in response.text:
            utils.print_if_allowed("Tor connection successful.")
            return True
        else:
            utils.print_if_allowed("Tor connection test failed: Unexpected response content.")
            if response:
                 utils.print_if_allowed(f"Status Code: {response.status_code}")
            return False
    # Catch specific exceptions make_tor_request might raise after retries
    except (TimeoutError, RequestException) as e:
         utils.print_if_allowed(f"Tor connection test failed: {e}")
         return False
    except Exception as e:
        utils.print_if_allowed(f"Tor connection test failed with unexpected error: {e}")
        return False

