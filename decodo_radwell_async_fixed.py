import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import os
import sys
from datetime import datetime
from config import PROXY_USERNAME, PROXY_PASSWORD

# --- Configuration --- 
PROXY_URL = f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@unblock.decodo.com:60000'

BASE_URL = 'https://www.radwell.com'
BRAND_LINKS_FILE = 'brand_links.txt'

# Concurrency & Retry
MAX_CONCURRENT_REQUESTS = 5  # Reduced from 10 to avoid 429
RETRY_ATTEMPTS = 4
RETRY_DELAY = 1.5  # Increased for better rate limiting
RATE_LIMIT = 1.2  # Requests per second (60 requests per minute)
MAX_TRAFFIC_MB = 95 # Max MB of traffic

# Request timeout
TIMEOUT = 30

# --- Metrics Tracking ---
total_requests = 0
successful_requests = 0
image_requests = 0
successful_images = 0
total_traffic_bytes = 0
page_traffic = []  # Total traffic per brand page (HTML + assets)
start_time = time.time()  # Global start time for interim reports
rate_limiter = None

# --- Rate Limiter Class ---
class RateLimiter:
    def __init__(self, rate_per_second=1.0):
        self.min_interval = 1.0 / rate_per_second
        self.lock = asyncio.Lock()
        self.last_request = time.monotonic() - self.min_interval  # Initialize to allow first request

    async def wait(self):
        async with self.lock:
            now = time.monotonic()
            next_time = self.last_request + self.min_interval
            wait_time = max(0, next_time - now)
            self.last_request = now + wait_time
        if wait_time > 0:
            await asyncio.sleep(wait_time)

# --- Load Brand Links ---
def load_brand_links():
    if not os.path.exists(BRAND_LINKS_FILE):
        print(f"❌ {BRAND_LINKS_FILE} not found!")
        sys.exit(1)
    with open(BRAND_LINKS_FILE, 'r', encoding='utf-8') as f:
        links = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return [urljoin(BASE_URL, link) for link in links]

# --- Print Interim Report ---
def print_interim_report():
    global total_requests, successful_requests, image_requests, successful_images, total_traffic_bytes
    
    if total_requests == 0:
        return
        
    success_rate = (successful_requests / total_requests * 100) if total_requests > 0 else 0
    image_success_rate = (successful_images / image_requests * 100) if image_requests > 0 else 0
    elapsed = time.time() - start_time
    
    print("\n" + "="*60)
    print(f"📊 INTERIM REPORT (at {total_requests} requests)")
    print("="*60)
    print(f"Execution time    : {elapsed:.2f} sec")
    print(f"Pages processed   : {len(page_traffic)}")
    print(f"Total requests    : {total_requests}")
    print(f"Successful        : {successful_requests} ({success_rate:.1f}%)")
    print(f"Image requests    : {image_requests}")
    print(f"Successful images : {successful_images} ({image_success_rate:.1f}%)")
    print(f"Total traffic     : {total_traffic_bytes / (1024*1024):.2f} MB")
    if page_traffic:
        print(f"Avg traffic/page  : {sum(page_traffic) / len(page_traffic) / 1024:.1f} KB")
    print("="*60)

# --- Async Fetch with Retry and Rate Limiting ---
async def fetch_url(session, url, is_image=False, max_retries=RETRY_ATTEMPTS):
    global total_requests, successful_requests, image_requests, successful_images, total_traffic_bytes 
    if total_traffic_bytes > MAX_TRAFFIC_MB * 1024 * 1024:
        print("🛑 Traffic limit reached. Exiting.")
        print_interim_report()
        sys.exit(0)
    
    if is_image:
        image_requests += 1
    total_requests += 1

    # Rate limiting - apply before every request
    await rate_limiter.wait()

    for attempt in range(max_retries):
        try:
            start_time = time.time()

            # Handle 429 specifically with Retry-After header
            async with session.get(url, proxy=PROXY_URL) as response:
                content = await response.read()
                request_time = time.time() - start_time

                status = response.status
                total_traffic_bytes += len(content)

                # Special handling for 429
                if status == 429:
                    retry_after = response.headers.get('Retry-After', '1')
                    try:
                        wait_time = int(retry_after)
                    except:
                        wait_time = 1
                    print(f"⏳ 429 Too Many Requests - Waiting {wait_time} seconds (Attempt {attempt+1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue  # Retry immediately after wait

                if status == 200:
                    successful_requests += 1
                    if is_image:
                        successful_images += 1
                    print(f"✅ {status} [{request_time:.2f}s] {url}")
                    return content
                else:
                    print(f"⚠️  {status} [{request_time:.2f}s] {url} (Attempt {attempt+1}/{max_retries})")

        except Exception as e:
            print(f"💥 ERROR [{attempt+1}/{max_retries}] {url}: {str(e)}")
        
        # Exponential backoff for non-429 failures
        if attempt < max_retries - 1:
            backoff = RETRY_DELAY * (2 ** attempt)
            await asyncio.sleep(backoff)

    print(f"❌ FAILED after {max_retries} attempts: {url}")
    
    # Print interim report every 100 requests
    if total_requests % 100 == 0:
        print_interim_report()
        
    return None

# --- Extract and Download Assets Concurrently ---
async def download_assets(session, soup, base_url):
    asset_urls = []
    asset_tasks = []

    for tag in soup.find_all(['img', 'script', 'link']):
        url_attr = 'src' if tag.has_attr('src') else 'href'
        if not tag.has_attr(url_attr):
            continue
        asset_url = urljoin(base_url, tag[url_attr])
        if 'radwell.com' in urlparse(asset_url).netloc and asset_url not in asset_urls:
            asset_urls.append(asset_url)

    # Create tasks for downloading assets
    for asset_url in asset_urls:
        is_image = 'image' in asset_url.lower() or \
                   any(ext in asset_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg'])
        task = fetch_url(session, asset_url, is_image=is_image)
        asset_tasks.append(task)

    # Run all asset downloads concurrently
    results = await asyncio.gather(*asset_tasks)
    return sum(len(r) for r in results if r)

# --- Crawl a Single Brand Page ---
async def crawl_brand_page(session, brand_url):
    print(f"\n🚀 Crawling: {brand_url}")
    page_start = time.time()
    page_traffic_bytes = 0

    # Fetch HTML
    html_content = await fetch_url(session, brand_url)
    if not html_content:
        return

    page_traffic_bytes += len(html_content)

    try:
        soup = BeautifulSoup(html_content.decode('utf-8', errors='ignore'), 'html.parser')
        asset_traffic = await download_assets(session, soup, brand_url)
        page_traffic_bytes += asset_traffic
    except Exception as e:
        print(f"❗ Failed to parse HTML from {brand_url}: {str(e)}")

    # Record page traffic
    page_traffic.append(page_traffic_bytes)
    duration = time.time() - page_start
    print(f"📊 Done in {duration:.2f}s | Traffic: {page_traffic_bytes / 1024:.1f} KB")

    # Print interim report every 100 requests
    if total_requests % 100 == 0:
        print_interim_report()

# --- Main Async Function ---
async def main():
    global rate_limiter
    rate_limiter = RateLimiter(rate_per_second=RATE_LIMIT)

    # Load brand URLs
    brand_urls = load_brand_links()
    print(f"🎯 Loaded {len(brand_urls)} brand URLs")

    # Setup session with proxy, headers, and timeout
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ssl=False)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'X-SU-Geo': 'United States',
        'X-SU-Locale': 'en-us',
        'X-SU-Headless': 'html',
    }

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=headers
    ) as session:
        # Crawl all brand pages concurrently
        tasks = [crawl_brand_page(session, url) for url in brand_urls]
        await asyncio.gather(*tasks)

# --- Run and Measure Time ---
if __name__ == '__main__':
    print("🚀 Starting Decodo Unblocker Test on radwell.com")
    start_time = time.time()  # Global start time

    asyncio.run(main())

    end_time = time.time()
    execution_time = end_time - start_time

    # --- Final Metrics ---
    success_rate = (successful_requests / total_requests * 100) if total_requests > 0 else 0
    image_success_rate = (successful_images / image_requests * 100) if image_requests > 0 else 0
    avg_traffic_per_page = (sum(page_traffic) / len(page_traffic)) if page_traffic else 0

    print("\n" + "="*60)
    print("📈 FINAL TEST SUMMARY")
    print("="*60)
    print(f"Started at        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Execution time    : {execution_time:.2f} sec")
    print(f"Pages processed   : {len(page_traffic)}")
    print(f"Total requests    : {total_requests}")
    print(f"Successful        : {successful_requests} ({success_rate:.1f}%)")
    print(f"Image requests    : {image_requests}")
    print(f"Successful images : {successful_images} ({image_success_rate:.1f}%)")
    print(f"Total traffic     : {total_traffic_bytes / (1024*1024):.2f} MB")
    print(f"Avg traffic/page  : {avg_traffic_per_page / 1024:.1f} KB")
    print("="*60)