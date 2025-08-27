import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import time
import os
import sys
from datetime import datetime
import random
from config import PROXY_USERNAME, PROXY_PASSWORD

# --- Ultra-Conservative Configuration ---

PROXY_URL = f'http://{PROXY_USERNAME}:{PROXY_PASSWORD}@unblock.decodo.com:60000'

BASE_URL = 'https://www.radwell.com'
BRAND_LINKS_FILE = 'brand_links.txt'

# Critical changes to avoid 429:
MAX_CONCURRENT_REQUESTS = 1  # Only ONE request at a time
RATE_LIMIT = 0.4  # 24 requests per minute (VERY conservative)
MIN_REQUEST_INTERVAL = 2.5  # Seconds between requests (minimum)
JITTER = 0.8  # Random jitter to mimic human behavior

# Retry configuration
RETRY_ATTEMPTS = 5
BASE_RETRY_DELAY = 3  # Base delay for retries (seconds)
MAX_RETRY_DELAY = 30  # Max delay between retries

# Request timeout
TIMEOUT = 45  # Slightly longer for slow responses

# --- Metrics Tracking ---
total_requests = 0
successful_requests = 0
image_requests = 0
successful_images = 0
total_traffic_bytes = 0
page_traffic = []
start_time = time.time()
last_request_time = 0
request_history = []  # Track recent requests for adaptive behavior

# --- Load Brand Links ---
def load_brand_links():
    if not os.path.exists(BRAND_LINKS_FILE):
        print(f"❌ {BRAND_LINKS_FILE} not found! Create it with brand URLs (one per line)")
        print("Example content:")
        print("/Brand?TopCategoryId=53&CategoryId=6027")
        print("/Brand?TopCategoryId=53&CategoryId=124")
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
    req_per_min = total_requests / (elapsed / 60) if elapsed > 0 else 0
    
    print("\n" + "="*70)
    print(f"📊 REAL-TIME METRICS (at {total_requests} requests | {req_per_min:.1f} req/min)")
    print("="*70)
    print(f"Elapsed time      : {elapsed:.1f} sec")
    print(f"Pages processed   : {len(page_traffic)}")
    print(f"Total requests    : {total_requests}")
    print(f"Successful        : {successful_requests} ({success_rate:.1f}%)")
    print(f"Image requests    : {image_requests}")
    print(f"Successful images : {successful_images} ({image_success_rate:.1f}%)")
    print(f"Total traffic     : {total_traffic_bytes / (1024*1024):.2f} MB")
    if page_traffic:
        print(f"Avg traffic/page  : {sum(page_traffic) / len(page_traffic) / 1024:.1f} KB")
    print("="*70)

# --- Adaptive Rate Control ---
async def enforce_rate_limit():
    global last_request_time
    
    # Calculate time since last request
    now = time.time()
    elapsed = now - last_request_time
    
    # Calculate required delay with jitter
    base_delay = max(0, MIN_REQUEST_INTERVAL - elapsed)
    jitter = random.uniform(0, JITTER)
    total_delay = base_delay + jitter
    
    # Record request time
    last_request_time = now + total_delay
    
    # Apply delay
    if total_delay > 0:
        await asyncio.sleep(total_delay)

# --- Async Fetch with Smart Retry ---
async def fetch_url(session, url, is_image=False, max_retries=RETRY_ATTEMPTS):
    global total_requests, successful_requests, image_requests, successful_images, total_traffic_bytes
    
    # Rate limiting enforcement
    await enforce_rate_limit()
    
    if is_image:
        image_requests += 1
    total_requests += 1

    # Track request for metrics
    request_start = time.time()
    attempt = 0
    
    while attempt < max_retries:
        attempt += 1
        try:
            # Calculate adaptive timeout
            adaptive_timeout = min(TIMEOUT, max(15, 5 + attempt * 5))
            
            async with session.get(url, proxy=PROXY_URL, timeout=adaptive_timeout) as response:
                content = await response.read()
                request_time = time.time() - request_start

                status = response.status
                total_traffic_bytes += len(content)

                # Handle 429 with exponential backoff
                if status == 429:
                    retry_after = response.headers.get('Retry-After', str(BASE_RETRY_DELAY * (2 ** (attempt-1))))
                    try:
                        wait_time = max(BASE_RETRY_DELAY, min(MAX_RETRY_DELAY, int(retry_after)))
                    except:
                        wait_time = BASE_RETRY_DELAY * (2 ** (attempt-1))
                    
                    # Record 429 in history for adaptive behavior
                    request_history.append(('429', time.time()))
                    
                    print(f"⏳ 429 Too Many Requests - Waiting {wait_time:.1f}s (Attempt {attempt}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue  # Retry immediately after wait

                # Handle other errors
                if status != 200:
                    print(f"⚠️  {status} [{request_time:.2f}s] {url} (Attempt {attempt}/{max_retries})")
                    # Exponential backoff for non-429 errors
                    if attempt < max_retries:
                        wait_time = BASE_RETRY_DELAY * (2 ** (attempt-1))
                        await asyncio.sleep(wait_time)
                    continue

                # Success!
                successful_requests += 1
                if is_image:
                    successful_images += 1
                print(f"✅ {status} [{request_time:.2f}s] {url}")
                return content

        except Exception as e:
            print(f"💥 ERROR [{attempt}/{max_retries}] {url}: {str(e)}")
            if attempt < max_retries:
                wait_time = BASE_RETRY_DELAY * (2 ** (attempt-1))
                await asyncio.sleep(wait_time)

    print(f"❌ FAILED after {max_retries} attempts: {url}")
    
    # Print interim report every 50 requests (more frequent for debugging)
    if total_requests % 50 == 0:
        print_interim_report()
        
    return None

# --- Extract and Download Assets (with extra caution) ---
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

    # Process assets sequentially (ultra-conservative)
    total_asset_traffic = 0
    for asset_url in asset_urls:
        is_image = 'image' in asset_url.lower() or \
                   any(ext in asset_url.lower() for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.svg'])
        
        content = await fetch_url(session, asset_url, is_image=is_image)
        if content:
            total_asset_traffic += len(content)
    
    return total_asset_traffic

# --- Crawl a Single Brand Page ---
async def crawl_brand_page(session, brand_url):
    print(f"\n🚀 Starting brand page: {brand_url}")
    page_start = time.time()
    page_traffic_bytes = 0

    # Fetch HTML (with extra retry for critical pages)
    html_content = await fetch_url(session, brand_url, max_retries=RETRY_ATTEMPTS + 2)
    if not html_content:
        return

    page_traffic_bytes += len(html_content)

    try:
        soup = BeautifulSoup(html_content.decode('utf-8', errors='ignore'), 'html.parser')
        print(f"   Found {len(soup.find_all(['img', 'script', 'link']))} assets to download")
        
        # Download assets with extra caution
        asset_traffic = await download_assets(session, soup, brand_url)
        page_traffic_bytes += asset_traffic
    except Exception as e:
        print(f"❗ Failed to parse HTML from {brand_url}: {str(e)}")

    # Record page traffic
    page_traffic.append(page_traffic_bytes)
    duration = time.time() - page_start
    print(f"✅ Brand page completed in {duration:.1f}s | Traffic: {page_traffic_bytes / 1024:.1f} KB")

    # Print interim report
    if total_requests % 50 == 0:
        print_interim_report()

# --- Main Async Function ---
async def main():
    global last_request_time
    last_request_time = time.time()  # Initialize request timer

    # Load brand URLs
    brand_urls = load_brand_links()
    print(f"\n🎯 Loaded {len(brand_urls)} brand URLs")
    print(f"⚡ Using ultra-conservative settings to avoid 429 errors:")
    print(f"   • {MAX_CONCURRENT_REQUESTS} concurrent request")
    print(f"   • {MIN_REQUEST_INTERVAL:.1f}s min delay between requests")
    print(f"   • {RATE_LIMIT*60:.0f} requests/minute max rate\n")

    # Setup session with proxy, headers, and timeout
    timeout = aiohttp.ClientTimeout(total=TIMEOUT)
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS, ssl=False)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
        'X-SU-Geo': 'United States',
        'X-SU-Locale': 'en-us',
        'X-SU-Headless': 'html',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=headers
    ) as session:
        # Crawl all brand pages sequentially (no concurrency)
        for url in brand_urls:
            await crawl_brand_page(session, url)
            # Extra pause between brand pages
            await asyncio.sleep(random.uniform(3.0, 5.0))

# --- Run and Measure Time ---
if __name__ == '__main__':
    print("="*70)
    print("🚀 RADWELL.COM TEST WITH DECODDO UNBLOCKER (429-PROOF MODE)")
    print("="*70)
    print("Configured for maximum reliability on strict sites")
    print("• Ultra-conservative request timing")
    print("• Smart 429 handling with exponential backoff")
    print("• Real-time metrics every 50 requests")
    print("="*70)
    
    start_time = time.time()
    asyncio.run(main())

    end_time = time.time()
    execution_time = end_time - start_time

    # --- Final Metrics ---
    success_rate = (successful_requests / total_requests * 100) if total_requests > 0 else 0
    image_success_rate = (successful_images / image_requests * 100) if image_requests > 0 else 0
    avg_traffic_per_page = (sum(page_traffic) / len(page_traffic)) if page_traffic else 0

    print("\n" + "="*70)
    print("📈 FINAL TEST SUMMARY")
    print("="*70)
    print(f"Started at        : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Execution time    : {execution_time:.1f} sec")
    print(f"Pages processed   : {len(page_traffic)}")
    print(f"Total requests    : {total_requests}")
    print(f"Successful        : {successful_requests} ({success_rate:.1f}%)")
    print(f"Image requests    : {image_requests}")
    print(f"Successful images : {successful_images} ({image_success_rate:.1f}%)")
    print(f"Total traffic     : {total_traffic_bytes / (1024*1024):.2f} MB")
    print(f"Avg traffic/page  : {avg_traffic_per_page / 1024:.1f} KB")
    print("="*70)
    
    # Calculate actual request rate
    req_per_min = total_requests / (execution_time / 60) if execution_time > 0 else 0
    print(f"\n💡 Actual request rate: {req_per_min:.1f} requests/minute (safe for strict sites)")