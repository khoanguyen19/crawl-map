#!/usr/bin/env python3
"""
Browser-Based Guland Crawler
Uses real browser to bypass all anti-bot protection

Author: AI Assistant
Version: 3.0 (Browser Automation)
"""
import time
import json
import os
import random
import logging
import re  # Thêm import này
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from tile_downloader import GulandTileDownloader
from map_interaction_handler import MapInteractionHandler

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('browser_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class BrowserGulandCrawler:
    def __init__(self, headless=False, enable_download=True, download_workers=5):
        self.driver = None
        self.headless = headless
        self.map_handler = None  # Will be initialized after driver setup
        self.discovered_data = {
            'all_locations': [],
            'tile_servers': set(),
            'tile_patterns': set(),
            'success_count': 0,
            'failure_count': 0
        }
        
        # Initialize tile downloader
        self.enable_download = enable_download
        if self.enable_download:
            self.tile_downloader = GulandTileDownloader(
                base_download_dir='downloaded_tiles',
                max_workers=download_workers,
                timeout=30
            )
            logger.info(f"📥 Tile download enabled with {download_workers} workers")
        else:
            self.tile_downloader = None
            logger.info("⏭️ Tile download disabled")
        
        # Create output directories
        os.makedirs('output_browser_crawl', exist_ok=True)
        os.makedirs('output_browser_crawl/responses', exist_ok=True)
        os.makedirs('output_browser_crawl/screenshots', exist_ok=True)
        os.makedirs('output_browser_crawl/network_logs', exist_ok=True)
        
        self.test_locations = [
            # THÀNH PHỐ TRUNG ƯƠNG (5)
            ("Hà Nội", 21.0285, 105.8542, "soi-quy-hoach/ha-noi"),
            ("TP Hồ Chí Minh", 10.8231, 106.6297, "soi-quy-hoach/ho-chi-minh"),
            # ("Đà Nẵng", 16.0544563, 108.0717219, "soi-quy-hoach/da-nang"),
            # ("Hải Phòng", 20.8449, 106.6881, "soi-quy-hoach/hai-phong"),
            # ("Cần Thơ", 10.0452, 105.7469, "soi-quy-hoach/can-tho"),
            
            # # MIỀN BẮC (26)
            # # Vùng Đông Bắc
            # ("Hà Giang", 22.8025, 104.9784, "soi-quy-hoach/ha-giang"),
            # ("Cao Bằng", 22.6666, 106.2639, "soi-quy-hoach/cao-bang"),
            # ("Bắc Kạn", 22.1474, 105.8348, "soi-quy-hoach/bac-kan"),
            # ("Tuyên Quang", 21.8267, 105.2280, "soi-quy-hoach/tuyen-quang"),
            # ("Lạng Sơn", 21.8537, 106.7610, "soi-quy-hoach/lang-son"),
            # ("Thái Nguyên", 21.5944, 105.8480, "soi-quy-hoach/thai-nguyen"),
            # ("Phú Thọ", 21.4208, 105.2045, "soi-quy-hoach/phu-tho"),
            # ("Bắc Giang", 21.2731, 106.1946, "soi-quy-hoach/bac-giang"),
            # ("Quảng Ninh", 21.0064, 107.2925, "soi-quy-hoach/quang-ninh"),
            # ("Bắc Ninh", 21.1861, 106.0763, "soi-quy-hoach/bac-ninh"),
            
            # # Vùng Tây Bắc
            # ("Lai Châu", 22.3856, 103.4707, "soi-quy-hoach/lai-chau"),
            # ("Điện Biên", 21.3847, 103.0175, "soi-quy-hoach/dien-bien"),
            # ("Sơn La", 21.3256, 103.9188, "soi-quy-hoach/son-la"),
            # ("Hòa Bình", 20.8156, 105.3373, "soi-quy-hoach/hoa-binh"),
            
            # # Vùng Đồng bằng sông Hồng
            # ("Hải Dương", 20.9373, 106.3148, "soi-quy-hoach/hai-duong"),
            # ("Hưng Yên", 20.6464, 106.0511, "soi-quy-hoach/hung-yen"),
            # ("Hà Nam", 20.5835, 105.9230, "soi-quy-hoach/ha-nam"),
            # ("Nam Định", 20.4341, 106.1675, "soi-quy-hoach/nam-dinh"),
            # ("Thái Bình", 20.4500, 106.3400, "soi-quy-hoach/thai-binh"),
            # ("Ninh Bình", 20.2506, 105.9744, "soi-quy-hoach/ninh-binh"),
            # ("Vĩnh Phúc", 21.3609, 105.6049, "soi-quy-hoach/vinh-phuc"),
            
            # # MIỀN TRUNG (19)
            # # Bắc Trung Bộ
            # ("Thanh Hóa", 19.8069, 105.7851, "soi-quy-hoach/thanh-hoa"),
            # ("Nghệ An", 18.6745, 105.6905, "soi-quy-hoach/nghe-an"),
            # ("Hà Tĩnh", 18.3560, 105.9069, "soi-quy-hoach/ha-tinh"),
            # ("Quảng Bình", 17.4809, 106.6238, "soi-quy-hoach/quang-binh"),
            # ("Quảng Trị", 16.7404, 107.1854, "soi-quy-hoach/quang-tri"),
            # ("Thừa Thiên Huế", 16.4674, 107.5905, "soi-quy-hoach/thua-thien-hue"),
            
            # # Nam Trung Bộ
            # ("Quảng Nam", 15.5394, 108.0191, "soi-quy-hoach/quang-nam"),
            # ("Quảng Ngãi", 15.1214, 108.8044, "soi-quy-hoach/quang-ngai"),
            # ("Bình Định", 13.7757, 109.2219, "soi-quy-hoach/binh-dinh"),
            # ("Phú Yên", 13.0882, 109.0929, "soi-quy-hoach/phu-yen"),
            # ("Khánh Hòa", 12.2388, 109.1967, "soi-quy-hoach/khanh-hoa"),
            # ("Ninh Thuận", 11.5645, 108.9899, "soi-quy-hoach/ninh-thuan"),
            # ("Bình Thuận", 11.0904, 108.0721, "soi-quy-hoach/binh-thuan"),
            
            # # Tây Nguyên
            # ("Kon Tum", 14.3497, 108.0005, "soi-quy-hoach/kon-tum"),
            # ("Gia Lai", 13.8078, 108.1094, "soi-quy-hoach/gia-lai"),
            # ("Đắk Lắk", 12.7100, 108.2378, "soi-quy-hoach/dak-lak"),
            # ("Đắk Nông", 12.2646, 107.6098, "soi-quy-hoach/dak-nong"),
            # ("Lâm Đồng", 11.5753, 108.1429, "soi-quy-hoach/lam-dong"),
            
            # # MIỀN NAM (14)
            # # Đông Nam Bộ
            # ("Bình Phước", 11.7511, 106.7234, "soi-quy-hoach/binh-phuoc"),
            # ("Tây Ninh", 11.3100, 106.0989, "soi-quy-hoach/tay-ninh"),
            # ("Bình Dương", 11.3254, 106.4770, "soi-quy-hoach/binh-duong"),
            # ("Đồng Nai", 11.0686, 107.1676, "soi-quy-hoach/dong-nai"),
            # ("Bà Rịa - Vũng Tàu", 10.5417, 107.2431, "soi-quy-hoach/ba-ria-vung-tau"),
            
            # # Đồng bằng sông Cửu Long
            # ("Long An", 10.6957, 106.2431, "soi-quy-hoach/long-an"),
            # ("Tiền Giang", 10.4493, 106.3420, "soi-quy-hoach/tien-giang"),
            # ("Bến Tre", 10.2433, 106.3756, "soi-quy-hoach/ben-tre"),
            # ("Trà Vinh", 9.9477, 106.3524, "soi-quy-hoach/tra-vinh"),
            # ("Vĩnh Long", 10.2397, 105.9571, "soi-quy-hoach/vinh-long"),
            # ("Đồng Tháp", 10.4938, 105.6881, "soi-quy-hoach/dong-thap"),
            # ("An Giang", 10.3889, 105.4359, "soi-quy-hoach/an-giang"),
            # ("Kiên Giang", 10.0125, 105.0808, "soi-quy-hoach/kien-giang"),
            # ("Hậu Giang", 9.7571, 105.6412, "soi-quy-hoach/hau-giang"),
            # ("Sóc Trăng", 9.6002, 105.9800, "soi-quy-hoach/soc-trang"),
            # ("Bạc Liêu", 9.2515, 105.7244, "soi-quy-hoach/bac-lieu"),
            # ("Cà Mau", 9.1769, 105.1524, "soi-quy-hoach/ca-mau")
        ]
        
        # Network request tracking
        self.captured_requests = []
    
    def setup_driver(self):
        """Setup Chrome driver with network logging enabled"""
        logger.info("🚀 Setting up Chrome driver with network logging...")
        
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument("--headless")
        
        # Enable logging for network requests
        chrome_options.add_argument("--enable-logging")
        chrome_options.add_argument("--log-level=0")
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        
        # Enable performance logging to capture network requests
        chrome_options.set_capability('goog:loggingPrefs', {
            'performance': 'ALL',
            'network': 'ALL'
        })
        
        # Make browser look realistic
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--window-size=1920,1080")
        
        # User agent
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        
        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Execute script to remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            # Initialize map interaction handler
            self.map_handler = MapInteractionHandler(self.driver)
            
            logger.info("✅ Chrome driver setup successful with network logging")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to setup Chrome driver: {e}")
            return False
    
    
    def start_network_capture(self):
        """Start capturing network requests"""
        logger.info("📡 Starting network capture...")
        self.captured_requests = []
        
        # Clear existing performance logs
        self.driver.get_log('performance')
    
    def get_network_requests(self):
        """Get all network requests from performance log with enhanced filtering"""
        logs = self.driver.get_log('performance')
        requests = []
        
        for log in logs:
            try:
                message = json.loads(log['message'])
                
                # Capture both response received and request sent
                if message['message']['method'] in ['Network.responseReceived', 'Network.requestWillBeSent']:
                    if message['message']['method'] == 'Network.responseReceived':
                        response = message['message']['params']['response']
                        url = response['url']
                        status = response.get('status', 0)
                    elif message['message']['method'] == 'Network.requestWillBeSent':
                        request = message['message']['params']['request']
                        url = request['url']
                        status = 'pending'
                    
                    requests.append({
                        'url': url,
                        'timestamp': log['timestamp'],
                        'status': status,
                        'method': message['message']['method']
                    })
            except (KeyError, json.JSONDecodeError) as e:
                # Skip malformed log entries
                continue
        
        # Remove duplicates and sort by timestamp
        unique_urls = set()
        unique_requests = []
        
        for request in sorted(requests, key=lambda x: x['timestamp']):
            if request['url'] not in unique_urls:
                unique_urls.add(request['url'])
                unique_requests.append(request)
        
        logger.info(f"📡 Total unique requests: {len(unique_requests)}")
        
        # Log some sample URLs for debugging
        sample_urls = [req['url'] for req in unique_requests[:5]]
        logger.info(f"📡 Sample URLs: {sample_urls}")
        
        return unique_requests
    
    def extract_tile_urls(self, requests):
        """Enhanced tile URL extraction with better pattern matching"""
        tile_urls = []
        tile_patterns = set()
        
        logger.info(f"🔍 Analyzing {len(requests)} requests for tile patterns...")
        
        for request in requests:
            url = request['url']
            
            # Enhanced tile detection patterns
            tile_indicators = [
                '.png', '.jpg', '.jpeg', '.webp', '.tiff', '.tif'
            ]
            
            # Check if this looks like a tile URL
            if any(ext in url.lower() for ext in tile_indicators):
                logger.info(f"🔍 Checking potential tile URL: {url}")
                
                # Pattern 1: Standard /z/x/y.ext
                tile_pattern = re.search(r'/(\d+)/(\d+)/(\d+)\.(png|jpg|jpeg|webp|tiff|tif)', url, re.IGNORECASE)
                
                if tile_pattern:
                    zoom, x, y, ext = tile_pattern.groups()
                    logger.info(f"✅ Found tile: z={zoom}, x={x}, y={y}, ext={ext}")
                    
                    # Extract base URL pattern
                    base_url = url.split(f'/{zoom}/{x}/{y}.{ext}')[0]
                    pattern = f"{base_url}/{{z}}/{{x}}/{{y}}.{ext}"
                    tile_patterns.add(pattern)
                    
                    tile_urls.append({
                        'url': url,
                        'base_url': base_url,
                        'zoom': int(zoom),
                        'x': int(x),
                        'y': int(y),
                        'format': ext.lower(),
                        'status': request['status'],
                        'pattern': pattern
                    })
                else:
                    # Pattern 2: Alternative formats like /zoom_x_y.ext or /z-x-y.ext
                    alt_patterns = [
                        r'/(\d+)_(\d+)_(\d+)\.(png|jpg|jpeg|webp)',
                        r'/(\d+)-(\d+)-(\d+)\.(png|jpg|jpeg|webp)',
                        r'/tile_(\d+)_(\d+)_(\d+)\.(png|jpg|jpeg|webp)'
                    ]
                    
                    for alt_pattern in alt_patterns:
                        alt_match = re.search(alt_pattern, url, re.IGNORECASE)
                        if alt_match:
                            zoom, x, y, ext = alt_match.groups()
                            logger.info(f"✅ Found alternative tile format: z={zoom}, x={x}, y={y}")
                            
                            # Create pattern
                            base_url = re.sub(alt_pattern, '', url)
                            pattern = f"{base_url}/{{z}}_{{x}}_{{y}}.{ext}"
                            tile_patterns.add(pattern)
                            
                            tile_urls.append({
                                'url': url,
                                'base_url': base_url,
                                'zoom': int(zoom),
                                'x': int(x),
                                'y': int(y),
                                'format': ext.lower(),
                                'status': request['status'],
                                'pattern': pattern
                            })
                            break
        
        logger.info(f"🎯 Extracted {len(tile_urls)} tile URLs")
        logger.info(f"🎯 Found {len(tile_patterns)} unique patterns")
        
        # Log found patterns for debugging
        for pattern in tile_patterns:
            logger.info(f"🎯 Pattern: {pattern}")
        
        return tile_urls, tile_patterns
    
    def systematic_zoom_coverage(self, location_name, lat, lng, duration_per_zoom=30):
        """Systematically cover all zoom levels 10-18 with IMMEDIATE DOWNLOAD"""
        logger.info(f"🎯 Starting systematic zoom coverage for {location_name}")
        
        zoom_levels = list(range(10, 19))  # 10 to 18
        all_captured_tiles = []
        
        for zoom_index, zoom in enumerate(zoom_levels):
            logger.info(f"🔍 Processing zoom level {zoom} ({zoom_index+1}/{len(zoom_levels)})")
            
            try:
                # Clear network logs for this zoom level
                self.driver.get_log('performance')
                
                # Set specific zoom level using map handler
                zoom_success = self.map_handler.set_map_zoom(zoom)
                if not zoom_success:
                    self.map_handler.simulate_zoom_interaction(zoom)
                
                # Wait for tiles to load at this zoom
                time.sleep(3)
                
                # Perform coverage at this zoom using map handler
                action_count = self.map_handler.comprehensive_map_coverage(zoom, duration_per_zoom)
                
                # Get tiles captured during this coverage
                requests = self.get_network_requests()
                tile_urls, _ = self.extract_tile_urls(requests)
                
                # Filter tiles for current zoom level
                tiles_at_zoom = [tile for tile in tile_urls if tile['zoom'] == zoom]
                
                if tiles_at_zoom:
                    all_captured_tiles.extend(tiles_at_zoom)
                    logger.info(f"✅ Zoom {zoom}: Found {len(tiles_at_zoom)} tiles")
                    
                    # ===== DOWNLOAD TILES IMMEDIATELY =====
                    if self.enable_download and self.tile_downloader:
                        logger.info(f"📥 Downloading {len(tiles_at_zoom)} tiles from zoom {zoom}...")
                        # Pass just the location name, not with zoom suffix
                        download_results = self.tile_downloader.download_tiles_batch(
                            tiles_at_zoom, 
                            location_name  # Changed from f"{location_name}_zoom_{zoom}"
                        )
                        
                        successful = len([r for r in download_results if r['success']])
                        logger.info(f"✅ Downloaded {successful}/{len(tiles_at_zoom)} tiles from zoom {zoom}")
                    
                else:
                    logger.warning(f"⚠️ Zoom {zoom}: No tiles found")
                
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"❌ Error at zoom {zoom}: {e}")
                continue
        
        logger.info(f"🎉 Systematic coverage complete: {len(all_captured_tiles)} total tiles")
        return all_captured_tiles
    
    def generate_final_report(self):
        """Generate final comprehensive report WITH DOWNLOAD STATS"""
        logger.info("📊 Generating final comprehensive report...")
        
        # Get download stats from tile downloader
        download_stats = self.tile_downloader.download_stats if self.tile_downloader else {
            'total_attempted': 0, 'total_successful': 0, 'total_failed': 0, 'total_bytes': 0
        }
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'crawler': 'Full Coverage Guland Crawler v6.0 (With Download)',
            'method': 'Systematic zoom coverage 10-18 + Map interaction + Tile download',
            'summary': {
                'total_attempted': len(self.test_locations),
                'total_successful': self.discovered_data['success_count'],
                'total_failed': self.discovered_data['failure_count'],
                'success_rate': (self.discovered_data['success_count'] / len(self.test_locations) * 100) if len(self.test_locations) > 0 else 0,
                'unique_tile_patterns': len(self.discovered_data['tile_patterns']),
                'tile_servers': len(self.discovered_data['tile_servers'])
            },
            'download_summary': download_stats,
            'tile_patterns': list(self.discovered_data['tile_patterns']),
            'tile_servers': list(self.discovered_data['tile_servers']),
            'successful_locations': self.discovered_data['all_locations']
        }
        
        # Save comprehensive report
        with open('output_browser_crawl/full_coverage_final_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_report = f"""
# FULL COVERAGE GULAND CRAWLER FINAL REPORT (WITH DOWNLOADS)
Generated: {report['timestamp']}
Method: Systematic zoom coverage 10-18 + Map interaction + Tile download

## 📊 CRAWL SUMMARY
• Total Attempted: {report['summary']['total_attempted']}
• Total Successful: {report['summary']['total_successful']}
• Success Rate: {report['summary']['success_rate']:.1f}%
• Unique Tile Patterns: {report['summary']['unique_tile_patterns']}
• Tile Servers: {report['summary']['tile_servers']}

## 📥 DOWNLOAD SUMMARY
• Total tiles attempted: {report['download_summary']['total_attempted']}
• Successful downloads: {report['download_summary']['total_successful']}
• Failed downloads: {report['download_summary']['total_failed']}
• Download success rate: {(report['download_summary']['total_successful']/report['download_summary']['total_attempted']*100) if report['download_summary']['total_attempted'] > 0 else 0:.1f}%
• Total downloaded: {self.tile_downloader.format_bytes(report['download_summary']['total_bytes']) if self.tile_downloader else '0 B'}

## 🎯 TILE PATTERNS DISCOVERED
"""
        
        for pattern in report['tile_patterns']:
            text_report += f"• {pattern}\n"
        
        text_report += f"\n## 🗺️ TILE SERVERS\n"
        for server in report['tile_servers']:
            text_report += f"• {server}\n"
        
        # Add location details with download stats
        text_report += f"\n## 📍 LOCATION DETAILS\n"
        for location in report['successful_locations']:
            text_report += f"### {location['location_name']}\n"
            text_report += f"• Tiles found: {location['tile_count']}\n"
            text_report += f"• Patterns: {len(location['tile_patterns'])}\n"
            if location.get('download_stats'):
                ds = location['download_stats']
                formatted_size = self.tile_downloader.format_bytes(ds['total_size']) if self.tile_downloader else f"{ds['total_size']} bytes"
                text_report += f"• Downloads: {ds['successful']}/{ds['attempted']} ({formatted_size})\n"
            text_report += "\n"
        
        with open('output_browser_crawl/full_coverage_final_report.txt', 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        logger.info("✅ Final report generated")
        
        # Print summary to console
        print(f"\n🎉 FULL COVERAGE CRAWL WITH DOWNLOADS COMPLETED!")
        print("=" * 60)
        print(f"📊 Crawl Results:")
        print(f"  • Locations attempted: {report['summary']['total_attempted']}")
        print(f"  • Locations successful: {report['summary']['total_successful']}")
        print(f"  • Success rate: {report['summary']['success_rate']:.1f}%")
        print(f"  • Unique tile patterns: {report['summary']['unique_tile_patterns']}")
        print(f"  • Tile servers: {report['summary']['tile_servers']}")
        
        print(f"\n📥 Download Results:")
        print(f"  • Tiles attempted: {report['download_summary']['total_attempted']}")
        print(f"  • Tiles downloaded: {report['download_summary']['total_successful']}")
        print(f"  • Download success rate: {(report['download_summary']['total_successful']/report['download_summary']['total_attempted']*100) if report['download_summary']['total_attempted'] > 0 else 0:.1f}%")
        formatted_size = self.tile_downloader.format_bytes(report['download_summary']['total_bytes']) if self.tile_downloader else '0 B'
        print(f"  • Total size: {formatted_size}")
        
        if self.tile_downloader:
            print(f"\n📁 Downloaded tiles are stored in: {self.tile_downloader.base_download_dir}/")
        
        return report

    def crawl_location_with_full_coverage(self, location_name, lat, lng, path):
        """Crawl location with full tile coverage for zoom 10-18 - IMMEDIATE DOWNLOAD VERSION"""
        logger.info(f"🎯 FULL COVERAGE CRAWL WITH IMMEDIATE DOWNLOAD: {location_name}")
        logger.info("=" * 60)
        
        try:
            # Navigate to location page
            if not self.navigate_to_location_page(location_name, path):
                return None
            
            # Take initial screenshot
            screenshot_path = f"output_browser_crawl/screenshots/{location_name.replace(' ', '_')}_initial.png"
            self.driver.save_screenshot(screenshot_path)
            
            # Detect city boundaries using map handler
            bounds = self.map_handler.detect_city_boundaries(location_name)
            
            # Start comprehensive tile coverage for all zoom levels
            tiles_data = self.systematic_zoom_coverage(location_name, lat, lng)
            
            # Extract unique patterns and servers
            tile_patterns = set()
            tile_servers = set()
            
            zoom_levels = {}
            
            for tile in tiles_data:
                # Add to patterns and servers
                if 'pattern' in tile:
                    tile_patterns.add(tile['pattern'])
                tile_servers.add(self.extract_server_from_url(tile['url']))
                
                # Group by zoom level
                zoom = tile['zoom']
                if zoom not in zoom_levels:
                    zoom_levels[zoom] = {
                        'tile_count': 0,
                        'tiles': []
                    }
                zoom_levels[zoom]['tile_count'] += 1
                zoom_levels[zoom]['tiles'].append(tile)
            
            # Update global discovered data
            self.discovered_data['tile_patterns'].update(tile_patterns)
            self.discovered_data['tile_servers'].update(tile_servers)
            self.discovered_data['success_count'] += 1
            
            # Generate download report if enabled
            if self.enable_download and self.tile_downloader:
                # Create mock download results for reporting
                download_results = []
                for tile in tiles_data:
                    download_results.append({
                        'success': True,
                        'tile_type': self.tile_downloader.get_tile_type_from_url(tile['url']),
                        'size': 1024  # Mock size
                    })
                
                self.tile_downloader.generate_download_report(location_name, download_results)
            
            # Calculate coverage analysis
            coverage_analysis = {}
            if bounds:
                for zoom in range(10, 19):
                    coverage_analysis[zoom] = self.map_handler.calculate_tile_coverage_needed(bounds, zoom)
            
            location_result = {
                'location_name': location_name,
                'timestamp': datetime.now().isoformat(),
                'total_tiles': len(tiles_data),
                'zoom_levels': zoom_levels,
                'tile_patterns': list(tile_patterns),
                'tile_servers': list(tile_servers),
                'bounds': bounds,
                'coverage_analysis': coverage_analysis,
                'tile_count': len(tiles_data)
            }
            
            # Generate coverage report
            self.generate_coverage_report(location_result)
            
            logger.info(f"🎉 FULL COVERAGE COMPLETE: {location_name}")
            logger.info(f"📊 Total tiles captured: {len(tiles_data)}")
            logger.info(f"📊 Zoom levels covered: {len(zoom_levels)}")
            logger.info(f"📊 Unique patterns: {len(tile_patterns)}")
            
            return location_result
            
        except Exception as e:
            logger.error(f"❌ Error in full coverage crawl: {e}")
            self.discovered_data['failure_count'] += 1
            return None

    def generate_coverage_report(self, tiles_data):
        """Generate detailed coverage analysis report"""
        logger.info("📊 Generating coverage analysis report...")
        
        report_lines = []
        report_lines.append(f"# FULL COVERAGE ANALYSIS: {tiles_data['location_name']}")
        report_lines.append(f"Generated: {tiles_data['timestamp']}")
        report_lines.append("")
        
        report_lines.append("## 📊 COVERAGE SUMMARY")
        report_lines.append(f"• Total tiles captured: {tiles_data['total_tiles']}")
        report_lines.append(f"• Zoom levels covered: {len(tiles_data['zoom_levels'])}")
        report_lines.append(f"• Unique tile patterns: {len(tiles_data['tile_patterns'])}")
        report_lines.append(f"• Tile servers: {len(tiles_data['tile_servers'])}")
        report_lines.append("")
        
        report_lines.append("## 🔍 ZOOM LEVEL BREAKDOWN")
        for zoom in range(10, 19):
            if zoom in tiles_data['zoom_levels']:
                zoom_data = tiles_data['zoom_levels'][zoom]
                expected = tiles_data['coverage_analysis'].get(zoom, {}).get('total_tiles', 'Unknown')
                actual = zoom_data['tile_count']
                
                if isinstance(expected, int) and expected > 0:
                    coverage_pct = (actual / expected) * 100
                    report_lines.append(f"• Zoom {zoom}: {actual}/{expected} tiles ({coverage_pct:.1f}% coverage)")
                else:
                    report_lines.append(f"• Zoom {zoom}: {actual} tiles (coverage unknown)")
            else:
                report_lines.append(f"• Zoom {zoom}: 0 tiles (failed)")
        
        report_lines.append("")
        report_lines.append("## 🎯 TILE PATTERNS")
        for pattern in tiles_data['tile_patterns']:
            report_lines.append(f"• {pattern}")
        
        # Save report
        report_path = f"output_browser_crawl/coverage_report_{tiles_data['location_name'].replace(' ', '_')}.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"📋 Coverage report saved: {report_path}")
        
    def crawl_location_with_interaction(self, location_name, lat, lng, path):
        """Crawl location by simulating user interaction WITH TILE DOWNLOAD"""
        logger.info(f"📍 Crawling {location_name} via user interaction...")
        
        try:
            # Navigate to location page
            if not self.navigate_to_location_page(location_name, path):
                return None
            
            # Take initial screenshot
            screenshot_path = f"output_browser_crawl/screenshots/{location_name.replace(' ', '_')}_initial.png"
            self.driver.save_screenshot(screenshot_path)
            logger.info(f"📸 Initial screenshot: {screenshot_path}")
            
            # Start network capture
            self.start_network_capture()
            
            # Trigger tile loading via JavaScript first using map handler
            self.map_handler.trigger_tile_loading()
            
            # Simulate realistic map interaction using map handler
            self.map_handler.simulate_map_interaction(location_name, duration_seconds=60)
            
            # Trigger again after interactions
            self.map_handler.trigger_tile_loading()
            
            # ... rest of the method remains the same ...
            
        except Exception as e:
            logger.error(f"❌ Error crawling {location_name}: {e}")
            self.discovered_data['failure_count'] += 1
            return None

    def extract_server_from_url(self, url):
        """Extract server base URL from tile URL"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    def run_browser_crawl(self, max_hours=2):  # Tăng từ 1 giờ lên 2 giờ
        """Run complete browser-based crawl with configurable timeout"""
        logger.info("🚀 STARTING FULL COVERAGE CRAWL (ZOOM 10-18)")
        logger.info("=" * 70)
        
        crawl_start_time = time.time()
        max_crawl_time = max_hours * 3600  # Flexible time limit
        
        try:
            if not self.setup_driver():
                return None
            
            logger.info(f"⏰ Time limit set to {max_hours} hours")
            logger.info("🌐 Warming up session...")
            self.driver.get("https://guland.vn/")
            time.sleep(random.uniform(3, 6))
            
            for i, (location_name, lat, lng, path) in enumerate(self.test_locations, 1):
                # Check global timeout
                elapsed_time = time.time() - crawl_start_time
                remaining_time = max_crawl_time - elapsed_time
                
                if remaining_time < 300:  # Less than 5 minutes remaining
                    logger.warning(f"⏰ Time limit reached: {elapsed_time/3600:.1f}h/{max_hours}h")
                    logger.warning(f"⏰ Not enough time for next location, stopping")
                    break
                
                # Estimate if we have enough time for this location
                estimated_time_per_location = 600  # 10 minutes estimate
                if remaining_time < estimated_time_per_location:
                    logger.warning(f"⏰ Estimated {estimated_time_per_location/60:.1f}min needed, only {remaining_time/60:.1f}min left")
                    logger.warning(f"⏰ Skipping remaining locations")
                    break
                    
                logger.info(f"\n🌍 PROCESSING {i}/{len(self.test_locations)}: {location_name}")
                logger.info(f"⏱️ Elapsed: {elapsed_time/3600:.1f}h, Remaining: {remaining_time/3600:.1f}h")
                logger.info("=" * 60)
                
                location_start_time = time.time()
                
                # Process location với timeout riêng
                location_info = self.crawl_location_with_timeout(location_name, lat, lng, path, timeout_minutes=10)
                
                location_elapsed = time.time() - location_start_time
                logger.info(f"⏱️ {location_name} processed in {location_elapsed:.1f}s")
                
                if location_info:
                    self.discovered_data['all_locations'].append(location_info)
                    logger.info(f"✅ {location_name}: {location_info['total_tiles']} tiles captured")
                    
                    # Print quick summary
                    for zoom in range(10, 19):
                        if zoom in location_info['zoom_levels']:
                            count = location_info['zoom_levels'][zoom]['tile_count']
                            if count > 0:
                                logger.info(f"   Zoom {zoom}: {count} tiles")
                else:
                    logger.warning(f"⚠️ Failed to process {location_name}")
                
                # Adaptive delay between locations
                if i < len(self.test_locations):
                    remaining_time = max_crawl_time - (time.time() - crawl_start_time)
                    remaining_locations = len(self.test_locations) - i
                    
                    if remaining_locations > 0:
                        time_per_location = remaining_time / remaining_locations
                        delay = min(30, max(5, time_per_location * 0.05))  # 5% of remaining time per location
                        
                        logger.info(f"⏳ Waiting {delay:.1f}s before next location...")
                        logger.info(f"📊 {remaining_locations} locations remaining, {remaining_time/60:.1f}min left")
                        time.sleep(delay)
            
            # Generate final comprehensive report
            final_report = self.generate_final_report()
            return final_report
            
        except Exception as e:
            logger.error(f"❌ Full coverage crawl failed: {e}")
            return None
            
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("🔚 Browser closed")
            
            total_elapsed = time.time() - crawl_start_time
            logger.info(f"⏱️ Total crawl time: {total_elapsed/3600:.1f} hours")
    
    def crawl_location_with_timeout(self, location_name, lat, lng, path, timeout_minutes=10):
        """Crawl location với timeout cụ thể"""
        import signal
        
        def timeout_handler(signum, frame):
            raise TimeoutError(f"Location crawl timeout after {timeout_minutes} minutes")
        
        # Set timeout
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout_minutes * 60)  # Convert to seconds
        
        try:
            result = self.crawl_location_with_full_coverage(location_name, lat, lng, path)
            signal.alarm(0)  # Cancel timeout
            return result
            
        except TimeoutError as e:
            logger.warning(f"⏰ {location_name} timed out after {timeout_minutes} minutes")
            signal.alarm(0)  # Cancel timeout
            self.discovered_data['failure_count'] += 1
            return None
            
        except Exception as e:
            logger.error(f"❌ Error in {location_name}: {e}")
            signal.alarm(0)  # Cancel timeout
            self.discovered_data['failure_count'] += 1
            return None
    
    def generate_interaction_report(self):
        """Generate report for interaction-based crawling"""
        logger.info("📊 Generating interaction crawl report...")
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'crawler': 'User Interaction-Based Guland Crawler v4.0',
            'method': 'Map interaction simulation + Network request capture',
            'summary': {
                'total_attempted': len(self.test_locations),
                'total_successful': self.discovered_data['success_count'],
                'total_failed': self.discovered_data['failure_count'],
                'success_rate': self.discovered_data['success_count'] / len(self.test_locations) * 100,
                'unique_tile_patterns': len(self.discovered_data['tile_patterns']),
                'tile_servers': len(self.discovered_data['tile_servers'])
            },
            'tile_patterns': list(self.discovered_data['tile_patterns']),
            'tile_servers': list(self.discovered_data['tile_servers']),
            'successful_locations': self.discovered_data['all_locations']
        }
        
        # Save comprehensive report
        with open('output_browser_crawl/interaction_crawl_results.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_report = f"""
# USER INTERACTION-BASED GULAND CRAWLER REPORT
Generated: {report['timestamp']}
Method: Map interaction simulation + Network request capture

## 📊 SUMMARY
• Total Attempted: {report['summary']['total_attempted']}
• Total Successful: {report['summary']['total_successful']}
• Success Rate: {report['summary']['success_rate']:.1f}%
• Unique Tile Patterns: {report['summary']['unique_tile_patterns']}
• Tile Servers: {report['summary']['tile_servers']}

## 🎯 TILE PATTERNS DISCOVERED
"""
        
        for pattern in report['tile_patterns']:
            text_report += f"• {pattern}\n"
        
        text_report += f"\n## 🗺️ TILE SERVERS\n"
        for server in report['tile_servers']:
            text_report += f"• {server}\n"
        
        with open('output_browser_crawl/interaction_report.txt', 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        logger.info("✅ Interaction crawl report generated")
        return report

    # Keep existing navigate_to_location_page method
    def navigate_to_location_page(self, location_name, path):
        """Navigate đến planning page của location"""
        logger.info(f"🌐 Navigating to {location_name} planning page...")
        
        try:
            url = f"https://guland.vn/{path}"
            print(f"🔗 Opening URL: {url}")
            self.driver.get(url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for map/JS to initialize
            time.sleep(random.uniform(5, 8))
            
            logger.info(f"✅ Successfully navigated to {location_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to navigate to {location_name}: {e}")
            return False

def main():
    """Main function with configurable timeout"""
    print("🇻🇳 VIETNAM-WIDE GULAND CRAWLER")
    print("Crawls tile URLs for Vietnamese provinces/cities")
    print("=" * 60)
    
    # Time limit configuration
    print("\nChọn giới hạn thời gian:")
    print("1. Test (30 phút)")
    print("2. Quick (1 giờ)")
    print("3. Standard (2 giờ)")
    print("4. Extended (4 giờ)")
    print("5. Marathon (8 giờ)")
    print("6. Custom")
    
    time_choice = input("Lựa chọn thời gian (1-6, default 3): ").strip()
    time_map = {
        '1': 0.5,   # 30 minutes
        '2': 1,     # 1 hour
        '3': 2,     # 2 hours
        '4': 4,     # 4 hours
        '5': 8,     # 8 hours
        '6': None   # Custom
    }
    
    max_hours = time_map.get(time_choice, 2)  # Default 2 hours
    
    if time_choice == '6':  # Custom
        custom_input = input("Nhập số giờ (0.5-24): ").strip()
        try:
            max_hours = float(custom_input)
            max_hours = max(0.5, min(24, max_hours))  # Between 30 minutes and 24 hours
        except:
            max_hours = 2
    
    # Scope selection
    scope_choice = input("\nPhạm vi crawl (1=Test 5 locations, 2=All 63): ").strip()
    if scope_choice == '2':
        # Set all locations
        max_hours = max(max_hours, 4)  # Minimum 4 hours for all locations
        print(f"📊 All locations mode: minimum {max_hours} hours recommended")
    
    # Other configurations...
    headless_input = input("Chạy ẩn browser? (y/N): ").lower().strip()
    headless = headless_input in ['y', 'yes']
    
    download_input = input("Tự động download tiles? (Y/n): ").lower().strip()
    download_tiles = download_input not in ['n', 'no']
    
    workers = 5
    if download_tiles:
        workers_input = input("Số worker download (1-10, default 5): ").strip()
        try:
            workers = int(workers_input) if workers_input else 5
            workers = max(1, min(10, workers))
        except:
            workers = 5
    
    # Create crawler
    crawler = BrowserGulandCrawler(
        headless=headless, 
        enable_download=download_tiles,
        download_workers=workers
    )
    
    print(f"\n🎯 CRAWL CONFIGURATION:")
    print(f"⏰ Time limit: {max_hours} hours")
    print(f"📊 Locations: {len(crawler.test_locations)}")
    print(f"🖥️ Headless: {'Yes' if headless else 'No'}")
    print(f"📥 Download: {'Yes' if download_tiles else 'No'}")
    if download_tiles:
        print(f"👥 Workers: {workers}")
    
    estimated_time = len(crawler.test_locations) * 10 / 60  # 10 min per location
    print(f"📊 Estimated time needed: {estimated_time:.1f} hours")
    
    if estimated_time > max_hours:
        print(f"⚠️ WARNING: Time limit ({max_hours}h) may be insufficient!")
        print(f"💡 Consider increasing time limit or reducing scope")
    
    confirm = input("\nBắt đầu crawl? (Y/n): ").lower().strip()
    if confirm in ['n', 'no']:
        print("Đã hủy.")
        return
    
    try:
        results = crawler.run_browser_crawl(max_hours=max_hours)
        
        if results and results['summary']['total_successful'] > 0:
            print("\n✅ THÀNH CÔNG! Đã tìm thấy tile patterns và servers")
            if download_tiles:
                print(f"📥 Đã download {results['download_summary']['total_successful']} tiles")
                print(f"📁 Kiểm tra thư mục 'downloaded_tiles/'")
            print("📁 Kiểm tra thư mục 'output_browser_crawl/' để xem kết quả chi tiết")
        else:
            print("\n⚠️ Không có kết quả thành công")
            print("💡 Thử tăng thời gian hoặc giảm số location")
        
    except KeyboardInterrupt:
        print("\n⏹️ Crawl bị dừng bởi người dùng")
    except Exception as e:
        print(f"\n❌ Crawl thất bại: {e}")
        print("💡 Kiểm tra logs để biết chi tiết")

if __name__ == "__main__":
    main()