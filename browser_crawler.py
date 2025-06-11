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
        
        # Test locations
        self.test_locations = [
            ("Đà Nẵng", 16.0544563, 108.0717219, "soi-quy-hoach/da-nang"),
            ("Hà Nội", 21.0285, 105.8542, "soi-quy-hoach/ha-noi"),
            ("TP Hồ Chí Minh", 10.8231, 106.6297, "soi-quy-hoach/ho-chi-minh"),
            ("Cần Thơ", 10.0452, 105.7469, "soi-quy-hoach/can-tho"),
            ("Bình Dương", 11.3254, 106.4770, "soi-quy-hoach/binh-duong")
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
                
                # Set specific zoom level
                zoom_success = self.set_map_zoom(zoom)
                if not zoom_success:
                    self.simulate_zoom_interaction(zoom)
                
                # Wait for tiles to load at this zoom
                time.sleep(3)
                
                # Perform coverage at this zoom
                actual_duration = min(duration_per_zoom, 25)
                tiles_at_zoom = self.comprehensive_map_coverage(zoom, actual_duration)
                
                if tiles_at_zoom:
                    all_captured_tiles.extend(tiles_at_zoom)
                    logger.info(f"✅ Zoom {zoom}: Found {len(tiles_at_zoom)} tiles")
                    
                    # ===== NEW: DOWNLOAD TILES IMMEDIATELY =====
                    if self.enable_download and self.tile_downloader:
                        logger.info(f"📥 Downloading {len(tiles_at_zoom)} tiles from zoom {zoom}...")
                        download_results = self.tile_downloader.download_tiles_batch(
                            tiles_at_zoom, 
                            f"{location_name}_zoom_{zoom}"
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
    
    def set_map_zoom(self, target_zoom):
        """Set map to specific zoom level via JavaScript"""
        logger.info(f"🎯 Setting map zoom to {target_zoom}")
        
        try:
            js_script = f"""
            function setMapZoom() {{
                var mapInstances = [
                    window.map,
                    window.mapInstance, 
                    window.leafletMap,
                    document.querySelector('.leaflet-container')?._leaflet_map
                ];
                
                for (var i = 0; i < mapInstances.length; i++) {{
                    var mapInstance = mapInstances[i];
                    if (mapInstance && mapInstance.setZoom) {{
                        console.log('Setting zoom to {target_zoom} on instance', i);
                        mapInstance.setZoom({target_zoom});
                        return true;
                    }}
                }}
                
                return false;
            }}
            
            return setMapZoom();
            """
            
            result = self.driver.execute_script(js_script)
            if result:
                logger.info(f"✅ Successfully set zoom to {target_zoom}")
                return True
            else:
                logger.warning(f"⚠️ Could not set zoom via JavaScript")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error setting zoom: {e}")
            return False

    def simulate_zoom_interaction(self, target_zoom):
        """Simulate user zoom interaction to reach target zoom"""
        logger.info(f"🖱️ Simulating zoom interaction to level {target_zoom}")
        
        try:
            map_container = self.driver.find_element(By.CSS_SELECTOR, 
                '#map, .map-container, [class*="map"], canvas, .leaflet-container')
            
            actions = ActionChains(self.driver)
            actions.move_to_element(map_container).perform()
            
            # Start from a known zoom (usually around 13-15)
            current_estimated_zoom = 14
            
            if target_zoom > current_estimated_zoom:
                # Zoom in
                zoom_steps = target_zoom - current_estimated_zoom
                for _ in range(zoom_steps):
                    self.driver.execute_script("""
                        arguments[0].dispatchEvent(new WheelEvent('wheel', {
                            deltaY: -100,
                            bubbles: true,
                            cancelable: true
                        }));
                    """, map_container)
                    time.sleep(0.5)
            else:
                # Zoom out
                zoom_steps = current_estimated_zoom - target_zoom
                for _ in range(zoom_steps):
                    self.driver.execute_script("""
                        arguments[0].dispatchEvent(new WheelEvent('wheel', {
                            deltaY: 100,
                            bubbles: true,
                            cancelable: true
                        }));
                    """, map_container)
                    time.sleep(0.5)
            
            logger.info(f"✅ Zoom interaction completed for level {target_zoom}")
            
        except Exception as e:
            logger.error(f"❌ Error in zoom interaction: {e}")    

    def comprehensive_map_coverage(self, zoom_level, duration_seconds):
        """Comprehensive map coverage using grid pattern"""
        logger.info(f"🗺️ Starting comprehensive coverage at zoom {zoom_level} for {duration_seconds}s")
        
        try:
            # Find map container with better error handling
            map_container = None
            selectors_to_try = [
                '#map', '.map-container', '[class*="map"]', 
                'canvas', '.leaflet-container', '.leaflet-map-pane'
            ]
            
            for selector in selectors_to_try:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements and elements[0].is_displayed():
                        map_container = elements[0]
                        logger.info(f"🎯 Found map container: {selector}")
                        break
                except:
                    continue
            
            if not map_container:
                logger.error("❌ No valid map container found")
                return []
            
            # Scroll map into view
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", map_container)
            time.sleep(1)
            
            map_size = map_container.size
            logger.info(f"📏 Map size: {map_size['width']}x{map_size['height']}")
            
            # Validate map size
            if map_size['width'] < 200 or map_size['height'] < 200:
                logger.warning(f"⚠️ Map too small for coverage: {map_size}")
                return []
            
            actions = ActionChains(self.driver)
            
            # FIXED: Simpler coverage pattern to avoid out of bounds
            coverage_actions = [
                'center_pan_up', 'center_pan_down', 'center_pan_left', 'center_pan_right',
                'zoom_in_center', 'zoom_out_center', 'small_drag_center'
            ]
            
            start_time = time.time()
            action_count = 0
            max_actions = 20  # FIXED: Limit total actions to prevent infinite loop
            
            while (time.time() - start_time < duration_seconds and 
                action_count < max_actions):
                
                action_count += 1
                action_type = coverage_actions[(action_count - 1) % len(coverage_actions)]
                
                logger.info(f"📍 Coverage action {action_count}/{max_actions}: {action_type}")
                
                try:
                    if action_type == 'center_pan_up':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.5)
                        actions.click_and_hold().move_by_offset(0, -50).release().perform()
                        
                    elif action_type == 'center_pan_down':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.5)
                        actions.click_and_hold().move_by_offset(0, 50).release().perform()
                        
                    elif action_type == 'center_pan_left':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.5)
                        actions.click_and_hold().move_by_offset(-50, 0).release().perform()
                        
                    elif action_type == 'center_pan_right':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.5)
                        actions.click_and_hold().move_by_offset(50, 0).release().perform()
                        
                    elif action_type == 'zoom_in_center':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.2)
                        self.driver.execute_script("""
                            arguments[0].dispatchEvent(new WheelEvent('wheel', {
                                deltaY: -100,
                                bubbles: true,
                                cancelable: true
                            }));
                        """, map_container)
                        
                    elif action_type == 'zoom_out_center':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.2)
                        self.driver.execute_script("""
                            arguments[0].dispatchEvent(new WheelEvent('wheel', {
                                deltaY: 100,
                                bubbles: true,
                                cancelable: true
                            }));
                        """, map_container)
                        
                    elif action_type == 'small_drag_center':
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.5)
                        # Small random drag from center
                        offset_x = random.randint(-30, 30)
                        offset_y = random.randint(-30, 30)
                        actions.click_and_hold().move_by_offset(offset_x, offset_y).release().perform()
                    
                    # Wait between actions
                    time.sleep(random.uniform(2, 4))
                    
                except Exception as action_error:
                    logger.warning(f"⚠️ Action {action_type} failed: {action_error}")
                    # Continue to next action
                    continue
            
            logger.info(f"✅ Completed {action_count} coverage actions")
            
            # Get tiles captured during this coverage
            requests = self.get_network_requests()
            tile_urls, _ = self.extract_tile_urls(requests)
            
            # Filter tiles for current zoom level
            zoom_tiles = [tile for tile in tile_urls if tile['zoom'] == zoom_level]
            
            logger.info(f"🎯 Coverage result for zoom {zoom_level}: {len(zoom_tiles)} tiles")
            return zoom_tiles
            
        except Exception as e:
            logger.error(f"❌ Error in comprehensive coverage: {e}")
            return []

    def detect_city_boundaries(self, location_name):
        """Detect city boundaries for complete coverage"""
        logger.info(f"🌍 Detecting boundaries for {location_name}")
        
        try:
            # Get current map bounds via JavaScript
            js_script = """
            function getCityBounds() {
                var mapInstances = [
                    window.map,
                    window.mapInstance, 
                    window.leafletMap,
                    document.querySelector('.leaflet-container')?._leaflet_map
                ];
                
                for (var i = 0; i < mapInstances.length; i++) {
                    var mapInstance = mapInstances[i];
                    if (mapInstance && mapInstance.getBounds) {
                        var bounds = mapInstance.getBounds();
                        return {
                            northeast: {
                                lat: bounds.getNorthEast().lat,
                                lng: bounds.getNorthEast().lng
                            },
                            southwest: {
                                lat: bounds.getSouthWest().lat,
                                lng: bounds.getSouthWest().lng
                            },
                            center: {
                                lat: mapInstance.getCenter().lat,
                                lng: mapInstance.getCenter().lng
                            },
                            zoom: mapInstance.getZoom()
                        };
                    }
                }
                return null;
            }
            
            return getCityBounds();
            """
            
            bounds = self.driver.execute_script(js_script)
            
            if bounds:
                logger.info(f"🌍 City bounds detected:")
                logger.info(f"  NE: {bounds['northeast']}")
                logger.info(f"  SW: {bounds['southwest']}")
                logger.info(f"  Center: {bounds['center']}")
                logger.info(f"  Current zoom: {bounds['zoom']}")
                return bounds
            else:
                logger.warning("⚠️ Could not detect city bounds")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error detecting boundaries: {e}")
            return None

    def calculate_tile_coverage_needed(self, bounds, zoom_level):
        """Calculate how many tiles needed for full coverage"""
        if not bounds:
            return None
        
        import math
        
        def deg2num(lat_deg, lon_deg, zoom):
            lat_rad = math.radians(lat_deg)
            n = 2.0 ** zoom
            xtile = int((lon_deg + 180.0) / 360.0 * n)
            ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
            return (xtile, ytile)
        
        # Calculate tile coordinates for corners
        ne_tile = deg2num(bounds['northeast']['lat'], bounds['northeast']['lng'], zoom_level)
        sw_tile = deg2num(bounds['southwest']['lat'], bounds['southwest']['lng'], zoom_level)
        
        # Calculate coverage area
        tile_count_x = abs(ne_tile[0] - sw_tile[0]) + 1
        tile_count_y = abs(ne_tile[1] - sw_tile[1]) + 1
        total_tiles = tile_count_x * tile_count_y
        
        logger.info(f"📊 Zoom {zoom_level} coverage calculation:")
        logger.info(f"  X tiles: {tile_count_x} (from {sw_tile[0]} to {ne_tile[0]})")
        logger.info(f"  Y tiles: {tile_count_y} (from {ne_tile[1]} to {sw_tile[1]})")
        logger.info(f"  Total tiles needed: {total_tiles}")
        
        return {
            'zoom': zoom_level,
            'x_range': (sw_tile[0], ne_tile[0]),
            'y_range': (ne_tile[1], sw_tile[1]),
            'x_count': tile_count_x,
            'y_count': tile_count_y,
            'total_tiles': total_tiles
        }

    def get_safe_coordinates(self, map_container):
        """Get safe coordinates within map bounds"""
        try:
            map_size = map_container.size
            map_rect = map_container.rect
            
            # Ensure map is visible and has reasonable size
            if map_size['width'] < 100 or map_size['height'] < 100:
                logger.warning(f"⚠️ Map too small: {map_size['width']}x{map_size['height']}")
                return None, None
            
            # Calculate safe area with margins
            margin = 80
            safe_x = margin + (map_size['width'] - 2 * margin) // 2
            safe_y = margin + (map_size['height'] - 2 * margin) // 2
            
            # Validate coordinates are within bounds
            if safe_x < margin or safe_x > map_size['width'] - margin:
                safe_x = map_size['width'] // 2
            if safe_y < margin or safe_y > map_size['height'] - margin:
                safe_y = map_size['height'] // 2
                
            logger.info(f"📍 Safe coordinates: ({safe_x}, {safe_y}) within {map_size['width']}x{map_size['height']}")
            return safe_x, safe_y
            
        except Exception as e:
            logger.error(f"❌ Error getting safe coordinates: {e}")
            return None, None

    def safe_move_to_element_with_offset(self, actions, element, x, y):
        """Safely move to element with offset validation"""
        try:
            element_size = element.size
            
            # Validate offsets are within element bounds
            if x < 0 or x > element_size['width'] or y < 0 or y > element_size['height']:
                logger.warning(f"⚠️ Invalid offset ({x}, {y}) for element size {element_size}")
                # Use center of element as fallback
                x = element_size['width'] // 2
                y = element_size['height'] // 2
            
            actions.move_to_element_with_offset(element, x, y).perform()
            return True
            
        except Exception as e:
            logger.warning(f"⚠️ Safe move failed: {e}")
            # Fallback to center of element
            try:
                actions.move_to_element(element).perform()
                return True
            except:
                return False
    
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
            
            # Detect city boundaries
            bounds = self.detect_city_boundaries(location_name)
            
            # Initialize tracking data
            all_tiles_data = {
                'location_name': location_name,
                'coordinates': {'lat': lat, 'lng': lng},
                'path': path,
                'bounds': bounds,
                'zoom_levels': {},
                'total_tiles': 0,
                'tile_patterns': set(),
                'tile_servers': set(),
                'download_summary': {
                    'total_attempted': 0,
                    'total_successful': 0,
                    'total_failed': 0,
                    'total_bytes': 0
                },
                'timestamp': datetime.now().isoformat()
            }
            
            # Systematic zoom coverage with immediate download
            zoom_levels = list(range(10, 19))
            
            for zoom in zoom_levels:
                logger.info(f"\n🔍 ZOOM LEVEL {zoom} COVERAGE WITH IMMEDIATE DOWNLOAD")
                logger.info("-" * 50)
                
                # Clear network logs
                self.driver.get_log('performance')
                
                # Perform coverage at this zoom
                tiles_at_zoom = self.systematic_zoom_coverage(location_name, lat, lng, duration_per_zoom=45)
                
                if tiles_at_zoom:
                    # Process tiles for this zoom
                    zoom_patterns = set()
                    zoom_servers = set()
                    
                    for tile in tiles_at_zoom:
                        if tile['zoom'] == zoom:
                            zoom_patterns.add(tile['pattern'])
                            zoom_servers.add(self.extract_server_from_url(tile['url']))
                    
                    all_tiles_data['zoom_levels'][zoom] = {
                        'tiles': [t for t in tiles_at_zoom if t['zoom'] == zoom],
                        'tile_count': len([t for t in tiles_at_zoom if t['zoom'] == zoom]),
                        'patterns': list(zoom_patterns),
                        'servers': list(zoom_servers)
                    }
                    
                    all_tiles_data['tile_patterns'].update(zoom_patterns)
                    all_tiles_data['tile_servers'].update(zoom_servers)
                    all_tiles_data['total_tiles'] += len([t for t in tiles_at_zoom if t['zoom'] == zoom])
                    
                    logger.info(f"✅ Zoom {zoom}: {len([t for t in tiles_at_zoom if t['zoom'] == zoom])} tiles processed")
                else:
                    logger.warning(f"⚠️ Zoom {zoom}: No tiles captured")
            
            # Convert sets to lists for JSON serialization
            all_tiles_data['tile_patterns'] = list(all_tiles_data['tile_patterns'])
            all_tiles_data['tile_servers'] = list(all_tiles_data['tile_servers'])
            
            # Get final download stats from tile downloader
            if self.tile_downloader:
                all_tiles_data['download_summary'] = self.tile_downloader.download_stats.copy()
            
            # Save comprehensive results
            results_path = f"output_browser_crawl/full_coverage_{location_name.replace(' ', '_')}.json"
            with open(results_path, 'w', encoding='utf-8') as f:
                json.dump(all_tiles_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"💾 Full coverage results saved: {results_path}")
            
            if all_tiles_data['total_tiles'] > 0:
                self.discovered_data['success_count'] += 1
                logger.info(f"🎉 FULL COVERAGE SUCCESS: {all_tiles_data['total_tiles']} total tiles")
                return all_tiles_data
            else:
                logger.warning(f"⚠️ No tiles captured for {location_name}")
                self.discovered_data['failure_count'] += 1
                return None
                
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

    def simulate_map_interaction(self, location_name, duration_seconds=45):
        """Simulate realistic map interaction to trigger tile loading"""
        logger.info(f"🗺️ Simulating map interaction for {location_name} ({duration_seconds}s)...")
        
        try:
            # Wait for map to load completely
            time.sleep(8)
            
            # Ensure proper window size first
            self.driver.maximize_window()
            time.sleep(1)
            
            # Get viewport dimensions
            viewport_width = self.driver.execute_script("return window.innerWidth")
            viewport_height = self.driver.execute_script("return window.innerHeight")
            logger.info(f"📐 Viewport size: {viewport_width}x{viewport_height}")
            
            # Find map container
            map_container = None
            possible_selectors = [
                '#map',
                '.map-container',
                '[class*="map"]',
                '[id*="map"]',
                'canvas',
                '.leaflet-container',
                '.leaflet-map-pane'
            ]
            
            for selector in possible_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        map_container = elements[0]
                        logger.info(f"🎯 Found map container: {selector}")
                        
                        # Scroll element into view
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", map_container)
                        time.sleep(1)
                        break
                except:
                    continue
            
            if not map_container:
                logger.warning("⚠️ Could not find map container, using body")
                map_container = self.driver.find_element(By.TAG_NAME, "body")
            
            actions = ActionChains(self.driver)
            
            # Get map container size and position
            map_rect = map_container.rect
            map_size = map_container.size
            logger.info(f"📏 Map container size: {map_size['width']}x{map_size['height']}")
            logger.info(f"📍 Map position: x={map_rect['x']}, y={map_rect['y']}")
            
            # Calculate safe interaction area with margins
            margin = 50
            safe_width = max(100, map_size['width'] - 2 * margin)
            safe_height = max(100, map_size['height'] - 2 * margin)
            
            # Start time
            start_time = time.time()
            interaction_count = 0
            
            while time.time() - start_time < duration_seconds:
                interaction_count += 1
                logger.info(f"🎮 Interaction #{interaction_count}")
                
                # Random interaction type with better distribution
                interaction_type = random.choice([
                    'scroll_zoom_in', 'scroll_zoom_out', 'double_click', 
                    'click_zoom', 'pan_drag', 'mouse_wheel', 'keyboard_zoom'
                ])
                
                try:
                    if interaction_type == 'scroll_zoom_in':
                        logger.info("🔍 Scroll zoom in")
                        # Move to center of map
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.2)
                        self.driver.execute_script("""
                            arguments[0].dispatchEvent(new WheelEvent('wheel', {
                                deltaY: -100,
                                bubbles: true,
                                cancelable: true
                            }));
                        """, map_container)
                        
                    elif interaction_type == 'scroll_zoom_out':
                        logger.info("🔍 Scroll zoom out")
                        actions.move_to_element(map_container).perform()
                        time.sleep(0.2)
                        self.driver.execute_script("""
                            arguments[0].dispatchEvent(new WheelEvent('wheel', {
                                deltaY: 100,
                                bubbles: true,
                                cancelable: true
                            }));
                        """, map_container)
                        
                    elif interaction_type == 'pan_drag':
                        logger.info("↔️ Pan drag")
                        # Safe coordinates within map bounds
                        start_x = random.randint(margin, margin + safe_width // 2)
                        start_y = random.randint(margin, margin + safe_height // 2)
                        
                        # Limited offset to stay in bounds
                        max_offset = min(safe_width, safe_height) // 4
                        offset_x = random.randint(-max_offset, max_offset)
                        offset_y = random.randint(-max_offset, max_offset)
                        
                        logger.info(f"   Drag from offset ({start_x}, {start_y}) by ({offset_x}, {offset_y})")
                        
                        actions.move_to_element_with_offset(map_container, start_x, start_y)\
                            .click_and_hold()\
                            .move_by_offset(offset_x, offset_y)\
                            .release()\
                            .perform()
                        
                    elif interaction_type == 'double_click':
                        logger.info("👆 Double click zoom")
                        # Safe click position
                        click_x = random.randint(margin, margin + safe_width // 2)
                        click_y = random.randint(margin, margin + safe_height // 2)
                        
                        logger.info(f"   Double click at offset ({click_x}, {click_y})")
                        
                        actions.move_to_element_with_offset(map_container, click_x, click_y)\
                            .double_click()\
                            .perform()
                        
                    elif interaction_type == 'click_zoom':
                        logger.info("👆 Click and zoom")
                        actions.move_to_element(map_container).click().perform()
                        time.sleep(0.5)
                        
                        # Use JavaScript to simulate zoom
                        self.driver.execute_script("""
                            // Try common map zoom methods
                            if (window.map && window.map.zoomIn) {
                                window.map.zoomIn();
                            } else if (window.mapInstance && window.mapInstance.zoomIn) {
                                window.mapInstance.zoomIn();
                            }
                        """)
                        
                    elif interaction_type == 'keyboard_zoom':
                        logger.info("⌨️ Keyboard zoom")
                        actions.move_to_element(map_container).click().perform()
                        time.sleep(0.5)
                        
                        # Use arrow keys and other keys that actually exist
                        zoom_key = random.choice([
                            Keys.ARROW_UP, Keys.ARROW_DOWN, 
                            Keys.ARROW_LEFT, Keys.ARROW_RIGHT,
                            '+', '-', '='
                        ])
                        actions.send_keys(zoom_key).perform()
                        
                    elif interaction_type == 'mouse_wheel':
                        logger.info("🖱️ Mouse wheel")
                        # Safe wheel position
                        wheel_x = random.randint(margin, margin + safe_width // 2)
                        wheel_y = random.randint(margin, margin + safe_height // 2)
                        
                        logger.info(f"   Mouse wheel at offset ({wheel_x}, {wheel_y})")
                        
                        actions.move_to_element_with_offset(map_container, wheel_x, wheel_y).perform()
                        time.sleep(0.2)
                        
                        # Multiple wheel events
                        for _ in range(random.randint(1, 3)):
                            direction = random.choice([-1, 1])
                            self.driver.execute_script(f"""
                                arguments[0].dispatchEvent(new WheelEvent('wheel', {{
                                    deltaY: {direction * 120},
                                    bubbles: true,
                                    cancelable: true
                                }}));
                            """, map_container)
                            time.sleep(0.2)
                    
                    # Wait between interactions to let tiles load
                    wait_time = random.uniform(3, 7)
                    logger.info(f"⏳ Waiting {wait_time:.1f}s for tiles to load...")
                    time.sleep(wait_time)
                    
                except Exception as interaction_error:
                    logger.warning(f"⚠️ Interaction error: {interaction_error}")
                    # Continue with next interaction
                    continue
            
            logger.info(f"✅ Completed {interaction_count} map interactions")
            
            # Final wait for any pending tile requests
            logger.info("⏳ Final wait for tile loading...")
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"❌ Error during map interaction: {e}")
            
    def trigger_tile_loading(self):
        """Trigger tile loading using JavaScript"""
        logger.info("🚀 Triggering tile loading via JavaScript...")
        
        try:
            # Script to trigger map events that should load tiles
            js_script = """
            // Function to trigger map updates
            function triggerMapUpdates() {
                console.log('Triggering map tile loading...');
                
                // Method 1: Trigger resize events
                window.dispatchEvent(new Event('resize'));
                
                // Method 2: Try to access common map instances
                var mapInstances = [
                    window.map,
                    window.mapInstance, 
                    window.leafletMap,
                    window.L,
                    document.querySelector('.leaflet-container')?._leaflet_map
                ];
                
                mapInstances.forEach(function(mapInstance, index) {
                    if (mapInstance) {
                        console.log('Found map instance:', index);
                        
                        try {
                            // Try different zoom operations
                            if (mapInstance.setZoom) {
                                var currentZoom = mapInstance.getZoom ? mapInstance.getZoom() : 15;
                                mapInstance.setZoom(currentZoom + 1);
                                setTimeout(function() {
                                    mapInstance.setZoom(currentZoom);
                                }, 1000);
                            }
                            
                            // Try pan operations
                            if (mapInstance.panBy) {
                                mapInstance.panBy([50, 50]);
                                setTimeout(function() {
                                    mapInstance.panBy([-50, -50]);
                                }, 1000);
                            }
                            
                            // Try invalidate size
                            if (mapInstance.invalidateSize) {
                                mapInstance.invalidateSize();
                            }
                            
                        } catch (e) {
                            console.log('Error manipulating map instance:', e);
                        }
                    }
                });
                
                // Method 3: Force refresh of map layers
                var canvases = document.querySelectorAll('canvas');
                canvases.forEach(function(canvas) {
                    if (canvas.getContext) {
                        try {
                            var ctx = canvas.getContext('2d');
                            // Trigger redraw
                            canvas.style.transform = 'scale(1.001)';
                            setTimeout(function() {
                                canvas.style.transform = '';
                            }, 100);
                        } catch (e) {
                            console.log('Canvas manipulation error:', e);
                        }
                    }
                });
                
                return 'Tile loading triggered';
            }
            
            return triggerMapUpdates();
            """
            
            result = self.driver.execute_script(js_script)
            logger.info(f"🚀 JavaScript trigger result: {result}")
            
            # Wait for any triggered requests
            time.sleep(3)
            
        except Exception as e:
            logger.error(f"❌ Error triggering tile loading: {e}")
    
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
            
            # Trigger tile loading via JavaScript first
            self.trigger_tile_loading()
            
            # Simulate realistic map interaction
            self.simulate_map_interaction(location_name, duration_seconds=60)
            
            # Trigger again after interactions
            self.trigger_tile_loading()
            
            # Get all network requests
            all_requests = self.get_network_requests()
            logger.info(f"📡 Captured {len(all_requests)} network requests")
            
            # Extract tile URLs
            tile_urls, tile_patterns = self.extract_tile_urls(all_requests)
            logger.info(f"🎯 Found {len(tile_urls)} tile requests")
            logger.info(f"🎯 Found {len(tile_patterns)} unique tile patterns")
            
            # ====== NEW: DOWNLOAD TILES ======
            download_results = []
            if tile_urls and self.enable_download and self.tile_downloader:
                logger.info(f"📥 Starting tile download for {location_name}...")
                download_results = self.tile_downloader.download_tiles_batch(tile_urls, location_name)
                
                # Generate download report
                self.tile_downloader.generate_download_report(location_name, download_results)
            
            # Take final screenshot
            final_screenshot = f"output_browser_crawl/screenshots/{location_name.replace(' ', '_')}_final.png"
            self.driver.save_screenshot(final_screenshot)
            logger.info(f"📸 Final screenshot: {final_screenshot}")
            
            if tile_urls:
                # Save network log
                network_log_path = f"output_browser_crawl/network_logs/{location_name.replace(' ', '_')}_network.json"
                with open(network_log_path, 'w', encoding='utf-8') as f:
                    json.dump({
                        'location': location_name,
                        'total_requests': len(all_requests),
                        'tile_requests': len(tile_urls),
                        'tile_patterns': list(tile_patterns),
                        'all_requests': all_requests,
                        'tile_urls': tile_urls,
                        'download_summary': {
                            'total_attempted': len(download_results),
                            'successful': len([r for r in download_results if r['success']]),
                            'failed': len([r for r in download_results if not r['success']])
                        } if download_results else None
                    }, f, indent=2, ensure_ascii=False)
                
                logger.info(f"💾 Network log saved: {network_log_path}")
                
                # Create location info
                location_info = {
                    'location_name': location_name,
                    'coordinates': {'lat': lat, 'lng': lng},
                    'path': path,
                    'tile_patterns': list(tile_patterns),
                    'tile_count': len(tile_urls),
                    'tile_servers': list(set([self.extract_server_from_url(url['url']) for url in tile_urls])),
                    'zoom_levels': list(set([url['zoom'] for url in tile_urls])),
                    'timestamp': datetime.now().isoformat(),
                    'download_stats': {
                        'attempted': len(download_results),
                        'successful': len([r for r in download_results if r['success']]),
                        'failed': len([r for r in download_results if not r['success']]),
                        'total_size': sum(r.get('size', 0) for r in download_results if r['success'])
                    } if download_results else None
                }
                
                # Update discovered data
                self.discovered_data['tile_patterns'].update(tile_patterns)
                for server in location_info['tile_servers']:
                    self.discovered_data['tile_servers'].add(server)
                
                self.discovered_data['success_count'] += 1
                logger.info(f"✅ Successfully crawled {location_name}: {len(tile_patterns)} patterns")
                
                # Print discovered patterns
                logger.info("🎯 DISCOVERED TILE PATTERNS:")
                for pattern in tile_patterns:
                    logger.info(f"  • {pattern}")
                
                return location_info
            else:
                logger.warning(f"⚠️ No tile URLs found for {location_name}")
                self.discovered_data['failure_count'] += 1
                return None
                
        except Exception as e:
            logger.error(f"❌ Error crawling {location_name}: {e}")
            self.discovered_data['failure_count'] += 1
            return None

    def extract_server_from_url(self, url):
        """Extract server base URL from tile URL"""
        from urllib.parse import urlparse
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"
    
    def run_browser_crawl(self):
        """Run complete browser-based crawl with full coverage - TIMEOUT PROTECTED"""
        logger.info("🚀 STARTING FULL COVERAGE CRAWL (ZOOM 10-18)")
        logger.info("=" * 70)
        logger.info("🎯 Systematic coverage of all zoom levels 10-18")
        logger.info("=" * 70)
        
        crawl_start_time = time.time()
        max_crawl_time = 3600  # 1 hour total limit
        
        try:
            if not self.setup_driver():
                return None
            
            logger.info("🌐 Warming up session...")
            self.driver.get("https://guland.vn/")
            time.sleep(random.uniform(3, 6))
            
            for i, (location_name, lat, lng, path) in enumerate(self.test_locations, 1):
                # Check global timeout
                if time.time() - crawl_start_time > max_crawl_time:
                    logger.warning(f"⏰ Global timeout reached, stopping crawl")
                    break
                    
                logger.info(f"\n🌍 PROCESSING {i}/{len(self.test_locations)}: {location_name}")
                logger.info("=" * 60)
                
                location_start_time = time.time()
                max_location_time = 600  # 10 minutes per location
                
                location_info = self.crawl_location_with_full_coverage(location_name, lat, lng, path)
                
                location_elapsed = time.time() - location_start_time
                logger.info(f"⏱️ {location_name} processed in {location_elapsed:.1f}s")
                
                if location_info:
                    self.discovered_data['all_locations'].append(location_info)
                    logger.info(f"✅ {location_name}: {location_info['total_tiles']} tiles captured")
                    
                    # Print quick summary
                    for zoom in range(10, 19):
                        if zoom in location_info['zoom_levels']:
                            count = location_info['zoom_levels'][zoom]['tile_count']
                            logger.info(f"   Zoom {zoom}: {count} tiles")
                else:
                    logger.warning(f"⚠️ Failed to process {location_name}")
                
                # Delay between cities
                if i < len(self.test_locations):
                    # Check if we have time for next location
                    remaining_time = max_crawl_time - (time.time() - crawl_start_time)
                    if remaining_time < 300:  # Less than 5 minutes remaining
                        logger.warning(f"⏰ Not enough time for next location, stopping")
                        break
                        
                    delay = min(30, remaining_time // 10)  # Adaptive delay
                    logger.info(f"⏳ Waiting {delay:.1f}s before next location...")
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
            logger.info(f"⏱️ Total crawl time: {total_elapsed:.1f}s")
    
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
    """Main function with download options"""
    print("🎮 GULAND CRAWLER WITH TILE DOWNLOAD")
    print("Crawls tile URLs and downloads them automatically")
    print("=" * 60)
    
    # Ask user preferences
    headless_input = input("Run in headless mode? (y/N): ").lower().strip()
    headless = headless_input in ['y', 'yes']
    
    download_input = input("Download tiles automatically? (Y/n): ").lower().strip()
    download_tiles = download_input not in ['n', 'no']
    
    workers = 5
    if download_tiles:
        workers_input = input("Number of download workers (1-10, default 5): ").strip()
        try:
            workers = int(workers_input) if workers_input else 5
            workers = max(1, min(10, workers))
        except:
            workers = 5
    
    crawler = BrowserGulandCrawler(
        headless=headless, 
        enable_download=download_tiles,
        download_workers=workers
    )
    
    if download_tiles:
        print(f"📥 Download enabled with {workers} parallel workers")
    else:
        print("⏭️ Download disabled - only URLs will be collected")
    
    try:
        results = crawler.run_browser_crawl()
        
        if results and results['summary']['total_successful'] > 0:
            print("\n✅ SUCCESS! Found tile patterns and servers")
            if download_tiles:
                print(f"📥 Downloaded {results['download_summary']['total_successful']} tiles")
                print(f"📁 Check 'downloaded_tiles/' for downloaded tiles")
            print("📁 Check 'output_browser_crawl/' for detailed results")
        else:
            print("\n⚠️ No successful results")
            print("💡 Try running in non-headless mode for debugging")
        
    except KeyboardInterrupt:
        print("\n⏹️ Crawl interrupted by user")
    except Exception as e:
        print(f"\n❌ Crawl failed: {e}")
        print("💡 Check logs for details")

if __name__ == "__main__":
    main()