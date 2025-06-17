#!/usr/bin/env python3
"""
Pattern-Based Tile Crawler for Guland
Crawls tiles using discovered URL patterns for verification

Author: AI Assistant
Version: 1.0
"""
import os
import json
import time
import logging
import requests
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import math
from tile_downloader import GulandTileDownloader

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pattern_crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PatternBasedTileCrawler:
    def __init__(self, max_workers=10, timeout=30, user_agent=None, enable_download=True):
        self.max_workers = max_workers
        self.timeout = timeout
        self.session = requests.Session()
        
        # Set realistic headers
        self.session.headers.update({
            'User-Agent': user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'image',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'cross-site'
        })
        
        # Create output directories
        self.base_output_dir = 'pattern_verification'
        os.makedirs(self.base_output_dir, exist_ok=True)
        os.makedirs(f'{self.base_output_dir}/tiles', exist_ok=True)
        os.makedirs(f'{self.base_output_dir}/reports', exist_ok=True)
        
        # Statistics
        self.stats = {
            'total_attempted': 0,
            'total_successful': 0,
            'total_failed': 0,
            'total_bytes': 0,
            'patterns_tested': 0,
            'valid_patterns': 0
        }
        self.stats_lock = threading.Lock()
        
        # Initialize tile downloader
        if enable_download:
            self.tile_downloader = GulandTileDownloader(
                base_download_dir='pattern_verification_downloads',
                max_workers=max_workers,
                timeout=timeout
            )
        else:
            self.tile_downloader = None
            
        logger.info(f"🔍 Pattern-based crawler initialized")
        logger.info(f"👥 Workers: {self.max_workers}, Timeout: {self.timeout}s")
        logger.info(f"📥 Download enabled: {enable_download}")

    def load_patterns_from_browser_results(self, city_name=None):
        """Load patterns from specific city or all cities"""
        if city_name:
            # Load patterns cho thành phố cụ thể
            city_file = f'output_browser_crawl/full_coverage_{city_name}.json'
            try:
                with open(city_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                return data.get('tile_patterns', [])
            except FileNotFoundError:
                logger.warning(f"❌ No results for {city_name}")
                return []
        else:
            # Load từ final report (tất cả patterns)
            return self.load_patterns_from_final_report()

    def deg2num(self, lat_deg, lon_deg, zoom):
        """Convert lat/lon to tile coordinates"""
        lat_rad = math.radians(lat_deg)
        n = 2.0 ** zoom
        x = int((lon_deg + 180.0) / 360.0 * n)
        y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
        return (x, y)

    def generate_tile_coordinates_for_vietnam(self, zoom_level):
        """Generate tile coordinates covering Vietnam"""
        # Vietnam boundaries (approximate)
        vietnam_bounds = {
            'north': 23.393,   # Cao Bằng
            'south': 8.560,    # Cà Mau
            'east': 109.464,   # Quảng Ninh
            'west': 102.170    # Lai Châu
        }
        
        # Convert to tile coordinates - FIX: Handle Y coordinates correctly
        # North boundary has smaller Y value, South boundary has larger Y value
        x_west, y_north = self.deg2num(vietnam_bounds['north'], vietnam_bounds['west'], zoom_level)
        x_east, y_south = self.deg2num(vietnam_bounds['south'], vietnam_bounds['east'], zoom_level)
        
        # Determine min/max values correctly
        x_min = min(x_west, x_east)
        x_max = max(x_west, x_east)
        y_min = min(y_north, y_south)  # FIX: Use min for y_min
        y_max = max(y_north, y_south)  # FIX: Use max for y_max
        
        # Add some padding
        padding = max(1, int(2**(zoom_level-10)))  # More padding at higher zooms
        x_min -= padding
        x_max += padding
        y_min -= padding
        y_max += padding
        
        # Ensure non-negative
        x_min = max(0, x_min)
        y_min = max(0, y_min)
        
        # Validate ranges
        if x_min >= x_max or y_min >= y_max:
            logger.error(f"❌ Invalid tile ranges: X({x_min}-{x_max}), Y({y_min}-{y_max})")
            # Fallback to minimal valid range
            x_min, x_max = x_min, x_min + 10
            y_min, y_max = y_min, y_min + 10
    
        logger.info(f"🗺️ Zoom {zoom_level} coverage:")
        logger.info(f"  X range: {x_min} to {x_max} ({x_max-x_min+1} tiles)")
        logger.info(f"  Y range: {y_min} to {y_max} ({y_max-y_min+1} tiles)")
        logger.info(f"  Total tiles: {(x_max-x_min+1) * (y_max-y_min+1)}")
        
        return {
            'x_min': x_min, 'x_max': x_max,
            'y_min': y_min, 'y_max': y_max,
            'total_tiles': (x_max-x_min+1) * (y_max-y_min+1)
        }

    def generate_sample_urls_from_pattern(self, pattern, zoom_levels, sample_size=50):
        """Generate sample URLs from a pattern for testing"""
        urls = []
        
        for zoom in zoom_levels:
            # Get tile coverage for this zoom
            coverage = self.generate_tile_coordinates_for_vietnam(zoom)
            
            # Validate coverage before proceeding
            if coverage['x_min'] >= coverage['x_max'] or coverage['y_min'] >= coverage['y_max']:
                logger.warning(f"⚠️ Invalid coverage for zoom {zoom}, skipping")
                continue
            
            # Generate sample coordinates
            import random
            samples_per_zoom = max(5, sample_size // len(zoom_levels))
            
            for _ in range(samples_per_zoom):
                # FIX: Ensure valid ranges for random.randint
                try:
                    x = random.randint(coverage['x_min'], coverage['x_max'])
                    y = random.randint(coverage['y_min'], coverage['y_max'])
                    
                    # Replace placeholders in pattern
                    url = pattern.replace('{z}', str(zoom))
                    url = url.replace('{x}', str(x))
                    url = url.replace('{y}', str(y))
                    
                    urls.append({
                        'url': url,
                        'zoom': zoom,
                        'x': x,
                        'y': y,
                        'pattern': pattern
                    })
                except ValueError as e:
                    logger.warning(f"⚠️ Invalid range for zoom {zoom}: {e}")
                    continue
    
        return urls

    def test_single_tile_url(self, tile_info):
        """Test if a single tile URL is valid"""
        url = tile_info['url']
        
        try:
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                # Check if it's actually an image
                content_type = response.headers.get('content-type', '').lower()
                if any(img_type in content_type for img_type in ['image/', 'application/octet-stream']):
                    size = len(response.content)
                    
                    # Additional validation - check image size
                    if size > 100:  # Minimum size for valid tile
                        with self.stats_lock:
                            self.stats['total_successful'] += 1
                            self.stats['total_bytes'] += size
                        
                        return {
                            'success': True,
                            'status_code': response.status_code,
                            'size': size,
                            'content_type': content_type,
                            'tile_info': tile_info
                        }
                
                # If we get here, it's not a valid tile
                with self.stats_lock:
                    self.stats['total_failed'] += 1
                
                return {
                    'success': False,
                    'status_code': response.status_code,
                    'reason': f'Invalid content type: {content_type}',
                    'tile_info': tile_info
                }
            else:
                # Handle non-200 status codes
                with self.stats_lock:
                    self.stats['total_failed'] += 1
                
                return {
                    'success': False,
                    'status_code': response.status_code,
                    'reason': f'HTTP {response.status_code}',
                    'tile_info': tile_info
                }
                
        except requests.exceptions.Timeout:
            with self.stats_lock:
                self.stats['total_failed'] += 1
            return {
                'success': False,
                'reason': 'Timeout',
                'tile_info': tile_info
            }
        except requests.exceptions.RequestException as e:
            with self.stats_lock:
                self.stats['total_failed'] += 1
            return {
                'success': False,
                'reason': str(e),
                'tile_info': tile_info
            }
        except Exception as e:
            # Catch any other unexpected errors
            with self.stats_lock:
                self.stats['total_failed'] += 1
            return {
                'success': False,
                'reason': f'Unexpected error: {str(e)}',
                'tile_info': tile_info
            }

    def test_pattern_validity(self, pattern, zoom_levels=[12, 14, 16], sample_size=20):
        """Test if a pattern is valid by sampling some tiles"""
        logger.info(f"🧪 Testing pattern: {pattern}")
        
        # Generate sample URLs
        sample_urls = self.generate_sample_urls_from_pattern(pattern, zoom_levels, sample_size)
        
        with self.stats_lock:
            self.stats['total_attempted'] += len(sample_urls)
            self.stats['patterns_tested'] += 1
        
        # Test URLs in parallel
        valid_tiles = []
        failed_tiles = []
        
        with ThreadPoolExecutor(max_workers=min(5, self.max_workers)) as executor:
            future_to_tile = {
                executor.submit(self.test_single_tile_url, tile_info): tile_info 
                for tile_info in sample_urls
            }
            
            for future in as_completed(future_to_tile):
                result = future.result()
                if result['success']:
                    valid_tiles.append(result)
                else:
                    failed_tiles.append(result)
        
        success_rate = len(valid_tiles) / len(sample_urls) * 100 if sample_urls else 0
        
        logger.info(f"📊 Pattern test results:")
        logger.info(f"  Valid: {len(valid_tiles)}/{len(sample_urls)} ({success_rate:.1f}%)")
        
        if success_rate > 50:  # Consider pattern valid if >50% success
            with self.stats_lock:
                self.stats['valid_patterns'] += 1
            logger.info(f"✅ Pattern is VALID")
            return True, {
                'pattern': pattern,
                'valid': True,
                'success_rate': success_rate,
                'valid_tiles': len(valid_tiles),
                'total_tested': len(sample_urls),
                'sample_tiles': valid_tiles[:3]  # Keep some samples
            }
        else:
            logger.info(f"❌ Pattern is INVALID")
            return False, {
                'pattern': pattern,
                'valid': False,
                'success_rate': success_rate,
                'valid_tiles': len(valid_tiles),
                'total_tested': len(sample_urls),
                'errors': [f['reason'] for f in failed_tiles[:5]]  # Sample errors
            }

    def comprehensive_pattern_crawl(self, pattern, location_name, zoom_levels, tile_limit=1000, download_tiles=True):
        """Comprehensively crawl tiles for a valid pattern with optional download"""
        logger.info(f"🚀 Starting comprehensive crawl for pattern")
        logger.info(f"📍 Location: {location_name}")
        logger.info(f"🎯 Zoom levels: {zoom_levels}")
        logger.info(f"🔢 Tile limit: {tile_limit}")
        logger.info(f"📥 Download enabled: {download_tiles and self.tile_downloader is not None}")
        
        all_tiles = []
        processed_count = 0
        
        for zoom in zoom_levels:
            if processed_count >= tile_limit:
                logger.info(f"🛑 Reached tile limit ({tile_limit})")
                break
                
            logger.info(f"🔍 Processing zoom level {zoom}")
            
            # Get coverage for this zoom
            coverage = self.generate_tile_coordinates_for_vietnam(zoom)
            zoom_tile_limit = min(200, tile_limit - processed_count)
            
            # Generate strategic tile coordinates
            tile_coords = self.generate_strategic_coordinates(coverage, zoom_tile_limit)
            
            # Generate URLs for this zoom
            zoom_urls = []
            for x, y in tile_coords:
                url = pattern.replace('{z}', str(zoom))
                url = url.replace('{x}', str(x))
                url = url.replace('{y}', str(y))
                
                zoom_urls.append({
                    'url': url,
                    'zoom': zoom,
                    'x': x,
                    'y': y,
                    'pattern': pattern
                })
            
            # Process tiles for this zoom
            if download_tiles and self.tile_downloader:
                # Full download mode
                zoom_results = self.download_tiles_batch(zoom_urls, location_name, zoom)
            else:
                # Test-only mode
                zoom_results = []
                for tile_info in zoom_urls:
                    test_result = self.test_single_tile_url(tile_info)
                    zoom_results.append(test_result)
        
            all_tiles.extend(zoom_results)
            
            processed_count += len(zoom_results)
            successful_count = len([r for r in zoom_results if r['success']])
            
            logger.info(f"📊 Zoom {zoom}: {successful_count}/{len(zoom_results)} tiles processed")
        
        return all_tiles

    def generate_strategic_coordinates(self, coverage, limit):
        """Generate strategic tile coordinates for better coverage"""
        coordinates = []
        
        # Validate coverage
        if coverage['x_min'] >= coverage['x_max'] or coverage['y_min'] >= coverage['y_max']:
            logger.warning("⚠️ Invalid coverage for strategic coordinates")
            return [(coverage['x_min'], coverage['y_min'])]  # Return minimal valid coordinate
        
        x_range = coverage['x_max'] - coverage['x_min'] + 1
        y_range = coverage['y_max'] - coverage['y_min'] + 1
        
        # Calculate grid size for strategic sampling
        grid_size = max(1, int(math.sqrt(x_range * y_range / limit)))
        
        for x in range(coverage['x_min'], coverage['x_max'] + 1, grid_size):
            for y in range(coverage['y_min'], coverage['y_max'] + 1, grid_size):
                coordinates.append((x, y))
                if len(coordinates) >= limit:
                    return coordinates
        
        # If no coordinates generated, return at least one
        if not coordinates:
            coordinates = [(coverage['x_min'], coverage['y_min'])]
        
        return coordinates

    def download_tiles_batch(self, tile_urls, location_name, zoom_level):
        """Download a batch of tiles using enhanced downloader"""
        if not tile_urls:
            return []
        
        if not self.tile_downloader:
            # Test-only mode
            results = []
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_tile = {
                    executor.submit(self.test_single_tile_url, tile_info): tile_info 
                    for tile_info in tile_urls
                }
                
                for future in as_completed(future_to_tile):
                    result = future.result()
                    results.append(result)
            
            return results
        
        logger.info(f"📥 Downloading {len(tile_urls)} tiles for {location_name} (zoom {zoom_level})")
        
        # Add format detection to tile_info
        enhanced_tile_urls = []
        for tile_info in tile_urls:
            # Detect format from URL
            url = tile_info['url']
            if '.png' in url:
                tile_info['format'] = 'png'
            elif '.jpg' in url or '.jpeg' in url:
                tile_info['format'] = 'jpg'
            elif '.webp' in url:
                tile_info['format'] = 'webp'
            else:
                tile_info['format'] = 'png'  # default
            
            enhanced_tile_urls.append(tile_info)
        
        # Use enhanced downloader
        results = self.tile_downloader.download_tiles_batch(enhanced_tile_urls, location_name)
        
        # Update stats
        successful = len([r for r in results if r['success']])
        failed = len([r for r in results if not r['success']])
        
        with self.stats_lock:
            self.stats['total_attempted'] += len(tile_urls)
            self.stats['total_successful'] += successful
            self.stats['total_failed'] += failed
            self.stats['total_bytes'] += sum(r.get('size', 0) for r in results if r['success'])
        
        return results

    def download_single_tile(self, tile_info, output_dir):
        """Download a single tile using enhanced downloader"""
        
        if not self.tile_downloader:
            # If downloader disabled, just test the URL
            return self.test_single_tile_url(tile_info)
        
        # Use GulandTileDownloader for actual download
        location_name = output_dir.split('/')[-2] if '/' in output_dir else 'unknown'
        
        result = self.tile_downloader.download_single_tile(tile_info, location_name)
        
        # Convert result format to match existing expectations
        if result['success']:
            return {
                'success': True,
                'tile_info': tile_info,
                'filepath': result['filepath'],
                'size': result['size'],
                'tile_type': result['tile_type'],
                'status_code': 200,
                'content_type': f"image/{tile_info.get('format', 'png')}"
            }
        else:
            with self.stats_lock:
                self.stats['total_failed'] += 1
        
        return {
            'success': False,
            'tile_info': tile_info,
            'reason': result['error']
        }

    def run_city_verification(self, test_locations):
        """Run verification theo từng thành phố"""
        logger.info("🔍 STARTING CITY-BY-CITY VERIFICATION")
        
        verification_results = []
        
        for city_name, lat, lng, path in test_locations:
            logger.info(f"\n🏙️ Verifying: {city_name}")
            
            # Load patterns cho city này từ browser results  
            city_patterns = self.load_patterns_from_browser_results(city_name)
            
            if not city_patterns:
                logger.warning(f"⚠️ No patterns found for {city_name}")
                continue
            
            # Verify patterns cho city này
            verified = self.verify_city_patterns(
                city_name, lat, lng, city_patterns, [10, 11, 12, 13, 14, 15, 16, 18]
            )
            
            # Comprehensive crawl cho verified patterns
            if verified:
                city_tiles = self.crawl_city_with_verified_patterns(
                    city_name, lat, lng, verified
                )
                
                verification_results.append({
                    'city': city_name,
                    'patterns': verified,
                    'tiles': city_tiles,
                    'comparison_with_browser': self.compare_with_browser_results(city_name, city_tiles)
                })
        
        return verification_results

    def test_pattern_for_city(self, pattern, city_coverage):
        """Test a pattern for a specific city using its tile coverage"""
        logger.info(f"🔍 Testing pattern for city coverage: {pattern}")
        
        # Generate sample URLs based on city coverage
        sample_urls = []
        for zoom, coverage in city_coverage.items():
            # Use center of coverage for sampling
            x_center = (coverage['x_min'] + coverage['x_max']) // 2
            y_center = (coverage['y_min'] + coverage['y_max']) // 2
            
            url = pattern.replace('{z}', str(zoom)).replace('{x}', str(x_center)).replace('{y}', str(y_center))
            sample_urls.append({
                'url': url,
                'zoom': zoom,
                'x': x_center,
                'y': y_center,
                'pattern': pattern
            })
        
        # Test URLs in parallel
        valid_tiles = []
        failed_tiles = []
        
        with ThreadPoolExecutor(max_workers=min(5, self.max_workers)) as executor:
            future_to_tile = {
                executor.submit(self.test_single_tile_url, tile_info): tile_info 
                for tile_info in sample_urls
            }
            
            for future in as_completed(future_to_tile):
                result = future.result()
                if result['success']:
                    valid_tiles.append(result)
                else:
                    failed_tiles.append(result)
        
        success_rate = len(valid_tiles) / len(sample_urls) * 100 if sample_urls else 0
        
        logger.info(f"📊 City pattern test results:")
        logger.info(f"  Valid: {len(valid_tiles)}/{len(sample_urls)} ({success_rate:.1f}%)")
        
        return success_rate

    def crawl_city_with_verified_patterns(self, city_name, lat, lng, verified_patterns):
        """Crawl tiles for a city using verified patterns"""
        logger.info(f"🚀 Starting city crawl for: {city_name}")
        
        all_tiles = []
        
        for pattern_info in verified_patterns:
            pattern = pattern_info['pattern']
            logger.info(f"🔍 Crawling with verified pattern: {pattern}")
            
            # Get tile coverage for this city
            city_coverage = self.generate_city_tile_coverage(lat, lng, [10, 11, 12, 13, 14, 15, 16, 18])
            
            # Comprehensive crawl for this pattern
            tiles = self.comprehensive_pattern_crawl(
                pattern, 
                city_name, 
                [10, 11, 12, 13, 14, 15, 16, 18],
                tile_limit=500  # Limit per pattern
            )
            
            # FIX: Extract tile_info for easier processing
            for tile in tiles:
                if tile.get('success') and 'tile_info' in tile:
                    all_tiles.append(tile['tile_info'])  # Extract tile_info only
    
        return all_tiles

    def compare_with_browser_results(self, city_name, crawled_tiles):
        """Compare crawled tiles with browser results for the city"""
        logger.info(f"📊 Comparing crawled tiles with browser results for {city_name}")
        
        # Load browser results
        city_file = f'output_browser_crawl/full_coverage_{city_name}.json'
        try:
            with open(city_file, 'r', encoding='utf-8') as f:
                browser_data = json.load(f)
        except FileNotFoundError:
            logger.warning(f"❌ No browser results found for {city_name}")
            return None
        
        browser_tiles = set()
        for pattern in browser_data.get('tile_patterns', []):
            # Generate expected tiles for this pattern
            expected_tiles = self.generate_sample_urls_from_pattern(pattern, [10, 11, 12, 13, 14, 15, 16, 18], sample_size=100)
            browser_tiles.update({(tile['x'], tile['y'], tile['zoom']) for tile in expected_tiles})
        
        crawled_tiles_set = {(tile['x'], tile['y'], tile['zoom']) for tile in crawled_tiles}
        
        # Compare
        matched_tiles = crawled_tiles_set.intersection(browser_tiles)
        unmatched_crawled = crawled_tiles_set.difference(browser_tiles)
        unmatched_browser = browser_tiles.difference(crawled_tiles_set)
        
        logger.info(f"  Matched tiles: {len(matched_tiles)}")
        logger.info(f"  Unmatched crawled tiles: {len(unmatched_crawled)}")
        logger.info(f"  Unmatched browser tiles: {len(unmatched_browser)}")
        
        return {
            'matched': len(matched_tiles),
            'unmatched_crawled': len(unmatched_crawled),
            'unmatched_browser': len(unmatched_browser),
            'details': {
                'matched': list(matched_tiles)[:5],
                'unmatched_crawled': list(unmatched_crawled)[:5],
                'unmatched_browser': list(unmatched_browser)[:5]
            }
        }

    def run_pattern_verification(self, patterns=None, zoom_levels=[10, 11, 12, 13, 14, 15, 16, 18]):
        """Run complete pattern verification and crawling"""
        logger.info("🔍 STARTING PATTERN-BASED VERIFICATION CRAWL")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        # Load patterns if not provided
        if patterns is None:
            patterns = self.load_patterns_from_browser_results()
        
        if not patterns:
            logger.error("❌ No patterns to test")
            return None
        
        logger.info(f"🧪 Testing {len(patterns)} patterns")
        
        # Phase 1: Test pattern validity
        valid_patterns = []
        pattern_results = []
        
        for i, pattern in enumerate(patterns, 1):
            logger.info(f"\n🧪 Testing pattern {i}/{len(patterns)}")
            
            is_valid, result = self.test_pattern_validity(pattern, zoom_levels[:3])  # Test with fewer zooms
            pattern_results.append(result)
            
            if is_valid:
                valid_patterns.append(pattern)
            
            # Small delay between pattern tests
            time.sleep(1)
        
        logger.info(f"\n✅ Pattern testing complete: {len(valid_patterns)}/{len(patterns)} valid")
        
        # Phase 2: Comprehensive crawl of valid patterns
        crawl_results = []
        
        for i, pattern in enumerate(valid_patterns, 1):
            logger.info(f"\n🚀 Comprehensive crawl {i}/{len(valid_patterns)}")
            
            # Extract a reasonable location name from pattern
            from urllib.parse import urlparse
            parsed = urlparse(pattern.replace('{z}', '1').replace('{x}', '1').replace('{y}', '1'))
            location_name = f"pattern_{i}_{parsed.netloc.replace('.', '_')}"
            
            tiles = self.comprehensive_pattern_crawl(
                pattern, 
                location_name, 
                zoom_levels,
                tile_limit=500  # Limit per pattern
            )
            
            crawl_results.append({
                'pattern': pattern,
                'location_name': location_name,
                'tiles': tiles,
                'successful_tiles': len([t for t in tiles if t['success']])
            })
        
        # Generate final report
        report = self.generate_verification_report(pattern_results, crawl_results, start_time)
        return report

    def generate_verification_report(self, pattern_results, crawl_results, start_time):
        """Generate comprehensive verification report"""
        elapsed_time = time.time() - start_time
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'crawler': 'Pattern-Based Verification Crawler v1.0',
            'method': 'URL pattern testing and systematic tile crawling',
            'execution_time_seconds': elapsed_time,
            'statistics': self.stats.copy(),
            'pattern_test_results': pattern_results,
            'crawl_results': crawl_results,
            'summary': {
                'patterns_tested': len(pattern_results),
                'valid_patterns': len([p for p in pattern_results if p['valid']]),
                'total_tiles_crawled': sum(len(c['tiles']) for c in crawl_results),
                'successful_downloads': sum(c['successful_tiles'] for c in crawl_results),
                'total_size_mb': self.stats['total_bytes'] / (1024 * 1024)
            }
        }
        
        # Save JSON report
        report_path = f"{self.base_output_dir}/reports/verification_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_report = f"""
# PATTERN VERIFICATION CRAWL REPORT
Generated: {report['timestamp']}
Execution time: {elapsed_time/60:.1f} minutes

## 📊 SUMMARY
• Patterns tested: {report['summary']['patterns_tested']}
• Valid patterns: {report['summary']['valid_patterns']}
• Total tiles crawled: {report['summary']['total_tiles_crawled']}
• Successful downloads: {report['summary']['successful_downloads']}
• Success rate: {(report['summary']['successful_downloads']/report['summary']['total_tiles_crawled']*100) if report['summary']['total_tiles_crawled'] > 0 else 0:.1f}%
• Total size: {report['summary']['total_size_mb']:.1f} MB

## 🧪 PATTERN TEST RESULTS
"""
        
        for i, result in enumerate(pattern_results, 1):
            status = "✅ VALID" if result['valid'] else "❌ INVALID"
            text_report += f"{i}. {status} ({result['success_rate']:.1f}% success)\n"
            text_report += f"   Pattern: {result['pattern']}\n\n"
        
        text_report += "## 🚀 CRAWL RESULTS\n"
        for result in crawl_results:
            text_report += f"• {result['location_name']}: {result['successful_tiles']} tiles downloaded\n"
        
        # Save text report
        text_path = f"{self.base_output_dir}/reports/verification_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(text_path, 'w', encoding='utf-8') as f:
            f.write(text_report)
        
        logger.info(f"📋 Reports saved:")
        logger.info(f"  JSON: {report_path}")
        logger.info(f"  Text: {text_path}")
        
        # Print summary
        print(f"\n🎉 PATTERN VERIFICATION COMPLETE!")
        print("=" * 50)
        print(f"⏱️ Time: {elapsed_time/60:.1f} minutes")
        print(f"🧪 Patterns tested: {report['summary']['patterns_tested']}")
        print(f"✅ Valid patterns: {report['summary']['valid_patterns']}")
        print(f"📥 Tiles downloaded: {report['summary']['successful_downloads']}")
        print(f"💾 Total size: {report['summary']['total_size_mb']:.1f} MB")
        print(f"📁 Output directory: {self.base_output_dir}/")
        
        return report
      
    def debug_tile_coordinates(self, zoom_levels=[10, 12, 14]):
      """Debug tile coordinate generation for Vietnam"""
      logger.info("🔍 DEBUGGING TILE COORDINATES FOR VIETNAM")
      logger.info("=" * 50)
      
      vietnam_bounds = {
          'north': 23.393,   # Cao Bằng
          'south': 8.560,    # Cà Mau
          'east': 109.464,   # Quảng Ninh
          'west': 102.170    # Lai Châu
      }
      
      for zoom in zoom_levels:
          logger.info(f"\n📍 Zoom level {zoom}:")
          
          # Calculate corner coordinates
          x_west, y_north = self.deg2num(vietnam_bounds['north'], vietnam_bounds['west'], zoom)
          x_east, y_south = self.deg2num(vietnam_bounds['south'], vietnam_bounds['east'], zoom)
          
          logger.info(f"  Northwest (Cao Bằng area): x={x_west}, y={y_north}")
          logger.info(f"  Southeast (Cà Mau area): x={x_east}, y={y_south}")
          
          # Show ranges
          x_min, x_max = min(x_west, x_east), max(x_west, x_east)
          y_min, y_max = min(y_north, y_south), max(y_north, y_south)
          
          logger.info(f"  X range: {x_min} to {x_max} ({x_max-x_min+1} tiles)")
          logger.info(f"  Y range: {y_min} to {y_max} ({y_max-y_min+1} tiles)")
          logger.info(f"  Total tiles: {(x_max-x_min+1) * (y_max-y_min+1)}")
          
          # Test sample coordinates
          coverage = self.generate_tile_coordinates_for_vietnam(zoom)
          logger.info(f"  Coverage validation: {coverage}")
    
    def debug_single_pattern(self, pattern, zoom=12, sample_size=5):
        """Debug a single pattern with detailed logging"""
        logger.info(f"🔬 DEBUGGING PATTERN: {pattern}")
        
        # First, let's try some known good coordinates for Hanoi area
        hanoi_coords = [
            # Hanoi center area - these should definitely exist for Hanoi data
            {'x': 3249, 'y': 1865, 'zoom': 12},  # Hanoi center
            {'x': 3250, 'y': 1865, 'zoom': 12},  # Near center
            {'x': 3248, 'y': 1864, 'zoom': 12},  # Near center
            {'x': 13000, 'y': 7460, 'zoom': 14}, # Hanoi center zoom 14
            {'x': 13001, 'y': 7461, 'zoom': 14}, # Near center zoom 14
        ]
        
        logger.info(f"🔍 Testing with known Hanoi coordinates first:")
        
        for i, coord in enumerate(hanoi_coords[:3], 1):
            url = pattern.replace('{z}', str(coord['zoom']))
            url = url.replace('{x}', str(coord['x']))
            url = url.replace('{y}', str(coord['y']))
            
            logger.info(f"\n🧪 Known coord {i}: {url}")
            
            try:
                # Test with detailed logging
                response = self.session.get(url, timeout=self.timeout)
                
                logger.info(f"  Status: {response.status_code}")
                logger.info(f"  Content-Type: {response.headers.get('content-type', 'N/A')}")
                logger.info(f"  Content-Length: {len(response.content)} bytes");
                
                if response.status_code == 200:
                    # Save sample for manual inspection
                    sample_dir = f"{self.base_output_dir}/debug_samples"
                    os.makedirs(sample_dir, exist_ok=True)
                    
                    sample_file = f"{sample_dir}/known_{coord['zoom']}_{coord['x']}_{coord['y']}.data"
                    with open(sample_file, 'wb') as f:
                        f.write(response.content)
                    
                    logger.info(f"  ✅ Sample saved: {sample_file}")
                    
                    # Try to detect if it's an image
                    content_start = response.content[:20]
                    logger.info(f"  Content start (hex): {content_start.hex()}")
                    
                    # Check for image signatures
                    if content_start.startswith(b'\x89PNG'):
                        logger.info("  ✅ Detected: PNG image")
                        return True  # Found working example!
                    elif content_start.startswith(b'\xff\xd8\xff'):
                        logger.info("  ✅ Detected: JPEG image")
                        return True
                    elif content_start.startswith(b'RIFF') and b'WEBP' in content_start:
                        logger.info("  ✅ Detected: WebP image")
                        return True
                    else:
                        logger.info("  ❌ Not a standard image format")
                        logger.info(f"  Content preview: {response.content[:100]}")
                else:
                    logger.error(f"  ❌ HTTP Error: {response.status_code}")
                    if response.content:
                        logger.error(f"  Error content: {response.content[:200].decode('utf-8', errors='ignore')}")
                        
            except Exception as e:
                logger.error(f"  ❌ Exception: {e}")
        
        # If known coordinates don't work, try random ones
        logger.info(f"\n🎲 Known coordinates failed, trying random samples:")
        
        # Generate sample URLs
        sample_urls = self.generate_sample_urls_from_pattern(pattern, [zoom], sample_size)
        
        if not sample_urls:
            logger.error("❌ No sample URLs generated")
            return False
        
        for i, tile_info in enumerate(sample_urls[:3], 1):  # Test first 3
            url = tile_info['url']
            logger.info(f"\n🧪 Random sample {i}: {url}")
            
            try:
                response = self.session.get(url, timeout=self.timeout)
                
                logger.info(f"  Status: {response.status_code}")
                logger.info(f"  Content-Type: {response.headers.get('content-type', 'N/A')}")
                logger.info(f"  Content-Length: {len(response.content)} bytes");
                
                if response.status_code == 200:
                    # Save sample
                    sample_dir = f"{self.base_output_dir}/debug_samples"
                    os.makedirs(sample_dir, exist_ok=True)
                    
                    sample_file = f"{sample_dir}/random_{zoom}_{tile_info['x']}_{tile_info['y']}.data"
                    with open(sample_file, 'wb') as f:
                        f.write(response.content)
                    
                    logger.info(f"  Sample saved: {sample_file}")
                    
                    # Check content
                    content_start = response.content[:20]
                    logger.info(f"  Content start (hex): {content_start.hex()}")
                    
                    if any(sig in content_start for sig in [b'\x89PNG', b'\xff\xd8\xff', b'RIFF']):
                        logger.info("  ✅ Valid image detected!")
                        return True
                    else:
                        logger.info("  ❌ Not an image")
                        
            except Exception as e:
                logger.error(f"  ❌ Exception: {e}")
    
        return False

    def test_manual_urls(self):
        """Test some manual URLs that we know should work"""
        logger.info("🧪 TESTING MANUAL URLS")
        
        # Try some URLs that definitely should work (if the servers are up)
        test_urls = [
            # Hanoi planning tiles that are likely to exist
            "https://l5cfglaebpobj.vcdn.cloud/ha-noi-2030-2/12/3249/1865.png",
            "https://l5cfglaebpobj.vcdn.cloud/ha-noi-2030-2/12/3250/1865.png",
            "https://s3-hn-2.cloud.cmctelecom.vn/guland7/land/ha-noi/12/3249/1865.png",
            "https://s3-hn-2.cloud.cmctelecom.vn/guland7/land/ha-noi/12/3250/1865.png",
        ]
        
        for i, url in enumerate(test_urls, 1):
            logger.info(f"\n🔗 Manual test {i}: {url}")
            
            try:
                response = self.session.get(url, timeout=self.timeout)
                logger.info(f"  Status: {response.status_code}")
                logger.info(f"  Content-Type: {response.headers.get('content-type', 'N/A')}")
                logger.info(f"  Content-Length: {len(response.content)} bytes");
                
                if response.status_code == 200:
                    # Save for inspection
                    sample_dir = f"{self.base_output_dir}/debug_samples"
                    os.makedirs(sample_dir, exist_ok=True)
                    
                    filename = f"manual_test_{i}.data"
                    sample_file = f"{sample_dir}/{filename}"
                    
                    with open(sample_file, 'wb') as f:
                        f.write(response.content)
                    
                    logger.info(f"  ✅ Saved: {sample_file}")
                    
                    # Check if it's an image
                    content_start = response.content[:20]
                    if content_start.startswith(b'\x89PNG'):
                        logger.info("  ✅ Valid PNG image!")
                    elif content_start.startswith(b'\xff\xd8\xff'):
                        logger.info("  ✅ Valid JPEG image!")
                    elif b'WEBP' in content_start:
                        logger.info("  ✅ Valid WebP image!")
                    else:
                        logger.info(f"  ❓ Unknown format: {content_start.hex()}")
                        
                else:
                    logger.error(f"  ❌ HTTP {response.status_code}")
                    
            except Exception as e:
                logger.error(f"  ❌ Exception: {e}")
    
    def verify_city_patterns(self, city_name, lat, lng, patterns, zoom_levels):
        """Verify patterns cho thành phố cụ thể"""
        logger.info(f"🔍 Verifying patterns for {city_name}")
        
        # Generate tile coordinates cho thành phố này
        city_coverage = self.generate_city_tile_coverage(lat, lng, zoom_levels)
        
        verified_patterns = []
        
        for pattern in patterns:
            logger.info(f"🧪 Testing pattern: {pattern}")
            
            # Test pattern với city coordinates
            success_rate = self.test_pattern_for_city(pattern, city_coverage)
            
            if success_rate > 50:
                verified_patterns.append({
                    'pattern': pattern,
                    'city': city_name,
                    'success_rate': success_rate,
                    'verified': True
                })
                logger.info(f"✅ Pattern VALID for {city_name}")
            else:
                logger.info(f"❌ Pattern INVALID for {city_name}")
        
        return verified_patterns

    def generate_city_tile_coverage(self, lat, lng, zoom_levels, radius_km=20):
        """Generate tile coverage cho thành phố cụ thể - OPTIMIZED"""
        city_coverages = {}
        
        for zoom in zoom_levels:
            # Tính tile coordinates cho center city
            center_x, center_y = self.deg2num(lat, lng, zoom)
            
            # Tính radius trong tiles - more accurate calculation
            lat_rad = math.radians(lat)
            meters_per_pixel = 156543.03392 * math.cos(lat_rad) / (2 ** zoom)
            pixels_per_tile = 256
            meters_per_tile = meters_per_pixel * pixels_per_tile
            radius_tiles = int((radius_km * 1000) / meters_per_tile)
            
            # Ensure reasonable coverage
            radius_tiles = max(radius_tiles, 5)   # At least 5 tiles radius
            radius_tiles = min(radius_tiles, 50)  # Max 50 tiles radius to avoid too many tiles
            
            city_coverages[zoom] = {
                'center_x': center_x,
                'center_y': center_y,
                'x_min': center_x - radius_tiles,
                'x_max': center_x + radius_tiles,
                'y_min': center_y - radius_tiles,
                'y_max': center_y + radius_tiles,
                'radius_tiles': radius_tiles,
                'total_tiles': (2 * radius_tiles + 1) ** 2
            }
            
            logger.info(f"  Zoom {zoom}: Center({center_x},{center_y}), Radius={radius_tiles} tiles, Total={city_coverages[zoom]['total_tiles']} tiles")
        
        return city_coverages

    def comprehensive_pattern_crawl_independent(self, pattern, zoom_levels, tile_limit=None):
        """Comprehensive crawl for a pattern - independent mode"""
        logger.info(f"🚀 INDEPENDENT COMPREHENSIVE CRAWL")
        logger.info(f"🎯 Pattern: {pattern}")
        logger.info(f"🔢 Zoom levels: {zoom_levels}")
        logger.info(f"🗺️ Coverage: Full Vietnam")
        
        all_tiles = []
        total_processed = 0
        
        for zoom in zoom_levels:
            if tile_limit and total_processed >= tile_limit:
                logger.info(f"🛑 Reached global tile limit ({tile_limit})")
                break
                
            logger.info(f"\n🔍 Processing zoom level {zoom}")
            
            # Get full Vietnam coverage for this zoom
            coverage = self.generate_tile_coordinates_for_vietnam(zoom)
            
            # Calculate how many tiles we can process for this zoom
            zoom_limit = None
            if tile_limit:
                remaining = tile_limit - total_processed
                zoom_limit = min(remaining, coverage['total_tiles'])
        
            # Generate ALL possible coordinates for this zoom
            all_coordinates = []
            for x in range(coverage['x_min'], coverage['x_max'] + 1):
                for y in range(coverage['y_min'], coverage['y_max'] + 1):
                    all_coordinates.append((x, y))
                    if zoom_limit and len(all_coordinates) >= zoom_limit:
                        break
                if zoom_limit and len(all_coordinates) >= zoom_limit:
                    break
        
            logger.info(f"📊 Zoom {zoom}: Testing {len(all_coordinates)} tiles")
            
            # Generate URLs for this zoom
            zoom_tiles = []
            for x, y in all_coordinates:
                url = pattern.replace('{z}', str(zoom))
                url = url.replace('{x}', str(x))
                url = url.replace('{y}', str(y))
                
                zoom_tiles.append({
                    'url': url,
                    'zoom': zoom,
                    'x': x,
                    'y': y,
                    'pattern': pattern
                })
            
            # Process tiles in batches to avoid memory issues
            batch_size = 100
            zoom_successful = 0
            
            for i in range(0, len(zoom_tiles), batch_size):
                batch = zoom_tiles[i:i+batch_size]
                logger.info(f"📦 Processing batch {i//batch_size + 1}/{(len(zoom_tiles)-1)//batch_size + 1}")
                
                # Download/test batch
                if self.tile_downloader:
                    batch_results = self.download_tiles_batch(batch, f"vietnam_pattern_crawl", zoom)
                else:
                    batch_results = self.test_tiles_batch(batch)
                
                # Count successful tiles
                successful_in_batch = len([r for r in batch_results if r.get('success')])
                zoom_successful += successful_in_batch
                
                all_tiles.extend(batch_results)
                total_processed += len(batch)
                
                logger.info(f"📊 Batch result: {successful_in_batch}/{len(batch)} successful")
                
                # Small delay between batches
                time.sleep(0.5)
        
        logger.info(f"✅ Zoom {zoom} complete: {zoom_successful}/{len(zoom_tiles)} tiles successful")
    
        return all_tiles

    def test_tiles_batch(self, tile_infos):
        """Test a batch of tiles without downloading"""
        results = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_tile = {
                executor.submit(self.test_single_tile_url, tile_info): tile_info 
                for tile_info in tile_infos
            }
            
            for future in as_completed(future_to_tile):
                result = future.result()
                results.append(result)
    
        return results

    def run_independent_pattern_crawl(self, patterns=None, zoom_levels=[10, 12, 14, 16, 18]):
        """Run independent pattern crawl - find ALL possible tiles"""
        logger.info("🚀 STARTING INDEPENDENT PATTERN CRAWL")
        logger.info("=" * 60)
        logger.info("🎯 Goal: Find ALL available tiles for each pattern")
        logger.info("🗺️ Coverage: Full Vietnam")
        logger.info("📥 Download: Save tiles with browser-like structure")
        
        start_time = time.time()
        
        # Load patterns if not provided
        if patterns is None:
            patterns = self.load_all_discovered_patterns()
        
        if not patterns:
            logger.error("❌ No patterns to crawl")
            return None
        
        logger.info(f"🔍 Found {len(patterns)} patterns to crawl")
        
        all_crawl_results = []
        
        for i, pattern in enumerate(patterns, 1):
            logger.info(f"\n🌟 CRAWLING PATTERN {i}/{len(patterns)}")
            logger.info(f"🔗 Pattern: {pattern}")
            
            # Extract pattern info for organizing downloads
            pattern_info = self.analyze_pattern(pattern)
            
            # Comprehensive crawl for this pattern
            pattern_tiles = self.comprehensive_pattern_crawl_independent(
                pattern, 
                zoom_levels,
                tile_limit=5000  # Limit per pattern to avoid overwhelming
            )
            
            # Organize results
            successful_tiles = [t for t in pattern_tiles if t.get('success')]
            failed_tiles = [t for t in pattern_tiles if not t.get('success')]
            
            logger.info(f"📊 Pattern {i} results:")
            logger.info(f"  ✅ Successful: {len(successful_tiles)}")
            logger.info(f"  ❌ Failed: {len(failed_tiles)}")
            logger.info(f"  📥 Downloaded size: {sum(t.get('size', 0) for t in successful_tiles) / 1024 / 1024:.1f} MB")
            
            crawl_result = {
                'pattern': pattern,
                'pattern_info': pattern_info,
                'total_tested': len(pattern_tiles),
                'successful': len(successful_tiles),
                'failed': len(failed_tiles),
                'success_rate': len(successful_tiles) / len(pattern_tiles) * 100 if pattern_tiles else 0,
                'tiles': successful_tiles,  # Only keep successful ones
                'download_size_mb': sum(t.get('size', 0) for t in successful_tiles) / 1024 / 1024
            }
            
            all_crawl_results.append(crawl_result)
            
            # Save intermediate results
            self.save_pattern_results(pattern_info['name'], crawl_result)
        
        # Generate final comprehensive report
        final_report = self.generate_independent_crawl_report(all_crawl_results, start_time)
        
        return final_report

    def analyze_pattern(self, pattern):
        """Analyze pattern to extract meaningful info for organization"""
        from urllib.parse import urlparse
        
        # Replace placeholders for parsing
        sample_url = pattern.replace('{z}', '12').replace('{x}', '1000').replace('{y}', '1000')
        parsed = urlparse(sample_url)
        
        # Extract pattern characteristics
        domain = parsed.netloc
        path_parts = parsed.path.split('/')
        
        # Try to extract location/type from path
        location = "unknown"
        map_type = "planning"
        
        for part in path_parts:
            if any(city in part.lower() for city in ['ha-noi', 'hanoi', 'hcm', 'ho-chi-minh']):
                location = part
            if any(type_word in part.lower() for type_word in ['planning', 'satellite', 'terrain']):
                map_type = part
    
        # Generate clean name for folders
        clean_domain = domain.replace('.', '_')
        pattern_name = f"{clean_domain}_{location}_{map_type}"
        
        return {
            'name': pattern_name,
            'domain': domain,
            'location': location,
            'map_type': map_type,
            'full_pattern': pattern
        }

    def load_all_discovered_patterns(self):
        """Load all discovered patterns from browser crawler results"""
        patterns = set()
        
        # Try to load from final report first
        try:
            with open('output_browser_crawl/full_coverage_final_report.json', 'r', encoding='utf-8') as f:
                data = json.load(f)
            patterns.update(data.get('tile_patterns', []))
            logger.info(f"📋 Loaded {len(patterns)} patterns from final report")
        except FileNotFoundError:
            logger.warning("⚠️ No final report found, loading from individual city files")
    
        # Also load from individual city files
        city_files_dir = Path('output_browser_crawl')
        if city_files_dir.exists():
            for city_file in city_files_dir.glob('full_coverage_*.json'):
                if 'final_report' in city_file.name:
                    continue
                
                try:
                    with open(city_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    city_patterns = data.get('tile_patterns', [])
                    patterns.update(city_patterns)
                    logger.info(f"📋 Loaded {len(city_patterns)} patterns from {city_file.name}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to load {city_file}: {e}")
    
        return list(patterns)

    def save_pattern_results(self, pattern_name, crawl_result):
        """Save results for individual pattern with browser-like structure"""
        
        # Create directory structure like browser crawler
        pattern_dir = f"{self.base_output_dir}/patterns/{pattern_name}"
        os.makedirs(pattern_dir, exist_ok=True)
        os.makedirs(f"{pattern_dir}/tiles", exist_ok=True)
        os.makedirs(f"{pattern_dir}/reports", exist_ok=True)
        
        # Save pattern summary
        summary_file = f"{pattern_dir}/pattern_summary.json"
        summary_data = {
            'pattern': crawl_result['pattern'],
            'pattern_info': crawl_result['pattern_info'],
            'crawl_timestamp': datetime.now().isoformat(),
            'statistics': {
                'total_tested': crawl_result['total_tested'],
                'successful': crawl_result['successful'],
                'failed': crawl_result['failed'],
                'success_rate': crawl_result['success_rate'],
                'download_size_mb': crawl_result['download_size_mb']
            },
            'zoom_breakdown': self.calculate_zoom_breakdown(crawl_result['tiles']),
            'tile_coverage': self.calculate_tile_coverage(crawl_result['tiles'])
        }
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
    
        # Save tile list
        tiles_file = f"{pattern_dir}/tiles_list.json"
        tiles_data = {
            'tiles': crawl_result['tiles'],
            'total_count': len(crawl_result['tiles'])
        }
    
        with open(tiles_file, 'w', encoding='utf-8') as f:
            json.dump(tiles_data, f, indent=2, ensure_ascii=False)
    
        logger.info(f"💾 Pattern results saved: {pattern_dir}")

    def calculate_zoom_breakdown(self, tiles):
        """Calculate tile count breakdown by zoom level"""
        zoom_counts = {}
        for tile in tiles:
            zoom = tile.get('tile_info', tile).get('zoom')
            if zoom:
                zoom_counts[zoom] = zoom_counts.get(zoom, 0) + 1
        return zoom_counts

    def calculate_tile_coverage(self, tiles):
        """Calculate coverage statistics"""
        if not tiles:
            return {}
        
        zooms = {}
        for tile in tiles:
            tile_info = tile.get('tile_info', tile)
            zoom = tile_info.get('zoom')
            x = tile_info.get('x')
            y = tile_info.get('y')
            
            if zoom and x is not None and y is not None:
                if zoom not in zooms:
                    zooms[zoom] = {'x_coords': [], 'y_coords': []}
                zooms[zoom]['x_coords'].append(x)
                zooms[zoom]['y_coords'].append(y)
        
        coverage = {}
        for zoom, coords in zooms.items():
            coverage[zoom] = {
                'x_range': [min(coords['x_coords']), max(coords['x_coords'])],
                'y_range': [min(coords['y_coords']), max(coords['y_coords'])],
                'tile_count': len(coords['x_coords'])
            }
        
        return coverage

    def generate_independent_crawl_report(self, all_crawl_results, start_time):
        """Generate comprehensive report for independent crawl"""
        elapsed_time = time.time() - start_time
        
        # Calculate totals
        total_patterns = len(all_crawl_results)
        total_tested = sum(r['total_tested'] for r in all_crawl_results)
        total_successful = sum(r['successful'] for r in all_crawl_results)
        total_failed = sum(r['failed'] for r in all_crawl_results)
        total_size_mb = sum(r['download_size_mb'] for r in all_crawl_results)
        
        report = {
            'crawl_type': 'Independent Pattern Crawl',
            'timestamp': datetime.now().isoformat(),
            'execution_time_seconds': elapsed_time,
            'execution_time_minutes': elapsed_time / 60,
            'summary': {
                'patterns_crawled': total_patterns,
                'total_tiles_tested': total_tested,
                'total_tiles_successful': total_successful,
                'total_tiles_failed': total_failed,
                'overall_success_rate': total_successful / total_tested * 100 if total_tested > 0 else 0,
                'total_download_size_mb': total_size_mb,
                'average_tiles_per_pattern': total_tested / total_patterns if total_patterns > 0 else 0
            },
            'pattern_results': all_crawl_results,
            'performance_stats': self.stats.copy()
        }
        
        # Save comprehensive report
        report_file = f"{self.base_output_dir}/independent_crawl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_summary = f"""
# INDEPENDENT PATTERN CRAWL REPORT
Generated: {report['timestamp']}
Duration: {elapsed_time/60:.1f} minutes

## 🎯 MISSION: Find ALL available tiles independently

## 📊 OVERALL SUMMARY
• Patterns crawled: {total_patterns}
• Total tiles tested: {total_tested:,}
• Successful downloads: {total_successful:,}
• Failed attempts: {total_failed:,}
• Success rate: {report['summary']['overall_success_rate']:.1f}%
• Total size downloaded: {total_size_mb:.1f} MB
• Average tiles per pattern: {report['summary']['average_tiles_per_pattern']:.0f}

## 🔍 PATTERN BREAKDOWN
"""
    
        for i, result in enumerate(all_crawl_results, 1):
            text_summary += f"""
{i}. {result['pattern_info']['name']}
   Pattern: {result['pattern']}
   Tested: {result['total_tested']:,} tiles
   Success: {result['successful']:,} ({result['success_rate']:.1f}%)
   Size: {result['download_size_mb']:.1f} MB
"""
    
        text_summary += f"""
## 📁 OUTPUT STRUCTURE
{self.base_output_dir}/
├── patterns/
│   ├── {all_crawl_results[0]['pattern_info']['name'] if all_crawl_results else 'pattern_name'}/
│   │   ├── tiles/           # Downloaded tiles
│   │   ├── pattern_summary.json
│   │   └── tiles_list.json
│   └── ...
└── independent_crawl_report_*.json
"""
    
        text_file = f"{self.base_output_dir}/independent_crawl_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(text_summary)
    
        # Print summary
        print(f"\n🎉 INDEPENDENT CRAWL COMPLETE!")
        print("=" * 50)
        print(f"⏱️ Duration: {elapsed_time/60:.1f} minutes")
        print(f"🔍 Patterns: {total_patterns}")
        print(f"📊 Tiles tested: {total_tested:,}")
        print(f"✅ Downloaded: {total_successful:,} ({report['summary']['overall_success_rate']:.1f}%)")
        print(f"💾 Total size: {total_size_mb:.1f} MB")
        print(f"📁 Output: {self.base_output_dir}/")
        
        logger.info(f"📋 Reports saved:")
        logger.info(f"  JSON: {report_file}")
        logger.info(f"  Text: {text_file}")
        
        return report
    
    def auto_assign_patterns_to_cities(self, patterns):
        """Auto-assign patterns to cities based on URL analysis"""
        city_mapping = {}
        
        for pattern in patterns:
            city = self.detect_city_from_pattern(pattern)
            if city:
                if city not in city_mapping:
                    city_mapping[city] = []
                city_mapping[city].append(pattern)
            else:
                # Unknown city, add to 'unknown' category
                if 'unknown' not in city_mapping:
                    city_mapping['unknown'] = []
                city_mapping['unknown'].append(pattern)
        
        logger.info(f"📋 Pattern assignment:")
        for city, patterns_list in city_mapping.items():
            logger.info(f"  {city}: {len(patterns_list)} patterns")
        
        return city_mapping
    
    def detect_city_from_pattern(self, pattern):
        """Detect city from URL pattern"""
        pattern_lower = pattern.lower()
        
        if any(keyword in pattern_lower for keyword in ['ha-noi', 'hanoi']):
            return 'hanoi'
        elif any(keyword in pattern_lower for keyword in ['da-nang', 'danang']):
            return 'danang'
        elif any(keyword in pattern_lower for keyword in ['ho-chi-minh', 'hcm', 'saigon']):
            return 'hcm'
        elif any(keyword in pattern_lower for keyword in ['can-tho']):
            return 'cantho'
        elif any(keyword in pattern_lower for keyword in ['hai-phong']):
            return 'haiphong'
        
        return None  # Unknown city
    
    def test_pattern_for_city_quick(self, pattern, city_coverage):
        """Quick test pattern với city center coordinates"""
        test_coords = []
        
        # Test với center coordinates của mỗi zoom
        for zoom, coverage in city_coverage.items():
            center_x = coverage['center_x']
            center_y = coverage['center_y']
            
            # Test center và một vài points xung quanh
            for dx, dy in [(0, 0), (1, 0), (0, 1), (-1, 0), (0, -1)]:
                test_coords.append({
                    'x': center_x + dx,
                    'y': center_y + dy,
                    'zoom': zoom
                })
        
        successful = 0
        for coord in test_coords[:10]:  # Test max 10 coordinates
            url = pattern.replace('{z}', str(coord['zoom']))
            url = url.replace('{x}', str(coord['x']))
            url = url.replace('{y}', str(coord['y']))
            
            try:
                response = self.session.get(url, timeout=10)
                if response.status_code == 200 and len(response.content) > 1000:
                    successful += 1
            except:
                pass
        
        success_rate = successful / len(test_coords) * 100 if test_coords else 0
        return success_rate > 20  # Lower threshold for city-focused

    def crawl_pattern_for_city(self, pattern, city_coverage, city_name):
        """Exhaustive crawl - thử TẤT CẢ tiles có thể trong city coverage"""
        all_tiles = []
        
        for zoom, coverage in city_coverage.items():
            logger.info(f"🔍 City {city_name} - Zoom {zoom}")
            logger.info(f"  Coverage: X({coverage['x_min']}-{coverage['x_max']}), Y({coverage['y_min']}-{coverage['y_max']})")
            
            # Generate ALL coordinates trong city coverage
            all_coordinates = []
            for x in range(coverage['x_min'], coverage['x_max'] + 1):
                for y in range(coverage['y_min'], coverage['y_max'] + 1):
                    all_coordinates.append((x, y))
            
            logger.info(f"📊 Trying ALL {len(all_coordinates)} coordinates for zoom {zoom}")
            
            # Generate ALL URLs for this zoom level
            zoom_urls = []
            for x, y in all_coordinates:
                url = pattern.replace('{z}', str(zoom))
                url = url.replace('{x}', str(x))
                url = url.replace('{y}', str(y))
                
                zoom_urls.append({
                    'url': url,
                    'zoom': zoom,
                    'x': x,
                    'y': y,
                    'pattern': pattern
                })
            
            # Process in batches for memory efficiency
            batch_size = 100
            zoom_successful = 0
            zoom_total = 0
            
            for i in range(0, len(zoom_urls), batch_size):
                batch = zoom_urls[i:i+batch_size]
                
                logger.info(f"📦 Processing batch {i//batch_size + 1}/{(len(zoom_urls)-1)//batch_size + 1} ({len(batch)} tiles)")
                
                # Try to download/test all tiles in batch
                if self.tile_downloader:
                    batch_results = self.download_tiles_batch(batch, f"{city_name}", zoom)
                else:
                    batch_results = self.test_tiles_batch(batch)
                
                # Count results
                successful_in_batch = len([r for r in batch_results if r.get('success')])
                zoom_successful += successful_in_batch
                zoom_total += len(batch)
                
                # Add successful tiles only
                all_tiles.extend([r for r in batch_results if r.get('success')])
                
                if successful_in_batch > 0:
                    logger.info(f"✅ Found {successful_in_batch}/{len(batch)} tiles in batch")
                else:
                    logger.info(f"❌ No tiles found in batch")
                
                # Short delay to be respectful
                time.sleep(0.1)
            
            logger.info(f"📊 Zoom {zoom} final: {zoom_successful}/{zoom_total} tiles successful ({zoom_successful/zoom_total*100:.1f}%)")
        
        return all_tiles

    def crawl_city_specific_patterns(self, patterns=None, zoom_levels=[10, 12, 14, 16], use_txt_source=True):
        """Simplified exhaustive crawling - cào hết tất cả tiles có thể"""
        
        # City coordinates (lat, lng, radius_km)
        city_coords = {
            'hanoi': (21.0285, 105.8542, 30),
            'danang': (16.0544563, 108.0717219, 25),
            'hcm': (10.8231, 106.6297, 40),
            'haiphong': (20.8449, 106.6881, 25),
            'cantho': (10.0452, 105.7469, 25)
        }
        
        # Load patterns
        if patterns is None:
            if use_txt_source:
                logger.info("📋 Loading patterns from TXT coverage reports...")
                patterns = self.load_all_discovered_patterns_from_txt()
            else:
                logger.info("📋 Loading patterns from JSON files...")
                patterns = self.load_all_discovered_patterns()

        if not patterns:
            logger.error("❌ No patterns found! Run browser crawler first.")
            return []

        # Auto-assign patterns to cities
        city_pattern_mapping = self.auto_assign_patterns_to_cities(patterns)
        
        all_results = []
        
        for city_name, city_patterns_list in city_pattern_mapping.items():
            if city_name not in city_coords:
                logger.info(f"⚠️ Skipping {city_name} - coordinates not configured")
                continue
                
            lat, lng, radius_km = city_coords[city_name]
            logger.info(f"\n🏙️ CRAWLING CITY: {city_name.upper()}")
            logger.info(f"📍 Center: {lat}, {lng} (radius: {radius_km}km)")
            logger.info(f"🔍 Found {len(city_patterns_list)} patterns for {city_name}")
            
            # Generate city-specific coverage
            city_coverage = self.generate_city_tile_coverage(lat, lng, zoom_levels, radius_km)
            
            city_results = []
            for pattern in city_patterns_list:
                logger.info(f"🚀 Exhaustive crawling pattern: {pattern}")
                
                # NO VALIDATION - Just crawl everything
                city_tiles = self.crawl_pattern_for_city(pattern, city_coverage, city_name)
                
                if city_tiles:
                    city_results.append({
                        'pattern': pattern,
                        'tiles': city_tiles,
                        'successful_count': len([t for t in city_tiles if t.get('success')])
                    })
            
            if city_results:
                all_results.append({
                    'city': city_name,
                    'coordinates': (lat, lng, radius_km),
                    'coverage': city_coverage,
                    'pattern_results': city_results,
                    'total_tiles': sum(len(r['tiles']) for r in city_results),
                    'successful_tiles': sum(r['successful_count'] for r in city_results)
                })
        
        return all_results

    def load_patterns_from_txt_reports(self, city_name=None):
        """Load patterns from TXT coverage reports instead of JSON"""
        patterns = set()
    
        if city_name:
            # Load patterns cho thành phố cụ thể từ TXT file
            txt_file = f'output_browser_crawl/coverage_report_{city_name.replace(" ", "_")}.txt'
            patterns_from_city = self.parse_patterns_from_txt(txt_file)
            patterns.update(patterns_from_city)
            logger.info(f"📋 Loaded {len(patterns_from_city)} patterns from {city_name} TXT report")
        else:
            # Load từ tất cả TXT files
            txt_files_dir = Path('output_browser_crawl')
            if txt_files_dir.exists():
                for txt_file in txt_files_dir.glob('coverage_report_*.txt'):
                    city_patterns = self.parse_patterns_from_txt(txt_file)
                    patterns.update(city_patterns)
                    logger.info(f"📋 Loaded {len(city_patterns)} patterns from {txt_file.name}")
    
        return list(patterns)

    def parse_patterns_from_txt(self, txt_file_path):
        """Parse tile patterns from TXT coverage report"""
        patterns = set()
    
        try:
            with open(txt_file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Tìm section "## 🎯 TILE PATTERNS"
            pattern_section_start = content.find("## 🎯 TILE PATTERNS")
            if pattern_section_start == -1:
                logger.warning(f"⚠️ No patterns section found in {txt_file_path}")
                return patterns
            
            # Extract phần patterns (từ "## 🎯 TILE PATTERNS" đến hết file hoặc section tiếp theo)
            pattern_section = content[pattern_section_start:]
            
            # Tìm section tiếp theo nếu có
            next_section = pattern_section.find("\n## ")
            if next_section != -1:
                pattern_section = pattern_section[:next_section]
            
            # Parse từng dòng pattern
            lines = pattern_section.split('\n')
            for line in lines:
                line = line.strip()
                if line.startswith('• ') and '{z}' in line and '{x}' in line and '{y}' in line:
                    pattern = line[2:].strip()  # Remove "• " prefix
                    patterns.add(pattern)
                    logger.info(f"🎯 Found pattern: {pattern}")
            
            logger.info(f"📋 Parsed {len(patterns)} patterns from {txt_file_path}")
            
        except FileNotFoundError:
            logger.warning(f"❌ TXT file not found: {txt_file_path}")
        except Exception as e:
            logger.error(f"❌ Error parsing {txt_file_path}: {e}")
    
        return patterns

    # Update load_all_discovered_patterns to support TXT
    def load_all_discovered_patterns_from_txt(self):
        """Load all discovered patterns from TXT reports"""
        patterns = set()
    
        # Load from individual city TXT files
        txt_files_dir = Path('output_browser_crawl')
        if txt_files_dir.exists():
            for txt_file in txt_files_dir.glob('coverage_report_*.txt'):
                try:
                    city_patterns = self.parse_patterns_from_txt(txt_file)
                    patterns.update(city_patterns)
                    city_name = txt_file.stem.replace('coverage_report_', '')
                    logger.info(f"📋 Added {len(city_patterns)} patterns from {city_name}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to load {txt_file}: {e}")
    
        logger.info(f"📋 Total unique patterns loaded: {len(patterns)}")
        return list(patterns)

    def generate_city_focused_report(self, city_results, start_time):
        """Generate report for city-focused crawl"""
        elapsed_time = time.time() - start_time
        
        # Calculate totals
        total_cities = len(city_results)
        total_patterns = sum(len(r['pattern_results']) for r in city_results)
        total_tiles = sum(r['total_tiles'] for r in city_results)
        total_successful = sum(r['successful_tiles'] for r in city_results)
        total_size_mb = sum(
            sum(
                sum(t.get('size', 0) for t in pr['tiles'] if t.get('success'))
                for pr in r['pattern_results']
            ) for r in city_results
        ) / 1024 / 1024
        
        report = {
            'crawl_type': 'City-Focused Pattern Crawl',
            'timestamp': datetime.now().isoformat(),
            'execution_time_seconds': elapsed_time,
            'execution_time_minutes': elapsed_time / 60,
            'summary': {
                'cities_crawled': total_cities,
                'patterns_used': total_patterns,
                'total_tiles_tested': total_tiles,
                'total_tiles_successful': total_successful,
                'overall_success_rate': total_successful / total_tiles * 100 if total_tiles > 0 else 0,
                'total_download_size_mb': total_size_mb,
                'average_tiles_per_city': total_tiles / total_cities if total_cities > 0 else 0
            },
            'city_results': city_results,
            'performance_stats': self.stats.copy()
        }
        
        # Save comprehensive report
        report_file = f"{self.base_output_dir}/city_focused_crawl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_summary = f"""
    # CITY-FOCUSED PATTERN CRAWL REPORT
    Generated: {report['timestamp']}
    Duration: {elapsed_time/60:.1f} minutes

    ## 🎯 MISSION: Find tiles for specific cities with known data

    ## 📊 OVERALL SUMMARY
    • Cities crawled: {total_cities}
    • Patterns used: {total_patterns}  
    • Total tiles tested: {total_tiles:,}
    • Successful downloads: {total_successful:,}
    • Success rate: {report['summary']['overall_success_rate']:.1f}%
    • Total size downloaded: {total_size_mb:.1f} MB
    • Average tiles per city: {report['summary']['average_tiles_per_city']:.0f}

    ## 🏙️ CITY BREAKDOWN
    """
        
        for result in city_results:
            city_name = result['city']
            success_rate = (result['successful_tiles'] / result['total_tiles'] * 100) if result['total_tiles'] > 0 else 0
            text_summary += f"""
    🏙️ {city_name.upper()}
    Coordinates: {result['coordinates']}
    Patterns tested: {len(result['pattern_results'])}
    Tiles tested: {result['total_tiles']:,}
    Successful: {result['successful_tiles']:,} ({success_rate:.1f}%)
    
    """
        
        text_file = f"{self.base_output_dir}/city_focused_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(text_summary)
        
        # Print summary
        print(f"\n🎉 CITY-FOCUSED CRAWL COMPLETE!")
        print("=" * 50)
        print(f"⏱️ Duration: {elapsed_time/60:.1f} minutes")
        print(f"🏙️ Cities: {total_cities}")
        print(f"🔍 Patterns: {total_patterns}")
        print(f"📊 Tiles tested: {total_tiles:,}")
        print(f"✅ Downloaded: {total_successful:,} ({report['summary']['overall_success_rate']:.1f}%)")
        print(f"💾 Total size: {total_size_mb:.1f} MB")
        print(f"📁 Output: {self.base_output_dir}/")
        
        # Print city breakdown
        for result in city_results:
            success_rate = (result['successful_tiles'] / result['total_tiles'] * 100) if result['total_tiles'] > 0 else 0
            print(f"  🏙️ {result['city']}: {result['successful_tiles']:,} tiles ({success_rate:.1f}%)")
        
        logger.info(f"📋 Reports saved:")
        logger.info(f"  JSON: {report_file}")
        logger.info(f"  Text: {text_file}")
        
        return report

# Update main function to use city-focused approach
def main():
    print("🚀 GULAND EXHAUSTIVE TILE CRAWLER")
    print("Downloads ALL available tiles for discovered patterns")
    print("=" * 60)
    
    # Source selection
    source_choice = input("Pattern source (1=TXT reports, 2=JSON files, default=1): ").strip()
    use_txt_source = source_choice != '2'
    
    if use_txt_source:
        print("📋 Using TXT coverage reports as pattern source")
    else:
        print("📋 Using JSON files as pattern source")
    
    # Download mode (recommended for exhaustive approach)
    download_choice = input("Enable tile downloads? (y/n, default=y): ").lower()
    enable_download = download_choice != 'n'
    
    if enable_download:
        print("✅ Download mode - will save all found tiles")
    else:
        print("📊 Test mode - will only count available tiles")
    
    # Zoom selection
    print("\nZoom level options:")
    print("1. Light (10, 12) - ~2K tiles per city")
    print("2. Standard (10, 12, 14) - ~30K tiles per city") 
    print("3. Heavy (10, 12, 14, 16) - ~500K tiles per city")
    print("4. Maximum (10-18) - ~8M tiles per city")
    
    zoom_choice = input("Choose zoom levels (1/2/3/4, default=2): ").strip()
    
    if zoom_choice == '1':
        zoom_levels = [10, 12]
    elif zoom_choice == '3':
        zoom_levels = [10, 12, 14, 16]
    elif zoom_choice == '4':
        zoom_levels = [10, 11, 12, 13, 14, 15, 16, 17, 18]
    else:
        zoom_levels = [10, 12, 14]
    
    print(f"🎯 Selected zoom levels: {zoom_levels}")
    
    # Warning for heavy crawls
    if len(zoom_levels) > 3:
        estimated_tiles = len(zoom_levels) * 100000  # Rough estimate
        print(f"⚠️ WARNING: This will try ~{estimated_tiles:,} tiles per city!")
        confirm = input("Continue? (y/n): ").lower()
        if confirm != 'y':
            print("❌ Cancelled")
            return
    
    # Initialize crawler
    crawler = PatternBasedTileCrawler(enable_download=enable_download)
    
    # Run exhaustive crawl
    start_time = time.time()
    
    results = crawler.crawl_city_specific_patterns(
        patterns=None,
        zoom_levels=zoom_levels, 
        use_txt_source=use_txt_source
    )
    
    if not results:
        logger.error("❌ No results from exhaustive crawling")
        return
    
    # Generate report
    report = crawler.generate_city_focused_report(results, start_time)
    
    if report:
        total_tiles = report['summary']['total_tiles_successful']
        print(f"\n🎉 EXHAUSTIVE CRAWL COMPLETE!")
        print(f"📈 Downloaded {total_tiles:,} tiles across all cities!")
        print(f"📁 Check {crawler.base_output_dir}/ for results")

if __name__ == "__main__":
    main()