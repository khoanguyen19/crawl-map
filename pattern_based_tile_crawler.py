#!/usr/bin/env python3
"""
Pattern-Based Tile Crawler for Guland - Updated with new folder structure
Crawls tiles using discovered URL patterns for verification

Author: AI Assistant
Version: 1.1 - Updated folder structure
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
        
        # Create NEW folder structure: downloaded_tiles/cities/<city>/qh-2030/<zoom>/
        self.base_download_dir = 'downloaded_tiles'
        self.base_output_dir = 'pattern_verification'
        
        # Create base directories
        os.makedirs(self.base_download_dir, exist_ok=True)
        os.makedirs(f'{self.base_download_dir}/cities', exist_ok=True)
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
        
        # Initialize tile downloader with new structure
        if enable_download:
            self.tile_downloader = GulandTileDownloader(
                base_download_dir=self.base_download_dir,
                max_workers=max_workers,
                timeout=timeout
            )
        else:
            self.tile_downloader = None
            
        logger.info(f"🔍 Pattern-based crawler initialized")
        logger.info(f"👥 Workers: {self.max_workers}, Timeout: {self.timeout}s")
        logger.info(f"📥 Download enabled: {enable_download}")
        logger.info(f"📁 Download structure: downloaded_tiles/cities/<city>/qh-2030/<zoom>/")

    def create_city_folder_structure(self, city_name, zoom_level):
        """Create folder structure: downloaded_tiles/cities/<city>/qh-2030/<zoom>/"""
        # Clean city name for folder
        clean_city_name = self.clean_city_name(city_name)
        
        # Create full path: downloaded_tiles/cities/<city>/qh-2030/<zoom>/
        city_path = Path(self.base_download_dir) / 'cities' / clean_city_name / 'qh-2030' / str(zoom_level)
        city_path.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"📁 Created folder structure: {city_path}")
        return str(city_path)
    
    def clean_city_name(self, city_name):
        """Clean city name for folder creation"""
        # Remove special characters and spaces
        clean_name = city_name.lower()
        clean_name = clean_name.replace(' ', '_')
        clean_name = clean_name.replace('tp_', '')
        clean_name = clean_name.replace('tp ', '')
        clean_name = clean_name.replace('-', '_')
        
        # Map common city names
        city_mapping = {
            'ha_noi': 'hanoi',
            'hanoi': 'hanoi',
            'ho_chi_minh': 'hcm',
            'hcm': 'hcm',
            'saigon': 'hcm',
            'da_nang': 'danang',
            'danang': 'danang',
            'hai_phong': 'haiphong',
            'haiphong': 'haiphong',
            'can_tho': 'cantho',
            'cantho': 'cantho'
        }
        
        return city_mapping.get(clean_name, clean_name)

    def download_single_tile_with_structure(self, tile_info, city_name):
        """Download single tile with new folder structure"""
        try:
            # Get tile details
            url = tile_info['url']
            zoom = tile_info['zoom']
            x = tile_info['x']
            y = tile_info['y']
            
            # Detect format from URL
            if '.png' in url.lower():
                format_ext = 'png'
            elif '.jpg' in url.lower() or '.jpeg' in url.lower():
                format_ext = 'jpg'
            elif '.webp' in url.lower():
                format_ext = 'webp'
            else:
                format_ext = 'png'  # default
            
            # Create folder structure
            folder_path = self.create_city_folder_structure(city_name, zoom)
            
            # Create filename: <x>_<y>.<format>
            filename = f"{x}_{y}.{format_ext}"
            filepath = os.path.join(folder_path, filename)
            
            # Skip if file already exists
            if os.path.exists(filepath):
                file_size = os.path.getsize(filepath)
                logger.debug(f"⏭️ File exists: {filename} ({file_size} bytes)")
                return {
                    'success': True,
                    'filepath': filepath,
                    'size': file_size,
                    'tile_info': tile_info,
                    'status': 'already_exists'
                }
            
            # Download tile
            response = self.session.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                # Check if it's actually an image
                content_type = response.headers.get('content-type', '').lower()
                if any(img_type in content_type for img_type in ['image/', 'application/octet-stream']):
                    size = len(response.content)
                    
                    # Additional validation - check image size
                    if size > 100:  # Minimum size for valid tile
                        # Save file
                        with open(filepath, 'wb') as f:
                            f.write(response.content)
                        
                        with self.stats_lock:
                            self.stats['total_successful'] += 1
                            self.stats['total_bytes'] += size
                        
                        logger.debug(f"✅ Downloaded: {filename} ({size} bytes)")
                        
                        return {
                            'success': True,
                            'filepath': filepath,
                            'size': size,
                            'tile_info': tile_info,
                            'content_type': content_type,
                            'status': 'downloaded'
                        }
                
                # Invalid content
                with self.stats_lock:
                    self.stats['total_failed'] += 1
                
                return {
                    'success': False,
                    'reason': f'Invalid content type: {content_type}',
                    'tile_info': tile_info
                }
            else:
                # HTTP error
                with self.stats_lock:
                    self.stats['total_failed'] += 1
                
                return {
                    'success': False,
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
        except Exception as e:
            with self.stats_lock:
                self.stats['total_failed'] += 1
            return {
                'success': False,
                'reason': f'Error: {str(e)}',
                'tile_info': tile_info
            }

    def download_tiles_batch_with_structure(self, tile_urls, city_name):
        """Download batch of tiles with new folder structure"""
        if not tile_urls:
            return []
        
        logger.info(f"📥 Downloading {len(tile_urls)} tiles for {city_name}")
        
        results = []
        
        # Download tiles in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_tile = {
                executor.submit(self.download_single_tile_with_structure, tile_info, city_name): tile_info 
                for tile_info in tile_urls
            }
            
            for future in as_completed(future_to_tile):
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Log progress every 50 tiles
                    if len(results) % 50 == 0:
                        successful = len([r for r in results if r['success']])
                        logger.info(f"📊 Progress: {len(results)}/{len(tile_urls)} ({successful} successful)")
                        
                except Exception as e:
                    tile_info = future_to_tile[future]
                    logger.error(f"❌ Error processing tile {tile_info.get('x', '?')},{tile_info.get('y', '?')}: {e}")
                    results.append({
                        'success': False,
                        'reason': f'Processing error: {str(e)}',
                        'tile_info': tile_info
                    })
        
        # Update stats
        successful = len([r for r in results if r['success']])
        failed = len([r for r in results if not r['success']])
        
        with self.stats_lock:
            self.stats['total_attempted'] += len(tile_urls)
            # Note: individual download functions already update successful/failed counts
        
        logger.info(f"📊 Batch complete: {successful}/{len(tile_urls)} successful")
        
        return results

    def load_patterns_from_browser_results(self, city_name=None):
        """Load patterns from specific city or all cities - UPDATED for new structure"""
        if city_name:
            # Load patterns cho thành phố cụ thể từ new structure
            clean_city_name = city_name.replace(' ', '_').replace('TP ', '')
            city_reports_dir = f'output_browser_crawl/cities/{clean_city_name}/reports'
            
            patterns = set()
            
            try:
                # Try to find latest patterns report
                if os.path.exists(city_reports_dir):
                    pattern_files = list(Path(city_reports_dir).glob('patterns_*.json'))
                    if pattern_files:
                        # Get latest file
                        latest_file = max(pattern_files, key=os.path.getctime)
                        with open(latest_file, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        patterns.update(data.get('discovered_patterns', []))
                        logger.info(f"📋 Loaded {len(patterns)} patterns from {latest_file}")
                    else:
                        logger.warning(f"❌ No pattern files found for {city_name}")
                else:
                    logger.warning(f"❌ No reports directory for {city_name}")
                    
            except Exception as e:
                logger.error(f"❌ Error loading patterns for {city_name}: {e}")
                
            return list(patterns)
        else:
            # Load từ final report (tất cả patterns)
            return self.load_patterns_from_final_report()
        
    def load_patterns_from_final_report(self):
        """Load patterns from new final report structure"""
        patterns = set()
        
        # Try new final report location first
        final_report_path = 'output_browser_crawl/reports/final_patterns_report.json'
        try:
            with open(final_report_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            patterns.update(data.get('tile_patterns', []))
            logger.info(f"📋 Loaded {len(patterns)} patterns from final report")
            return list(patterns)
        except FileNotFoundError:
            logger.warning(f"⚠️ Final report not found at {final_report_path}")
        
        # Fallback: load from all city reports
        cities_dir = Path('output_browser_crawl/cities')
        if cities_dir.exists():
            for city_dir in cities_dir.iterdir():
                if city_dir.is_dir():
                    reports_dir = city_dir / 'reports'
                    if reports_dir.exists():
                        # Find latest patterns file for this city
                        pattern_files = list(reports_dir.glob('patterns_*.json'))
                        if pattern_files:
                            latest_file = max(pattern_files, key=os.path.getctime)
                            try:
                                with open(latest_file, 'r', encoding='utf-8') as f:
                                    data = json.load(f)
                                city_patterns = data.get('discovered_patterns', [])
                                patterns.update(city_patterns)
                                logger.info(f"📋 Added {len(city_patterns)} patterns from {city_dir.name}")
                            except Exception as e:
                                logger.warning(f"⚠️ Failed to load {latest_file}: {e}")
        
        logger.info(f"📋 Total patterns loaded from all cities: {len(patterns)}")
        return list(patterns)

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

    def crawl_pattern_for_city(self, pattern, city_coverage, city_name):
        """Exhaustive crawl - thử TẤT CẢ tiles có thể trong city coverage với NEW FOLDER STRUCTURE"""
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
                
                # Download tiles using NEW STRUCTURE
                batch_results = self.download_tiles_batch_with_structure(batch, city_name)
                
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
        """Detect city from URL pattern - ENHANCED with more city mappings"""
        pattern_lower = pattern.lower()
        
        # City mappings from URL patterns
        city_mappings = {
            # Major cities
            'ha-noi': 'hanoi',
            'hanoi': 'hanoi',
            'ho-chi-minh': 'hcm',
            'tp-ho-chi-minh': 'hcm',
            'hcm': 'hcm',
            'saigon': 'hcm',
            'da-nang': 'danang',
            'danang': 'danang',
            'hai-phong': 'haiphong',
            'haiphong': 'haiphong',
            'can-tho': 'cantho',
            'cantho': 'cantho',
            
            # All provinces/cities
            'dong-nai': 'dongnai',
            'ba-ria-vung-tau': 'baria_vungtau',
            'baria-vungtau': 'baria_vungtau',
            'an-giang': 'angiang',
            'bac-giang': 'bacgiang',
            'bac-kan': 'backan',
            'bac-lieu': 'baclieu',
            'bac-ninh': 'bacninh',
            'ben-tre': 'bentre',
            'binh-duong': 'binhduong',
            'binh-phuoc': 'binhphuoc',
            'binh-thuan': 'binhthuan',
            'binh-dinh': 'binhdinh',
            'ca-mau': 'camau',
            'cao-bang': 'caobang',
            'gia-lai': 'gialai',
            'ha-nam': 'hanam',
            'ha-giang': 'hagiang',
            'ha-tinh': 'hatinh',
            'hau-giang': 'haugiang',
            'hoa-binh': 'hoabinh',
            'hung-yen': 'hungyen',
            'khanh-hoa': 'khanhhoa',
            'kien-giang': 'kiengiang',
            'kon-tum': 'kontum',
            'lai-chau': 'laichau',
            'lam-dong': 'lamdong',
            'lang-son': 'langson',
            'lao-cai': 'laocai',
            'long-an': 'longan',
            'nam-dinh': 'namdinh',
            'nghe-an': 'nghean',
            'ninh-binh': 'ninhbinh',
            'ninh-thuan': 'ninhthuan',
            'phu-tho': 'phutho',
            'phu-yen': 'phuyen',
            'quang-binh': 'quangbinh',
            'quang-nam': 'quangnam',
            'quang-ngai': 'quangngai',
            'quang-ninh': 'quangninh',
            'quang-tri': 'quangtri',
            'soc-trang': 'soctrang',
            'son-la': 'sonla',
            'tay-ninh': 'tayninh',
            'thai-binh': 'thaibinh',
            'thai-nguyen': 'thainguyen',
            'thanh-hoa': 'thanhhoa',
            'thua-thien-hue': 'thuathienhue',
            'tien-giang': 'tiengiang',
            'tra-vinh': 'travinh',
            'tuyen-quang': 'tuyenquang',
            'vinh-long': 'vinhlong',
            'vinh-phuc': 'vinhphuc',
            'yen-bai': 'yenbai',
            'dak-lak': 'daklak',
            'dak-nong': 'daknong',
            'dien-bien': 'dienbien',
            'dong-thap': 'dongthap'
        }
        
        # Check each mapping
        for url_pattern, clean_name in city_mappings.items():
            if url_pattern in pattern_lower:
                return clean_name
        
        return None  # Unknown city

    def crawl_city_specific_patterns(self, patterns=None, zoom_levels=[10, 12, 14, 16], use_txt_source=True, skip_existing=True):
        """Simplified exhaustive crawling with NEW FOLDER STRUCTURE - cào hết tất cả tiles có thể"""
        
        # COMPLETE City coordinates (lat, lng, radius_km) for ALL Vietnamese provinces/cities
        city_coords = {
        # Major cities - Extra large radius
            'hanoi': (21.0285, 105.8542, 150),      # Hà Nội + vùng phụ cận
            'hcm': (10.8231, 106.6297, 200),       # HCM + toàn bộ vùng Đông Nam Bộ
            'danang': (16.0544563, 108.0717219, 120), # Đà Nẵng + vùng miền Trung
            'haiphong': (20.8449, 106.6881, 100),  # Hải Phòng + vùng ven biển
            'cantho': (10.0452, 105.7469, 120),    # Cần Thơ + ĐBSCL
            
            # All provinces - Large radius for complete coverage
            'dongnai': (11.0686, 107.1676, 150),
            'baria_vungtau': (10.5417, 107.2431, 100),
            'angiang': (10.3889, 105.4359, 120),
            'bacgiang': (21.2731, 106.1946, 100),
            'backan': (22.1474, 105.8348, 120),
            'baclieu': (9.2515, 105.7244, 100),
            'bacninh': (21.1861, 106.0763, 80),
            'bentre': (10.2433, 106.3756, 100),
            'binhduong': (11.3254, 106.4770, 120),
            'binhphuoc': (11.7511, 106.7234, 150),
            'binhthuan': (11.0904, 108.0721, 150),
            'binhdinh': (13.7757, 109.2219, 120),
            'camau': (9.1769, 105.1524, 150),       # Cà Mau - southernmost
            'caobang': (22.6666, 106.2639, 120),
            'gialai': (13.8078, 108.1094, 180),     # Gia Lai - tỉnh lớn
            'hanam': (20.5835, 105.9230, 80),
            'hagiang': (22.8025, 104.9784, 150),    # Hà Giang - northernmost
            'hatinh': (18.3560, 105.9069, 120),
            'haugiang': (9.7571, 105.6412, 100),
            'hoabinh': (20.8156, 105.3373, 150),
            'hungyen': (20.6464, 106.0511, 80),
            'khanhhoa': (12.2388, 109.1967, 120),
            'kiengiang': (10.0125, 105.0808, 200),  # Kiên Giang - có Phú Quốc
            'kontum': (14.3497, 108.0005, 150),
            'laichau': (22.3856, 103.4707, 150),
            'lamdong': (11.5753, 108.1429, 150),    # Lâm Đồng - cao nguyên
            'langson': (21.8537, 106.7610, 120),
            'laocai': (22.4809, 103.9755, 150),     # Lào Cai - có Sa Pa
            'longan': (10.6957, 106.2431, 100),
            'namdinh': (20.4341, 106.1675, 100),
            'nghean': (18.6745, 105.6905, 200),     # Nghệ An - tỉnh lớn nhất
            'ninhbinh': (20.2506, 105.9744, 100),
            'ninhthuan': (11.5645, 108.9899, 120),
            'phutho': (21.4208, 105.2045, 120),
            'phuyen': (13.0882, 109.0929, 100),
            'quangbinh': (17.4809, 106.6238, 150),
            'quangnam': (15.5394, 108.0191, 150),
            'quangngai': (15.1214, 108.8044, 120),
            'quangninh': (21.0064, 107.2925, 150),  # Quảng Ninh - có Hạ Long
            'quangtri': (16.7404, 107.1854, 100),
            'soctrang': (9.6002, 105.9800, 100),
            'sonla': (21.3256, 103.9188, 200),      # Sơn La - tỉnh lớn thứ 2
            'tayninh': (11.3100, 106.0989, 120),
            'thaibinh': (20.4500, 106.3400, 80),
            'thainguyen': (21.5944, 105.8480, 120),
            'thanhhoa': (19.8069, 105.7851, 180),   # Thanh Hóa - tỉnh lớn
            'thuathienhue': (16.4674, 107.5905, 120),
            'tiengiang': (10.4493, 106.3420, 100),
            'travinh': (9.9477, 106.3524, 100),
            'tuyenquang': (21.8267, 105.2280, 120),
            'vinhlong': (10.2397, 105.9571, 100),
            'vinhphuc': (21.3609, 105.6049, 100),
            'yenbai': (21.7168, 104.8986, 120),
            'daklak': (12.7100, 108.2378, 180),     # Đắk Lắk - tỉnh lớn Tây Nguyên
            'daknong': (12.2646, 107.6098, 150),
            'dienbien': (21.3847, 103.0175, 150),
            'dongthap': (10.4938, 105.6881, 120)
        }
        
        # Load patterns
        if patterns is None:
            if use_txt_source:
                logger.info("📋 Loading patterns from TXT coverage reports...")
                patterns = self.load_all_discovered_patterns_from_txt()
            else:
                logger.info("📋 Loading patterns from JSON files...")
                patterns = self.load_patterns_from_final_report()

        if not patterns:
            logger.error("❌ No patterns found! Run browser crawler first.")
            return []

        # Auto-assign patterns to cities
        city_pattern_mapping = self.auto_assign_patterns_to_cities(patterns)
        
        all_results = []
        skipped_cities = []
        
        for city_name, city_patterns_list in city_pattern_mapping.items():
            if city_name not in city_coords:
                logger.info(f"⚠️ Skipping {city_name} - coordinates not configured")
                continue
            
            # Check if city already downloaded
            if skip_existing:
                already_downloaded, status_msg = self.check_city_already_downloaded(city_name)
                if already_downloaded:
                    logger.info(f"⏭️ SKIPPING {city_name.upper()} - {status_msg}")
                    
                    # Get summary of existing tiles
                    summary = self.get_city_download_summary(city_name)
                    if summary:
                        logger.info(f"📊 Existing tiles: {summary['total_tiles']:,} tiles, {summary['total_size_mb']:.1f} MB")
                        zoom_info = ", ".join([f"Z{z}:{info['tiles']}" for z, info in summary['zoom_levels'].items()])
                        logger.info(f"📊 Zoom breakdown: {zoom_info}")
                    
                    skipped_cities.append({
                        'city': city_name,
                        'reason': 'already_downloaded',
                        'summary': summary
                    })
                    continue
                else:
                    logger.info(f"📂 {city_name.upper()} status: {status_msg}")
            
            lat, lng, radius_km = city_coords[city_name]
            logger.info(f"\n🏙️ CRAWLING CITY: {city_name.upper()}")
            logger.info(f"📍 Center: {lat}, {lng} (radius: {radius_km}km)")
            logger.info(f"🔍 Found {len(city_patterns_list)} patterns for {city_name}")
            logger.info(f"📁 Tiles will be saved to: downloaded_tiles/cities/{self.clean_city_name(city_name)}/qh-2030/<zoom>/")
            
            # Generate city-specific coverage
            city_coverage = self.generate_city_tile_coverage(lat, lng, zoom_levels, radius_km)
            
            city_results = []
            for pattern in city_patterns_list:
                logger.info(f"🚀 Exhaustive crawling pattern: {pattern}")
                
                # NO VALIDATION - Just crawl everything with NEW FOLDER STRUCTURE
                city_tiles = self.crawl_pattern_for_city(pattern, city_coverage, city_name)
                
                if city_tiles:
                    city_results.append({
                        'pattern': pattern,
                        'tiles': city_tiles,
                        'successful_count': len([t for t in city_tiles if t.get('success')])
                    })
            
            if city_results:
                total_tiles = sum(len(r['tiles']) for r in city_results)
                successful_tiles = sum(r['successful_count'] for r in city_results);
                
                all_results.append({
                    'city': city_name,
                    'coordinates': (lat, lng, radius_km),
                    'coverage': city_coverage,
                    'pattern_results': city_results,
                    'total_tiles': total_tiles,
                    'successful_tiles': successful_tiles,
                    'folder_structure': f"downloaded_tiles/cities/{self.clean_city_name(city_name)}/qh-2030/<zoom>/"
                })
                
                logger.info(f"✅ {city_name} complete: {successful_tiles}/{total_tiles} tiles downloaded")
        
        # Log summary of skipped cities
        if skipped_cities:
            logger.info(f"\n📋 SKIPPED CITIES SUMMARY:")
            total_existing_tiles = 0
            total_existing_size = 0
            
            for skipped in skipped_cities:
                if skipped['summary']:
                    tiles = skipped['summary']['total_tiles']
                    size_mb = skipped['summary']['total_size_mb']
                    total_existing_tiles += tiles
                    total_existing_size += size_mb
                    logger.info(f"  ⏭️ {skipped['city']}: {tiles:,} tiles ({size_mb:.1f} MB)")
            
            logger.info(f"📊 Total existing: {total_existing_tiles:,} tiles ({total_existing_size:.1f} MB)")
        
        return all_results

    def generate_city_focused_report(self, city_results, start_time, skipped_cities=None):
        """Generate report for city-focused crawl with NEW FOLDER STRUCTURE info"""
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
        
        # Calculate skipped cities info
        skipped_count = len(skipped_cities) if skipped_cities else 0
        skipped_tiles = sum(s.get('summary', {}).get('total_tiles', 0) for s in (skipped_cities or []))
        skipped_size_mb = sum(s.get('summary', {}).get('total_size_mb', 0) for s in (skipped_cities or []))
        
        report = {
            'crawl_type': 'City-Focused Pattern Crawl with New Folder Structure',
            'folder_structure': 'downloaded_tiles/cities/<city_name>/qh-2030/<zoom_level>/',
            'timestamp': datetime.now().isoformat(),
            'execution_time_seconds': elapsed_time,
            'execution_time_minutes': elapsed_time / 60,
            'summary': {
                'cities_crawled': total_cities,
                'cities_skipped': skipped_count,
                'patterns_used': total_patterns,
                'total_tiles_tested': total_tiles,
                'total_tiles_successful': total_successful,
                'overall_success_rate': total_successful / total_tiles * 100 if total_tiles > 0 else 0,
                'total_download_size_mb': total_size_mb,
                'average_tiles_per_city': total_tiles / total_cities if total_cities > 0 else 0,
                'existing_tiles_count': skipped_tiles,
                'existing_tiles_size_mb': skipped_size_mb
            },
            'city_results': city_results,
            'skipped_cities': skipped_cities or [],
            'performance_stats': self.stats.copy()
        }
        
        # Save comprehensive report
        report_file = f"{self.base_output_dir}/city_focused_crawl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text summary
        text_summary = f"""
# CITY-FOCUSED PATTERN CRAWL REPORT (NEW FOLDER STRUCTURE)
Generated: {report['timestamp']}
Duration: {elapsed_time/60:.1f} minutes

## 📁 NEW FOLDER STRUCTURE
Tiles are now saved in: downloaded_tiles/cities/<city_name>/qh-2030/<zoom_level>/
Format: <x>_<y>.<format> (e.g., 3249_1865.png)

## 🎯 MISSION: Find tiles for specific cities with organized structure

## 📊 OVERALL SUMMARY
• Cities crawled: {total_cities}
• Cities skipped (already downloaded): {skipped_count}
• Patterns used: {total_patterns}  
• Total tiles tested: {total_tiles:,}
• Successful downloads: {total_successful:,}
• Success rate: {report['summary']['overall_success_rate']:.1f}%
• Total size downloaded: {total_size_mb:.1f} MB
• Average tiles per city: {report['summary']['average_tiles_per_city']:.0f}

## 📊 EXISTING TILES (SKIPPED)
• Existing tiles: {skipped_tiles:,}
• Existing size: {skipped_size_mb:.1f} MB

## 🏙️ CITY BREAKDOWN
"""
        
        for result in city_results:
            city_name = result['city']
            success_rate = (result['successful_tiles'] / result['total_tiles'] * 100) if result['total_tiles'] > 0 else 0
            folder_path = result.get('folder_structure', f"downloaded_tiles/cities/{self.clean_city_name(city_name)}/qh-2030/<zoom>/")
            text_summary += f"""
🏙️ {city_name.upper()} (CRAWLED)
Coordinates: {result['coordinates']}
Patterns tested: {len(result['pattern_results'])}
Tiles tested: {result['total_tiles']:,}
Successful: {result['successful_tiles']:,} ({success_rate:.1f}%)
Folder: {folder_path}

"""
        
        # Add skipped cities section
        if skipped_cities:
            text_summary += "\n## ⏭️ SKIPPED CITIES (ALREADY DOWNLOADED)\n"
            for skipped in skipped_cities:
                summary = skipped.get('summary', {})
                if summary:
                    text_summary += f"""
⏭️ {skipped['city'].upper()} (SKIPPED)
Existing tiles: {summary.get('total_tiles', 0):,}
Existing size: {summary.get('total_size_mb', 0):.1f} MB
Zoom levels: {len(summary.get('zoom_levels', {}))}

"""
        
        text_summary += f"""
## 📁 FOLDER STRUCTURE EXAMPLE
downloaded_tiles/
├── cities/
│   ├── hanoi/
│   │   └── qh-2030/
│   │       ├── 10/ (contains 812_466.png, etc.)"
│   │       ├── 12/ (contains 3249_1865.png, etc.)"
│   │       └── 14/
│   ├── danang/
│   │   └── qh-2030/
│   │       └── ...
│   └── hcm/
│       └── qh-2030/
│           └── ...
"""
        
        text_file = f"{self.base_output_dir}/city_focused_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(text_file, 'w', encoding='utf-8') as f:
            f.write(text_summary)
        
        # Print summary
        print(f"\n🎉 CITY-FOCUSED CRAWL COMPLETE!")
        print("=" * 50)
        print(f"⏱️ Duration: {elapsed_time/60:.1f} minutes")
        print(f"🏙️ Cities crawled: {total_cities}")
        print(f"⏭️ Cities skipped: {skipped_count}")
        if skipped_count > 0:
            print(f"📊 Existing tiles: {skipped_tiles:,} ({skipped_size_mb:.1f} MB)")
        print(f"🔍 Patterns: {total_patterns}")
        print(f"📊 New tiles tested: {total_tiles:,}")
        print(f"✅ New downloaded: {total_successful:,} ({report['summary']['overall_success_rate']:.1f}%)")
        print(f"💾 New size: {total_size_mb:.1f} MB")
        print(f"📁 Structure: downloaded_tiles/cities/<city>/qh-2030/<zoom>/")
        
        # Print city breakdown
        for result in city_results:
            success_rate = (result['successful_tiles'] / result['total_tiles'] * 100) if result['total_tiles'] > 0 else 0
            print(f"  🏙️ {result['city']}: {result['successful_tiles']:,} tiles ({success_rate:.1f}%)")
        
        # Print skipped cities
        if skipped_cities:
            print(f"\n⏭️ Skipped cities:")
            for skipped in skipped_cities:
                summary = skipped.get('summary', {})
                if summary:
                    print(f"  ⏭️ {skipped['city']}: {summary.get('total_tiles', 0):,} existing tiles")
        
        logger.info(f"📋 Reports saved:")
        logger.info(f"  JSON: {report_file}")
        logger.info(f"  Text: {text_file}")
        
        return report

    def load_all_discovered_patterns_from_txt(self):
        """Load patterns from TXT coverage reports - NEW METHOD"""
        patterns = set()
        
        # Look for TXT coverage reports in output_browser_crawl structure
        cities_dir = Path('output_browser_crawl/cities')
        
        if not cities_dir.exists():
            logger.warning(f"❌ Cities directory not found: {cities_dir}")
            return []
        
        for city_dir in cities_dir.iterdir():
            if city_dir.is_dir():
                reports_dir = city_dir / 'reports'
                if reports_dir.exists():
                    # Look for coverage TXT files
                    txt_files = list(reports_dir.glob('coverage_*.txt'))
                    
                    for txt_file in txt_files:
                        try:
                            logger.debug(f"📋 Reading patterns from {txt_file}")
                            
                            with open(txt_file, 'r', encoding='utf-8') as f:
                                content = f.read()
                            
                            # Extract patterns from TXT content
                            lines = content.split('\n')
                            in_patterns_section = False
                            
                            for line in lines:
                                line = line.strip()
                                
                                # Look for patterns section
                                if 'discovered patterns:' in line.lower() or 'patterns found:' in line.lower():
                                    in_patterns_section = True
                                    continue
                                
                                # Stop at next section
                                if in_patterns_section and line.startswith('##'):
                                    in_patterns_section = False
                                    continue
                                
                                # Extract pattern URLs
                                if in_patterns_section and line:
                                    # Skip numbered list markers and bullet points
                                    if line.startswith(('•', '-', '*')):
                                        line = line[1:].strip()
                                    
                                    # Extract URL from line
                                    if 'http' in line:
                                        # Handle different formats:
                                        # "1. https://example.com/{z}/{x}/{y}.png"
                                        # "• https://example.com/{z}/{x}/{y}.png - Status: 200"
                                        # "https://example.com/{z}/{x}/{y}.png"
                                        
                                        # Remove numbering
                                        import re
                                        url_match = re.search(r'(https?://[^\s]+)', line)
                                        if url_match:
                                            url = url_match.group(1)
                                            
                                            # Clean up URL (remove trailing punctuation)
                                            url = url.rstrip('.,;-')
                                            
                                            # Validate it looks like a tile pattern
                                            if all(placeholder in url for placeholder in ['{z}', '{x}', '{y}']):
                                                patterns.add(url)
                                                logger.debug(f"  ✅ Found pattern: {url}")
                    
                        except Exception as e:
                            logger.warning(f"⚠️ Error reading {txt_file}: {e}")
                            continue
    
        logger.info(f"📋 Loaded {len(patterns)} unique patterns from TXT reports")
        
        # If no patterns found from TXT, fallback to JSON
        if not patterns:
            logger.warning("⚠️ No patterns found in TXT files, falling back to JSON")
            return self.load_patterns_from_final_report()
        
        return list(patterns)

    def check_city_already_downloaded(self, city_name):
        """Check if city already has downloaded tiles - NEW METHOD"""
        clean_city_name = self.clean_city_name(city_name)
        city_path = Path(self.base_download_dir) / 'cities' / clean_city_name / 'qh-2030'
        
        if not city_path.exists():
            return False, "No download folder found"
        
        # Check if any zoom folders exist with tiles
        zoom_folders = [d for d in city_path.iterdir() if d.is_dir() and d.name.isdigit()]
        
        if not zoom_folders:
            return False, "No zoom folders found"
        
        # Count total tiles
        total_tiles = 0
        for zoom_folder in zoom_folders:
            tile_files = list(zoom_folder.glob('*.*'))
            total_tiles += len(tile_files)
    
        if total_tiles == 0:
            return False, "Zoom folders exist but no tiles found"
    
        return True, f"Found {total_tiles:,} tiles in {len(zoom_folders)} zoom levels"

    def get_city_download_summary(self, city_name):
        """Get summary of already downloaded tiles for a city - NEW METHOD"""
        clean_city_name = self.clean_city_name(city_name)
        city_path = Path(self.base_download_dir) / 'cities' / clean_city_name / 'qh-2030'
        
        if not city_path.exists():
            return None
        
        summary = {
            'city': city_name,
            'clean_name': clean_city_name,
            'path': str(city_path),
            'zoom_levels': {},
            'total_tiles': 0,
            'total_size_bytes': 0,
            'total_size_mb': 0
        }
        
        # Check each zoom folder
        zoom_folders = [d for d in city_path.iterdir() if d.is_dir() and d.name.isdigit()]
        
        for zoom_folder in zoom_folders:
            zoom_level = zoom_folder.name
            tile_files = list(zoom_folder.glob('*.*'))
            
            zoom_tiles = len(tile_files)
            zoom_size = sum(f.stat().st_size for f in tile_files if f.is_file())
            
            summary['zoom_levels'][zoom_level] = {
                'tiles': zoom_tiles,
                'size_bytes': zoom_size,
                'size_mb': zoom_size / 1024 / 1024
            }
            
            summary['total_tiles'] += zoom_tiles
            summary['total_size_bytes'] += zoom_size
    
        summary['total_size_mb'] = summary['total_size_bytes'] / 1024 / 1024
    
        return summary

# Update main function to add skip option
def main():
    print("🚀 GULAND EXHAUSTIVE TILE CRAWLER v1.1")
    print("Downloads ALL available tiles with organized folder structure")
    print("📁 NEW: downloaded_tiles/cities/<city>/qh-2030/<zoom>/")
    print("=" * 60)
    
    # Source selection
    source_choice = input("Pattern source (1=TXT reports, 2=JSON files, default=1): ").strip()
    use_txt_source = source_choice != '2'
    
    if use_txt_source:
        print("📋 Using TXT coverage reports as pattern source")
    else:
        print("📋 Using JSON files as pattern source")
    
    # Skip existing option
    skip_choice = input("Skip cities that already have downloaded tiles? (y/n, default=y): ").lower()
    skip_existing = skip_choice != 'n'
    
    if skip_existing:
        print("⏭️ Will skip cities with existing tiles")
    else:
        print("🔄 Will re-download all cities (may overwrite existing)")
    
    # Download mode (recommended for exhaustive approach)
    download_choice = input("Enable tile downloads? (y/n, default=y): ").lower()
    enable_download = download_choice != 'n'
    
    if enable_download:
        print("✅ Download mode - will save all found tiles with new structure")
        print("📁 Structure: downloaded_tiles/cities/<city>/qh-2030/<zoom>/")
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
    
    print(f"\n📁 Tiles will be organized as:")
    print("downloaded_tiles/")
    print("├── cities/")
    print("│   ├── hanoi/")
    print("│   │   └── qh-2030/")
    print("│   │       ├── 10/ (contains 812_466.png, etc.)")
    print("│   │       ├── 12/ (contains 3249_1865.png, etc.)")
    print("│   │       └── 14/")
    print("│   ├── danang/")
    print("│   └── hcm/")
    print()
    
    # Run exhaustive crawl
    start_time = time.time()
    
    results = crawler.crawl_city_specific_patterns(
        patterns=None,
        zoom_levels=zoom_levels, 
        use_txt_source=use_txt_source,
        skip_existing=skip_existing
    )
    
    if not results:
        logger.error("❌ No results from exhaustive crawling")
        return
    
    # Generate report
    report = crawler.generate_city_focused_report(results, start_time)
    
    if report:
        total_tiles = report['summary']['total_tiles_successful']
        existing_tiles = report['summary']['existing_tiles_count']
        print(f"\n🎉 EXHAUSTIVE CRAWL COMPLETE!")
        print(f"📈 Downloaded {total_tiles:,} NEW tiles")
        if existing_tiles > 0:
            print(f"📊 Skipped {existing_tiles:,} existing tiles")
        print(f"📁 Check downloaded_tiles/cities/<city>/qh-2030/<zoom>/ for results")

if __name__ == "__main__":
    main()