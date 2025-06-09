#!/usr/bin/env python3
"""
Systematic Tile Downloader for Guland
Download map tiles based on geocoding data discovered

Usage: python tile_downloader.py
"""

import requests
import json
import os
import time
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import logging
from datetime import datetime
import sqlite3
import hashlib
from urllib.parse import urlparse
import threading

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tile_downloader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GulandTileDownloader:
    def __init__(self, data_file='output_geocoding/all_locations_data.json'):
        self.data_file = data_file
        self.locations_data = self.load_locations_data()
        
        # Download statistics
        self.stats = {
            'total_downloaded': 0,
            'total_failed': 0,
            'total_skipped': 0,
            'start_time': None,
            'servers_used': set(),
            'bytes_downloaded': 0
        }
        
        # Thread-safe session
        self.session_lock = threading.Lock()
        
        # Create output structure
        self.setup_output_directories()
        
        # Setup database for tracking
        self.setup_database()
        
        # Setup request session
        self.setup_session()
    
    def load_locations_data(self):
        """Load geocoding data từ previous crawl"""
        try:
            with open(self.data_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            locations = data.get('all_locations', [])
            logger.info(f"✅ Loaded data for {len(locations)} locations")
            return locations
            
        except Exception as e:
            logger.error(f"❌ Could not load data file {self.data_file}: {e}")
            return []
    
    def setup_output_directories(self):
        """Create organized directory structure"""
        self.base_dir = Path('guland_tiles_download')
        self.base_dir.mkdir(exist_ok=True)
        
        # Create directories for each layer type
        self.layer_dirs = {
            'quy_hoach_2030': self.base_dir / 'quy_hoach_2030',
            'quy_hoach_2025': self.base_dir / 'quy_hoach_2025', 
            'quy_hoach_phan_khu': self.base_dir / 'quy_hoach_phan_khu',
            'quy_hoach_xay_dung': self.base_dir / 'quy_hoach_xay_dung',
            'land_use': self.base_dir / 'land_use',
            'other': self.base_dir / 'other'
        }
        
        for layer_dir in self.layer_dirs.values():
            layer_dir.mkdir(exist_ok=True)
        
        # Metadata directory
        self.metadata_dir = self.base_dir / 'metadata'
        self.metadata_dir.mkdir(exist_ok=True)
        
        logger.info(f"📁 Output directory: {self.base_dir}")
    
    def setup_database(self):
        """Setup SQLite database để track downloads"""
        self.db_path = self.base_dir / 'download_tracking.db'
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS tiles (
                    id INTEGER PRIMARY KEY,
                    location TEXT,
                    layer_name TEXT,
                    layer_type TEXT,
                    zoom INTEGER,
                    x INTEGER,
                    y INTEGER,
                    url TEXT,
                    file_path TEXT,
                    file_size INTEGER,
                    download_status TEXT,
                    download_time TIMESTAMP,
                    hash TEXT,
                    UNIQUE(location, layer_name, zoom, x, y)
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_location_layer 
                ON tiles(location, layer_name)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_download_status 
                ON tiles(download_status)
            ''')
        
        logger.info("✅ Database setup completed")
    
    def setup_session(self):
        """Setup requests session với proper headers"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
            'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Referer': 'https://guland.vn/'
        })
    
    def calculate_vietnam_tile_bounds(self, zoom_level):
        """Calculate tile bounds cho toàn bộ Việt Nam"""
        # Vietnam bounding box
        vietnam_bounds = {
            'north': 23.393395,   # Northernmost point
            'south': 8.560168,    # Southernmost point  
            'west': 102.144778,   # Westernmost point
            'east': 109.464638    # Easternmost point
        }
        
        def deg2num(lat_deg, lon_deg, zoom):
            lat_rad = math.radians(lat_deg)
            n = 2.0 ** zoom
            x = int((lon_deg + 180.0) / 360.0 * n)
            y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
            return (x, y)
        
        min_x, max_y = deg2num(vietnam_bounds['north'], vietnam_bounds['west'], zoom_level)
        max_x, min_y = deg2num(vietnam_bounds['south'], vietnam_bounds['east'], zoom_level)
        
        return {
            'min_x': min_x,
            'max_x': max_x,
            'min_y': min_y,
            'max_y': max_y,
            'total_tiles': (max_x - min_x + 1) * (max_y - min_y + 1)
        }
    
    def get_layer_directory(self, layer_type, layer_name):
        """Get appropriate directory cho layer"""
        layer_name_lower = layer_name.lower()
        
        if '2030' in layer_name_lower or 'qh_2030' in layer_type:
            return self.layer_dirs['quy_hoach_2030']
        elif '2025' in layer_name_lower or 'kh_2025' in layer_type:
            return self.layer_dirs['quy_hoach_2025']
        elif 'phan_khu' in layer_name_lower or 'qhpk' in layer_type:
            return self.layer_dirs['quy_hoach_phan_khu']
        elif 'xay_dung' in layer_name_lower or 'qhxd' in layer_type:
            return self.layer_dirs['quy_hoach_xay_dung']
        elif 'land' in layer_type or 'dat' in layer_name_lower:
            return self.layer_dirs['land_use']
        else:
            return self.layer_dirs['other']
    
    def download_single_tile(self, location_name, layer_name, layer_info, zoom, x, y):
        """Download một tile và save vào database"""
        
        # Generate tile URL
        url_template = layer_info['url']
        tile_url = url_template.replace('{z}', str(zoom)).replace('{x}', str(x)).replace('{y}', str(y))
        
        # Determine file path
        layer_type = layer_info.get('type', 'unknown')
        layer_dir = self.get_layer_directory(layer_type, layer_name)
        
        location_clean = location_name.replace(' ', '_').replace('-', '_')
        layer_clean = layer_name.replace(' ', '_').replace('-', '_')
        
        tile_filename = f"{location_clean}_{layer_clean}_{zoom}_{x}_{y}.png"
        tile_path = layer_dir / tile_filename
        
        # Check if already downloaded
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                'SELECT download_status FROM tiles WHERE location=? AND layer_name=? AND zoom=? AND x=? AND y=?',
                (location_name, layer_name, zoom, x, y)
            )
            result = cursor.fetchone()
            
            if result and result[0] == 'success':
                self.stats['total_skipped'] += 1
                return 'skipped'
        
        try:
            # Download tile
            with self.session_lock:
                response = self.session.get(tile_url, timeout=15)
            
            if response.status_code == 200:
                # Save tile
                with open(tile_path, 'wb') as f:
                    f.write(response.content)
                
                # Calculate hash
                file_hash = hashlib.md5(response.content).hexdigest()
                file_size = len(response.content)
                
                # Record in database
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute('''
                        INSERT OR REPLACE INTO tiles 
                        (location, layer_name, layer_type, zoom, x, y, url, file_path, file_size, download_status, download_time, hash)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (location_name, layer_name, layer_type, zoom, x, y, tile_url, 
                          str(tile_path), file_size, 'success', datetime.now().isoformat(), file_hash))
                
                # Update stats
                self.stats['total_downloaded'] += 1
                self.stats['bytes_downloaded'] += file_size
                
                # Track server
                server = urlparse(tile_url).netloc
                self.stats['servers_used'].add(server)
                
                return 'success'
                
            else:
                # Record failure
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute('''
                        INSERT OR REPLACE INTO tiles 
                        (location, layer_name, layer_type, zoom, x, y, url, download_status, download_time)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (location_name, layer_name, layer_type, zoom, x, y, tile_url, 
                          f'failed_{response.status_code}', datetime.now().isoformat()))
                
                self.stats['total_failed'] += 1
                return f'failed_{response.status_code}'
                
        except Exception as e:
            # Record error
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO tiles 
                    (location, layer_name, layer_type, zoom, x, y, url, download_status, download_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (location_name, layer_name, layer_type, zoom, x, y, tile_url, 
                      f'error_{str(e)[:50]}', datetime.now().isoformat()))
            
            self.stats['total_failed'] += 1
            return f'error_{str(e)}'
    
    def download_layer_tiles(self, location_name, layer_name, layer_info, zoom_range=(10, 16), max_workers=4):
        """Download tất cả tiles cho một layer"""
        logger.info(f"🔽 Downloading {layer_name} for {location_name} (zoom {zoom_range[0]}-{zoom_range[1]})")
        
        total_tasks = 0
        successful_tasks = 0
        
        for zoom in range(zoom_range[0], zoom_range[1] + 1):
            # Calculate tile bounds for this zoom level
            bounds = self.calculate_vietnam_tile_bounds(zoom)
            
            # Create download tasks
            tasks = []
            for x in range(bounds['min_x'], bounds['max_x'] + 1):
                for y in range(bounds['min_y'], bounds['max_y'] + 1):
                    tasks.append((location_name, layer_name, layer_info, zoom, x, y))
            
            logger.info(f"  Zoom {zoom}: {len(tasks)} tiles to download")
            total_tasks += len(tasks)
            
            # Download tiles với thread pool
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(self.download_single_tile, *task) for task in tasks]
                
                completed = 0
                for future in as_completed(futures):
                    result = future.result()
                    completed += 1
                    
                    if result == 'success':
                        successful_tasks += 1
                    
                    # Progress update
                    if completed % 100 == 0:
                        logger.info(f"    Progress: {completed}/{len(tasks)} tiles ({completed/len(tasks)*100:.1f}%)")
                    
                    # Rate limiting
                    time.sleep(0.1)
        
        logger.info(f"✅ Completed {layer_name}: {successful_tasks}/{total_tasks} successful")
        return successful_tasks, total_tasks
    
    def download_location_data(self, location_data, zoom_range=(10, 16), selected_layers=None):
        """Download tất cả data cho một location"""
        location_name = location_data['location_name']
        layers = location_data['layers']
        
        logger.info(f"📍 Processing location: {location_name} ({len(layers)} layers)")
        
        location_stats = {
            'location': location_name,
            'total_layers': len(layers),
            'processed_layers': 0,
            'successful_downloads': 0,
            'total_attempts': 0
        }
        
        for layer_name, layer_info in layers.items():
            if selected_layers and layer_name not in selected_layers:
                continue
            
            try:
                successful, total = self.download_layer_tiles(
                    location_name, layer_name, layer_info, zoom_range
                )
                
                location_stats['processed_layers'] += 1
                location_stats['successful_downloads'] += successful
                location_stats['total_attempts'] += total
                
            except Exception as e:
                logger.error(f"❌ Error processing layer {layer_name}: {e}")
        
        return location_stats
    
    def generate_download_report(self):
        """Generate báo cáo download"""
        logger.info("📊 Generating download report...")
        
        # Get statistics from database
        with sqlite3.connect(self.db_path) as conn:
            # Overall stats
            cursor = conn.execute('SELECT download_status, COUNT(*) FROM tiles GROUP BY download_status')
            status_counts = dict(cursor.fetchall())
            
            # Size statistics
            cursor = conn.execute('SELECT SUM(file_size) FROM tiles WHERE download_status = "success"')
            total_size = cursor.fetchone()[0] or 0
            
            # Layer statistics
            cursor = conn.execute('SELECT layer_type, COUNT(*) FROM tiles GROUP BY layer_type')
            layer_counts = dict(cursor.fetchall())
            
            # Location statistics
            cursor = conn.execute('SELECT location, COUNT(*) FROM tiles GROUP BY location')
            location_counts = dict(cursor.fetchall())
        
        # Calculate time elapsed
        time_elapsed = time.time() - self.stats['start_time'] if self.stats['start_time'] else 0
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'download_statistics': {
                'total_downloaded': status_counts.get('success', 0),
                'total_failed': sum(count for status, count in status_counts.items() 
                                  if status != 'success' and status != 'skipped'),
                'total_skipped': status_counts.get('skipped', 0),
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'time_elapsed_seconds': round(time_elapsed, 2),
                'download_rate_tiles_per_minute': round((status_counts.get('success', 0) / (time_elapsed / 60)), 2) if time_elapsed > 0 else 0
            },
            'layer_statistics': layer_counts,
            'location_statistics': location_counts,
            'servers_used': list(self.stats['servers_used']),
            'status_breakdown': status_counts
        }
        
        # Save report
        with open(self.base_dir / 'download_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        # Generate text report
        self.generate_text_report(report)
        
        return report
    
    def generate_text_report(self, report):
        """Generate human-readable report"""
        stats = report['download_statistics']
        
        text = f"""
# GULAND TILE DOWNLOAD REPORT
Generated: {report['timestamp']}

## 📊 DOWNLOAD STATISTICS
✅ Successfully Downloaded: {stats['total_downloaded']:,} tiles
❌ Failed Downloads: {stats['total_failed']:,} tiles  
⏭️ Skipped (already exists): {stats['total_skipped']:,} tiles
💾 Total Size: {stats['total_size_mb']:,.2f} MB
⏱️ Time Elapsed: {stats['time_elapsed_seconds']:,.1f} seconds
🚀 Download Rate: {stats['download_rate_tiles_per_minute']:,.1f} tiles/minute

## 🗺️ LAYER BREAKDOWN
"""
        
        for layer_type, count in report['layer_statistics'].items():
            text += f"• {layer_type}: {count:,} tiles\n"
        
        text += "\n## 📍 TOP LOCATIONS BY TILES\n"
        sorted_locations = sorted(report['location_statistics'].items(), 
                                key=lambda x: x[1], reverse=True)
        
        for location, count in sorted_locations[:10]:
            text += f"• {location}: {count:,} tiles\n"
        
        text += f"\n## 🌐 TILE SERVERS USED\n"
        for server in report['servers_used']:
            text += f"• {server}\n"
        
        text += f"\n## 📁 OUTPUT STRUCTURE\n"
        text += f"• guland_tiles_download/quy_hoach_2030/ - Planning 2030 tiles\n"
        text += f"• guland_tiles_download/quy_hoach_2025/ - Planning 2025 tiles\n"
        text += f"• guland_tiles_download/quy_hoach_phan_khu/ - Zoning tiles\n"
        text += f"• guland_tiles_download/quy_hoach_xay_dung/ - Construction planning tiles\n"
        text += f"• guland_tiles_download/land_use/ - Land use tiles\n"
        text += f"• guland_tiles_download/metadata/ - Download metadata\n"
        
        with open(self.base_dir / 'download_report.txt', 'w', encoding='utf-8') as f:
            f.write(text)
    
    def run_systematic_download(self, selected_locations=None, zoom_range=(10, 16), 
                              selected_layers=None, max_concurrent_locations=2):
        """Run systematic download cho tất cả locations"""
        logger.info("🚀 STARTING SYSTEMATIC TILE DOWNLOAD")
        logger.info("=" * 70)
        
        self.stats['start_time'] = time.time()
        
        if not self.locations_data:
            logger.error("❌ No location data loaded!")
            return
        
        # Filter locations if specified
        if selected_locations:
            locations_to_process = [loc for loc in self.locations_data 
                                  if loc['location_name'] in selected_locations]
        else:
            locations_to_process = self.locations_data
        
        logger.info(f"📍 Processing {len(locations_to_process)} locations")
        logger.info(f"🎯 Zoom range: {zoom_range[0]}-{zoom_range[1]}")
        
        all_location_stats = []
        
        # Process locations
        for i, location_data in enumerate(locations_to_process, 1):
            logger.info(f"\n📍 Location {i}/{len(locations_to_process)}: {location_data['location_name']}")
            
            try:
                location_stats = self.download_location_data(
                    location_data, zoom_range, selected_layers
                )
                all_location_stats.append(location_stats)
                
                # Print progress
                logger.info(f"✅ Completed {location_data['location_name']}: "
                          f"{location_stats['successful_downloads']}/{location_stats['total_attempts']} tiles")
                
            except Exception as e:
                logger.error(f"❌ Failed to process {location_data['location_name']}: {e}")
        
        # Generate final report
        logger.info("\n📊 Generating final report...")
        report = self.generate_download_report()
        
        # Print summary
        print(f"\n🎉 SYSTEMATIC DOWNLOAD COMPLETED!")
        print("=" * 70)
        print(f"📊 Final Statistics:")
        print(f"  • Total Downloaded: {report['download_statistics']['total_downloaded']:,} tiles")
        print(f"  • Total Size: {report['download_statistics']['total_size_mb']:,.2f} MB")
        print(f"  • Time Elapsed: {report['download_statistics']['time_elapsed_seconds']:,.1f} seconds")
        print(f"  • Download Rate: {report['download_statistics']['download_rate_tiles_per_minute']:,.1f} tiles/min")
        
        print(f"\n📁 Check '{self.base_dir}' for:")
        print("  • Downloaded tiles organized by layer type")
        print("  • download_report.txt (summary)")
        print("  • download_tracking.db (detailed tracking)")
        
        return report

def main():
    """Main function"""
    print("🔽 GULAND SYSTEMATIC TILE DOWNLOADER")
    print("Downloads map tiles based on geocoding discovery")
    print("=" * 70)
    
    # Check if geocoding data exists
    if not os.path.exists('output_geocoding/all_locations_data.json'):
        print("❌ Geocoding data not found!")
        print("Please run the geocoding crawler first:")
        print("python geocoding_crawler.py")
        return
    
    downloader = GulandTileDownloader()
    
    if not downloader.locations_data:
        print("❌ No location data loaded!")
        return
    
    print(f"✅ Loaded data for {len(downloader.locations_data)} locations")
    
    # Configuration
    zoom_range = (10, 14)  # Start conservative  
    selected_locations = None  # Download all locations
    # selected_locations = ["Đà Nẵng", "Hà Nội"]  # Or specify specific locations
    
    try:
        report = downloader.run_systematic_download(
            selected_locations=selected_locations,
            zoom_range=zoom_range
        )
        
        print("\n✅ Download completed successfully!")
        print("💡 You now have systematic backup of Guland map data!")
        
    except KeyboardInterrupt:
        print("\n⏹️ Download interrupted by user")
        print("💡 Progress has been saved. You can resume later.")
    except Exception as e:
        print(f"\n❌ Download failed: {e}")

if __name__ == "__main__":
    main()