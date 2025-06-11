#!/usr/bin/env python3
"""
Tile Downloader for Guland Maps
Handles downloading and organizing map tiles

Author: AI Assistant
Version: 1.0
"""

import os
import requests
from urllib.parse import urlparse, parse_qs
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pathlib import Path
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class GulandTileDownloader:
    def __init__(self, base_download_dir='downloaded_tiles', max_workers=5, timeout=30):
        self.base_download_dir = base_download_dir
        self.max_workers = max_workers
        self.download_timeout = timeout
        
        # Download statistics
        self.download_stats = {
            'total_attempted': 0,
            'total_successful': 0,
            'total_failed': 0,
            'total_bytes': 0
        }
        
        # Thread lock for download stats
        self.download_lock = threading.Lock()
        
        # Create base directory
        os.makedirs(self.base_download_dir, exist_ok=True)
        
        logger.info(f"📥 Tile Downloader initialized: {self.base_download_dir}")
        logger.info(f"🧵 Max workers: {self.max_workers}, Timeout: {self.download_timeout}s")
    
    def get_tile_type_from_url(self, url):
        """Enhanced tile type detection for Guland specific map types"""
        url_lower = url.lower()
        
        # Guland specific map types based on URL patterns
        if 'qh-2030' in url_lower or '/qh/' in url_lower:
            return 'quy_hoach_2030'
        elif 'qh-2025' in url_lower:
            return 'ke_hoach_2025'
        elif 'qhc' in url_lower or 'phan-khu' in url_lower:
            return 'quy_hoach_phan_khu'
        elif 'hien-trang' in url_lower or 'current' in url_lower:
            return 'hien_trang'
        
        # Generic detection patterns
        elif 'satellite' in url_lower or 'sat' in url_lower:
            return 'satellite'
        elif 'terrain' in url_lower or 'topo' in url_lower:
            return 'terrain'
        elif 'street' in url_lower or 'road' in url_lower:
            return 'street'
        elif 'hybrid' in url_lower:
            return 'hybrid'
        elif 'planning' in url_lower or 'quy-hoach' in url_lower:
            return 'planning_generic'
        elif 'administrative' in url_lower or 'admin' in url_lower:
            return 'administrative'
        else:
            # Try to detect from server domain
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            
            if 'google' in domain:
                return 'google_maps'
            elif 'openstreetmap' in domain or 'osm' in domain:
                return 'openstreetmap'
            elif 'bing' in domain:
                return 'bing_maps'
            elif 'guland' in domain:
                return 'guland_generic'
            else:
                return 'unknown'
    
    def create_enhanced_directory_structure(self, location_name, tile_info):
        """Create enhanced directory structure for Vietnamese map types"""
        # Clean location name for filesystem
        clean_location = self.clean_filename(location_name)
        
        # Extract zoom level info if location_name contains zoom info
        zoom_info = ""
        if "_zoom_" in location_name:
            parts = location_name.split("_zoom_")
            clean_location = self.clean_filename(parts[0])
            zoom_info = f"_Z{parts[1]}"
        
        # Get tile type
        tile_type = self.get_tile_type_from_url(tile_info['url'])
        
        # Create directory structure with zoom info
        type_mapping = {
            'quy_hoach_2030': f'01_Quy_Hoach_2030{zoom_info}',
            'ke_hoach_2025': f'02_Ke_Hoach_2025{zoom_info}',
            'quy_hoach_phan_khu': f'03_Quy_Hoach_Phan_Khu{zoom_info}',
            'hien_trang': f'04_Hien_Trang{zoom_info}',
            # ... other mappings
        }
        
        folder_name = type_mapping.get(tile_type, f'99_Unknown_{tile_type}{zoom_info}')
        
        # Create directory structure: downloaded_tiles/location/map_type_zoom/zoom/
        base_path = os.path.join(self.base_download_dir, clean_location, folder_name)
        os.makedirs(base_path, exist_ok=True)
        
        return base_path, tile_type
    
    def clean_filename(self, filename):
        """Clean filename for filesystem compatibility"""
        # Remove/replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Remove extra spaces and Vietnamese diacritics issues
        filename = filename.replace(' ', '_').replace('__', '_')
        return filename.strip('_')
    
    def generate_enhanced_tile_filename(self, tile_info, tile_type):
        """Generate enhanced filename with metadata"""
        zoom = tile_info['zoom']
        x = tile_info['x']
        y = tile_info['y']
        format_ext = tile_info['format']
        
        # Add tile type prefix for better organization
        type_prefix = {
            'quy_hoach_2030': 'QH2030',
            'ke_hoach_2025': 'KH2025',
            'quy_hoach_phan_khu': 'QHPK',
            'hien_trang': 'HT'
        }.get(tile_type, 'TILE')
        
        # Standard naming: TYPE_z_x_y.ext
        return f"{type_prefix}_{zoom}_{x}_{y}.{format_ext}"
    
    def validate_image_file(self, filepath):
        """Validate that downloaded file is a valid image"""
        try:
            # Check file size
            if os.path.getsize(filepath) < 100:  # Too small to be valid image
                return False
            
            # Check file headers
            with open(filepath, 'rb') as f:
                header = f.read(16)
                
                # PNG signature
                if header.startswith(b'\x89PNG\r\n\x1a\n'):
                    return True
                # JPEG signature
                elif header.startswith(b'\xff\xd8\xff'):
                    return True
                # WebP signature
                elif b'WEBP' in header:
                    return True
                # TIFF signature
                elif header.startswith(b'II*\x00') or header.startswith(b'MM\x00*'):
                    return True
            
            return False
            
        except Exception as e:
            logger.warning(f"⚠️ Error validating image file {filepath}: {e}")
            return False
    
    def download_single_tile(self, tile_info, location_name):
        """Download a single tile with enhanced error handling"""
        try:
            url = tile_info['url']
            
            # Create enhanced directory structure
            base_path, tile_type = self.create_enhanced_directory_structure(location_name, tile_info)
            zoom_path = os.path.join(base_path, str(tile_info['zoom']))
            os.makedirs(zoom_path, exist_ok=True)
            
            # Generate filename with additional metadata
            filename = self.generate_enhanced_tile_filename(tile_info, tile_type)
            filepath = os.path.join(zoom_path, filename)
            
            # Skip if file already exists and is valid
            if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                logger.info(f"⏭️ Tile already exists: {filename}")
                return {
                    'success': True,
                    'filepath': filepath,
                    'size': os.path.getsize(filepath),
                    'tile_type': tile_type,
                    'skipped': True
                }
            
            # Enhanced headers for Guland tiles
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'image',
                'Sec-Fetch-Mode': 'no-cors',
                'Sec-Fetch-Site': 'cross-site',
                'Referer': 'https://guland.vn/',
                'Origin': 'https://guland.vn'
            }
            
            # Add specific headers for different CDNs
            if 'digitaloceanspaces.com' in url:
                headers['Cache-Control'] = 'no-cache'
            elif 'cmctelecom.vn' in url:
                headers['X-Requested-With'] = 'XMLHttpRequest'
            
            response = requests.get(url, headers=headers, timeout=self.download_timeout, stream=True)
            response.raise_for_status()
            
            # Enhanced content validation
            content_type = response.headers.get('content-type', '').lower()
            if not any(img_type in content_type for img_type in ['image/', 'application/octet-stream']):
                logger.warning(f"⚠️ Non-image response for {filename}: {content_type}")
                return {'success': False, 'error': f'Non-image content: {content_type}', 'tile_type': tile_type}
            
            # Write file with progress tracking
            total_size = 0
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        total_size += len(chunk)
            
            # Validate downloaded file
            if total_size == 0:
                os.remove(filepath)
                return {'success': False, 'error': 'Empty file downloaded', 'tile_type': tile_type}
            
            # Additional validation for image files
            if not self.validate_image_file(filepath):
                os.remove(filepath)
                return {'success': False, 'error': 'Invalid image file', 'tile_type': tile_type}
            
            logger.info(f"✅ Downloaded: {filename} ({self.format_bytes(total_size)}) - {tile_type}")
            
            return {
                'success': True,
                'filepath': filepath,
                'size': total_size,
                'tile_type': tile_type,
                'url': url,
                'skipped': False
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Download failed for {tile_info.get('url', 'unknown')}: {e}")
            return {'success': False, 'error': str(e), 'tile_type': tile_type}
        except Exception as e:
            logger.error(f"❌ Unexpected error downloading tile: {e}")
            return {'success': False, 'error': str(e), 'tile_type': tile_type}
    
    def download_tiles_batch(self, tile_urls, location_name):
        """Download tiles in parallel batches"""
        if not tile_urls:
            return []
        
        logger.info(f"📥 Starting download of {len(tile_urls)} tiles for {location_name}")
        logger.info(f"🧵 Using {self.max_workers} parallel workers")
        
        download_results = []
        successful_downloads = 0
        failed_downloads = 0
        total_bytes = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all download tasks
            future_to_tile = {
                executor.submit(self.download_single_tile, tile_info, location_name): tile_info 
                for tile_info in tile_urls
            }
            
            # Process completed downloads
            for future in as_completed(future_to_tile):
                tile_info = future_to_tile[future]
                
                try:
                    result = future.result()
                    download_results.append(result)
                    
                    if result['success']:
                        successful_downloads += 1
                        total_bytes += result['size']
                        
                        if not result.get('skipped', False):
                            logger.info(f"✅ {successful_downloads}/{len(tile_urls)}: {tile_info['zoom']}/{tile_info['x']}/{tile_info['y']}")
                    else:
                        failed_downloads += 1
                        logger.warning(f"❌ Failed: {tile_info['zoom']}/{tile_info['x']}/{tile_info['y']} - {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    failed_downloads += 1
                    logger.error(f"❌ Download task failed: {e}")
                    download_results.append({'success': False, 'error': str(e)})
        
        # Update global stats
        with self.download_lock:
            self.download_stats['total_attempted'] += len(tile_urls)
            self.download_stats['total_successful'] += successful_downloads
            self.download_stats['total_failed'] += failed_downloads
            self.download_stats['total_bytes'] += total_bytes
        
        # Log summary
        logger.info(f"📊 Download Summary for {location_name}:")
        logger.info(f"  ✅ Successful: {successful_downloads}")
        logger.info(f"  ❌ Failed: {failed_downloads}")
        logger.info(f"  📁 Total size: {self.format_bytes(total_bytes)}")
        
        # Group by tile type for reporting
        tile_types = {}
        for result in download_results:
            if result['success'] and 'tile_type' in result:
                tile_type = result['tile_type']
                if tile_type not in tile_types:
                    tile_types[tile_type] = {'count': 0, 'size': 0}
                tile_types[tile_type]['count'] += 1
                tile_types[tile_type]['size'] += result['size']
        
        logger.info("📂 Downloaded by tile type:")
        for tile_type, stats in tile_types.items():
            logger.info(f"  • {tile_type}: {stats['count']} tiles ({self.format_bytes(stats['size'])})")
        
        return download_results
    
    def format_bytes(self, bytes_count):
        """Format bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_count < 1024.0:
                return f"{bytes_count:.1f} {unit}"
            bytes_count /= 1024.0
        return f"{bytes_count:.1f} TB"
    
    def generate_download_report(self, location_name, download_results):
        """Generate enhanced download report with Vietnamese map types"""
        if not download_results:
            return
        
        successful = [r for r in download_results if r['success']]
        failed = [r for r in download_results if not r['success']]
        
        # Group by tile type
        tile_type_stats = {}
        for result in successful:
            if 'tile_type' in result:
                tile_type = result['tile_type']
                if tile_type not in tile_type_stats:
                    tile_type_stats[tile_type] = {
                        'count': 0,
                        'size': 0,
                        'zoom_levels': set()
                    }
                tile_type_stats[tile_type]['count'] += 1
                tile_type_stats[tile_type]['size'] += result['size']
                
                # Extract zoom from filepath if available
                if 'filepath' in result:
                    try:
                        zoom = result['filepath'].split('/')[-2]  # zoom folder
                        tile_type_stats[tile_type]['zoom_levels'].add(zoom)
                    except:
                        pass
        
        # Generate detailed report
        report_lines = []
        report_lines.append(f"# CHI TIẾT TẢI XUỐNG BÌNH ĐỒ: {location_name}")
        report_lines.append(f"Được tạo: {datetime.now().isoformat()}")
        report_lines.append("")
        
        report_lines.append("## 📊 TỔNG QUAN")
        report_lines.append(f"• Tổng số tile thử tải: {len(download_results)}")
        report_lines.append(f"• Tải thành công: {len(successful)}")
        report_lines.append(f"• Tải thất bại: {len(failed)}")
        report_lines.append(f"• Tỷ lệ thành công: {(len(successful)/len(download_results)*100):.1f}%")
        
        if successful:
            total_size = sum(r['size'] for r in successful)
            report_lines.append(f"• Tổng dung lượng: {self.format_bytes(total_size)}")
        
        report_lines.append("")
        
        # Detailed breakdown by map type
        if tile_type_stats:
            report_lines.append("## 🗺️ CHI TIẾT THEO LOẠI BẢN ĐỒ")
            
            # Sort by tile type for better presentation
            type_names = {
                'quy_hoach_2030': 'Quy Hoạch 2030',
                'ke_hoach_2025': 'Kế Hoạch 2025',
                'quy_hoach_phan_khu': 'Quy Hoạch Phân Khu',
                'hien_trang': 'Hiện Trạng',
                'satellite': 'Bản đồ vệ tinh',
                'terrain': 'Bản đồ địa hình',
                'street': 'Bản đồ đường phố'
            }
            
            for tile_type, stats in sorted(tile_type_stats.items()):
                readable_name = type_names.get(tile_type, tile_type.replace('_', ' ').title())
                zoom_levels = sorted(list(stats['zoom_levels']))
                
                report_lines.append(f"### {readable_name}")
                report_lines.append(f"• Số tile: {stats['count']}")
                report_lines.append(f"• Dung lượng: {self.format_bytes(stats['size'])}")
                report_lines.append(f"• Mức zoom: {', '.join(zoom_levels)}")
                report_lines.append("")
        
        # Error analysis
        if failed:
            report_lines.append("## ❌ PHÂN TÍCH LỖI")
            error_counts = {}
            for result in failed:
                error = result.get('error', 'Lỗi không xác định')
                tile_type = result.get('tile_type', 'unknown')
                error_key = f"{tile_type}: {error}"
                error_counts[error_key] = error_counts.get(error_key, 0) + 1
            
            for error, count in sorted(error_counts.items()):
                report_lines.append(f"• {error}: {count} tile(s)")
        
        # Directory structure info
        report_lines.append("")
        report_lines.append("## 📁 CẤU TRÚC THƯ MỤC")
        report_lines.append("```")
        report_lines.append(f"downloaded_tiles/")
        report_lines.append(f"└── {self.clean_filename(location_name)}/")
        
        for tile_type in sorted(tile_type_stats.keys()):
            type_folder = {
                'quy_hoach_2030': '01_Quy_Hoach_2030',
                'ke_hoach_2025': '02_Ke_Hoach_2025',
                'quy_hoach_phan_khu': '03_Quy_Hoach_Phan_Khu'
            }.get(tile_type, f'99_{tile_type}')
            
            report_lines.append(f"    ├── {type_folder}/")
            
            # Show zoom levels for this type
            stats = tile_type_stats[tile_type]
            for zoom in sorted(stats['zoom_levels']):
                report_lines.append(f"    │   └── {zoom}/")
        
        report_lines.append("```")
        
        # Save report
        clean_location = self.clean_filename(location_name)
        report_path = os.path.join(self.base_download_dir, clean_location, 'chi_tiet_tai_xuong.txt')
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(report_lines))
        
        logger.info(f"📋 Báo cáo chi tiết đã lưu: {report_path}")
        return report_path