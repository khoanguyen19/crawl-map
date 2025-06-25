#!/usr/bin/env python3
"""
Complete Enhanced Multi-Map Digital Ocean Spaces Uploader
Uploads city tiles to Digital Ocean Spaces with map type awareness and advanced filtering

Author: AI Assistant  
Version: 2.0 - Complete Enhanced Edition
Structure: downloaded_tiles/cities/<city>/<map_type>/<zoom>/

Features:
- Multi-map type awareness (QH 2030, KH 2025, etc.)
- Advanced filtering by city and map type
- Efficient duplicate detection (city+map combinations)
- Enhanced metadata with map information
- Comprehensive progress tracking and reporting
- Smart resume capability
- Rate limit optimization for Digital Ocean Spaces
"""

import os
import json
import boto3
import hashlib
import logging
import mimetypes
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError, NoCredentialsError
from tqdm import tqdm
import time
import argparse
import sys
import unicodedata
import re

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_spaces_upload.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Map type configurations (aligned with downloader)
MAP_TYPE_CONFIG = {
    'qh-2030': {
        'display_name': 'QH 2030',
        'priority': 1,
        'description': 'Quy hoạch tổng thể 2030',
        'color': '🟢'
    },
    'kh-2025': {
        'display_name': 'KH 2025', 
        'priority': 2,
        'description': 'Kế hoạch sử dụng đất 2025',
        'color': '🟡'
    },
    'qh-phan-khu': {
        'display_name': 'QH phân khu',
        'priority': 3,
        'description': 'Quy hoạch phân khu chi tiết',
        'color': '🔵'
    },
    'qh-khac': {
        'display_name': 'QH khác',
        'priority': 4,
        'description': 'Các loại quy hoạch khác',
        'color': '🟣'
    },
    'unknown': {
        'display_name': 'Unknown Type',
        'priority': 99,
        'description': 'Loại bản đồ không xác định',
        'color': '⚫'
    }
}

# Predefined city configurations for better UX
# CITY_CONFIG = {
#     'hanoi': {
#         'display_name': 'Hà Nội',
#         'priority': 1,
#         'region': 'North'
#     },
#     'hcmc': {
#         'display_name': 'TP. Hồ Chí Minh',
#         'priority': 2,
#         'region': 'South'
#     },
#     'danang': {
#         'display_name': 'Đà Nẵng',
#         'priority': 3,
#         'region': 'Central'
#     },
#     'haiphong': {
#         'display_name': 'Hải Phòng',
#         'priority': 4,
#         'region': 'North'
#     },
#     'cantho': {
#         'display_name': 'Cần Thơ',
#         'priority': 5,
#         'region': 'South'
#     }
# }

class EnhancedMultiMapSpacesUploader:
    def __init__(self, access_key, secret_key, endpoint_url, bucket_name, region='sgp1'):
        """
        Initialize Enhanced Multi-Map Digital Ocean Spaces uploader
        
        Args:
            access_key: DO Spaces access key
            secret_key: DO Spaces secret key
            endpoint_url: DO Spaces endpoint URL
            bucket_name: Target bucket name
            region: DO Spaces region (default: sgp1)
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.bucket_name = bucket_name
        self.region = region
        
        # Initialize boto3 client for DO Spaces
        try:
            self.s3_client = boto3.client(
                's3',
                region_name=region,
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key
            )
            
            # Test connection
            self.s3_client.head_bucket(Bucket=bucket_name)
            logger.info(f"✅ Connected to Digital Ocean Spaces: {bucket_name}")
            
        except NoCredentialsError:
            logger.error("❌ Invalid credentials provided")
            raise
        except ClientError as e:
            logger.error(f"❌ Error connecting to Spaces: {e}")
            raise
        
        # Enhanced statistics with comprehensive tracking
        self.stats = {
            'total_files': 0,
            'uploaded_files': 0,
            'skipped_files': 0,
            'failed_files': 0,
            'total_bytes': 0,
            'uploaded_bytes': 0,
            'start_time': None,
            'end_time': None,
            'cities_processed': 0,
            'map_types_processed': 0,
            'city_stats': {},        # Per-city detailed statistics
            'map_type_stats': {},    # Per-map-type detailed statistics
            'combination_stats': {}, # Per city+map_type combination stats
            'zoom_level_stats': {},  # Per zoom level statistics
            'hourly_progress': []    # Hourly progress tracking
        }
        
        # Resume state management
        self.resume_file = 'enhanced_upload_resume_state.json'
        self.uploaded_files = self.load_resume_state()
        
        # Rate limiting and performance optimization
        self.api_call_count = 0
        self.last_api_reset = time.time()
        self.max_api_calls_per_second = 200  # Increase from 100
        self.batch_check_size = 100  # Check existence in batches
        self.upload_timeout = 30  # Add timeout for uploads
        self.retry_attempts = 3  # Add retry logic

        # Connection pooling for better performance
        self.session = boto3.Session()
        self.s3_client = self.session.client(
            's3',
            region_name=region,
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=boto3.session.Config(
                retries={'max_attempts': 3, 'mode': 'adaptive'},
                max_pool_connections=50,  # Increase connection pool
                region_name=region
            )
        )
        self.max_cache_size = 1000
        self.stats_save_interval = 50  # Save stats every 50 operations
        self.last_stats_save = 0
        
        # Optimize API settings
        self.max_api_calls_per_second = 250  # Increase further if stable
    
    def batch_check_existence(self, s3_keys):
        """Check multiple files existence in batch for better performance"""
        existing_files = set()
        
        # Group files by common prefixes for efficient checking
        prefix_groups = {}
        for s3_key in s3_keys:
            prefix = '/'.join(s3_key.split('/')[:-1])  # Get directory path
            if prefix not in prefix_groups:
                prefix_groups[prefix] = []
            prefix_groups[prefix].append(s3_key)
        
        for prefix, keys in prefix_groups.items():
            try:
                self.rate_limit_check()
                
                # List objects with this prefix
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=prefix + '/',
                    MaxKeys=1000
                )
                
                if 'Contents' in response:
                    existing_keys = {obj['Key'] for obj in response['Contents']}
                    # Find which of our target keys exist
                    for key in keys:
                        if key in existing_keys:
                            existing_files.add(key)
                            
            except Exception as e:
                logger.warning(f"Error batch checking {prefix}: {e}")
                # Fallback to individual checks
                for key in keys:
                    if self.file_exists_in_spaces(key):
                        existing_files.add(key)
        
        return existing_files
    
    def upload_single_file_optimized(self, local_path, s3_key, file_info=None, city=None, map_type=None, zoom=None, district=None):
        """Optimized upload with retry logic and better error handling"""
        for attempt in range(self.retry_attempts):
            try:
                # Resume functionality check (optimized)
                resume_key = f"{s3_key}:{file_info['md5'] if file_info else 'unknown'}"
                if resume_key in self.uploaded_files:
                    self.stats['skipped_files'] += 1
                    self.update_comprehensive_stats(city, map_type, zoom, 'skipped', file_info['size'] if file_info else 0, district)
                    return {'success': True, 'skipped': True, 'file': s3_key, 'size': file_info['size'] if file_info else 0}
                
                # Get file info if not provided (cached)
                if not file_info:
                    file_info = self.get_file_info_cached(local_path)
                    if not file_info:
                        return {'success': False, 'error': 'Could not get file info', 'file': s3_key}
                
                # Prepare upload with optimized settings
                content_type = self.determine_content_type(local_path, file_info)
                metadata = self.create_file_metadata(local_path, file_info, city, map_type, zoom, district)
                
                # Debug log for problematic metadata
                if district and any('đ' in str(v) or any(ord(c) > 127 for c in str(v)) for v in metadata.values()):
                    logger.debug(f"🔍 Metadata for {s3_key}: {metadata}")
                
                extra_args = {
                    'ACL': 'public-read',
                    'ContentType': content_type,
                    'CacheControl': 'max-age=31536000, public',
                    'ContentDisposition': 'inline',
                    'Metadata': metadata
                }
                
                # Upload with timeout
                self.rate_limit_check()
                self.s3_client.upload_file(
                    local_path,
                    self.bucket_name,
                    s3_key,
                    ExtraArgs=extra_args,
                    Config=boto3.s3.transfer.TransferConfig(
                        multipart_threshold=1024 * 25,  # 25MB
                        max_concurrency=10,
                        multipart_chunksize=1024 * 25,
                        use_threads=True
                    )
                )
                
                # Success - update tracking
                self.uploaded_files.add(resume_key)
                self.stats['uploaded_files'] += 1
                self.stats['uploaded_bytes'] += file_info['size']
                self.update_comprehensive_stats(city, map_type, zoom, 'uploaded', file_info['size'])
                
                return {
                    'success': True,
                    'file': s3_key,
                    'size': file_info['size'],
                    'attempt': attempt + 1
                }
                
            except Exception as e:
                error_msg = str(e)
                if "Non ascii characters found" in error_msg:
                    logger.error(f"❌ ASCII metadata error for {s3_key}")
                    logger.error(f"    District: {district}")
                    logger.error(f"    City: {city}")
                    logger.error(f"    Full error: {error_msg}")
                    
                if attempt < self.retry_attempts - 1:
                    wait_time = (2 ** attempt) * 0.5  # Exponential backoff
                    logger.warning(f"Upload attempt {attempt + 1} failed for {s3_key}: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ All {self.retry_attempts} upload attempts failed for {s3_key}: {e}")
                    self.stats['failed_files'] += 1
                    self.update_comprehensive_stats(city, map_type, zoom, 'failed', 0, district)
                    return {'success': False, 'error': str(e), 'file': s3_key}

    # Cache for file info to avoid repeated disk operations
    _file_info_cache = {}
    def get_file_info_cached(self, file_path):
        """Get file info with caching to avoid repeated disk operations"""
        stat = os.stat(file_path)
        cache_key = f"{file_path}:{stat.st_mtime}"
        
        if cache_key in self._file_info_cache:
            return self._file_info_cache[cache_key]
        
        file_info = self.get_file_info(file_path)
        if file_info:
            self._file_info_cache[cache_key] = file_info
            
            # Limit cache size
            if len(self._file_info_cache) > 1000:
                # Remove oldest entries
                oldest_keys = list(self._file_info_cache.keys())[:100]
                for key in oldest_keys:
                    del self._file_info_cache[key]
        
        return file_info

    def load_resume_state(self):
        """Load resume state from file with validation"""
        try:
            if os.path.exists(self.resume_file):
                with open(self.resume_file, 'r') as f:
                    data = json.load(f)
                    
                # Validate resume state structure
                if isinstance(data, dict) and 'uploaded_files' in data:
                    uploaded_files = set(data['uploaded_files'])
                    logger.info(f"📋 Loaded resume state: {len(uploaded_files)} files already uploaded")
                    
                    # Load previous stats if available
                    if 'stats' in data:
                        logger.info(f"📊 Previous session stats: {data['stats'].get('uploaded_files', 0)} uploaded")
                    
                    return uploaded_files
                elif isinstance(data, list):
                    # Legacy format compatibility
                    uploaded_files = set(data)
                    logger.info(f"📋 Loaded legacy resume state: {len(uploaded_files)} files")
                    return uploaded_files
                    
        except Exception as e:
            logger.warning(f"⚠️ Could not load resume state: {e}")
        
        return set()

    def save_resume_state(self):
        """Save resume state to file with proper error handling"""
        try:
            resume_data = {
                'uploaded_files': list(self.uploaded_files),
                'stats': {
                    'uploaded_files': self.stats['uploaded_files'],
                    'skipped_files': self.stats['skipped_files'],
                    'failed_files': self.stats['failed_files'],
                    'total_bytes': self.stats['total_bytes'],
                    'uploaded_bytes': self.stats['uploaded_bytes']
                },
                'timestamp': datetime.now().isoformat(),
                'version': '2.0-enhanced'
            }
            
            # Write to temporary file first, then rename for atomic operation
            temp_file = f"{self.resume_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(resume_data, f, indent=2)
            
            # Atomic rename
            os.rename(temp_file, self.resume_file)
            
            logger.debug(f"💾 Resume state saved: {len(self.uploaded_files)} files")
            
        except Exception as e:
            logger.warning(f"⚠️ Could not save resume state: {e}")
    
    def save_resume_state_optimized(self):
        """Optimized resume state saving - only save when needed"""
        current_count = self.stats['uploaded_files'] + self.stats['skipped_files']
        
        if current_count - self.last_stats_save >= self.stats_save_interval:
            self.save_resume_state()  # ✅ Gọi function chính, không đệ quy
            self.last_stats_save = current_count

    def rate_limit_check(self):
        """Implement rate limiting to avoid DO Spaces throttling"""
        current_time = time.time()
        
        # Reset counter every second
        if current_time - self.last_api_reset >= 1.0:
            self.api_call_count = 0
            self.last_api_reset = current_time
        
        # If approaching limit, sleep briefly
        if self.api_call_count >= self.max_api_calls_per_second:
            sleep_time = 1.0 - (current_time - self.last_api_reset)
            if sleep_time > 0:
                time.sleep(sleep_time)
                self.api_call_count = 0
                self.last_api_reset = time.time()
        
        self.api_call_count += 1

    def get_file_info(self, file_path):
        """Optimized file info gathering"""
        try:
            stat = os.stat(file_path)
            size = stat.st_size
            
            # Skip MD5 for very small files (under 1KB) - not worth the overhead
            if size < 1024:
                md5 = f"small_file_{size}_{stat.st_mtime}"
            else:
                # Optimized MD5 calculation with larger buffer
                hash_md5 = hashlib.md5()
                with open(file_path, "rb") as f:
                    while chunk := f.read(65536):  # 64KB chunks instead of 4KB
                        hash_md5.update(chunk)
                md5 = hash_md5.hexdigest()
            
            return {
                'size': size,
                'md5': md5,
                'modified_time': stat.st_mtime,
                'created_time': stat.st_ctime,
                'file_extension': os.path.splitext(file_path)[1].lower()
            }
        except Exception as e:
            logger.error(f"❌ Error getting file info for {file_path}: {e}")
            return None

    def file_exists_in_spaces(self, s3_key):
        """Check if file exists in Spaces with rate limiting"""
        try:
            self.rate_limit_check()
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                logger.warning(f"⚠️ Error checking file existence: {e}")
                return False

    def upload_single_file(self, local_path, s3_key, file_info=None, city=None, map_type=None, zoom=None, district=None):
        """Upload single file with comprehensive metadata and tracking"""
        try:
            # Resume functionality check
            resume_key = f"{s3_key}:{file_info['md5'] if file_info else 'unknown'}"
            if resume_key in self.uploaded_files:
                logger.debug(f"⏭️ Skipping already uploaded: {s3_key}")
                self.stats['skipped_files'] += 1
                self.update_comprehensive_stats(city, map_type, zoom, 'skipped', file_info['size'] if file_info else 0, district)
                return {
                    'success': True,
                    'skipped': True,
                    'file': s3_key,
                    'size': file_info['size'] if file_info else 0
                }
            
            # Double-check existence in Spaces
            if self.file_exists_in_spaces(s3_key):
                logger.debug(f"⏭️ File exists in Spaces: {s3_key}")
                self.uploaded_files.add(resume_key)
                self.stats['skipped_files'] += 1
                self.update_comprehensive_stats(city, map_type, zoom, 'skipped', file_info['size'] if file_info else 0)
                return {
                    'success': True,
                    'skipped': True,
                    'file': s3_key,
                    'size': file_info['size'] if file_info else 0
                }
            
            # Get file info if not provided
            if not file_info:
                file_info = self.get_file_info(local_path)
                if not file_info:
                    return {
                        'success': False,
                        'error': 'Could not get file info',
                        'file': s3_key
                    }
            
            # Determine optimal content type
            content_type = self.determine_content_type(local_path, file_info)
            
            # Create comprehensive metadata
            metadata = self.create_file_metadata(local_path, file_info, city, map_type, zoom, district)
            
            # Configure upload parameters for optimal performance
            extra_args = {
                'ACL': 'public-read',                           # Public access
                'ContentType': content_type,                    # Proper MIME type
                'CacheControl': 'max-age=31536000, public',     # 1 year cache
                'ContentDisposition': 'inline',                 # Display in browser
                'Metadata': metadata
            }
            
            # Add compression hints for better CDN performance
            if content_type.startswith('image/'):
                extra_args['ContentEncoding'] = 'identity'
            
            # Perform upload with rate limiting
            self.rate_limit_check()
            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            # Update tracking and statistics
            self.uploaded_files.add(resume_key)
            self.stats['uploaded_files'] += 1
            self.stats['uploaded_bytes'] += file_info['size']
            self.update_comprehensive_stats(city, map_type, zoom, 'uploaded', file_info['size'])
            
            # Generate public access URLs
            direct_url = f"https://{self.bucket_name}.{self.region}.digitaloceanspaces.com/{s3_key}"
            cdn_url = f"https://{self.bucket_name}.{self.region}.cdn.digitaloceanspaces.com/{s3_key}"
            
            logger.debug(f"✅ Uploaded: {s3_key} ({file_info['size']} bytes)")
            
            return {
                'success': True,
                'file': s3_key,
                'size': file_info['size'],
                'content_type': content_type,
                'public_url': direct_url,
                'cdn_url': cdn_url,
                'city': city,
                'map_type': map_type,
                'district': district,
                'zoom': zoom,
                'md5': file_info['md5']
            }
            
        except Exception as e:
            logger.error(f"❌ Error uploading {local_path}: {e}")
            self.stats['failed_files'] += 1
            self.update_comprehensive_stats(city, map_type, zoom, 'failed', 0, district)
            return {
                'success': False,
                'error': str(e),
                'file': s3_key
            }

    def determine_content_type(self, local_path, file_info):
        """Determine optimal content type for file"""
        content_type, _ = mimetypes.guess_type(local_path)
        
        if not content_type:
            extension = file_info.get('file_extension', '').lower()
            if extension == '.png':
                content_type = 'image/png'
            elif extension in ['.jpg', '.jpeg']:
                content_type = 'image/jpeg'
            elif extension == '.webp':
                content_type = 'image/webp'
            elif extension == '.svg':
                content_type = 'image/svg+xml'
            else:
                content_type = 'application/octet-stream'
        
        return content_type

    def sanitize_metadata_value(self, value):
        """Sanitize metadata value to ensure ASCII-only characters"""
        if not value:
            return value
        
        try:
            # First try to normalize Unicode characters
            normalized = unicodedata.normalize('NFKD', str(value))
            
            # Remove diacritics and convert to ASCII
            ascii_value = normalized.encode('ascii', 'ignore').decode('ascii')
            
            # Replace common Vietnamese characters manually if needed
            vietnamese_replacements = {
                'đ': 'd', 'Đ': 'D',
                'ă': 'a', 'â': 'a', 'á': 'a', 'à': 'a', 'ả': 'a', 'ã': 'a', 'ạ': 'a',
                'Ă': 'A', 'Â': 'A', 'Á': 'A', 'À': 'A', 'Ả': 'A', 'Ã': 'A', 'Ạ': 'A',
                'ê': 'e', 'é': 'e', 'è': 'e', 'ẻ': 'e', 'ẽ': 'e', 'ẹ': 'e',
                'Ê': 'E', 'É': 'E', 'È': 'E', 'Ẻ': 'E', 'Ẽ': 'E', 'Ẹ': 'E',
                'ô': 'o', 'ơ': 'o', 'ó': 'o', 'ò': 'o', 'ỏ': 'o', 'õ': 'o', 'ọ': 'o',
                'Ô': 'O', 'Ơ': 'O', 'Ó': 'O', 'Ò': 'O', 'Ỏ': 'O', 'Õ': 'O', 'Ọ': 'O',
                'ư': 'u', 'ú': 'u', 'ù': 'u', 'ủ': 'u', 'ũ': 'u', 'ụ': 'u',
                'Ư': 'U', 'Ú': 'U', 'Ù': 'U', 'Ủ': 'U', 'Ũ': 'U', 'Ụ': 'U',
                'í': 'i', 'ì': 'i', 'ỉ': 'i', 'ĩ': 'i', 'ị': 'i',
                'Í': 'I', 'Ì': 'I', 'Ỉ': 'I', 'Ĩ': 'I', 'Ị': 'I',
                'ý': 'y', 'ỳ': 'y', 'ỷ': 'y', 'ỹ': 'y', 'ỵ': 'y',
                'Ý': 'Y', 'Ỳ': 'Y', 'Ỷ': 'Y', 'Ỹ': 'Y', 'Ỵ': 'Y'
            }
            
            # Apply Vietnamese replacements if ASCII conversion failed
            if not ascii_value or len(ascii_value) < len(value) * 0.5:
                for vietnamese_char, replacement in vietnamese_replacements.items():
                    value = value.replace(vietnamese_char, replacement)
                ascii_value = value
            
            # Ensure only ASCII characters remain
            ascii_value = re.sub(r'[^\x00-\x7F]', '', ascii_value)
            
            # Replace spaces and special characters with hyphens
            ascii_value = re.sub(r'[^\w\-.]', '-', ascii_value)
            
            # Clean up multiple hyphens
            ascii_value = re.sub(r'-+', '-', ascii_value).strip('-')
            
            return ascii_value if ascii_value else 'unknown'
            
        except Exception as e:
            logger.warning(f"⚠️ Error sanitizing metadata value '{value}': {e}")
            # Fallback: remove all non-ASCII characters
            return re.sub(r'[^\x00-\x7F]', '', str(value)) or 'unknown'

    def create_file_metadata(self, local_path, file_info, city, map_type, zoom, district=None):
        """Create comprehensive metadata for uploaded files with district support and ASCII sanitization"""
        metadata = {
            'original-name': self.sanitize_metadata_value(os.path.basename(local_path)),
            'upload-time': datetime.now().isoformat(),
            'md5-hash': file_info['md5'],
            'file-size': str(file_info['size']),
            'tile-type': 'map-tile',
            'public-access': 'enabled',
            'uploader-version': '2.0-enhanced'
        }
        
        # Add location and map type metadata with sanitization
        if city:
            metadata['city'] = self.sanitize_metadata_value(city)
            metadata['city-original'] = city  # Keep original for reference (will be sanitized)
        
        if district:
            metadata['district'] = self.sanitize_metadata_value(district)
            metadata['district-original'] = self.sanitize_metadata_value(district)  # Also sanitize original
            metadata['structure-type'] = 'kh-2025-district'
        else:
            metadata['structure-type'] = 'standard'
        
        if map_type:
            metadata['map-type'] = self.sanitize_metadata_value(map_type)
            map_config = MAP_TYPE_CONFIG.get(map_type, {})
            if map_config:
                metadata['map-type-display'] = self.sanitize_metadata_value(map_config.get('display_name', map_type))
                metadata['map-type-priority'] = str(map_config.get('priority', 99))
        
        if zoom:
            metadata['zoom-level'] = str(zoom)
            metadata['tile-detail'] = 'high' if int(zoom) >= 14 else 'medium' if int(zoom) >= 10 else 'low'
        
        # Final sanitization pass for all metadata values
        sanitized_metadata = {}
        for key, value in metadata.items():
            sanitized_key = self.sanitize_metadata_value(key).replace(' ', '-')
            sanitized_value = self.sanitize_metadata_value(str(value))
            sanitized_metadata[sanitized_key] = sanitized_value
        
        return sanitized_metadata

    def update_comprehensive_stats(self, city, map_type, zoom, status, size, district=None):
        """Update comprehensive statistics across all dimensions with safety checks"""
        try:
            # City statistics
            if city:
                if city not in self.stats['city_stats']:
                    self.stats['city_stats'][city] = {
                        'uploaded': 0, 'skipped': 0, 'failed': 0, 'bytes': 0
                    }
                self.stats['city_stats'][city][status] += 1
                if status in ['uploaded', 'skipped']:
                    self.stats['city_stats'][city]['bytes'] += size
            
            # Map type statistics
            if map_type:
                if map_type not in self.stats['map_type_stats']:
                    self.stats['map_type_stats'][map_type] = {
                        'uploaded': 0, 'skipped': 0, 'failed': 0, 'bytes': 0
                    }
                self.stats['map_type_stats'][map_type][status] += 1
                if status in ['uploaded', 'skipped']:
                    self.stats['map_type_stats'][map_type]['bytes'] += size
            
            # District statistics (for KH_2025)
            if district:
                if 'district_stats' not in self.stats:
                    self.stats['district_stats'] = {}
                if district not in self.stats['district_stats']:
                    self.stats['district_stats'][district] = {
                        'uploaded': 0, 'skipped': 0, 'failed': 0, 'bytes': 0
                    }
                self.stats['district_stats'][district][status] += 1
                if status in ['uploaded', 'skipped']:
                    self.stats['district_stats'][district]['bytes'] += size
            
            # Combination statistics (city + map_type + district)
            if city and map_type:
                combo_key = f"{city}:{map_type}"
                if district:
                    combo_key += f":{district}"
                
                if combo_key not in self.stats['combination_stats']:
                    self.stats['combination_stats'][combo_key] = {
                        'uploaded': 0, 'skipped': 0, 'failed': 0, 'bytes': 0
                    }
                self.stats['combination_stats'][combo_key][status] += 1
                if status in ['uploaded', 'skipped']:
                    self.stats['combination_stats'][combo_key]['bytes'] += size
            
            # Zoom level statistics
            if zoom:
                if zoom not in self.stats['zoom_level_stats']:
                    self.stats['zoom_level_stats'][zoom] = {
                        'uploaded': 0, 'skipped': 0, 'failed': 0, 'bytes': 0
                    }
                self.stats['zoom_level_stats'][zoom][status] += 1
                if status in ['uploaded', 'skipped']:
                    self.stats['zoom_level_stats'][zoom]['bytes'] += size
        
        except Exception as e:
            logger.warning(f"⚠️ Error updating stats: {e}")

    def parse_file_path(self, file_path, base_dir):
        """
        Enhanced file path parsing with support for KH_2025 district structure
        Expected structures:
        - Standard: downloaded_tiles/cities/<city>/<map_type>/<zoom>/<file>
        - KH_2025: downloaded_tiles/cities/<city>/kh-2025/<district>/<zoom>/<file>
        """
        try:
            # Get relative path from base directory
            rel_path = os.path.relpath(file_path, base_dir)
            path_parts = rel_path.split(os.sep)
            
            # Handle different structures based on path length and map type
            if len(path_parts) >= 4:
                city = path_parts[0]
                map_type = path_parts[1]
                
                # Special handling for KH_2025 with district structure
                if map_type == 'kh-2025' and len(path_parts) >= 5:
                    # Structure: cities/<city>/kh-2025/<district>/<zoom>/<file>
                    district = path_parts[2]
                    zoom_str = path_parts[3]
                    filename = path_parts[-1]
                    
                    # Validate zoom level
                    try:
                        zoom = int(zoom_str)
                        if zoom < 1 or zoom > 20:
                            logger.warning(f"⚠️ Unusual zoom level {zoom} for {file_path}")
                    except ValueError:
                        logger.warning(f"⚠️ Invalid zoom level '{zoom_str}' for {file_path}")
                        zoom = zoom_str  # Keep as string for tracking
                    
                    return {
                        'city': city,
                        'map_type': map_type,
                        'district': district,
                        'zoom': zoom,
                        'filename': filename,
                        'valid': True,
                        'path_depth': len(path_parts),
                        'structure_type': 'kh_2025_district'
                    }
                else:
                    # Standard structure: cities/<city>/<map_type>/<zoom>/<file>
                    zoom_str = path_parts[2]
                    filename = path_parts[-1]
                    
                    # Validate zoom level
                    try:
                        zoom = int(zoom_str)
                        if zoom < 1 or zoom > 20:
                            logger.warning(f"⚠️ Unusual zoom level {zoom} for {file_path}")
                    except ValueError:
                        logger.warning(f"⚠️ Invalid zoom level '{zoom_str}' for {file_path}")
                        zoom = zoom_str  # Keep as string for tracking
                    
                    # Validate map type
                    if map_type not in MAP_TYPE_CONFIG:
                        logger.debug(f"🔍 Unknown map type '{map_type}' for {file_path}")
                    
                    return {
                        'city': city,
                        'map_type': map_type,
                        'district': None,
                        'zoom': zoom,
                        'filename': filename,
                        'valid': True,
                        'path_depth': len(path_parts),
                        'structure_type': 'standard'
                    }
            else:
                # Handle alternative structures gracefully
                return {
                    'city': path_parts[0] if len(path_parts) > 0 else None,
                    'map_type': path_parts[1] if len(path_parts) > 1 else 'unknown',
                    'district': None,
                    'zoom': path_parts[2] if len(path_parts) > 2 else 'unknown',
                    'filename': os.path.basename(file_path),
                    'valid': False,
                    'path_depth': len(path_parts),
                    'structure_type': 'unknown'
                }
        except Exception as e:
            logger.warning(f"⚠️ Could not parse file path {file_path}: {e}")
            return {
                'city': None,
                'map_type': 'unknown',
                'district': None,
                'zoom': 'unknown',
                'filename': os.path.basename(file_path),
                'valid': False,
                'path_depth': 0,
                'structure_type': 'error'
            }

    def scan_multi_map_directory(self, local_dir, s3_prefix='', target_cities=None, target_map_types=None, target_zoom_levels=None):
        """Parallel directory scanning for better performance"""
        import concurrent.futures
        from pathlib import Path
        
        files_to_upload = []
        scan_summary = {
            'cities': {},
            'map_types': set(),
            'zoom_levels': set(),
            'total_size': 0,
            'file_count': 0
        }
        
        logger.info(f"🔍 Starting parallel directory scan: {local_dir}")
        
        # Use pathlib for faster directory traversal
        base_path = Path(local_dir)
        if not base_path.exists():
            logger.error(f"Directory not found: {local_dir}")
            return []
        
        # Find all image files in parallel
        def scan_city_directory(city_path):
            city_files = []
            city_name = city_path.name
            
            # Apply city filter early
            if target_cities and city_name not in target_cities:
                return city_files
            
            try:
                for map_type_path in city_path.iterdir():
                    if not map_type_path.is_dir():
                        continue
                    
                    map_type_name = map_type_path.name
                    
                    # Apply map type filter early
                    if target_map_types and map_type_name not in target_map_types:
                        continue
                    
                    # Handle both standard and KH-2025 district structure
                    if map_type_name == 'kh-2025':
                        # KH-2025 structure: city/kh-2025/district/zoom/files
                        for district_path in map_type_path.iterdir():
                            if not district_path.is_dir():
                                continue
                                
                            for zoom_path in district_path.iterdir():
                                if not zoom_path.is_dir():
                                    continue
                                    
                                zoom_str = zoom_path.name
                                try:
                                    zoom_int = int(zoom_str)
                                    if target_zoom_levels and zoom_int not in target_zoom_levels:
                                        continue
                                except ValueError:
                                    continue
                                
                                # Scan files in this zoom directory
                                for file_path in zoom_path.iterdir():
                                    if file_path.is_file() and not self.should_skip_file(file_path.name):
                                        city_files.append((str(file_path), city_name, map_type_name, district_path.name, zoom_str))
                    else:
                        # Standard structure: city/map_type/zoom/files
                        for zoom_path in map_type_path.iterdir():
                            if not zoom_path.is_dir():
                                continue
                                
                            zoom_str = zoom_path.name
                            try:
                                zoom_int = int(zoom_str)
                                if target_zoom_levels and zoom_int not in target_zoom_levels:
                                    continue
                            except ValueError:
                                continue
                            
                            # Scan files in this zoom directory
                            for file_path in zoom_path.iterdir():
                                if file_path.is_file() and not self.should_skip_file(file_path.name):
                                    city_files.append((str(file_path), city_name, map_type_name, None, zoom_str))
                                    
            except Exception as e:
                logger.warning(f"Error scanning city {city_name}: {e}")
            
            return city_files
        
        # Get all city directories
        city_paths = [p for p in base_path.iterdir() if p.is_dir()]
        
        # Scan cities in parallel
        all_files = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(city_paths), 10)) as executor:
            future_to_city = {executor.submit(scan_city_directory, city_path): city_path.name 
                             for city_path in city_paths}
            
            with tqdm(total=len(future_to_city), desc="Scanning cities") as pbar:
                for future in concurrent.futures.as_completed(future_to_city):
                    city_name = future_to_city[future]
                    try:
                        city_files = future.result()
                        all_files.extend(city_files)
                        pbar.set_postfix(files=len(all_files))
                    except Exception as e:
                        logger.error(f"Error processing city {city_name}: {e}")
                    pbar.update(1)
        
        # Process file information in parallel
        def process_file(file_data):
            local_path, city, map_type, district, zoom = file_data
            
            # Generate S3 key
            rel_path = os.path.relpath(local_path, local_dir)
            s3_key = os.path.join(s3_prefix, rel_path).replace('\\', '/') if s3_prefix else rel_path.replace('\\', '/')
            
            # Get file information
            file_info = self.get_file_info_cached(local_path)
            if not file_info:
                return None
            
            return {
                'local_path': local_path,
                's3_key': s3_key,
                'file_info': file_info,
                'city': city,
                'map_type': map_type,
                'district': district,
                'zoom': zoom
            }
        
        # Process files in parallel
        logger.info(f"📊 Processing {len(all_files)} files...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
            with tqdm(total=len(all_files), desc="Processing files") as pbar:
                future_to_file = {executor.submit(process_file, file_data): file_data 
                                for file_data in all_files}
                
                for future in concurrent.futures.as_completed(future_to_file):
                    try:
                        result = future.result()
                        if result:
                            files_to_upload.append(result)
                            
                            # Update scan summary
                            path_info = {
                                'city': result['city'],
                                'map_type': result['map_type'],
                                'zoom': result['zoom']
                            }
                            self.update_scan_summary(scan_summary, path_info, result['file_info'])
                            self.stats['total_files'] += 1
                            self.stats['total_bytes'] += result['file_info']['size']
                            
                    except Exception as e:
                        logger.warning(f"Error processing file: {e}")
                    pbar.update(1)
        
        self.log_scan_results(scan_summary, len(files_to_upload))
        return files_to_upload

    def should_skip_file(self, filename):
        """Determine if file should be skipped during scanning"""
        # Skip hidden files, logs, and non-image files
        if filename.startswith('.'):
            return True
        if filename.endswith(('.log', '.tmp', '.temp')):
            return True
        if not filename.lower().endswith(('.png', '.jpg', '.jpeg', '.webp')):
            return True
        return False

    def passes_filters(self, path_info, target_cities, target_map_types, target_zoom_levels):
        """Check if file passes all applied filters"""
        # City filter
        if target_cities and path_info['city'] and path_info['city'] not in target_cities:
            return False
        
        # Map type filter
        if target_map_types and path_info['map_type'] and path_info['map_type'] not in target_map_types:
            return False
        
        # Zoom level filter
        if target_zoom_levels and path_info['zoom']:
            try:
                zoom_int = int(path_info['zoom'])
                if zoom_int not in target_zoom_levels:
                    return False
            except (ValueError, TypeError):
                # Skip files with invalid zoom levels if zoom filter is active
                return False
        
        return True

    def update_scan_summary(self, summary, path_info, file_info):
        """Update scan summary with file information"""
        city = path_info['city'] or 'unknown'
        map_type = path_info['map_type'] or 'unknown'
        zoom = path_info['zoom'] or 'unknown'
        
        # Update city summary
        if city not in summary['cities']:
            summary['cities'][city] = {}
        if map_type not in summary['cities'][city]:
            summary['cities'][city][map_type] = {'files': 0, 'size': 0}
        
        summary['cities'][city][map_type]['files'] += 1
        summary['cities'][city][map_type]['size'] += file_info['size']
        
        # Update global sets
        summary['map_types'].add(map_type)
        summary['zoom_levels'].add(str(zoom))
        summary['total_size'] += file_info['size']
        summary['file_count'] += 1

    def log_scan_results(self, summary, files_to_upload_count):
        """Log comprehensive scan results"""
        logger.info(f"📊 Scan complete: {files_to_upload_count:,} files found")
        logger.info(f"📊 Total size: {summary['total_size'] / 1024 / 1024:.1f} MB")
        logger.info(f"📊 Cities found: {len(summary['cities'])}")
        logger.info(f"📊 Map types found: {len(summary['map_types'])}")
        logger.info(f"📊 Zoom levels found: {len(summary['zoom_levels'])}")
        
        # Detailed breakdown
        print(f"\n📋 DETAILED SCAN SUMMARY:")
        print("=" * 50)
        
        for city, map_types in summary['cities'].items():
            # Just use city name directly
            city_total_files = sum(mt['files'] for mt in map_types.values())
            city_total_size = sum(mt['size'] for mt in map_types.values()) / 1024 / 1024
            
            print(f"🏙️ {city}: {city_total_files:,} files ({city_total_size:.1f} MB)")
            
            # Sort map types by priority
            sorted_map_types = sorted(map_types.items(), 
                                    key=lambda x: MAP_TYPE_CONFIG.get(x[0], {}).get('priority', 99))
            
            for map_type, stats in sorted_map_types:
                map_display = MAP_TYPE_CONFIG.get(map_type, {}).get('display_name', map_type)
                color = MAP_TYPE_CONFIG.get(map_type, {}).get('color', '⚫')
                size_mb = stats['size'] / 1024 / 1024
                print(f"   {color} {map_display}: {stats['files']:,} files ({size_mb:.1f} MB)")

    def check_city_map_type_exists_in_spaces(self, city_name, map_type, s3_prefix=''):
        """
        Efficiently check if city+map_type combination exists in Spaces
        """
        try:
            # Construct prefix for this specific combination
            if s3_prefix:
                check_prefix = f"{s3_prefix}/{city_name}/{map_type}/"
            else:
                check_prefix = f"{city_name}/{map_type}/"
            
            logger.debug(f"🔍 Checking combination: {check_prefix}")
            
            # Use rate limiting for API calls
            self.rate_limit_check()
            
            # List objects with this prefix (limited check for efficiency)
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=check_prefix,
                MaxKeys=10  # Just need to know if it exists
            )
            
            if 'Contents' not in response:
                return {
                    'exists': False,
                    'file_count': 0,
                    'total_size': 0,
                    'sample_files': []
                }
            
            contents = response['Contents']
            total_size = sum(obj['Size'] for obj in contents)
            sample_files = [obj['Key'] for obj in contents[:3]]
            
            return {
                'exists': True,
                'file_count': len(contents),
                'total_size': total_size,
                'sample_files': sample_files
            }
            
        except Exception as e:
            logger.error(f"❌ Error checking combination {city_name}/{map_type}: {e}")
            return {
                'exists': False,
                'file_count': 0,
                'total_size': 0,
                'sample_files': []
            }

    def scan_existing_combinations_in_spaces(self, local_dir, s3_prefix='', target_cities=None, target_map_types=None):
        """
        Efficiently scan what combinations already exist in Spaces
        """
        existing_combinations = {}
        
        # First analyze local structure to know what to check
        logger.info("🔍 Analyzing local structure for combinations...")
        local_combinations = set()
        
        if os.path.exists(local_dir):
            for city_item in os.listdir(local_dir):
                city_path = os.path.join(local_dir, city_item)
                if os.path.isdir(city_path):
                    # Apply city filter
                    if target_cities and city_item not in target_cities:
                        continue
                        
                    for map_type_item in os.listdir(city_path):
                        map_type_path = os.path.join(city_path, map_type_item)
                        if os.path.isdir(map_type_path):
                            # Apply map type filter
                            if target_map_types and map_type_item not in target_map_types:
                                continue
                                
                            local_combinations.add((city_item, map_type_item))
        
        logger.info(f"📊 Found {len(local_combinations)} local combinations to check")
        
        # Check each combination in Spaces
        logger.info("🔍 Checking existing combinations in Spaces...")
        with tqdm(total=len(local_combinations), desc="Checking combinations") as pbar:
            for city, map_type in local_combinations:
                check_result = self.check_city_map_type_exists_in_spaces(city, map_type, s3_prefix)
                
                if check_result['exists']:
                    if city not in existing_combinations:
                        existing_combinations[city] = {}
                    existing_combinations[city][map_type] = check_result
                
                pbar.update(1)
                pbar.set_postfix(existing=len(existing_combinations))
        
        return existing_combinations

    def upload_with_enhanced_filtering(self, local_dir, s3_prefix='', max_workers=5, 
                                     target_cities=None, target_map_types=None, target_zoom_levels=None,
                                     skip_existing_combinations=True, dry_run=False):
        """Optimized upload method with batch processing and better performance"""
        
        # Increase default workers for better performance
        max_workers = min(max_workers, 15)  # Cap at 15 for DO Spaces
        
        logger.info(f"🚀 Starting Optimized Enhanced Multi-Map Upload")
        logger.info(f"👥 Using {max_workers} parallel workers")
        
        self.stats['start_time'] = time.time()
        
        # Use parallel scanning
        files_to_upload = self.scan_multi_map_directory(
            local_dir, s3_prefix, target_cities, target_map_types, target_zoom_levels
        )
        
        if not files_to_upload:
            logger.warning("⚠️ No files found to upload after filtering")
            return
        
        # Batch check existing files for better performance
        if skip_existing_combinations:
            logger.info("🔍 Batch checking existing files...")
            s3_keys = [f['s3_key'] for f in files_to_upload]
            existing_files = self.batch_check_existence(s3_keys)
            
            if existing_files:
                # Filter out existing files
                original_count = len(files_to_upload)
                files_to_upload = [f for f in files_to_upload if f['s3_key'] not in existing_files]
                skipped_count = original_count - len(files_to_upload)
                logger.info(f"⏭️ Skipping {skipped_count} existing files")
        
        if not files_to_upload:
            logger.info("✅ All content already exists!")
            return
        
        # Update statistics
        self.stats['total_files'] = len(files_to_upload)
        self.stats['total_bytes'] = sum(f['file_info']['size'] for f in files_to_upload)
        
        if dry_run:
            self.show_dry_run_summary(files_to_upload)
            return
        
        # Optimized parallel upload
        self.perform_optimized_parallel_upload(files_to_upload, max_workers)
        
        # Generate report
        self.stats['end_time'] = time.time()
        self.generate_comprehensive_report()
    
    def perform_optimized_parallel_upload(self, files_to_upload, max_workers):
        """Optimized parallel upload with better resource management"""
        logger.info(f"📤 Starting optimized upload of {len(files_to_upload):,} files...")
        
        # Sort files by size (upload smaller files first for faster initial progress)
        files_to_upload.sort(key=lambda x: x['file_info']['size'])
        
        upload_queue = files_to_upload.copy()
        completed_uploads = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with tqdm(total=len(files_to_upload), desc="Uploading", unit="file") as pbar:
                
                # Submit initial batch
                active_futures = {}
                for _ in range(min(max_workers * 2, len(upload_queue))):  # Queue more tasks
                    if upload_queue:
                        file_data = upload_queue.pop(0)
                        future = executor.submit(
                            self.upload_single_file_optimized,
                            file_data['local_path'],
                            file_data['s3_key'],
                            file_data['file_info'],
                            file_data['city'],
                            file_data['map_type'],
                            file_data['zoom'],
                            file_data.get('district')
                        )
                        active_futures[future] = file_data
                
                # Process completions and submit new tasks
                while active_futures:
                    # Wait for at least one to complete
                    done_futures = []
                    for future in list(active_futures.keys()):
                        if future.done():
                            done_futures.append(future)
                    
                    if not done_futures:
                        time.sleep(0.01)  # Small sleep to prevent busy waiting
                        continue
                    
                    # Process completed futures
                    for future in done_futures:
                        file_data = active_futures.pop(future)
                        try:
                            result = future.result()
                            self.update_progress_bar(pbar, result, file_data)
                            completed_uploads += 1
                            
                            # Save resume state periodically - FIX: Use optimized version
                            if completed_uploads % 100 == 0:
                                self.save_resume_state_optimized()
                                
                        except Exception as e:
                            logger.error(f"❌ Task error for {file_data['s3_key']}: {e}")
                            pbar.update(1)
                    
                    # Submit new tasks to keep queue full
                    while len(active_futures) < max_workers * 2 and upload_queue:
                        file_data = upload_queue.pop(0)
                        future = executor.submit(
                            self.upload_single_file_optimized,
                            file_data['local_path'],
                            file_data['s3_key'],
                            file_data['file_info'],
                            file_data['city'],
                            file_data['map_type'],
                            file_data['zoom'],
                            file_data.get('district')
                        )
                        active_futures[future] = file_data
        
        # Final save
        self.save_resume_state()

    def log_existing_combinations(self, existing_combinations):
        """Log existing combinations found in Spaces"""
        print("\n📋 EXISTING COMBINATIONS IN SPACES:")
        print("=" * 40)
        
        total_existing_files = 0
        total_existing_size = 0
        
        for city, map_types in existing_combinations.items():
            # city_display = CITY_CONFIG.get(city, {}).get('display_name', city)
            print(f"🏙️ ({city}):")
            
            for map_type, info in map_types.items():
                map_display = MAP_TYPE_CONFIG.get(map_type, {}).get('display_name', map_type)
                color = MAP_TYPE_CONFIG.get(map_type, {}).get('color', '⚫')
                size_mb = info['total_size'] / 1024 / 1024
                
                print(f"   {color} {map_display}: {info['file_count']:,} files ({size_mb:.1f} MB)")
                
                total_existing_files += info['file_count']
                total_existing_size += info['total_size']
        
        print(f"\n📊 TOTAL EXISTING: {total_existing_files:,} files ({total_existing_size/1024/1024:.1f} MB)")
        print("⏭️ These combinations will be SKIPPED during upload")

    def filter_existing_combinations(self, files_to_upload, existing_combinations):
        """Filter out files from existing combinations"""
        original_count = len(files_to_upload)
        filtered_files = []
        
        for file_data in files_to_upload:
            city = file_data['city']
            map_type = file_data['map_type']
            
            # Skip if this combination already exists
            if (city and map_type and 
                city in existing_combinations and 
                map_type in existing_combinations[city]):
                logger.debug(f"⏭️ Skipping existing combination: {city}/{map_type}")
                continue
            
            filtered_files.append(file_data)
        
        skipped_count = original_count - len(filtered_files)
        if skipped_count > 0:
            logger.info(f"⏭️ Filtered out {skipped_count:,} files from existing combinations")
            logger.info(f"📤 Will upload {len(filtered_files):,} files")
        
        return filtered_files

    def show_dry_run_summary(self, files_to_upload):
        """Show what would be uploaded in dry run mode"""
        print(f"\n🧪 DRY RUN SUMMARY - WHAT WOULD BE UPLOADED:")
        print("=" * 60)
        
        upload_summary = {}
        total_size = 0
        
        for file_data in files_to_upload:
            city = file_data['city'] or 'unknown'
            map_type = file_data['map_type'] or 'unknown'
            size = file_data['file_info']['size']
            
            if city not in upload_summary:
                upload_summary[city] = {}
            if map_type not in upload_summary[city]:
                upload_summary[city][map_type] = {'files': 0, 'size': 0}
            
            upload_summary[city][map_type]['files'] += 1
            upload_summary[city][map_type]['size'] += size
            total_size += size
        
        for city, map_types in upload_summary.items():
            # city_display = CITY_CONFIG.get(city, {}).get('display_name', city)
            print(f"🏙️ ({city}):")
            
            for map_type, stats in map_types.items():
                map_display = MAP_TYPE_CONFIG.get(map_type, {}).get('display_name', map_type)
                color = MAP_TYPE_CONFIG.get(map_type, {}).get('color', '⚫')
                size_mb = stats['size'] / 1024 / 1024
                print(f"   {color} {map_display}: {stats['files']:,} files ({size_mb:.1f} MB)")
        
        print(f"\n📊 TOTAL TO UPLOAD: {len(files_to_upload):,} files ({total_size/1024/1024:.1f} MB)")
        print("🧪 This was a DRY RUN - no files were actually uploaded")

    def perform_parallel_upload(self, files_to_upload, max_workers):
        """Perform parallel upload with progress tracking"""
        logger.info(f"📤 Starting parallel upload of {len(files_to_upload):,} files...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            with tqdm(total=len(files_to_upload), desc="Uploading files", unit="file") as pbar:
                
                # Submit all upload tasks
                future_to_file = {
                    executor.submit(
                        self.upload_single_file,
                        file_data['local_path'],
                        file_data['s3_key'],
                        file_data['file_info'],
                        file_data['city'],
                        file_data['map_type'],
                        file_data['zoom'],
                        file_data.get('district')
                    ): file_data for file_data in files_to_upload
                }
                
                # Process completed uploads
                for future in as_completed(future_to_file):
                    file_data = future_to_file[future]
                    try:
                        result = future.result()
                        
                        # Update progress bar
                        self.update_progress_bar(pbar, result, file_data)
                        
                        # Save resume state periodically - FIX: Use optimized version
                        if (self.stats['uploaded_files'] + self.stats['skipped_files']) % 50 == 0:
                            self.save_resume_state_optimized()
                            
                    except Exception as e:
                        logger.error(f"❌ Task error for {file_data['s3_key']}: {e}")
                        pbar.update(1)
        
        # Final save of resume state
        self.save_resume_state()

    def update_progress_bar(self, pbar, result, file_data):
        """Update progress bar with meaningful status"""
        if result['success']:
            city = file_data['city'] or 'unknown'
            map_type = file_data['map_type'] or 'unknown'
            
            if result.get('skipped'):
                pbar.set_postfix(status=f"⏭️ {city}/{map_type}")
            else:
                pbar.set_postfix(status=f"✅ {city}/{map_type}")
        else:
            pbar.set_postfix(status=f"❌ {result['file']}")
            logger.error(f"❌ Upload failed: {result['file']} - {result.get('error', 'Unknown error')}")
        
        pbar.update(1)

    def generate_comprehensive_report(self):
        """Generate comprehensive upload report with all statistics"""
        elapsed_time = self.stats['end_time'] - self.stats['start_time']
        
        # Calculate performance metrics
        upload_rate_mbps = (self.stats['uploaded_bytes'] / 1024 / 1024) / elapsed_time if elapsed_time > 0 else 0
        files_per_second = self.stats['uploaded_files'] / elapsed_time if elapsed_time > 0 else 0
        
        # Create comprehensive report
        report = {
            'session_info': {
                'uploader_version': '2.0-enhanced',
                'structure_type': 'multi-map',
                'structure_pattern': 'cities/<city>/<map_type>/<zoom>/',
                'timestamp': datetime.now().isoformat(),
                'session_duration_seconds': elapsed_time,
                'session_duration_minutes': elapsed_time / 60
            },
            'summary': {
                'total_files': self.stats['total_files'],
                'uploaded_files': self.stats['uploaded_files'],
                'skipped_files': self.stats['skipped_files'],
                'failed_files': self.stats['failed_files'],
                'success_rate': (self.stats['uploaded_files'] / self.stats['total_files'] * 100) if self.stats['total_files'] > 0 else 0,
                'cities_processed': len(self.stats['city_stats']),
                'map_types_processed': len(self.stats['map_type_stats']),
                'combinations_processed': len(self.stats['combination_stats'])
            },
            'data_transfer': {
                'total_size_bytes': self.stats['total_bytes'],
                'total_size_mb': self.stats['total_bytes'] / 1024 / 1024,
                'uploaded_size_bytes': self.stats['uploaded_bytes'],
                'uploaded_size_mb': self.stats['uploaded_bytes'] / 1024 / 1024,
                'upload_rate_mbps': upload_rate_mbps
            },
            'performance': {
                'files_per_second': files_per_second,
                'start_time': datetime.fromtimestamp(self.stats['start_time']).isoformat(),
                'end_time': datetime.fromtimestamp(self.stats['end_time']).isoformat(),
                'api_calls_estimated': self.api_call_count
            },
            'breakdowns': {
                'city_stats': self.stats['city_stats'],
                'map_type_stats': self.stats['map_type_stats'],
                'combination_stats': self.stats['combination_stats'],
                'zoom_level_stats': self.stats['zoom_level_stats']
            },
            'spaces_info': {
                'bucket': self.bucket_name,
                'endpoint': self.endpoint_url,
                'region': self.region,
                'cdn_enabled': True
            }
        }
        
        # Save reports
        self.save_reports(report)
        
        # Print summary
        self.print_upload_summary(report)

    def save_reports(self, report):
        """Save both JSON and text reports"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save JSON report
        json_report_file = f"enhanced_upload_report_{timestamp}.json"
        with open(json_report_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Generate and save text summary
        text_summary = self.generate_text_summary(report)
        text_report_file = f"enhanced_upload_summary_{timestamp}.txt"
        with open(text_report_file, 'w', encoding='utf-8') as f:
            f.write(text_summary)
        
        logger.info(f"📋 Reports saved:")
        logger.info(f"  📄 JSON: {json_report_file}")
        logger.info(f"  📝 Text: {text_report_file}")

    def generate_text_summary(self, report):
        """Generate detailed text summary"""
        summary = f"""
    # ENHANCED MULTI-MAP UPLOAD REPORT
    Generated: {report['session_info']['timestamp']}
    Duration: {report['session_info']['session_duration_minutes']:.1f} minutes
    Uploader: v{report['session_info']['uploader_version']}

    ## 📁 STRUCTURE UPLOADED
    Pattern: {report['session_info']['structure_pattern']}
    Example: hanoi/qh-2030/12/3249_1865.png

    ## 📊 OVERALL SUMMARY
    • Total files: {report['summary']['total_files']:,}
    • Uploaded: {report['summary']['uploaded_files']:,}
    • Skipped: {report['summary']['skipped_files']:,}
    • Failed: {report['summary']['failed_files']:,}
    • Success rate: {report['summary']['success_rate']:.1f}%
    • Total size: {report['data_transfer']['total_size_mb']:.1f} MB
    • Upload rate: {report['data_transfer']['upload_rate_mbps']:.1f} MB/s
    • Cities: {report['summary']['cities_processed']}
    • Map types: {report['summary']['map_types_processed']}
    • Combinations: {report['summary']['combinations_processed']}

    ## 🏙️ CITY BREAKDOWN
    """
        
        # Add city breakdown - use city name directly
        for city, stats in report['breakdowns']['city_stats'].items():
            total_city = stats['uploaded'] + stats['skipped']
            city_size_mb = stats['bytes'] / 1024 / 1024
            summary += f"""
    🏙️ {city}
    • Files: {total_city:,} ({stats['uploaded']:,} uploaded, {stats['skipped']:,} skipped)
    • Size: {city_size_mb:.1f} MB
    • Failures: {stats['failed']}
    """
            
            # Add map type breakdown
            summary += "\n## 🗺️ MAP TYPE BREAKDOWN\n"
            
            for map_type, stats in report['breakdowns']['map_type_stats'].items():
                map_config = MAP_TYPE_CONFIG.get(map_type, {})
                map_display = map_config.get('display_name', map_type)
                color = map_config.get('color', '⚫')
                total_map = stats['uploaded'] + stats['skipped']
                map_size_mb = stats['bytes'] / 1024 / 1024
                summary += f"""
    {color} {map_display} ({map_type})
    • Files: {total_map:,} ({stats['uploaded']:,} uploaded, {stats['skipped']:,} skipped)
    • Size: {map_size_mb:.1f} MB
    • Failures: {stats['failed']}
    """
            
            # Add zoom level breakdown if available
            if report['breakdowns']['zoom_level_stats']:
                summary += "\n## 🔍 ZOOM LEVEL BREAKDOWN\n"
                
                sorted_zooms = sorted(report['breakdowns']['zoom_level_stats'].items(), 
                                    key=lambda x: int(x[0]) if str(x[0]).isdigit() else 999)
                
                for zoom, stats in sorted_zooms:
                    total_zoom = stats['uploaded'] + stats['skipped']
                    zoom_size_mb = stats['bytes'] / 1024 / 1024
                    detail_level = 'High detail' if str(zoom).isdigit() and int(zoom) >= 14 else 'Medium detail' if str(zoom).isdigit() and int(zoom) >= 10 else 'Low detail'
                    summary += f"""
    🔍 Zoom {zoom} ({detail_level})
    • Files: {total_zoom:,} ({stats['uploaded']:,} uploaded, {stats['skipped']:,} skipped)
    • Size: {zoom_size_mb:.1f} MB
    • Failures: {stats['failed']}
    """
            
            return summary

    def print_upload_summary(self, report):
        """Print concise upload summary to console"""
        print(f"\n🎉 ENHANCED UPLOAD COMPLETE!")
        print("=" * 60)
        print(f"⏱️ Duration: {report['session_info']['session_duration_minutes']:.1f} minutes")
        print(f"📁 Total files: {report['summary']['total_files']:,}")
        print(f"✅ Uploaded: {report['summary']['uploaded_files']:,}")
        print(f"⏭️ Skipped: {report['summary']['skipped_files']:,}")
        print(f"❌ Failed: {report['summary']['failed_files']:,}")
        print(f"📊 Success rate: {report['summary']['success_rate']:.1f}%")
        print(f"💾 Size: {report['data_transfer']['total_size_mb']:.1f} MB")
        print(f"📈 Speed: {report['data_transfer']['upload_rate_mbps']:.1f} MB/s")
        print(f"🏙️ Cities: {report['summary']['cities_processed']}")
        print(f"🗺️ Map types: {report['summary']['map_types_processed']}")
        
        # Print top cities
        if report['breakdowns']['city_stats']:
            print(f"\n🏙️ TOP CITIES:")
            sorted_cities = sorted(report['breakdowns']['city_stats'].items(), 
                                key=lambda x: x[1]['uploaded'] + x[1]['skipped'], reverse=True)
            for city, stats in sorted_cities[:5]:
                total_city = stats['uploaded'] + stats['skipped']
                print(f"  • {city}: {total_city:,} files ({stats['bytes']/1024/1024:.1f} MB)")
        
        # Print map types
        if report['breakdowns']['map_type_stats']:
            print(f"\n🗺️ MAP TYPES:")
            sorted_maps = sorted(report['breakdowns']['map_type_stats'].items(),
                               key=lambda x: MAP_TYPE_CONFIG.get(x[0], {}).get('priority', 99))
            for map_type, stats in sorted_maps:
                map_config = MAP_TYPE_CONFIG.get(map_type, {})
                map_display = map_config.get('display_name', map_type)
                color = map_config.get('color', '⚫')
                total_map = stats['uploaded'] + stats['skipped']
                print(f"  {color} {map_display}: {total_map:,} files ({stats['bytes']/1024/1024:.1f} MB)")

    def cleanup_resume_state(self):
        """Clean up resume state file after successful upload"""
        try:
            if os.path.exists(self.resume_file):
                os.remove(self.resume_file)
                logger.info("🧹 Cleaned up resume state file")
        except Exception as e:
            logger.warning(f"⚠️ Could not clean up resume state: {e}")


def load_config():
    """Load configuration from multiple sources with validation"""
    config = {}
    
    # Try to load from config file
    config_file = 'spaces_config.json'
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
            logger.info(f"📋 Loaded config from {config_file}")
        except Exception as e:
            logger.warning(f"⚠️ Could not load config file: {e}")
    
    # Override with environment variables if available
    env_mapping = {
        'access_key': 'DO_SPACES_ACCESS_KEY',
        'secret_key': 'DO_SPACES_SECRET_KEY',
        'endpoint_url': 'DO_SPACES_ENDPOINT',
        'bucket_name': 'DO_SPACES_BUCKET',
        'region': 'DO_SPACES_REGION'
    }
    
    for config_key, env_key in env_mapping.items():
        env_value = os.getenv(env_key)
        if env_value:
            config[config_key] = env_value
            logger.info(f"📋 Using environment variable for {config_key}")
    
    # Set defaults
    config.setdefault('region', 'sgp1')
    
    return config

def create_sample_config():
    """Create a comprehensive sample configuration file"""
    sample_config = {
        "access_key": "YOUR_DO_SPACES_ACCESS_KEY",
        "secret_key": "YOUR_DO_SPACES_SECRET_KEY",
        "endpoint_url": "https://sgp1.digitaloceanspaces.com",
        "bucket_name": "your-bucket-name",
        "region": "sgp1",
        "_comments": {
            "access_key": "Your Digital Ocean Spaces access key",
            "secret_key": "Your Digital Ocean Spaces secret key", 
            "endpoint_url": "Spaces endpoint URL (replace sgp1 with your region)",
            "bucket_name": "Your Spaces bucket name",
            "region": "Region code (sgp1, nyc3, ams3, fra1, blr1, sfo3, tor1)"
        },
        "_example_usage": {
            "basic": "python enhanced_spaces_uploader.py",
            "with_filters": "python enhanced_spaces_uploader.py --cities hanoi,danang --map-types qh-2030,kh-2025",
            "dry_run": "python enhanced_spaces_uploader.py --dry-run"
        }
    }
    
    config_file = 'spaces_config.json'
    with open(config_file, 'w') as f:
        json.dump(sample_config, f, indent=2)
    
    print(f"📝 Created comprehensive config file: {config_file}")
    print("Please edit this file with your actual Digital Ocean Spaces credentials")
    print("\n🔧 Available regions:")
    print("  • sgp1 (Singapore)")
    print("  • nyc3 (New York)")
    print("  • ams3 (Amsterdam)")
    print("  • fra1 (Frankfurt)")
    print("  • blr1 (Bangalore)")
    print("  • sfo3 (San Francisco)")

def parse_command_line_args():
    """Parse command line arguments for advanced usage"""
    parser = argparse.ArgumentParser(
        description='Enhanced Multi-Map Digital Ocean Spaces Uploader',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                                    # Interactive mode
  %(prog)s --cities hanoi,danang             # Upload specific cities  
  %(prog)s --map-types qh-2030,kh-2025      # Upload specific map types
  %(prog)s --zoom-levels 10,11,12            # Upload specific zoom levels
  %(prog)s --dry-run                         # Preview what would be uploaded
  %(prog)s --skip-existing=false             # Force re-upload existing files
  %(prog)s --workers 10                      # Use 10 parallel workers
        """
    )
    
    parser.add_argument('--local-dir', 
                       default='downloaded_tiles/cities',
                       help='Local directory to upload (default: downloaded_tiles/cities)')
    
    parser.add_argument('--s3-prefix',
                       default='guland-tiles',
                       help='S3 prefix for uploads (default: guland-tiles)')
    
    parser.add_argument('--cities',
                       help='Comma-separated list of cities to upload (e.g., hanoi,danang,hcmc)')
    
    parser.add_argument('--map-types',
                       help='Comma-separated list of map types (e.g., qh-2030,kh-2025)')
    
    parser.add_argument('--zoom-levels',
                       help='Comma-separated list of zoom levels (e.g., 10,11,12,13,14)')
    
    parser.add_argument('--skip-existing',
                       type=bool, default=True,
                       help='Skip existing combinations (default: true)')
    
    parser.add_argument('--workers',
                       type=int, default=5,
                       help='Number of parallel workers (default: 5)')
    
    parser.add_argument('--dry-run',
                       action='store_true',
                       help='Show what would be uploaded without actually uploading')
    
    parser.add_argument('--create-config',
                       action='store_true',
                       help='Create sample configuration file and exit')
    
    return parser.parse_args()

def interactive_mode():
    """Full interactive mode for user-friendly configuration"""
    print("🚀 ENHANCED MULTI-MAP SPACES UPLOADER v2.0")
    print("Upload city tiles with advanced map type awareness")
    print("Structure: cities/<city>/<map_type>/<zoom>/")
    print("=" * 70)
    
    # Load configuration
    config = load_config()
    
    # Check if we have required config
    required_keys = ['access_key', 'secret_key', 'endpoint_url', 'bucket_name']
    missing_keys = [key for key in required_keys if not config.get(key)]
    
    if missing_keys:
        print(f"❌ Missing configuration: {', '.join(missing_keys)}")
        print("\nConfiguration options:")
        print("1. 📝 Create a spaces_config.json file with your credentials")
        print("2. 🔧 Set environment variables:")
        for key in missing_keys:
            env_key = {
                'access_key': 'DO_SPACES_ACCESS_KEY',
                'secret_key': 'DO_SPACES_SECRET_KEY',
                'endpoint_url': 'DO_SPACES_ENDPOINT',
                'bucket_name': 'DO_SPACES_BUCKET'
            }.get(key, key.upper())
            print(f"   {env_key}")
        
        create_config = input("\n📝 Create sample config file? (y/n): ").lower()
        if create_config == 'y':
            create_sample_config()
        return
    
    print(f"✅ Configuration loaded:")
    print(f"  🪣 Bucket: {config['bucket_name']}")
    print(f"  🌐 Endpoint: {config['endpoint_url']}")
    print(f"  🗺️ Region: {config['region']}")
    
    # Get local directory
    default_dir = "downloaded_tiles/cities"
    local_dir = input(f"\n📁 Local directory to upload (default: {default_dir}): ").strip()
    if not local_dir:
        local_dir = default_dir
    
    if not os.path.exists(local_dir):
        print(f"❌ Directory not found: {local_dir}")
        return
    
    # S3 prefix configuration
    print(f"\n📂 S3 PREFIX CONFIGURATION:")
    print("=" * 30)
    print("1. 📁 Use prefix: 'guland-tiles' (recommended)")
    print("   → Result: bucket/guland-tiles/hanoi/qh-2030/12/tile.png")
    print("2. 🚫 No prefix: direct to bucket root")
    print("   → Result: bucket/hanoi/qh-2030/12/tile.png")
    print("3. ✏️ Custom prefix")
    
    while True:
        prefix_choice = input("\nChoose option (1/2/3, default: 1): ").strip()
        
        if prefix_choice == '' or prefix_choice == '1':
            s3_prefix = 'guland-tiles'
            break
        elif prefix_choice == '2':
            s3_prefix = ''
            break
        elif prefix_choice == '3':
            custom_prefix = input("Enter custom prefix: ").strip()
            s3_prefix = custom_prefix
            break
        else:
            print("❌ Invalid choice")
            continue
    
    # Map type filtering
    print(f"\n🗺️ MAP TYPE FILTERING:")
    print("=" * 25)
    print("Available map types:")
    for i, (folder_name, map_config) in enumerate(MAP_TYPE_CONFIG.items(), 1):
        if folder_name != 'unknown':
            color = map_config.get('color', '⚫')
            print(f"{i}. {color} {map_config['display_name']} ({folder_name})")
    
    print("\nPreset options:")
    print("A. 🌟 Upload ALL map types")
    print("B. 🎯 Upload only QH 2030 (recommended)")
    print("C. 🎯 Upload only KH 2025")
    print("D. 🎯 Upload QH 2030 + KH 2025 (popular combo)")
    print("E. ✏️ Custom selection")
    
    map_choice = input("Choose option (A/B/C/D/E, default: B): ").upper().strip()
    
    target_map_types = None
    if map_choice == '' or map_choice == 'B':
        target_map_types = ['qh-2030']
    elif map_choice == 'C':
        target_map_types = ['kh-2025']
    elif map_choice == 'D':
        target_map_types = ['qh-2030', 'kh-2025']
    elif map_choice == 'E':
        available_types = [k for k in MAP_TYPE_CONFIG.keys() if k != 'unknown']
        print(f"Available: {', '.join(available_types)}")
        selection = input("Enter map types (comma-separated): ").strip()
        target_map_types = [t.strip() for t in selection.split(',') if t.strip()]
    
    if target_map_types:
        selected_names = [MAP_TYPE_CONFIG.get(mt, {}).get('display_name', mt) for mt in target_map_types]
        print(f"🎯 Selected map types: {', '.join(selected_names)}")
    else:
        print("🎯 Selected: ALL map types")
    
    # City filtering
    print(f"\n🏙️ CITY FILTERING:")
    print("=" * 20)
    print("1. 🌟 Upload ALL cities")
    print("2. 🎯 Select specific cities")
    print("3. 🎯 Major cities only (Hanoi, HCMC, Da Nang)")
    
    city_choice = input("Choose option (1/2/3, default: 1): ").strip()
    
    target_cities = None
    if city_choice == '2':
        # Scan for available cities
        available_cities = []
        if os.path.exists(local_dir):
            for item in os.listdir(local_dir):
                item_path = os.path.join(local_dir, item)
                if os.path.isdir(item_path):
                    available_cities.append(item)
        
        if available_cities:
            print(f"Available cities: {', '.join(sorted(available_cities))}")
            selection = input("Enter cities (comma-separated): ").strip()
            target_cities = [c.strip() for c in selection.split(',') if c.strip()]
            
            if target_cities:
                print(f"🎯 Selected cities: {', '.join(target_cities)}")
            else:
                print("🎯 No cities selected - will upload ALL")
                target_cities = None
    elif city_choice == '3':
        target_cities = ['hanoi', 'hcmc', 'danang']
        print("🎯 Selected: Major cities (Hanoi, HCMC, Da Nang)")
    
    # Performance and upload options
    print(f"\n⚡ PERFORMANCE & UPLOAD OPTIONS:")
    print("=" * 35)
    
    # Existing content handling
    print("Existing content handling:")
    print("1. 🚀 Auto-skip existing combinations (recommended)")
    print("2. 🔄 Upload all (may overwrite existing)")
    
    skip_choice = input("Choose option (1/2, default: 1): ").strip()
    skip_existing = skip_choice != '2'
    
    # Max workers
    print(f"\n⚡ PERFORMANCE OPTIMIZATION:")
    print("=" * 35)
    
    print("Performance profile:")
    print("1. 🚀 Maximum Speed (15 workers, aggressive caching)")
    print("2. ⚖️ Balanced (10 workers, moderate caching)") 
    print("3. 🐌 Conservative (5 workers, minimal caching)")
    print("4. ✏️ Custom settings")
    
    perf_choice = input("Choose profile (1/2/3/4, default: 2): ").strip()
    
    if perf_choice == '1':
        max_workers = 15
        print("🚀 Maximum speed profile selected")
    elif perf_choice == '3':
        max_workers = 5  
        print("🐌 Conservative profile selected")
    elif perf_choice == '4':
        max_workers_input = input("Max parallel uploads (1-20, default: 10): ").strip()
        try:
            max_workers = int(max_workers_input) if max_workers_input else 10
            max_workers = max(1, min(20, max_workers))
        except ValueError:
            max_workers = 10
    else:
        max_workers = 10
        print("⚖️ Balanced profile selected")
    
    # Dry run option
    dry_run_choice = input("Dry run mode (preview only, no upload)? (y/n, default: n): ").lower().strip()
    dry_run = dry_run_choice == 'y'
    
    # Show final configuration
    print(f"\n📋 FINAL CONFIGURATION:")
    print("=" * 30)
    print(f"  📁 Local: {local_dir}")
    print(f"  🪣 Bucket: {config['bucket_name']}")
    print(f"  📂 Prefix: {s3_prefix if s3_prefix else '(none)'}")
    print(f"  🗺️ Map types: {target_map_types if target_map_types else 'ALL'}")
    print(f"  🏙️ Cities: {target_cities if target_cities else 'ALL'}")
    print(f"  ⏭️ Skip existing: {skip_existing}")
    print(f"  👥 Workers: {max_workers}")
    print(f"  🧪 Dry run: {dry_run}")
    
    if s3_prefix:
        print(f"  🔗 Example URL: https://{config['bucket_name']}.{config['region']}.digitaloceanspaces.com/{s3_prefix}/hanoi/qh-2030/12/tile.png")
    else:
        print(f"  🔗 Example URL: https://{config['bucket_name']}.{config['region']}.digitaloceanspaces.com/hanoi/qh-2030/12/tile.png")
    
    if dry_run:
        print("\n🧪 DRY RUN MODE: Will show what would be uploaded without actually uploading")
    
    confirm = input("\n✅ Proceed with upload? (y/n): ").lower()
    if confirm != 'y':
        print("❌ Upload cancelled")
        return
    
    try:
        # Initialize enhanced uploader
        print("\n🔧 Initializing enhanced uploader...")
        uploader = EnhancedMultiMapSpacesUploader(
            access_key=config['access_key'],
            secret_key=config['secret_key'],
            endpoint_url=config['endpoint_url'],
            bucket_name=config['bucket_name'],
            region=config['region']
        )
        
        # Start enhanced upload
        print("🚀 Starting enhanced upload process...")
        
        uploader.upload_with_enhanced_filtering(
            local_dir=local_dir,
            s3_prefix=s3_prefix,
            max_workers=max_workers,
            target_cities=target_cities,
            target_map_types=target_map_types,
            skip_existing_combinations=skip_existing,
            dry_run=dry_run
        )
        
        # Post-upload actions (only if not dry run and no failures)
        if not dry_run and uploader.stats.get('failed_files', 0) == 0:
            cleanup = input("\n🧹 Clean up resume state file? (y/n): ").lower()
            if cleanup == 'y':
                uploader.cleanup_resume_state()
        
    except Exception as e:
        logger.error(f"❌ Upload failed: {e}")
        print(f"❌ Upload failed: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main entry point with support for both CLI and interactive modes"""
    args = parse_command_line_args()
    
    # Handle special commands
    if args.create_config:
        create_sample_config()
        return
    
    # Determine if running in CLI mode or interactive mode
    cli_mode = any([
        args.cities, args.map_types, args.zoom_levels, 
        args.dry_run, args.local_dir != 'downloaded_tiles/cities',
        args.s3_prefix != 'guland-tiles', args.workers != 5
    ])
    
    if cli_mode:
        # CLI mode
        print("🚀 Enhanced Multi-Map Spaces Uploader (CLI Mode)")
        print("=" * 60)
        
        config = load_config()
        required_keys = ['access_key', 'secret_key', 'endpoint_url', 'bucket_name']
        missing_keys = [key for key in required_keys if not config.get(key)]
        
        if missing_keys:
            print(f"❌ Missing configuration: {', '.join(missing_keys)}")
            print("Run with --create-config to create a sample configuration file")
            return
        
        # Parse CLI arguments
        target_cities = args.cities.split(',') if args.cities else None
        target_map_types = args.map_types.split(',') if args.map_types else None
        target_zoom_levels = [int(z) for z in args.zoom_levels.split(',')] if args.zoom_levels else None
        
        # Initialize and run uploader
        try:
            uploader = EnhancedMultiMapSpacesUploader(
                access_key=config['access_key'],
                secret_key=config['secret_key'],
                endpoint_url=config['endpoint_url'],
                bucket_name=config['bucket_name'],
                region=config['region']
            )
            
            uploader.upload_with_enhanced_filtering(
                local_dir=args.local_dir,
                s3_prefix=args.s3_prefix,
                max_workers=args.workers,
                target_cities=target_cities,
                target_map_types=target_map_types,
                target_zoom_levels=target_zoom_levels,
                skip_existing_combinations=args.skip_existing,
                dry_run=args.dry_run
            )
            
        except Exception as e:
            logger.error(f"❌ Upload failed: {e}")
            print(f"❌ Upload failed: {e}")
            return
    else:
        # Interactive mode
        interactive_mode()

if __name__ == "__main__":
    main()