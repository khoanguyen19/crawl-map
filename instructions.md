# GULAND CRAWLER INSTRUCTION

## Context
Bạn đang làm việc với hệ thống Guland Crawler - một web scraper chuyên dụng để thu thập bản đồ quy hoạch Việt Nam từ guland.vn bằng Selenium automation.

## Project Structure
```
guland_analysis/
├── browser_crawler.py          # Main crawler module
├── map_interaction_handler.py  # Map interaction logic  
├── tile_downloader.py         # Download and organize tiles
├── requirements.txt           # Python dependencies
├── setup.py                   # Package setup
├── chromedriver              # Chrome WebDriver binary
├── browser_crawler.log       # Runtime logs
├── output_browser_crawl/     # Crawl results and reports
├── downloaded_tiles/         # Downloaded map tiles organized by location
└── instructions.md           # This instruction file
```

## Key Components

### 1. BrowserGulandCrawler Class (`browser_crawler.py`)
**Purpose**: Main controller class orchestrating the entire crawling process
**Key methods**:
- `setup_driver()` - Initialize Chrome browser with network logging capabilities
- `systematic_zoom_coverage()` - Systematically crawl all zoom levels 10-18 for comprehensive coverage
- `extract_tile_urls()` - Parse network requests to extract tile URLs using regex patterns
- `run_browser_crawl(max_hours)` - Main execution method with configurable timeout
- `crawl_location_with_full_coverage()` - Full coverage crawl for a single location with immediate download
- `generate_final_report()` - Create comprehensive reports with download statistics

**Configuration Options**:
- `headless`: Run browser in background (default: False for debugging)
- `enable_download`: Auto-download discovered tiles (default: True)
- `download_workers`: Number of parallel download threads (default: 5)

### 2. MapInteractionHandler Class (`map_interaction_handler.py`)
**Purpose**: Handle realistic map interactions to trigger tile loading
**Key methods**:
- `set_map_zoom(zoom)` - Set specific zoom level via JavaScript
- `comprehensive_map_coverage(zoom, duration)` - Systematic coverage pattern for a zoom level
- `simulate_map_interaction(location, duration)` - Realistic user behavior simulation
- `detect_city_boundaries()` - Extract map bounds for coverage calculation
- `calculate_tile_coverage_needed()` - Calculate expected tile count for validation

**Interaction Types**:
- Zoom in/out via mouse wheel and JavaScript
- Pan and drag operations
- Double-click zoom
- Keyboard navigation
- Safe coordinate calculation to avoid out-of-bounds errors

### 3. GulandTileDownloader Class (`tile_downloader.py`)
**Purpose**: Download and organize map tiles with Vietnamese-specific categorization
**Key methods**:
- `download_tiles_batch(tiles, location)` - Parallel download with ThreadPoolExecutor
- `get_tile_type_from_url(url)` - Detect Vietnamese map types from URL patterns
- `create_enhanced_directory_structure()` - Organize tiles by location and map type
- `validate_image_file()` - Verify downloaded files are valid images
- `generate_download_report()` - Create detailed Vietnamese download reports

**Download Features**:
- Concurrent downloads with configurable worker threads
- Automatic retry on failures
- File validation and cleanup
- Progress tracking and statistics
- Organized folder structure with Vietnamese naming

## Vietnam Map Types Detected
- `quy_hoach_2030` - Quy hoạch 2030 (Planning 2030)
- `ke_hoach_2025` - Kế hoạch 2025 (Plan 2025)
- `quy_hoach_phan_khu` - Quy hoạch phân khu (District planning)
- `hien_trang` - Hiện trạng (Current state)
- `satellite` - Bản đồ vệ tinh (Satellite imagery)
- `terrain` - Bản đồ địa hình (Terrain map)
- `street` - Bản đồ đường phố (Street map)
- `administrative` - Bản đồ hành chính (Administrative boundaries)
- `guland_generic` - Generic Guland tiles

## Coverage Strategy
**Zoom Levels**: 10-18 (from province level to detailed street level)
**Location List**: 63 Vietnamese provinces and cities including:
- 5 Central cities: Hà Nội, TP Hồ Chí Minh, Đà Nẵng, Hải Phòng, Cần Thơ
- 58 Provinces across Miền Bắc, Miền Trung, Miền Nam regions

**Systematic Process**:
1. Navigate to location-specific planning page
2. For each zoom level 10-18:
   - Set zoom level via JavaScript
   - Perform comprehensive map coverage
   - Extract tile URLs from network requests
   - Immediately download discovered tiles
3. Generate coverage reports and statistics

## Usage Patterns

### Basic Usage
```python
# Initialize crawler
crawler = BrowserGulandCrawler(
    headless=False,           # Show browser for debugging
    enable_download=True,     # Auto-download tiles
    download_workers=5        # 5 parallel download threads
)

# Run full crawl with 2-hour timeout
results = crawler.run_browser_crawl(max_hours=2)
```

### Time Configuration Options
```python
# Predefined options in main():
# 1. Test (30 minutes) - Limited locations
# 2. Quick (1 hour) - Fast run
# 3. Standard (2 hours) - Default
# 4. Extended (4 hours) - Thorough coverage  
# 5. Marathon (8 hours) - All 63 provinces
# 6. Custom - User-defined duration
```

### Location Format
```python
# Format: (name, latitude, longitude, url_path)
("Hà Nội", 21.0285, 105.8542, "soi-quy-hoach/ha-noi")
("TP Hồ Chí Minh", 10.8231, 106.6297, "soi-quy-hoach/ho-chi-minh")
```

## Directory Structure Output
```
downloaded_tiles/
└── {location_name}/
    ├── 01_Quy_Hoach_2030/
    │   ├── 10/  # Zoom level folders
    │   ├── 11/
    │   └── ...
    ├── 02_Ke_Hoach_2025/
    ├── 04_Hien_Trang/
    ├── 05_Satellite/
    └── chi_tiet_tai_xuong.txt  # Vietnamese download report
```

## Common Tasks

### Adding New Locations
1. Add to `test_locations` list in `BrowserGulandCrawler.__init__()`
2. Follow format: `(vietnamese_name, lat, lng, "soi-quy-hoach/url-slug")`
3. Ensure URL path matches guland.vn structure

### Modifying Crawl Behavior
```python
# Adjust zoom range in systematic_zoom_coverage()
zoom_levels = list(range(10, 19))  # Currently 10-18

# Modify interaction duration per zoom
duration_per_zoom = 30  # seconds per zoom level

# Change timeout per location
timeout_minutes = 10  # in crawl_location_with_timeout()
```

### Extending Tile Detection
```python
# Add new regex patterns in extract_tile_urls()
tile_pattern = re.search(r'/(\d+)/(\d+)/(\d+)\.(png|jpg|jpeg|webp|tiff|tif)', url)

# Update tile type detection in get_tile_type_from_url()
if 'new-map-type' in url_lower:
    return 'new_map_type'

# Add to directory mapping in create_enhanced_directory_structure()
type_mapping = {
    'new_map_type': '15_New_Map_Type'
}
```

### Performance Tuning
```python
# Adjust download workers based on system capacity
download_workers = 3  # Reduce for slower systems
download_workers = 10  # Increase for powerful systems

# Modify interaction timing
time.sleep(random.uniform(2, 4))  # Between actions
duration_per_zoom = 45  # More thorough coverage
```

## Error Handling & Recovery
- **Network timeouts**: Automatic retry with exponential backoff
- **Invalid tiles**: File validation and automatic cleanup
- **Browser crashes**: Driver restart and session recovery
- **Memory issues**: Batch processing and garbage collection
- **Anti-bot detection**: Realistic timing and headers

## Output Files & Reports
- `full_coverage_final_report.json` - Complete crawl results with statistics
- `full_coverage_final_report.txt` - Human-readable summary
- `coverage_report_{location}.txt` - Per-location detailed analysis
- `chi_tiet_tai_xuong.txt` - Vietnamese download details per location
- `browser_crawler.log` - Detailed runtime logs
- Screenshots in `output_browser_crawl/screenshots/`

## Performance Metrics
- **Processing time**: ~10 minutes per location (zoom 10-18)
- **Memory usage**: ~500MB per browser instance
- **Network bandwidth**: Depends on tile server response time
- **Storage**: 50-200MB per location depending on coverage
- **Success rate**: Typically 85-95% for tile discovery

## Anti-Bot Evasion Techniques
- Real Chrome browser (not headless Chromium) 
- Realistic user-agent strings and browser fingerprinting
- Human-like interaction patterns with random delays
- Proper HTTP headers (Referer, Origin, Accept)
- JavaScript execution to mimic real user behavior
- Mouse movement and scroll simulation

## Debugging & Troubleshooting

### Common Issues
1. **No tiles found**: Check network logs, verify guland.vn accessibility
2. **Download failures**: Verify internet connection, check tile URL validity
3. **Browser crashes**: Increase system memory, reduce concurrent workers
4. **Timeout errors**: Increase `max_hours` or `timeout_minutes` settings

### Debug Mode
```python
# Run with visible browser for debugging
crawler = BrowserGulandCrawler(headless=False)

# Check logs
tail -f browser_crawler.log

# Monitor network requests
# Open DevTools → Network tab while crawler runs
```

### Log Analysis
```bash
# Filter for successful tile discoveries
grep "Found tile:" browser_crawler.log

# Check download statistics  
grep "Download Summary" browser_crawler.log

# Monitor errors
grep "ERROR\|❌" browser_crawler.log
```

## Environment Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Ensure ChromeDriver is executable
chmod +x chromedriver

# Run with proper permissions
python browser_crawler.py
```

## Legal & Ethical Notes
- This tool is designed for educational and research purposes
- Always respect website terms of service and robots.txt
- Implement reasonable delays to avoid server overload
- Consider contacting guland.vn for permission for large-scale crawling
- Use responsibly and in compliance with Vietnamese data protection laws

## Future Enhancements
- Add support for custom geographic boundaries
- Implement incremental updates (only download new/changed tiles)
- Add tile stitching capabilities for larger map compositions
- Support for additional Vietnamese mapping services
- Integration with GIS tools for spatial analysis

---

## How to Use This Instruction with GitHub Copilot

When starting a new conversation about this project, paste this instruction and say:

> "Tôi đang làm việc với Guland Crawler project. Đây là instruction file cho project này: [paste this file]. Hãy giúp tôi [specific task]."

This helps GitHub Copilot understand the context and provide accurate suggestions for:
- Code modifications and enhancements
- Debugging and troubleshooting
- Performance optimization
- Adding new features
- Understanding existing functionality

Remember: This is a specialized tool for Vietnamese urban planning data. Always verify data accuracy and respect the source website's terms of service.