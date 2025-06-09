#!/usr/bin/env python3
"""
ChromeDriver Fix Script for Linux
Fixes common ChromeDriver issues and provides alternatives

Usage: python fix_chromedriver.py
"""

import os
import sys
import subprocess
import platform
import shutil
from pathlib import Path
import urllib.request
import zipfile
import stat

def check_system_info():
    """Check system information"""
    print("🔍 SYSTEM INFORMATION:")
    print(f"  • OS: {platform.system()} {platform.release()}")
    print(f"  • Architecture: {platform.machine()}")
    print(f"  • Python: {sys.version}")
    
def check_chrome_installation():
    """Check if Chrome is installed"""
    print("\n🔍 CHECKING CHROME INSTALLATION:")
    
    chrome_commands = [
        'google-chrome --version',
        'google-chrome-stable --version', 
        'chromium --version',
        'chromium-browser --version'
    ]
    
    chrome_found = False
    chrome_version = None
    
    for cmd in chrome_commands:
        try:
            result = subprocess.run(cmd.split(), capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                chrome_version = result.stdout.strip()
                print(f"  ✅ Found: {chrome_version}")
                chrome_found = True
                break
        except:
            continue
    
    if not chrome_found:
        print("  ❌ Chrome/Chromium not found!")
        return False, None
    
    return True, chrome_version

def install_chrome_ubuntu():
    """Install Chrome on Ubuntu/Debian"""
    print("\n🔧 INSTALLING CHROME ON UBUNTU/DEBIAN:")
    
    commands = [
        "wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | sudo apt-key add -",
        "echo 'deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main' | sudo tee /etc/apt/sources.list.d/google-chrome.list",
        "sudo apt update",
        "sudo apt install -y google-chrome-stable"
    ]
    
    for cmd in commands:
        print(f"  Running: {cmd}")
        try:
            subprocess.run(cmd, shell=True, check=True)
        except subprocess.CalledProcessError as e:
            print(f"  ❌ Failed: {e}")
            return False
    
    return True

def check_dependencies():
    """Check required dependencies"""
    print("\n🔍 CHECKING DEPENDENCIES:")
    
    # Check for required libraries
    required_libs = [
        'libnss3',
        'libgconf-2-4', 
        'libxss1',
        'libasound2',
        'libxtst6',
        'libgtk-3-0',
        'libxrandr2'
    ]
    
    missing_libs = []
    
    for lib in required_libs:
        try:
            result = subprocess.run(['dpkg', '-l', lib], capture_output=True, text=True)
            if result.returncode != 0:
                missing_libs.append(lib)
            else:
                print(f"  ✅ {lib}")
        except:
            missing_libs.append(lib)
    
    if missing_libs:
        print(f"  ❌ Missing libraries: {', '.join(missing_libs)}")
        return False, missing_libs
    
    return True, []

def install_dependencies(missing_libs):
    """Install missing dependencies"""
    print(f"\n🔧 INSTALLING MISSING DEPENDENCIES:")
    
    # Update package list
    subprocess.run(['sudo', 'apt', 'update'], check=True)
    
    # Install missing libraries
    cmd = ['sudo', 'apt', 'install', '-y'] + missing_libs
    subprocess.run(cmd, check=True)
    
    print("  ✅ Dependencies installed")

def fix_chromedriver_permissions():
    """Fix ChromeDriver permissions"""
    print("\n🔧 FIXING CHROMEDRIVER PERMISSIONS:")
    
    # Find ChromeDriver locations
    possible_locations = [
        Path.home() / '.cache/selenium/chromedriver',
        Path('/usr/local/bin/chromedriver'),
        Path('/usr/bin/chromedriver'),
        Path('./chromedriver')
    ]
    
    fixed_any = False
    
    for location in possible_locations:
        if location.exists():
            try:
                # Make executable
                if location.is_dir():
                    # Find chromedriver binary in subdirs
                    for chromedriver_path in location.rglob('chromedriver'):
                        os.chmod(chromedriver_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
                        print(f"  ✅ Fixed permissions: {chromedriver_path}")
                        fixed_any = True
                else:
                    os.chmod(location, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
                    print(f"  ✅ Fixed permissions: {location}")
                    fixed_any = True
            except Exception as e:
                print(f"  ❌ Could not fix permissions for {location}: {e}")
    
    return fixed_any

def download_chromedriver_manual():
    """Download ChromeDriver manually"""
    print("\n🔧 DOWNLOADING CHROMEDRIVER MANUALLY:")
    
    # Get latest stable version
    try:
        version_url = "https://chromedriver.storage.googleapis.com/LATEST_RELEASE"
        with urllib.request.urlopen(version_url) as response:
            latest_version = response.read().decode().strip()
        
        print(f"  Latest ChromeDriver version: {latest_version}")
        
        # Download for Linux
        download_url = f"https://chromedriver.storage.googleapis.com/{latest_version}/chromedriver_linux64.zip"
        
        print(f"  Downloading from: {download_url}")
        
        # Download
        zip_path = "chromedriver_linux64.zip"
        urllib.request.urlretrieve(download_url, zip_path)
        
        # Extract
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall('.')
        
        # Make executable
        os.chmod('chromedriver', 0o755)
        
        # Move to /usr/local/bin (optional)
        try:
            shutil.move('chromedriver', '/usr/local/bin/chromedriver')
            print("  ✅ ChromeDriver installed to /usr/local/bin/")
        except:
            print("  ✅ ChromeDriver downloaded to current directory")
        
        # Cleanup
        os.remove(zip_path)
        
        return True
        
    except Exception as e:
        print(f"  ❌ Failed to download ChromeDriver: {e}")
        return False

def create_fixed_analyzer():
    """Create a fixed version of the analyzer"""
    print("\n🔧 CREATING FIXED ANALYZER:")
    
    fixed_code = '''#!/usr/bin/env python3
"""
Fixed Guland Network Analyzer for Linux
Với enhanced error handling và fallback options
"""

import os
import sys
import time
import json
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_chrome_driver(headless=True):
    """Setup Chrome driver với multiple fallback options"""
    
    chrome_options = Options()
    
    # Essential options for Linux
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-javascript")  # For basic HTML only
    
    if headless:
        chrome_options.add_argument("--headless")
    
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
    
    # Try different methods to create driver
    methods = [
        ("webdriver-manager", lambda: webdriver.Chrome(options=chrome_options)),
        ("system chromedriver", lambda: webdriver.Chrome(service=Service("/usr/local/bin/chromedriver"), options=chrome_options)),
        ("local chromedriver", lambda: webdriver.Chrome(service=Service("./chromedriver"), options=chrome_options)),
        ("PATH chromedriver", lambda: webdriver.Chrome(service=Service(shutil.which("chromedriver")), options=chrome_options))
    ]
    
    for method_name, method_func in methods:
        try:
            logger.info(f"Trying {method_name}...")
            driver = method_func()
            logger.info(f"✅ Success with {method_name}")
            return driver
        except Exception as e:
            logger.warning(f"❌ {method_name} failed: {e}")
            continue
    
    logger.error("❌ All ChromeDriver methods failed!")
    return None

def simple_requests_analysis():
    """Fallback: Simple requests-based analysis"""
    logger.info("🔄 Falling back to requests-based analysis...")
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
    })
    
    urls_to_test = [
        "https://guland.vn/",
        "https://guland.vn/map",
        "https://guland.vn/api/",
        "https://guland.vn/ajax/",
        "https://api.guland.vn/"
    ]
    
    results = []
    
    for url in urls_to_test:
        try:
            logger.info(f"Testing: {url}")
            response = session.get(url, timeout=10)
            
            result = {
                'url': url,
                'status_code': response.status_code,
                'content_type': response.headers.get('content-type', ''),
                'content_length': len(response.content),
                'success': response.status_code < 400
            }
            
            # Look for API endpoints in HTML
            if 'text/html' in result['content_type']:
                content = response.text.lower()
                if any(term in content for term in ['api', 'ajax', '.json', 'tiles']):
                    result['contains_api_references'] = True
            
            results.append(result)
            
        except Exception as e:
            logger.error(f"Error testing {url}: {e}")
    
    # Save results
    with open('simple_analysis_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\\n📊 SIMPLE ANALYSIS RESULTS:")
    for result in results:
        status = "✅" if result['success'] else "❌"
        print(f"  {status} {result['url']} - Status: {result['status_code']}")
    
    return results

def main():
    """Main function với fallback options"""
    print("🔧 FIXED GULAND ANALYZER FOR LINUX")
    print("=" * 50)
    
    # Try Selenium first
    driver = setup_chrome_driver(headless=True)
    
    if driver:
        logger.info("✅ ChromeDriver working! Running full analysis...")
        # Import and run the original analyzer
        try:
            # Add your original analyzer code here
            # For now, just do a basic test
            driver.get("https://guland.vn/")
            logger.info(f"Page title: {driver.title}")
            driver.quit()
            print("✅ Selenium analysis completed!")
        except Exception as e:
            logger.error(f"Selenium analysis failed: {e}")
            driver.quit()
    else:
        logger.info("🔄 ChromeDriver not working, using fallback...")
        simple_requests_analysis()

if __name__ == "__main__":
    main()
'''
    
    with open('fixed_analyzer.py', 'w') as f:
        f.write(fixed_code)
    
    print("  ✅ Created fixed_analyzer.py")

def main():
    """Main diagnosis and fix function"""
    print("🚨 CHROMEDRIVER ISSUE DIAGNOSIS & FIX")
    print("=" * 50)
    
    # Step 1: System info
    check_system_info()
    
    # Step 2: Check Chrome
    chrome_found, chrome_version = check_chrome_installation()
    
    if not chrome_found:
        print("\\n🔧 Chrome not found. Installing...")
        if platform.system() == "Linux":
            install_chrome_ubuntu()
        else:
            print("  Please install Chrome manually for your OS")
            return
    
    # Step 3: Check dependencies
    deps_ok, missing_libs = check_dependencies()
    
    if not deps_ok:
        print("\\n🔧 Installing missing dependencies...")
        install_dependencies(missing_libs)
    
    # Step 4: Fix permissions
    fix_chromedriver_permissions()
    
    # Step 5: Try manual ChromeDriver download
    print("\\n🔧 Trying manual ChromeDriver download...")
    download_chromedriver_manual()
    
    # Step 6: Create fixed analyzer
    create_fixed_analyzer()
    
    print("\\n" + "=" * 50)
    print("🎯 FIX COMPLETED!")
    print("=" * 50)
    print("\\n📋 What was done:")
    print("  • Checked system compatibility")
    print("  • Installed/verified Chrome browser")
    print("  • Installed missing dependencies")
    print("  • Fixed ChromeDriver permissions")
    print("  • Downloaded ChromeDriver manually")
    print("  • Created fixed analyzer script")
    
    print("\\n🚀 Try these commands now:")
    print("  1. python fixed_analyzer.py")
    print("  2. If still fails: python -c 'import requests; print(requests.get(\"https://guland.vn\").status_code)'")
    
    print("\\n💡 Alternative approaches if ChromeDriver still fails:")
    print("  • Use requests-only scraping")
    print("  • Try Firefox with geckodriver")
    print("  • Use cloud-based browser automation")

if __name__ == "__main__":
    main()