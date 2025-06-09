#!/usr/bin/env python3
import os
import sys
import subprocess
import json
from pathlib import Path

def setup_environment():
    """Setup complete environment"""
    
    print("🚀 Setting up Guland Analysis Environment...")
    
    # Create directories
    directories = [
        'output/network_logs',
        'output/screenshots', 
        'output/api_responses',
        'logs'
    ]
    
    for dir_path in directories:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {dir_path}")
    
    # Create config file
    config = {
        "analysis_settings": {
            "headless": False,
            "timeout": 30,
            "capture_duration": 20,
            "max_provinces": 3,
            "screenshot_quality": "high"
        },
        "target_urls": {
            "base": "https://guland.vn",
            "map": "https://guland.vn/map",
            "provinces": ["ha-noi", "ho-chi-minh", "da-nang"]
        },
        "browser_settings": {
            "window_size": "1920,1080",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "disable_images": False,
            "enable_javascript": True
        }
    }
    
    with open('config.json', 'w') as f:
        json.dump(config, f, indent=2)
    print("✅ Created config.json")
    
    # Create requirements.txt
    requirements = """
      selenium>=4.15.0
      requests>=2.31.0
      beautifulsoup4>=4.12.0
      lxml>=4.9.0
      pillow>=10.0.0
      webdriver-manager>=4.0.0
      tqdm>=4.65.0
      colorama>=0.4.6
      jsonschema>=4.17.0
    """.strip()
      
    with open('requirements.txt', 'w') as f:
        f.write(requirements)
    print("✅ Created requirements.txt")
    
    print("\n🎉 Setup completed!")
    print("\nNext steps:")
    print("1. pip install -r requirements.txt")
    print("2. python network_analyzer.py")

if __name__ == "__main__":
    setup_environment()