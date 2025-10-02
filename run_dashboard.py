#!/usr/bin/env python3
"""
Script to run the Streamlit dashboard for data quality monitoring
"""
import subprocess
import sys
import os

def run_streamlit_dashboard():
    """Run the Streamlit dashboard"""
    try:
        # Set environment variables for S3
        os.environ['AWS_ACCESS_KEY_ID'] = 'test'
        os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
        os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
        os.environ['S3_ENDPOINT_URL'] = 'https://legendary-waddle-wpxjw6pjxp7f9pvp-4566.app.github.dev'
        os.environ['S3_VERIFY_SSL'] = 'false'
        os.environ['S3_ADDRESSING_STYLE'] = 'path'
        os.environ['DATA_LAKE_BUCKET'] = 'incircl'
        
        # Run Streamlit
        cmd = [
            sys.executable, '-m', 'streamlit', 'run',
            'Stream_lite_batch.py',
            '--server.port=8501',
            '--server.address=0.0.0.0',
            '--server.headless=true'
        ]
        
        print("Starting Streamlit dashboard...")
        subprocess.run(cmd, check=True)
        
    except subprocess.CalledProcessError as e:
        print(f"Error running Streamlit: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    run_streamlit_dashboard()
