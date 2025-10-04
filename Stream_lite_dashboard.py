import streamlit as st
import os
import json
import pandas as pd
from datetime import datetime, timedelta
import requests
import socket
import subprocess
import time

# Page configuration
st.set_page_config(
    page_title="Data Quality Monitoring Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Set environment variables for S3 connection
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'
os.environ['S3_ENDPOINT_URL'] = 'https://legendary-waddle-wpxjw6pjxp7f9pvp-4566.app.github.dev'
os.environ['S3_VERIFY_SSL'] = 'false'
os.environ['S3_ADDRESSING_STYLE'] = 'path'
os.environ['DATA_LAKE_BUCKET'] = 'incircl'

# Import S3 utilities after setting environment variables
try:
    from dags.scripts.Utils.s3_utils import (
        get_s3_client,
        list_objects,
        read_parquet_from_s3,
        read_json_from_s3,
        get_latest_parquet_key,
        get_latest_json_key
    )
    S3_AVAILABLE = True
except ImportError as e:
    st.error(f"Failed to import S3 utilities: {e}")
    S3_AVAILABLE = False

# Service monitoring functions
def check_kafka_status():
    """Check if Kafka is running and accessible"""
    try:
        kafka_host = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092').split(':')[0]
        kafka_port = int(os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092').split(':')[1])
        
        # Try to connect to Kafka
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((kafka_host, kafka_port))
        sock.close()
        
        if result == 0:
            return True, f"Kafka is running on {kafka_host}:{kafka_port}"
        else:
            return False, f"Kafka is not accessible on {kafka_host}:{kafka_port}"
    except Exception as e:
        return False, f"Error checking Kafka: {str(e)}"

def check_airflow_status():
    """Check if Airflow webserver is running"""
    try:
        airflow_host = os.getenv('AIRFLOW_HOST', 'localhost')
        airflow_port = os.getenv('AIRFLOW_PORT', '8080')
        
        # Try to connect to Airflow webserver
        response = requests.get(f"http://{airflow_host}:{airflow_port}/health", timeout=5)
        if response.status_code == 200:
            return True, f"Airflow webserver is running on {airflow_host}:{airflow_port}"
        else:
            return False, f"Airflow webserver returned status {response.status_code}"
    except requests.exceptions.ConnectionError:
        return False, f"Airflow webserver is not accessible on {airflow_host}:{airflow_port}"
    except Exception as e:
        return False, f"Error checking Airflow: {str(e)}"

def check_s3_status():
    """Check if S3 service is accessible"""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False, "S3 client could not be initialized"
        
        # Try to list buckets
        s3_client.list_buckets()
        return True, "S3 service is accessible"
    except Exception as e:
        return False, f"Error checking S3: {str(e)}"

def check_docker_services():
    """Check if Docker services are running"""
    try:
        # Check if docker-compose services are running
        result = subprocess.run(['docker-compose', 'ps', '--services', '--filter', 'status=running'], 
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            running_services = result.stdout.strip().split('\n') if result.stdout.strip() else []
            return True, f"Running services: {', '.join(running_services) if running_services else 'None'}"
        else:
            return False, "Docker services check failed"
    except Exception as e:
        return False, f"Error checking Docker services: {str(e)}"

def get_all_service_status():
    """Get status of all services"""
    services = {
        "Kafka": check_kafka_status(),
        "Airflow": check_airflow_status(),
        "S3": check_s3_status(),
        "Docker Services": check_docker_services()
    }
    return services

def get_all_s3_files():
    """Get all files in the S3 bucket for comprehensive browsing"""
    if not S3_AVAILABLE:
        return {}
    
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return {}
        
        bucket = 'incircl'
        all_files = {}
        
        # Get all objects in the bucket
        all_objects = list(list_objects(s3_client, bucket, ''))
        
        # Organize by folder structure
        for obj in all_objects:
            key = obj['Key']
            if key.endswith('/'):  # Skip folder markers
                continue
                
            # Extract folder path
            parts = key.split('/')
            if len(parts) > 1:
                folder = parts[0]
                if folder not in all_files:
                    all_files[folder] = []
                all_files[folder].append(obj)
            else:
                # Root level files
                if 'root' not in all_files:
                    all_files['root'] = []
                all_files['root'].append(obj)
        
        return all_files
    except Exception as e:
        st.error(f"Error accessing S3: {e}")
        return {}

def preview_file_data(bucket: str, key: str, max_rows: int = 10):
    """Preview data from different file types"""
    try:
        if key.endswith('.parquet'):
            df = read_parquet_from_s3(bucket, key)
            if df is not None and not df.empty:
                return df.head(max_rows)
        elif key.endswith('.json'):
            df = read_json_from_s3(bucket, key)
            if df is not None and not df.empty:
                return df.head(max_rows)
        elif key.endswith('.csv'):
            # Read CSV file
            s3_client = get_s3_client()
            if not s3_client:
                return None
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            body = obj['Body'].read()
            buffer = BytesIO(body)
            df = pd.read_csv(buffer)
            return df.head(max_rows)
        elif key.endswith('.txt') or key.endswith('.log'):
            # Read text file
            s3_client = get_s3_client()
            if not s3_client:
                return None
            obj = s3_client.get_object(Bucket=bucket, Key=key)
            body = obj['Body'].read()
            text = body.decode('utf-8')
            lines = text.split('\n')[:max_rows]
            return pd.DataFrame({'Line': lines})
        else:
            return None
    except Exception as e:
        st.error(f"Error previewing file {key}: {e}")
        return None

def download_file_data(bucket: str, key: str):
    """Download file data as bytes"""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return None
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return obj['Body'].read()
    except Exception as e:
        st.error(f"Error downloading file {key}: {e}")
        return None

def sanitize_key(key: str) -> str:
    """Sanitize key for use in session state by replacing special characters"""
    return key.replace('/', '_').replace('.', '_').replace('-', '_').replace(':', '_')

def get_batch_bucket_contents():
    """Get contents of the S3 bucket for batch job data"""
    if not S3_AVAILABLE:
        return {}
    
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return {}
        
        bucket = 'incircl'
        contents = {}
        
        # Check bronze layer for batch job - show all files
        bronze_objects = list(list_objects(s3_client, bucket, 'bronze_layer/batch_job/'))
        if bronze_objects:
            contents['bronze'] = bronze_objects
        
        # Check silver layer - valid data - show all files
        valid_objects = list(list_objects(s3_client, bucket, 'silver_layer/valid_data/'))
        if valid_objects:
            contents['valid'] = valid_objects
            
        # Check silver layer - rejected data - show all files
        reject_objects = list(list_objects(s3_client, bucket, 'silver_layer/reject_data/'))
        if reject_objects:
            contents['rejected'] = reject_objects
            
        return contents
    except Exception as e:
        st.error(f"Error accessing S3: {e}")
        return {}

def get_stream_bucket_contents():
    """Get contents of the S3 bucket for stream job data"""
    if not S3_AVAILABLE:
        return {}
    
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return {}
        
        bucket = 'incircl'
        contents = {}
        
        # Check bronze layer for stream job - show all files
        bronze_objects = list(list_objects(s3_client, bucket, 'bronze_layer/stream_job/'))
        if bronze_objects:
            contents['bronze'] = bronze_objects
        
        # Check silver layer - valid data for stream - show all files
        valid_objects = list(list_objects(s3_client, bucket, 'silver_layer/valid_data/events/'))
        if valid_objects:
            contents['valid'] = valid_objects
            
        # Check silver layer - rejected data for stream - show all files
        reject_objects = list(list_objects(s3_client, bucket, 'silver_layer/reject_data/events/'))
        if reject_objects:
            contents['rejected'] = reject_objects
            
        return contents
    except Exception as e:
        st.error(f"Error accessing S3: {e}")
        return {}

def get_batch_rejected_data_details():
    """Get detailed information about rejected batch data with reasons"""
    if not S3_AVAILABLE:
        return {}
    
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return {}
        
        bucket = 'incircl'
        rejected_details = {}
        
        # Get rejected data for each batch table
        tables = ['customers', 'orders', 'products', 'categories', 'employees', 'order_details']
        
        for table in tables:
            reject_prefix = f"silver_layer/reject_data/{table}/"
            reject_objects = list(list_objects(s3_client, bucket, reject_prefix))
            
            if reject_objects:
                # Get the latest reject file
                latest_reject = max(reject_objects, key=lambda x: x['LastModified'])
                
                try:
                    # Read the rejected data
                    df = read_parquet_from_s3(bucket, latest_reject['Key'])
                    if df is not None and not df.empty:
                        rejected_details[table] = {
                            'file': latest_reject['Key'],
                            'size': latest_reject['Size'],
                            'last_modified': latest_reject['LastModified'],
                            'data': df,
                            'total_rows': len(df),
                            'rejection_reasons': df['rejection_reason'].value_counts().to_dict() if 'rejection_reason' in df.columns else {}
                        }
                except Exception as e:
                    st.warning(f"Could not read rejected data for {table}: {e}")
                    
        return rejected_details
    except Exception as e:
        st.error(f"Error getting rejected data details: {e}")
        return {}

def get_stream_rejected_data_details():
    """Get detailed information about rejected stream data with reasons"""
    if not S3_AVAILABLE:
        return {}
    
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return {}
        
        bucket = 'incircl'
        rejected_details = {}
        
        # Get rejected data for stream events
        table = 'events'
        reject_prefix = f"silver_layer/reject_data/{table}/"
        reject_objects = list(list_objects(s3_client, bucket, reject_prefix))
        
        if reject_objects:
            # Get the latest reject file
            latest_reject = max(reject_objects, key=lambda x: x['LastModified'])
            
            try:
                # Read the rejected data
                df = read_parquet_from_s3(bucket, latest_reject['Key'])
                if df is not None and not df.empty:
                    rejected_details[table] = {
                        'file': latest_reject['Key'],
                        'size': latest_reject['Size'],
                        'last_modified': latest_reject['LastModified'],
                        'data': df,
                        'total_rows': len(df),
                        'rejection_reasons': df['rejection_reason'].value_counts().to_dict() if 'rejection_reason' in df.columns else {}
                    }
            except Exception as e:
                st.warning(f"Could not read rejected data for {table}: {e}")
                
        return rejected_details
    except Exception as e:
        st.error(f"Error getting rejected data details: {e}")
        return {}

def display_batch_job_tab():
    """Display the batch job monitoring tab"""
    st.subheader("🔄 Batch Job Monitoring")
    
    # Service status section
    display_service_status()
    
    # Get batch data
    with st.spinner("Loading batch job data from S3..."):
        contents = get_batch_bucket_contents()
        rejected_details = get_batch_rejected_data_details()
    
    # Calculate total rejected rows
    total_rejected_rows = sum(details['total_rows'] for details in rejected_details.values())
    
    # Display summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Bronze Layer Files",
            value=len(contents.get('bronze', [])),
            delta=None
        )
    
    with col2:
        st.metric(
            label="Valid Data Files",
            value=len(contents.get('valid', [])),
            delta=None
        )
    
    with col3:
        st.metric(
            label="Rejected Data Files",
            value=len(contents.get('rejected', [])),
            delta=None
        )
    
    with col4:
        st.metric(
            label="Total Rejected Rows",
            value=total_rejected_rows,
            delta=None
        )
    
    st.markdown("---")
    
    # Display detailed information
    tab1, tab2, tab3 = st.tabs(["📥 Bronze Layer", "✅ Valid Data", "❌ Rejected Data"])
    
    with tab1:
        st.subheader("Bronze Layer Data (Batch Job)")
        if contents.get('bronze'):
            for obj in contents['bronze'][:10]:  # Show first 10
                key = obj['Key']
                size = obj['Size']
                last_modified = obj['LastModified']
                
                safe_key = sanitize_key(key)
                
                col1, col2, col3, col4 = st.columns([4, 1, 1, 1])
                with col1:
                    st.write(f"📄 `{key}`")
                with col2:
                    st.write(f"**{size} bytes**")
                with col3:
                    st.write(f"*{last_modified.strftime('%Y-%m-%d %H:%M')}*")
                with col4:
                    if st.button("👁️ Preview", key=f"batch_preview_{safe_key}"):
                        st.session_state.selected_file = key
                        st.rerun()
        else:
            st.info("No bronze layer data found for batch job")
    
    with tab2:
        st.subheader("Valid Data (Batch Job)")
        if contents.get('valid'):
            for obj in contents['valid'][:10]:  # Show first 10
                key = obj['Key']
                size = obj['Size']
                last_modified = obj['LastModified']
                
                safe_key = sanitize_key(key)
                
                col1, col2, col3, col4 = st.columns([4, 1, 1, 1])
                with col1:
                    st.write(f"✅ `{key}`")
                with col2:
                    st.write(f"**{size} bytes**")
                with col3:
                    st.write(f"*{last_modified.strftime('%Y-%m-%d %H:%M')}*")
                with col4:
                    if st.button("👁️ Preview", key=f"batch_valid_preview_{safe_key}"):
                        st.session_state.selected_file = key
                        st.rerun()
        else:
            st.info("No valid data found for batch job")
    
    with tab3:
        st.subheader("Rejected Data Analysis (Batch Job)")
        
        if rejected_details:
            # Summary of rejection reasons
            st.markdown("### 📊 Rejection Summary by Table")
            
            for table, details in rejected_details.items():
                with st.expander(f"🔍 {table.upper()} - {details['total_rows']} rejected rows"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**File:** `{details['file']}`")
                        st.write(f"**Size:** {details['size']} bytes")
                        st.write(f"**Last Modified:** {details['last_modified']}")
                    
                    with col2:
                        if details['rejection_reasons']:
                            st.write("**Rejection Reasons:**")
                            for reason, count in details['rejection_reasons'].items():
                                st.write(f"• {reason}: {count} rows")
                        else:
                            st.write("No rejection reasons available")
                    
                    # Show sample of rejected data
                    st.write("**Sample Rejected Data:**")
                    sample_data = details['data'].head(10)
                    st.dataframe(sample_data, use_container_width=True)
            
            # Overall rejection reasons chart
            st.markdown("### 📈 Overall Rejection Reasons")
            
            all_reasons = {}
            for details in rejected_details.values():
                for reason, count in details['rejection_reasons'].items():
                    all_reasons[reason] = all_reasons.get(reason, 0) + count
            
            if all_reasons:
                reasons_df = pd.DataFrame(list(all_reasons.items()), columns=['Reason', 'Count'])
                st.bar_chart(reasons_df.set_index('Reason'))
            else:
                st.info("No rejection reasons data available")
                
        else:
            st.info("No rejected data found for batch job")

def display_service_status():
    """Display service status monitoring"""
    st.subheader("🔧 Service Status Monitoring")
    
    # Get service status
    with st.spinner("Checking service status..."):
        services = get_all_service_status()
    
    # Display service status in columns
    cols = st.columns(len(services))
    
    for i, (service_name, (is_running, message)) in enumerate(services.items()):
        with cols[i]:
            if is_running:
                st.success(f"✅ {service_name}")
                st.caption(message)
            else:
                st.error(f"❌ {service_name}")
                st.caption(message)
    
    st.markdown("---")
    
    # Detailed service information
    with st.expander("🔍 Detailed Service Information"):
        for service_name, (is_running, message) in services.items():
            st.write(f"**{service_name}:**")
            if is_running:
                st.success(f"Status: Running - {message}")
            else:
                st.error(f"Status: Down - {message}")
            st.write("---")

def display_stream_job_tab():
    """Display the stream job monitoring tab"""
    st.subheader("🌊 Stream Job Monitoring")
    
    # Service status section
    display_service_status()
    
    # Get stream data
    with st.spinner("Loading stream job data from S3..."):
        contents = get_stream_bucket_contents()
        rejected_details = get_stream_rejected_data_details()
    
    # Calculate total rejected rows
    total_rejected_rows = sum(details['total_rows'] for details in rejected_details.values())
    
    # Display summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="Bronze Layer Files",
            value=len(contents.get('bronze', [])),
            delta=None
        )
    
    with col2:
        st.metric(
            label="Valid Data Files",
            value=len(contents.get('valid', [])),
            delta=None
        )
    
    with col3:
        st.metric(
            label="Rejected Data Files",
            value=len(contents.get('rejected', [])),
            delta=None
        )
    
    with col4:
        st.metric(
            label="Total Rejected Rows",
            value=total_rejected_rows,
            delta=None
        )
    
    st.markdown("---")
    
    # Display detailed information
    tab1, tab2, tab3 = st.tabs(["📥 Bronze Layer", "✅ Valid Data", "❌ Rejected Data"])
    
    with tab1:
        st.subheader("Bronze Layer Data (Stream Job)")
        if contents.get('bronze'):
            for obj in contents['bronze'][:10]:  # Show first 10
                st.write(f"📄 {obj['Key']} - {obj['Size']} bytes - {obj['LastModified']}")
        else:
            st.info("No bronze layer data found for stream job")
    
    with tab2:
        st.subheader("Valid Data (Stream Job)")
        if contents.get('valid'):
            for obj in contents['valid'][:10]:  # Show first 10
                st.write(f"✅ {obj['Key']} - {obj['Size']} bytes - {obj['LastModified']}")
        else:
            st.info("No valid data found for stream job")
    
    with tab3:
        st.subheader("Rejected Data Analysis (Stream Job)")
        
        if rejected_details:
            # Summary of rejection reasons
            st.markdown("### 📊 Rejection Summary for Events")
            
            for table, details in rejected_details.items():
                with st.expander(f"🔍 {table.upper()} - {details['total_rows']} rejected rows"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.write(f"**File:** `{details['file']}`")
                        st.write(f"**Size:** {details['size']} bytes")
                        st.write(f"**Last Modified:** {details['last_modified']}")
                    
                    with col2:
                        if details['rejection_reasons']:
                            st.write("**Rejection Reasons:**")
                            for reason, count in details['rejection_reasons'].items():
                                st.write(f"• {reason}: {count} rows")
                        else:
                            st.write("No rejection reasons available")
                    
                    # Show sample of rejected data
                    st.write("**Sample Rejected Data:**")
                    sample_data = details['data'].head(10)
                    st.dataframe(sample_data, use_container_width=True)
            
            # Overall rejection reasons chart
            st.markdown("### 📈 Overall Rejection Reasons")
            
            all_reasons = {}
            for details in rejected_details.values():
                for reason, count in details['rejection_reasons'].items():
                    all_reasons[reason] = all_reasons.get(reason, 0) + count
            
            if all_reasons:
                reasons_df = pd.DataFrame(list(all_reasons.items()), columns=['Reason', 'Count'])
                st.bar_chart(reasons_df.set_index('Reason'))
            else:
                st.info("No rejection reasons data available")
                
        else:
            st.info("No rejected data found for stream job")

def display_all_files_tab():
    """Display all files in the S3 bucket"""
    st.subheader("📁 All Files in S3 Bucket")
    
    # Get all files
    with st.spinner("Loading all files from S3..."):
        all_files = get_all_s3_files()
    
    if not all_files:
        st.info("No files found in the S3 bucket")
        return
    
    # Initialize session state for selected file
    if 'selected_file' not in st.session_state:
        st.session_state.selected_file = None
    
    # Display files organized by folder
    for folder, files in all_files.items():
        with st.expander(f"📂 {folder} ({len(files)} files)", expanded=True):
            # Sort files by last modified date (newest first)
            files.sort(key=lambda x: x.get('LastModified', datetime.min), reverse=True)
            
            for obj in files:
                key = obj['Key']
                size = obj['Size']
                last_modified = obj['LastModified']
                
                # Determine file type icon
                if key.endswith('.parquet'):
                    icon = "📊"
                elif key.endswith('.json'):
                    icon = "📄"
                elif key.endswith('.csv'):
                    icon = "📋"
                elif key.endswith('.txt'):
                    icon = "📝"
                elif key.endswith('.log'):
                    icon = "📋"
                else:
                    icon = "📄"
                
                # Format file size
                if size < 1024:
                    size_str = f"{size} B"
                elif size < 1024 * 1024:
                    size_str = f"{size / 1024:.1f} KB"
                else:
                    size_str = f"{size / (1024 * 1024):.1f} MB"
                
                # Sanitize key for session state
                safe_key = sanitize_key(key)
                
                # Display file info with action buttons
                col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
                with col1:
                    st.write(f"{icon} `{key}`")
                with col2:
                    st.write(f"**{size_str}**")
                with col3:
                    st.write(f"*{last_modified.strftime('%Y-%m-%d %H:%M')}*")
                with col4:
                    if st.button("👁️ Preview", key=f"preview_{safe_key}", help="Preview file data"):
                        st.session_state.selected_file = key
                        st.rerun()
                with col5:
                    if st.button("⬇️ Download", key=f"download_{safe_key}", help="Download file"):
                        st.session_state.selected_file = f"download_{key}"
                        st.rerun()
                
                st.write("---")
    

def main():
    st.title("📊 Data Quality Monitoring Dashboard")
    st.markdown("---")
    
    if not S3_AVAILABLE:
        st.error("S3 utilities are not available. Please check the import paths.")
        return
    
    # Sidebar
    st.sidebar.title("Dashboard Controls")
    
    # Auto-refresh settings
    st.sidebar.markdown("### ⚙️ Auto-Refresh Settings")
    auto_refresh = st.sidebar.checkbox("Enable Auto-Refresh", value=True)
    refresh_interval = st.sidebar.selectbox(
        "Refresh Interval",
        options=[30, 60, 120, 300],
        index=1,
        format_func=lambda x: f"{x} seconds"
    )
    
    # Manual refresh button
    if st.sidebar.button("🔄 Manual Refresh", type="primary"):
        st.rerun()
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(refresh_interval)
        st.rerun()
    
    # Service status overview in sidebar
    st.sidebar.markdown("### 🔧 Service Status")
    services = get_all_service_status()
    for service_name, (is_running, message) in services.items():
        if is_running:
            st.sidebar.success(f"✅ {service_name}")
        else:
            st.sidebar.error(f"❌ {service_name}")
    
    # Handle file preview/download for all tabs
    if st.session_state.get('selected_file'):
        if st.session_state.selected_file.startswith("download_"):
            # Handle download
            file_key = st.session_state.selected_file[9:]  # Remove "download_" prefix
            st.write("---")
            st.write(f"**⬇️ Download: `{file_key}`**")
            
            with st.spinner("Preparing download..."):
                file_data = download_file_data('incircl', file_key)
                
                if file_data:
                    # Determine file extension for download
                    file_extension = file_key.split('.')[-1] if '.' in file_key else 'txt'
                    file_name = file_key.split('/')[-1]
                    
                    st.download_button(
                        label=f"Download {file_name}",
                        data=file_data,
                        file_name=file_name,
                        mime=f"application/{file_extension}",
                        key="download_button"
                    )
                    
                    st.success(f"Ready to download {file_name} ({len(file_data)} bytes)")
                else:
                    st.error("Could not download file")
            
            # Clear selection
            if st.button("Close Download"):
                st.session_state.selected_file = None
                st.rerun()
        else:
            # Handle preview
            file_key = st.session_state.selected_file
            st.write("---")
            st.write(f"**📊 Data Preview for: `{file_key}`**")
            
            with st.spinner("Loading data preview..."):
                preview_data = preview_file_data('incircl', file_key, max_rows=20)
                
                if preview_data is not None and not preview_data.empty:
                    st.dataframe(preview_data, use_container_width=True)
                    
                    # Show data info
                    st.write(f"**Data Info:** {len(preview_data)} rows, {len(preview_data.columns)} columns")
                    
                    # Show column info
                    if len(preview_data.columns) > 0:
                        st.write("**Columns:**")
                        for col in preview_data.columns:
                            dtype = preview_data[col].dtype
                            null_count = preview_data[col].isnull().sum()
                            st.write(f"- `{col}`: {dtype} ({null_count} nulls)")
                else:
                    st.warning("Could not preview data from this file")
            
            # Clear selection
            if st.button("Close Preview"):
                st.session_state.selected_file = None
                st.rerun()
    
    # Main tabs for different job types
    tab1, tab2, tab3 = st.tabs(["🔄 Batch Job", "🌊 Stream Job", "📁 All Files"])
    
    with tab1:
        display_batch_job_tab()
    
    with tab2:
        display_stream_job_tab()
    
    with tab3:
        display_all_files_tab()
    
    # Footer
    st.markdown("---")
    st.markdown("**Dashboard Status:** ✅ Running")
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if auto_refresh:
        st.markdown(f"**Auto-Refresh:** Enabled ({refresh_interval}s)")
    else:
        st.markdown("**Auto-Refresh:** Disabled")

if __name__ == "__main__":
    main()
