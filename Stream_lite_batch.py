import streamlit as st
import os
import json
import pandas as pd
from datetime import datetime, timedelta

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

# Import S3 utilities after setting environment variables
try:
    from dags.scripts.Utils.s3_utils import (
        get_s3_client,
        list_objects,
        read_parquet_from_s3,
        get_latest_parquet_key
    )
    S3_AVAILABLE = True
except ImportError as e:
    st.error(f"Failed to import S3 utilities: {e}")
    S3_AVAILABLE = False

def get_bucket_contents():
    """Get contents of the S3 bucket"""
    if not S3_AVAILABLE:
        return {}
    
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return {}
        
        bucket = 'incircl'
        contents = {}
        
        # Check bronze layer
        bronze_objects = list(list_objects(s3_client, bucket, 'bronze_layer/batch_job/'))
        if bronze_objects:
            contents['bronze'] = bronze_objects
        
        # Check silver layer - valid data
        valid_objects = list(list_objects(s3_client, bucket, 'silver_layer/valid_data/'))
        if valid_objects:
            contents['valid'] = valid_objects
            
        # Check silver layer - rejected data
        reject_objects = list(list_objects(s3_client, bucket, 'silver_layer/reject_data/'))
        if reject_objects:
            contents['rejected'] = reject_objects
            
        return contents
    except Exception as e:
        st.error(f"Error accessing S3: {e}")
        return {}

def get_rejected_data_details():
    """Get detailed information about rejected data with reasons"""
    if not S3_AVAILABLE:
        return {}
    
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return {}
        
        bucket = 'incircl'
        rejected_details = {}
        
        # Get rejected data for each table
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

def main():
    st.title("📊 Data Quality Monitoring Dashboard")
    st.markdown("---")
    
    if not S3_AVAILABLE:
        st.error("S3 utilities are not available. Please check the import paths.")
        return
    
    # Sidebar
    st.sidebar.title("Dashboard Controls")
    
    # Refresh button
    if st.sidebar.button("🔄 Refresh Data", type="primary"):
        st.rerun()
    
    # Get bucket contents and rejected data details
    with st.spinner("Loading data from S3..."):
        contents = get_bucket_contents()
        rejected_details = get_rejected_data_details()
    
    # Calculate total rejected rows
    total_rejected_rows = sum(details['total_rows'] for details in rejected_details.values())
    
    # Display summary
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
        st.subheader("Bronze Layer Data")
        if contents.get('bronze'):
            for obj in contents['bronze'][:10]:  # Show first 10
                st.write(f"📄 {obj['Key']} - {obj['Size']} bytes - {obj['LastModified']}")
        else:
            st.info("No bronze layer data found")
    
    with tab2:
        st.subheader("Valid Data")
        if contents.get('valid'):
            for obj in contents['valid'][:10]:  # Show first 10
                st.write(f"✅ {obj['Key']} - {obj['Size']} bytes - {obj['LastModified']}")
        else:
            st.info("No valid data found")
    
    with tab3:
        st.subheader("Rejected Data Analysis")
        
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
            st.info("No rejected data found")
    
    # Footer
    st.markdown("---")
    st.markdown("**Dashboard Status:** ✅ Running")
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()
