# ./app.py
import streamlit as st
import pandas as pd
from crochet import setup, run_in_reactor
from scrapy.crawler import CrawlerRunner
from procore_spider import ProcoreSpider
import threading
import time
import io  # For handling Excel file in memory
import os

# Set the reactor before importing twisted
os.environ['SCRAPY_SETTINGS_MODULE'] = 'settings'
setup()

# Initialize the crawler runner with proper settings
runner = CrawlerRunner({
    'TWISTED_REACTOR': 'twisted.internet.selectreactor.SelectReactor',
    'CONCURRENT_REQUESTS': 20,
    'DOWNLOAD_DELAY': 0,
    'LOG_LEVEL': 'DEBUG',  # Changed to DEBUG to see more details
    'LOG_ENABLED': True,
})

# Function to run the spider
@run_in_reactor
def crawl(state_code):
    ProcoreSpider.state_code = state_code.lower()
    return runner.crawl(ProcoreSpider)

def main():
    st.title("Procore Business Data Scraper")
    st.write("Scrape business data from Procore's network.")
    
    # Add instructions
    with st.expander("ℹ️ How to use this app"):
        st.write("""
        1. Enter a US state code (e.g., ca, ny, tx, mi)
        2. Click "Start Scraping" button
        3. Wait for the data to load (this may take a few moments)
        4. Data will appear in the table below as it's being scraped
        5. Once complete, you can download the data as CSV or Excel
        """)

    # Input for state code
    state_code = st.text_input("Enter state code (e.g., ca, mi, ny):", "ca")

    if 'scraping' not in st.session_state:
        st.session_state.scraping = False
    if 'stop_requested' not in st.session_state:
        st.session_state.stop_requested = False
    if 'data' not in st.session_state:
        st.session_state.data = []
    if 'crawl_thread' not in st.session_state:
        st.session_state.crawl_thread = None

    # Create columns for buttons
    col1, col2 = st.columns(2)
    with col1:
        start_button = st.button("🚀 Start Scraping", type="primary")
    with col2:
        stop_button = st.button("⏹️ Stop Scraping")

    if start_button and not st.session_state.scraping:
        st.session_state.scraping = True
        st.session_state.stop_requested = False
        ProcoreSpider.stop_requested = False
        ProcoreSpider.scraped_data = []
        ProcoreSpider.state_code = state_code.lower()
        ProcoreSpider.consecutive_empty_count = 0  # Reset the counter
        
        st.info(f"🔄 Starting scraper for state: {state_code.upper()}...")

        # Run the spider
        crawl(state_code)
        st.session_state.crawl_thread = threading.current_thread()

    if stop_button and st.session_state.scraping:
        st.session_state.stop_requested = True
        ProcoreSpider.stop_requested = True
        st.session_state.scraping = False
        st.warning("⏹️ Stopping scraper...")
        # Do not stop the reactor

    if st.session_state.scraping:
        data_placeholder = st.empty()
        progress_text = st.empty()
        status_text = st.empty()
        scraped_count = 0
        
        status_text.info("🔄 Scraping in progress... Please wait for data to appear.")

        while st.session_state.scraping:
            # Make a copy of the data to prevent concurrent modification issues
            data = ProcoreSpider.scraped_data.copy()
            new_count = len(data)

            if new_count > scraped_count:
                scraped_count = new_count
                status_text.success(f"✅ Scraping... Found {scraped_count} businesses so far!")

                # Ensure all items are dictionaries and have the same keys
                all_keys = set()
                valid_data = []
                for item in data:
                    if isinstance(item, dict):
                        all_keys.update(item.keys())
                        valid_data.append(item)

                # Standardize dictionaries
                for item in valid_data:
                    for key in all_keys:
                        item.setdefault(key, None)

                if valid_data:
                    df = pd.DataFrame.from_records(valid_data)
                    data_placeholder.dataframe(df, use_container_width=True)
                    progress_text.text(f"📊 Total items scraped: {scraped_count}")

            time.sleep(1)  # Sleep for a short time before updating

            # Check if the spider has stopped itself
            if ProcoreSpider.stop_requested:
                st.session_state.scraping = False
                if st.session_state.stop_requested:
                    status_text.warning("⏹️ Scraper stopped by user.")
                else:
                    status_text.warning("⏹️ Scraper stopped after encountering more than 8 consecutive empty rows.")
                break

        # After scraping ends, save data to session state
        st.session_state.data = ProcoreSpider.scraped_data.copy()

    # Display data and download button when scraping has stopped
    if not st.session_state.scraping and st.session_state.data:
        data = st.session_state.data
        # Ensure all items have the same keys
        all_keys = set()
        valid_data = []
        for item in data:
            if isinstance(item, dict):
                all_keys.update(item.keys())
                valid_data.append(item)

        # Standardize dictionaries
        for item in valid_data:
            for key in all_keys:
                item.setdefault(key, None)

        if valid_data:
            df = pd.DataFrame.from_records(valid_data)
            st.success(f"✅ Scraping completed! Total items scraped: {len(df)}")
            
            # Display the data
            st.subheader("📊 Scraped Data")
            st.dataframe(df, use_container_width=True)

            # Provide download buttons for CSV and Excel
            st.subheader("💾 Download Data")
            col1, col2 = st.columns(2)
            
            with col1:
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name='procore_business_data.csv',
                    mime='text/csv',
                )
            
            with col2:
                # Generate Excel file in memory
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False, sheet_name='Sheet1')
                    writer.save()
                    processed_data = output.getvalue()

                st.download_button(
                    label="📥 Download as Excel",
                    data=processed_data,
                    file_name='procore_business_data.xlsx',
                    mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                )
    elif not st.session_state.scraping and not st.session_state.data:
        st.info('👆 Click "Start Scraping" button above to begin scraping business data.')

if __name__ == '__main__':
    main()
