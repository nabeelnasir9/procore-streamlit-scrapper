import streamlit as st
import pandas as pd
from crochet import setup, run_in_reactor
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor
from procore_spider import ProcoreSpider
import threading
import time

setup()

# Initialize the crawler runner
runner = CrawlerRunner()

# Function to run the spider
@run_in_reactor
def crawl(state_code):
    ProcoreSpider.state_code = state_code.lower()
    return runner.crawl(ProcoreSpider)

def main():
    st.title("Procore Business Data Scraper")
    st.write("Scrape business data from Procore's network.")

    # Input for state code
    state_code = st.text_input("Enter state code (e.g., ca, mi, ny):", "ca")

    if 'scraping' not in st.session_state:
        st.session_state.scraping = False
    if 'stop_requested' not in st.session_state:
        st.session_state.stop_requested = False
    if 'data' not in st.session_state:
        st.session_state.data = []

    start_button = st.button("Start Scraping")
    stop_button = st.button("Stop Scraping")

    if start_button and not st.session_state.scraping:
        st.session_state.scraping = True
        st.session_state.stop_requested = False
        ProcoreSpider.stop_requested = False
        ProcoreSpider.scraped_data = []
        ProcoreSpider.state_code = state_code.lower()
        ProcoreSpider.consecutive_empty_count = 0  # Reset the counter

        # Run the spider in a separate thread
        crawl_thread = threading.Thread(target=crawl, args=(state_code,))
        crawl_thread.start()

    if stop_button and st.session_state.scraping:
        st.session_state.stop_requested = True
        ProcoreSpider.stop_requested = True
        st.session_state.scraping = False
        reactor.callFromThread(reactor.stop)  # Stop the reactor

    if st.session_state.scraping:
        data_placeholder = st.empty()
        progress_text = st.empty()
        scraped_count = 0

        while st.session_state.scraping:
            # Update data
            data = ProcoreSpider.scraped_data
            new_count = len(data)
            if new_count > scraped_count:
                scraped_count = new_count
                df = pd.DataFrame(data)
                data_placeholder.dataframe(df)
                progress_text.text(f"Scraped {scraped_count} items...")

            # Check if the spider has stopped itself
            if ProcoreSpider.stop_requested and not reactor.running:
                st.session_state.scraping = False
                st.warning("Scraper stopped after encountering more than 8 consecutive empty rows.")
                break

            # Allow user to stop scraping
            if st.session_state.stop_requested:
                st.session_state.scraping = False
                reactor.callFromThread(reactor.stop)  # Stop the reactor
                break

            time.sleep(1)  # Sleep for a short time before updating

        # Display final data
        data = ProcoreSpider.scraped_data
        if data:
            # Ensure all items have the same keys
            all_keys = set()
            for item in data:
                if isinstance(item, dict):
                    all_keys.update(item.keys())

            for item in data:
                if isinstance(item, dict):
                    for key in all_keys:
                        item.setdefault(key, None)

            df = pd.DataFrame.from_records(data)
            st.dataframe(df)
            st.success(f"Scraping completed! Total items scraped: {len(df)}")
            # Provide a download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download data as CSV",
                data=csv,
                file_name='procore_business_data.csv',
                mime='text/csv',
            )
        else:
            st.error('No data scraped or an error occurred.')

    elif st.session_state.data:
        # Display data if scraping has stopped
        df = pd.DataFrame(st.session_state.data)
        st.dataframe(df)

if __name__ == '__main__':
    main()
