# ./app_simple.py
import streamlit as st
import pandas as pd
import threading
import time
import io
from simple_scraper import SimpleProcoreScraper

def main():
    st.title("Procore Business Data Scraper")
    st.write("Scrape business data from Procore's network.")
    
    # Add instructions
    with st.expander("ℹ️ How to use this app"):
        st.write("""
        1. Enter a US state code (e.g., ca, ny, tx, mi)
        2. Click "Start Scraping" button
        3. Wait for the data to load (this may take a few moments)
        4. Data will appear in the table below
        5. Once complete, you can download the data as CSV or Excel
        """)

    # Input for state code
    state_code = st.text_input("Enter state code (e.g., ca, mi, ny):", "ca")

    if 'scraping' not in st.session_state:
        st.session_state.scraping = False
    if 'data' not in st.session_state:
        st.session_state.data = []
    if 'scraper' not in st.session_state:
        st.session_state.scraper = None

    # Create columns for buttons
    col1, col2 = st.columns(2)
    with col1:
        start_button = st.button("🚀 Start Scraping", type="primary")
    with col2:
        stop_button = st.button("⏹️ Stop Scraping")

    def run_scraper():
        """Run scraper in background"""
        scraper = SimpleProcoreScraper(state_code)
        st.session_state.scraper = scraper
        scraper.scrape(max_pages=3)

    if start_button and not st.session_state.scraping:
        st.session_state.scraping = True
        st.session_state.data = []
        
        st.info(f"🔄 Starting scraper for state: {state_code.upper()}...")
        st.write("⏳ This uses a simplified scraper that should work better in Streamlit Cloud...")

        # Run the scraper in background
        try:
            spider_thread = threading.Thread(target=run_scraper)
            spider_thread.daemon = True
            spider_thread.start()
            st.success("✅ Scraper started! Waiting for data...")
        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            st.session_state.scraping = False

    if stop_button and st.session_state.scraping:
        st.session_state.scraping = False
        st.warning("⏹️ Stopping scraper...")

    if st.session_state.scraping:
        data_placeholder = st.empty()
        progress_text = st.empty()
        status_text = st.empty()
        scraped_count = 0
        wait_cycles = 0
        
        status_text.info("🔄 Scraping in progress...")

        while st.session_state.scraping:
            # Check if scraper has data
            if st.session_state.scraper and st.session_state.scraper.scraped_data:
                data = st.session_state.scraper.scraped_data.copy()
                new_count = len(data)

                if new_count > scraped_count:
                    scraped_count = new_count
                    wait_cycles = 0
                    status_text.success(f"✅ Found {scraped_count} businesses!")

                    df = pd.DataFrame(data)
                    data_placeholder.dataframe(df, use_container_width=True)
                    progress_text.text(f"📊 Total items: {scraped_count}")
            else:
                wait_cycles += 1
                if wait_cycles <= 60:
                    status_text.info(f"🔄 Waiting for data... ({wait_cycles}s)")
                else:
                    status_text.warning("⚠️ Taking longer than expected...")

            time.sleep(1)

            # Auto-stop after data is collected
            if scraped_count > 0 and wait_cycles > 10:
                st.session_state.scraping = False
                st.session_state.data = data
                status_text.success(f"✅ Scraping complete! Found {scraped_count} businesses.")
                break

    # Display final results
    if not st.session_state.scraping and st.session_state.data:
        df = pd.DataFrame(st.session_state.data)
        st.success(f"✅ Scraping completed! Total: {len(df)}")
        
        st.subheader("📊 Scraped Data")
        st.dataframe(df, use_container_width=True)

        # Download buttons
        st.subheader("💾 Download Data")
        col1, col2 = st.columns(2)
        
        with col1:
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name='procore_data.csv',
                mime='text/csv',
            )
        
        with col2:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Sheet1')
            
            st.download_button(
                label="📥 Download as Excel",
                data=output.getvalue(),
                file_name='procore_data.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            )
    elif not st.session_state.scraping and not st.session_state.data:
        st.info('👆 Click "Start Scraping" button above to begin.')

if __name__ == '__main__':
    main()

