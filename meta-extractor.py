
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urlparse
import logging
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('meta-extraction.log'),
        logging.StreamHandler()
    ]
)

# Configuration
INPUT_FILE = '4.csv'  # Your current file
OUTPUT_FILE = 'meta-enhanced.csv'
REQUEST_TIMEOUT = 15
DELAY_BETWEEN_REQUESTS = 2

class MetaExtractor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    def extract_meta(self, url):
        """Extract metadata from homepage"""
        result = {
            'meta_title': None,
            'meta_description': None,
            'og_title': None,
            'og_description': None,
            'first_paragraph': None,
            'h1_text': None,
            'extraction_status': 'failed'
        }
        
        try:
            logging.info(f"Fetching: {url}")
            response = self.session.get(url, timeout=REQUEST_TIMEOUT, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract page title
            title_tag = soup.find('title')
            if title_tag:
                result['meta_title'] = title_tag.get_text().strip()
            
            # Extract meta description
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if meta_desc and meta_desc.get('content'):
                result['meta_description'] = meta_desc.get('content').strip()
            
            # Extract Open Graph title
            og_title = soup.find('meta', property='og:title')
            if og_title and og_title.get('content'):
                result['og_title'] = og_title.get('content').strip()
            
            # Extract Open Graph description
            og_desc = soup.find('meta', property='og:description')
            if og_desc and og_desc.get('content'):
                result['og_description'] = og_desc.get('content').strip()
            
            # Extract first H1
            h1 = soup.find('h1')
            if h1:
                result['h1_text'] = h1.get_text().strip()
            
            # Extract first substantial paragraph
            paragraphs = soup.find_all('p')
            for p in paragraphs:
                text = p.get_text().strip()
                if len(text) > 50:  # Only substantial paragraphs
                    result['first_paragraph'] = text[:500]  # Limit to 500 chars
                    break
            
            result['extraction_status'] = 'success'
            logging.info(f"✓ Successfully extracted metadata from {url}")
            
        except requests.exceptions.Timeout:
            result['extraction_status'] = 'timeout'
            logging.warning(f"✗ Timeout: {url}")
            
        except requests.exceptions.ConnectionError:
            result['extraction_status'] = 'connection_error'
            logging.warning(f"✗ Connection error: {url}")
            
        except requests.exceptions.HTTPError as e:
            result['extraction_status'] = f'http_error_{e.response.status_code}'
            logging.warning(f"✗ HTTP error {e.response.status_code}: {url}")
            
        except Exception as e:
            result['extraction_status'] = f'error_{type(e).__name__}'
            logging.error(f"✗ Unexpected error for {url}: {str(e)}")
        
        return result
    
    def process_csv(self, input_file, output_file):
        """Process all URLs in CSV"""
        print("\n" + "="*60)
        print("Enhanced Metadata Extractor")
        print("="*60)
        
        # Read input CSV
        try:
            df = pd.read_csv(input_file)
            print(f"\n✓ Loaded {len(df)} rows from {input_file}")
        except Exception as e:
            logging.error(f"Failed to read input file: {e}")
            return
        
        # Find URL column
        url_col = None
        for col in df.columns:
            if col.lower() == 'url' or 'url' in col.lower():
                url_col = col
                break
        
        if not url_col:
            logging.error("No URL column found!")
            return
        
        print(f"✓ Using column: {url_col}")
        
        # Initialize new columns
        new_cols = ['meta_title', 'meta_description', 'og_title', 'og_description', 
                   'first_paragraph', 'h1_text', 'extraction_status']
        for col in new_cols:
            df[col] = None
        
        # Process each URL
        print(f"\n{'='*60}")
        print("Starting extraction...")
        print(f"{'='*60}\n")
        
        success_count = 0
        
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Extracting metadata"):
            url = row[url_col]
            
            if pd.isna(url) or not url:
                logging.warning(f"Row {idx}: Empty URL, skipping")
                continue
            
            # Extract metadata
            meta = self.extract_meta(url)
            
            # Update dataframe
            for col, value in meta.items():
                df.at[idx, col] = value
            
            if meta['extraction_status'] == 'success':
                success_count += 1
            
            # Save progress every 50 rows
            if (idx + 1) % 50 == 0:
                df.to_csv(output_file, index=False)
                logging.info(f"Progress saved: {idx + 1}/{len(df)} rows processed")
            
            # Delay between requests
            time.sleep(DELAY_BETWEEN_REQUESTS)
        
        # Final save
        df.to_csv(output_file, index=False)
        
        # Summary
        print("\n" + "="*60)
        print("EXTRACTION COMPLETE")
        print("="*60)
        print(f"\nTotal URLs processed: {len(df)}")
        print(f"Successful extractions: {success_count} ({success_count/len(df)*100:.1f}%)")
        print(f"\nResults saved to: {output_file}")
        print(f"Log saved to: meta-extraction.log")
        
        # Data quality report
        print("\n" + "="*60)
        print("DATA QUALITY REPORT")
        print("="*60)
        for col in new_cols[:-1]:  # Exclude status column
            non_empty = df[col].notna().sum()
            pct = non_empty/len(df)*100
            print(f"{col}: {non_empty}/{len(df)} ({pct:.1f}%)")
        
        print("\n✓ All done!")

def main():
    extractor = MetaExtractor()
    extractor.process_csv(INPUT_FILE, OUTPUT_FILE)

if __name__ == "__main__":
    main()