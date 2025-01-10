import pandas as pd
import logging
import sys
import os

# Add the project root directory to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(project_root)

from scripts.batch_processing_jan import process_ner_batch

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def test_batch_processing():
    try:
        csv_path = os.path.join(project_root, 'data', 'sicil_records.csv')
        logger.info(f"Attempting to read CSV from: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # Combine summary and case text
        df['case_text_summary'] = "Summary: " + df['case_summary'].astype(str) + "\n" + "Case: " + df['case_text'].astype(str)
        
        # Create a unique ID by concatenating 'court_title', 'sicil_number', and 'case_number' with a separator
        df['case_id'] = (df['court_title'] + '_' + 
                      df['sicil_number'].astype(str) + '_' + 
                      df['case_number']).str.lower()
        
        # For testing, let's use a small subset
        df['sicil_id'] = df['court_title'] + '_' + df['sicil_number'].astype(str)
        # selected_sicil = "Evkaf-ı Hümâyûn Müfettişliği_1"
        filtered_df = df[df['sicil_id'] == selected_sicil].copy()

        # Define column names
        summary_column = "case_text_summary"
        court_title_column = "court_title"  # Replace with actual column name
        case_id_column = "case_id"  # Replace with actual column name

        # Process the batch
        batch_id = process_ner_batch(
            df=filtered_df,
            summary_column_name=summary_column,
            case_id_column_name=case_id_column,
        )

        logger.info(f"Processed the batch with {batch_id}")
    except Exception as e:
        logger.error(f"Error in test batch processing: {e}", exc_info=True)

if __name__ == "__main__":
    test_batch_processing()