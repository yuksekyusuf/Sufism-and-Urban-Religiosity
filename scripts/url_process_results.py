import os
import sys
import json
import logging
import pandas as pd
from pathlib import Path

# Get absolute path to project root
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from scripts.batch_processing_jan import process_batch_results, write_to_csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def clean_case_id(custom_id):
    """Clean and format the custom_id to match the format: 'Adalar Mahkemesi_1_Hüküm no: 1'"""
    try:
        parts = custom_id.split('_')
        if len(parts) >= 3:
            # Keep original court title case
            court = 'Adalar Mahkemesi'  # From the original data sample
            
            # Get sicil number
            sicil = parts[2]
            
            # Get the case number part and format it with proper capitalization
            case_part = parts[3] if len(parts) > 3 else 'Hüküm no:'
            
            # Handle the case where we have a number
            if case_part.endswith(':'):
                case_id = f"{court}_{sicil}_{case_part.replace('hüküm', 'Hüküm')}"
            else:
                # Split by colon to separate "Hüküm no:" from the number
                number = case_part.split(':')[-1].strip()
                case_id = f"{court}_{sicil}_Hüküm no: {number}"
            
            return case_id  # Don't convert to lowercase
            
        return custom_id
    except Exception as e:
        logger.error(f"Error cleaning case_id '{custom_id}': {e}")
        return custom_id

def process_and_save_results():
    try:
        # Read the original dataset
        csv_path = os.path.join(project_root, 'data', 'sicil_records.csv')
        df = pd.read_csv(csv_path)
        
        df['case_id'] = df.apply(lambda row: f"{row['court_title']}_{row['sicil_number']}_{row['case_number']}", axis=1)


        # # Print some examples of original court titles and case IDs
        # logger.info("Sample of original data:")
        # sample_data = df[['court_title', 'sicil_number', 'case_number', 'case_id']].head()
        # logger.info(sample_data)
        
        # Read the batch results
        with open('batch_results.jsonl', 'r') as file:
            results = [json.loads(line.strip()) for line in file]
        
        # Print sample of custom_ids before and after cleaning
        logger.info("\nSample ID transformations:")
        for result in results[:5]:
            original_id = result.get('custom_id')
            # cleaned_id = clean_case_id(original_id)
            logger.info(f"Original: {original_id}")
            # logger.info(f"Cleaned : {cleaned_id}")
            logger.info("-" * 50)
            # result['custom_id'] = cleaned_id
        
        # Create results directory
        results_dir = os.path.join(project_root, 'results')
        os.makedirs(results_dir, exist_ok=True)
        
        # Generate output filename
        timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
        output_file = os.path.join(results_dir, f'ner_results_{timestamp}.csv')
        
        # Process results
        result_df = process_batch_results(
            results=results,
            df=df,
            case_id_column_name='case_id',
            output_file=output_file
        )
        
        if not result_df.empty:
            logger.info(f"Processing complete. Results saved to {output_file}")
            logger.info(f"Total rows in processed results: {len(result_df)}")
            
            # Also save as Excel
            excel_output = output_file.replace('.csv', '.xlsx')
            result_df.to_excel(excel_output, index=False)
            logger.info(f"Results also saved as Excel file: {excel_output}")
        else:
            logger.warning("No results were processed successfully.")
        
        return result_df, output_file
        
    except Exception as e:
        logger.error(f"Error processing batch results: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    process_and_save_results()