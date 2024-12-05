import sys
import os
# Get the absolute path to the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)

import pandas as pd
import random
import time
from scripts.ner_automation import process_dataframe_ner



def create_sample_df(size=15):
    """Create a sample DataFrame with realistic Ottoman legal document examples"""
    data = []
    for i in range(size):
        data.append({
            'summary': (
                f"es-Seyyid Mehmed Efendi b. Abdullah filed a complaint against "
                f"Kirkor v. Yanos regarding a property dispute in Istanbul Mahmutpaşa "
                f"district on 15 Ramadan 1255. The dispute was resolved through mediation "
                f"(muslihûn) with a settlement."
            ),
            'court': f"Court_{random.randint(1,5)}",
            'case_id': f"CASE_{i:03d}"
        })
    return pd.DataFrame(data)

def test_batch_processing():
    # Create sample data
    df = create_sample_df(15)
    print(f"Created test DataFrame with {len(df)} rows")
    
    try:
        # First run - process first portion
        result_df = process_dataframe_ner(
            df=df,
            summary_column_name='summary',
            court_title_column_name='court',
            case_id_column_name='case_id',
            output_file='test_ner_results2.csv'
        )
        
        # Simulate interruption halfway
        raise Exception("Simulated connection failure!")
        
    except Exception as e:
        print(f"\nProcessing interrupted: {str(e)}")
        print("Waiting 5 seconds before resuming...")
        time.sleep(5)
        
        # Resume processing
        try:
            if os.path.exists('test_ner_results2.csv'):
                existing_results = pd.read_csv('test_ner_results2.csv')
                last_processed = existing_results['row_index'].max()
                print(f"\nLast processed row index: {last_processed}")
                resume_index = last_processed + 1
            else:
                print("\nNo existing results found, starting from beginning")
                resume_index = 0
            
            # Resume processing from the appropriate index
            result_df = process_dataframe_ner(
                df=df,
                summary_column_name='summary',
                court_title_column_name='court',
                case_id_column_name='case_id',
                output_file='test_ner_results2.csv',
                resume_from_index=resume_index
            )
            
        except Exception as resume_error:
            print(f"Error during resume: {str(resume_error)}")

if __name__ == "__main__":
    test_batch_processing()