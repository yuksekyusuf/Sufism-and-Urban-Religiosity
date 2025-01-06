import pandas as pd
import sys
import os
import logging
# Get the absolute path to the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
from scripts.batch_ner_processing import process_dataframe_ner_batch

# Create sample test data
test_data = [
    {
        'summary': "es-Seyyid Mehmed Efendi b. Abdullah filed a complaint against Kirkor v. Yanos regarding a property dispute in Istanbul Mahmutpaşa district on 15 Ramadan 1255. The dispute was resolved through mediation (muslihûn) with a settlement.",
        'court': 'Court_1',
        'case_id': 'TEST_001'
    },
    {
        'summary': "Derviş Ahmed filed a complaint against es-Seyyid Hüseyin Ağa regarding a debt of 1000 kuruş in Eyüp on 3 Şevval 1255. The case was resolved with oath.",
        'court': 'Court_2',
        'case_id': 'TEST_002'
    },
    {
        'summary': "Fatma Hatun bt. Mustafa filed for divorce from her husband İbrahim Efendi in Üsküdar on 20 Zilhicce 1255. The court granted the divorce through khul'.",
        'court': 'Court_3',
        'case_id': 'TEST_003'
    }
]

# Create test DataFrame
test_df = pd.DataFrame(test_data)

def test_batch_processing():
    print("Starting batch processing test...")
    print(f"Test data size: {len(test_df)} records")
    
    try:
        result_df = process_dataframe_ner_batch(
            df=test_df,
            summary_column_name='summary',
            court_title_column_name='court',
            case_id_column_name='case_id',
            output_file='test_batch_results.csv'
        )
        
        print("\nTest completed successfully!")
        print(f"Results shape: {result_df.shape if result_df is not None else 'No results'}")
        
        # Display sample of results
        if result_df is not None:
            print("\nSample of processed results:")
            print(result_df.head())
            
            # Count entities per case
            print("\nEntities per case:")
            case_counts = result_df['case_unique_id'].value_counts()
            print(case_counts)
            
    except Exception as e:
        print(f"Test failed with error: {e}")

if __name__ == "__main__":
    test_batch_processing()