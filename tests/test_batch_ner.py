import sys
import os
# Get the absolute path to the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
import pandas as pd
import logging
from scripts.openai_ner_batch_operations import submit_batch_job, check_batch_status, process_batch_results

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def create_test_data():
    """Create a small test DataFrame"""
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
        }
    ]
    return pd.DataFrame(test_data)

def test_batch_submission():
    """Test batch job submission"""
    print("\nTesting batch job submission...")
    df = create_test_data()
    
    try:
        job_id = submit_batch_job(
            df=df,
            summary_column_name='summary',
            court_title_column_name='court',
            case_id_column_name='case_id'
        )
        print(f"Successfully submitted batch job. Job ID: {job_id}")
        return job_id
    except Exception as e:
        print(f"Error submitting batch job: {e}")
        return None

def test_status_check(job_id):
    """Test batch status checking"""
    print(f"\nChecking status for job {job_id}...")
    try:
        status = check_batch_status(job_id)
        print(f"Job status: {status}")
        return status
    except Exception as e:
        print(f"Error checking job status: {e}")
        return None

def test_result_processing(job_id):
    """Test result processing"""
    print(f"\nProcessing results for job {job_id}...")
    df = create_test_data()
    
    try:
        results = process_batch_results(
            job_id=job_id,
            df=df,
            case_id_column_name='case_id',
            output_file='test_results.csv'
        )
        print("Results processed successfully")
        if results is not None:
            print("\nSample of processed results:")
            print(results.head())
            
    except Exception as e:
        print(f"Error processing results: {e}")

if __name__ == "__main__":
    print("Starting batch NER processor test...")
    
    # Test submission
    job_id = test_batch_submission()
    if job_id:
        # Test status check
        status = test_status_check(job_id)
        
        # If job is completed, test result processing
        if status == 'completed':
            test_result_processing(job_id)
        else:
            print("\nJob not completed yet. Run status check again later.")
            print(f"Save this job ID to check later: {job_id}")