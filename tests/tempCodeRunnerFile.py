import sys
import os
# Get the absolute path to the project root directory
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, project_root)
from scripts.openai_ner_batch_operations import process_batch_results
import pandas as pd
from utils.openai_client import client


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
df = pd.DataFrame(test_data)

job_id = "batch_6752129ebdf881918611d5a9adc0d4d3"

def check_job_and_process(job_id, df):
    """Check a specific job and process if complete"""
    try:
        batch_job = client.batches.retrieve(job_id)
        print(f"Job {job_id} status: {batch_job.status}")
        
        if batch_job.status == 'completed':
            results = process_batch_results(
                job_id=job_id,
                df=df,
                case_id_column_name='case_id',
                output_file=f'results_{job_id}.csv'
            )
            print("Processing completed successfully")
            return results
        else:
            print("Job not yet completed")
            return None
            
    except Exception as e:
        print(f"Error: {e}")
        return None

results = check_job_and_process(job_id, df)


if results is not None:
    print("\nProcessed Results Preview:")
    print(results.head())
    print("\nTotal rows:", len(results))