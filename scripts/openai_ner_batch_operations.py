import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import pandas as pd
import csv
import logging
import time
from utils.openai_client import client
from scripts.prompt import prompt

# Your existing logging configuration
logging.basicConfig(
    level=logging.INFO,
    filename='batch_ner_processing.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Keep your existing helper functions
def write_to_csv(file_path, batch_data, headers=None, mode='a'):
    """Your existing write_to_csv function"""
    with open(file_path, mode, newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        if mode == 'w' and headers:
            writer.writeheader()
        writer.writerows(batch_data)

def prepare_batch_tasks(df, summary_column_name, court_title_column_name, case_id_column_name):
    """Your existing prepare_batch_tasks function"""
    logger.info("Preparing batch tasks...")
    tasks = []
    
    for index, row in df.iterrows():
        task = {
            "custom_id": f"{row[court_title_column_name]}_{row[case_id_column_name]}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-4o",
                "temperature": 0.05,
                "response_format": {
                    "type": "json_object"
                },
                "messages": [
                    {"role": "system", "content": prompt},
                    {"role": "user", "content": row[summary_column_name]}
                ]
            }
        }
        tasks.append(task)
    
    # Create JSONL file
    file_name = f"batch_tasks_ner_{int(time.time())}.jsonl"  # Add timestamp to avoid conflicts
    with open(file_name, 'w', encoding='utf-8') as file:
        for task in tasks:
            file.write(json.dumps(task, ensure_ascii=False) + '\n')
    
    logger.info(f"Created batch file with {len(tasks)} tasks")
    return file_name

# Split your main process into three separate functions
def submit_batch_job(df, summary_column_name, court_title_column_name, case_id_column_name):
    """Step 1: Submit batch job and return job ID"""
    try:
        # Prepare and upload batch file
        batch_file_name = prepare_batch_tasks(df, summary_column_name, 
                                            court_title_column_name, case_id_column_name)
        
        batch_file = client.files.create(
            file=open(batch_file_name, "rb"),
            purpose="batch"
        )
        logger.info(f"Batch file uploaded: {batch_file.id}")
        
        # Create batch job
        batch_job = client.batches.create(
            input_file_id=batch_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        logger.info(f"Batch job created: {batch_job.id}")
        
        # Save job ID for later use
        job_info = {
            'job_id': batch_job.id,
            'file_id': batch_file.id,
            'submitted_at': time.time()
        }
        with open(f'job_info_{batch_job.id}.json', 'w') as f:
            json.dump(job_info, f)
        
        # Cleanup batch file
        if os.path.exists(batch_file_name):
            os.remove(batch_file_name)
            
        return batch_job.id
        
    except Exception as e:
        logger.error(f"Error submitting batch job: {e}")
        raise

def check_batch_status(job_id):
    """Step 2: Check status of a batch job"""
    try:
        batch_job = client.batches.retrieve(job_id)
        status = batch_job.status
        logger.info(f"Job {job_id} status: {status}")
        return status
        
    except Exception as e:
        logger.error(f"Error checking batch status for job {job_id}: {e}")
        raise

def process_batch_results(job_id, df, case_id_column_name, output_file):
    """Step 3: Process completed batch results"""
    try:
        # Verify job is completed
        batch_job = client.batches.retrieve(job_id)
        if batch_job.status != 'completed':
            logger.warning(f"Job {job_id} is not completed yet. Current status: {batch_job.status}")
            return None
        
        # Get results
        result_file_id = batch_job.output_file_id
        result = client.files.content(result_file_id).content
        
        # Save results temporarily
        result_file_name = f"batch_results_{job_id}.jsonl"
        with open(result_file_name, 'wb') as file:
            file.write(result)
        
        # Load results
        results = []
        with open(result_file_name, 'r') as file:
            for line in file:
                results.append(json.loads(line.strip()))
        
        # Process results using your existing function
        result_df = process_batch_results(results, df, case_id_column_name, output_file)
        
        # Cleanup
        if os.path.exists(result_file_name):
            os.remove(result_file_name)
        
        logger.info(f"Successfully processed results for job {job_id}")
        return result_df
        
    except Exception as e:
        logger.error(f"Error processing batch results for job {job_id}: {e}")
        raise

# Example usage:
if __name__ == "__main__":
    try:
        # Step 1: Submit job
        df = pd.read_csv('your_input_file.csv')
        job_id = submit_batch_job(
            df=df,
            summary_column_name='summary',
            court_title_column_name='court',
            case_id_column_name='case_id'
        )
        print(f"Job submitted with ID: {job_id}")
        
        # Step 2: Check status (can be run separately)
        status = check_batch_status(job_id)
        print(f"Current status: {status}")
        
        # Step 3: Process results (when ready)
        if status == 'completed':
            result_df = process_batch_results(
                job_id=job_id,
                df=df,
                case_id_column_name='case_id',
                output_file='batch_ner_results.csv'
            )
            print("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Main execution error: {e}")
        print(f"An error occurred: {e}")