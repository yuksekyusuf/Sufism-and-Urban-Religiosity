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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    filename='batch_ner_processing.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def write_to_csv(file_path, batch_data, headers=None, mode='a'):
    """Writes a batch of data to a CSV file."""
    with open(file_path, mode, newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        if mode == 'w' and headers:
            writer.writeheader()
        writer.writerows(batch_data)

def prepare_batch_tasks(df, summary_column_name, court_title_column_name, case_id_column_name):
    """Prepare batch tasks in JSONL format."""
    logger.info("Preparing batch tasks...")
    tasks = []
    
    for index, row in df.iterrows():
        task = {
            "custom_id": f"{row[court_title_column_name]}_{row[case_id_column_name]}",
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": "gpt-4o-mini",
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
    file_name = "batch_tasks_ner.jsonl"
    with open(file_name, 'w', encoding='utf-8') as file:
        for task in tasks:
            file.write(json.dumps(task, ensure_ascii=False) + '\n')
    
    logger.info(f"Created batch file with {len(tasks)} tasks")
    return file_name

def process_batch_results(results, df, case_id_column_name, output_file):
    """Process batch results and write to CSV."""
    headers = ['person_id', 'name', 'gender', 'religion_ethnicity', 
              'social_status_job', 'role_in_case', 'titles',
              'date', 'calendar', 'place_name', 'place_type', 
              'legal_case_type', 'case_result', 'case_result_type',
              'row_index', 'case_unique_id']
    
    all_rows = []
    
    for result in results:
        try:
            custom_id = result['custom_id']
            court_title, case_id = custom_id.split('_', 1)
            row_index = df[df[case_id_column_name] == case_id].index[0]
            
            # Parse the NER result from the response
            content = result['response']['body']['choices'][0]['message']['content']
            ner_result = json.loads(content)
            
            # Process entities and create rows
            rows_to_write = []
            base_row = {
                'person_id': 'N/A', 'name': 'N/A', 'gender': 'N/A', 
                'religion_ethnicity': 'N/A', 'social_status_job': 'N/A', 
                'role_in_case': 'N/A', 'titles': 'N/A',
                'date': 'N/A', 'calendar': 'N/A',
                'place_name': 'N/A', 'place_type': 'N/A',
                'legal_case_type': 'N/A', 'case_result': 'N/A', 
                'case_result_type': 'N/A',
                'row_index': row_index,
                'case_unique_id': case_id
            }
            
            # Process each entity type
            # Process persons
            for idx, person in enumerate(ner_result.get('persons', []), start=1):
                person_row = base_row.copy()
                person_row.update({
                    'person_id': f"{court_title}_{case_id}_{idx}",
                    'name': person.get('name', 'N/A'),
                    'gender': person.get('gender', 'N/A'),
                    'religion_ethnicity': person.get('religion_ethnicity', 'N/A'),
                    'social_status_job': person.get('social_status_job', 'N/A'),
                    'role_in_case': person.get('role_in_case', 'N/A'),
                    'titles': person.get('titles', 'N/A')
                })
                rows_to_write.append(person_row)
            
            # Process hijri dates
            for date in ner_result.get('hijri_dates', []):
                date_row = base_row.copy()
                date_row.update({
                    'date': date,
                    'calendar': 'Hijri',
                    'row_index': row_index,
                    'case_unique_id': case_id
                })
                rows_to_write.append(date_row)
            
            # Process miladi dates
            for date in ner_result.get('miladi_dates', []):
                date_row = base_row.copy()
                date_row.update({
                    'date': date,
                    'calendar': 'Miladi',
                    'row_index': row_index,
                    'case_unique_id': case_id
                })
                rows_to_write.append(date_row)

            # Process places
            for place in ner_result.get('places', []):
                place_row = base_row.copy()
                place_row.update({
                    'place_name': place.get('place_name', 'N/A'),
                    'place_type': place.get('place_type', 'N/A'),
                    'row_index': row_index,
                    'case_unique_id': case_id
                })
                rows_to_write.append(place_row)

            # Process legal case type    
            legal_type = ner_result.get('legal_case_type', 'N/A')
            legal_type_row = base_row.copy()
            legal_type_row.update({
                'legal_case_type': legal_type,
                'row_index': row_index,
                'case_unique_id': case_id
            })
            rows_to_write.append(legal_type_row)

            # Process case result
            case_result = ner_result.get('case_result', 'N/A')
            case_result_row = base_row.copy()
            case_result_row.update({
                'case_result': case_result,
                'row_index': row_index,
                'case_unique_id': case_id
            })
            rows_to_write.append(case_result_row)

            # Process case result type
            case_result_type = ner_result.get('case_result_type', 'N/A')
            case_result_type_row = base_row.copy()
            case_result_type_row.update({
                'case_result_type': case_result_type,
                'row_index': row_index,
                'case_unique_id': case_id
            })
            rows_to_write.append(case_result_type_row)
            
            # Add other entities processing here (dates, places, etc.)
            all_rows.extend(rows_to_write)
            
        except Exception as e:
            logger.error(f"Error processing result for {custom_id}: {e}")
    
    # Write all processed results to CSV
    if all_rows:
        write_to_csv(output_file, all_rows, headers=headers, mode='w')
    
    return pd.DataFrame(all_rows)

def process_dataframe_ner_batch(df, summary_column_name, court_title_column_name, 
                              case_id_column_name, output_file):
    """Main function to process DataFrame using OpenAI's batch API."""
    try:
        # Step 1: Prepare and upload batch file
        batch_file_name = prepare_batch_tasks(df, summary_column_name, 
                                            court_title_column_name, case_id_column_name)
        
        batch_file = client.files.create(
            file=open(batch_file_name, "rb"),
            purpose="batch"
        )
        logger.info(f"Batch file uploaded: {batch_file.id}")
        
        # Step 2: Create batch job
        batch_job = client.batches.create(
            input_file_id=batch_file.id,
            endpoint="/v1/chat/completions",
            completion_window="24h"
        )
        logger.info(f"Batch job created: {batch_job.id}")
        
        # Step 3: Check job status
        while True:
            batch_job = client.batches.retrieve(batch_job.id)
            logger.info(f"Batch job status: {batch_job.status}")
            
            if batch_job.status == 'completed':
                break
            elif batch_job.status == 'failed':
                raise Exception("Batch job failed")
            
            time.sleep(60)  # Check every minute
        
        # Step 4: Retrieve and process results
        result_file_id = batch_job.output_file_id
        result = client.files.content(result_file_id).content
        
        # Save results to temporary file
        result_file_name = "batch_job_results.jsonl"
        with open(result_file_name, 'wb') as file:
            file.write(result)
        
        # Load and process results
        results = []
        with open(result_file_name, 'r') as file:
            for line in file:
                results.append(json.loads(line.strip()))
        
        # Process results and write to CSV
        result_df = process_batch_results(results, df, case_id_column_name, output_file)
        
        logger.info("Batch processing completed successfully")
        return result_df
        
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        raise
        
    finally:
        # Cleanup temporary files
        for file_name in [batch_file_name, result_file_name]:
            if os.path.exists(file_name):
                os.remove(file_name)

if __name__ == "__main__":
    # Example usage
    try:
        df = pd.read_csv('your_input_file.csv')
        
        result_df = process_dataframe_ner_batch(
            df=df,
            summary_column_name='summary',
            court_title_column_name='court',
            case_id_column_name='case_id',
            output_file='batch_ner_results.csv'
        )
        
        print("Processing completed successfully")
        
    except Exception as e:
        logger.error(f"Main execution error: {e}")
        print(f"An error occurred: {e}")