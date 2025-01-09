import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import json
import pandas as pd
import csv
import logging
import re
from utils.openai_client import client
from scripts.prompt import prompt

logging.basicConfig(
    level=logging.INFO, 
    filename='ner_automation.log', 
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def write_to_csv(file_path, batch_data, headers=None, mode='a'):
    """
    Writes a batch of data to a CSV file.

    Parameters:
        file_path (str): Path to the output CSV file.
        batch_data (list): List of rows (dictionaries) to write.
        headers (list): Optional list of column headers for the CSV file.
        mode (str): File mode ('w' for write, 'a' for append). Defaults to 'a'.
    """
    with open(file_path, mode, newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=headers)
        if mode == 'w' and headers:  # Write headers only if in write mode
            writer.writeheader()
        writer.writerows(batch_data)



# def automate_ner(case_input, court_title, case_id):
#     """
#     Automates NER tasks using an API call.
#     Parameters:
#         case_input (str): The text input for NER processing.
#         court_title (str): The court title for generating person_id.
#         case_id (str): The unique case identifier.
#         model (str): Model name for the API call. Defaults to "gpt-4o".
#         temperature (float): Temperature for the API call. Defaults to 0.05.
#         n (int): Number of completions to generate. Defaults to 1.
#         stop (list): Stop sequences for the API call. Defaults to None.
#         add_person_id (bool): Whether to add unique person_id to each person. Defaults to True.

#     Returns:
#         dict: Parsed NER result or error details.
#     """
#     # Enhanced logging with more context
#     logger = logging.getLogger(__name__)
    
#     # Detailed context logging before API call
#     logger.info(f"Starting NER processing for case_id: {case_id}")

#     prompt_messages = [
#         {"role": "system", "content": prompt},
#         {"role": "user", "content": case_input}
#     ]

#     try:
#         # Make the API call
#         completion = client.chat.completions.create(
#             model="gpt-4o",
#             messages=prompt_messages,
#             temperature=0.05,
#             n=1,
#             stop=None
#         )
#         # Clean and parse the result
#         result = completion.choices[0].message.content
        
#         # Debug raw response
#         logger.debug(f"Raw API response for case_id {case_id}: {result}")
        

#         if not result:
#             logger.warning(f"Empty API response for case_id: {case_id}")
#             return {'error': 'Empty API response', 'case_id': case_id}
        
#         # More robust JSON cleaning
#         cleaned_output_str = re.sub(r'```json\n|\n```', '', result).strip()
                
#         try:
#             ner_result = json.loads(cleaned_output_str)
#         except json.JSONDecodeError as jde:
#             # Detailed logging for JSON decoding errors
#             logger.error(f"JSON decoding error for case_id={case_id}: {jde}")
#             logger.error(f"Problematic JSON string: {cleaned_output_str[:500]}...")  # Log first 500 chars
#             return {
#                 'error': 'Invalid JSON in API response', 
#                 'case_id': case_id, 
#                 'raw_response': cleaned_output_str
#             }
#         # Validate the structure of NER result
#         if not isinstance(ner_result, dict):
#             logger.error(f"Invalid NER result structure for case_id: {case_id}")
#             return {'error': 'Invalid NER result structure', 'case_id': case_id}
        
#         # Add unique person_id if required
#         for idx, person in enumerate(ner_result.get('persons', []), start=1):
#             try:
#                 person_id = f"{court_title}_{case_id}_{idx}"
#                 person['person_id'] = person_id
#             except Exception as e:
#                 logger.warning(f"Error adding person_id for case_id {case_id}, person {idx}: {e}")
        
#         # Log successful processing
#         logger.info(f"Successfully processed NER for case_id: {case_id}")
#         return ner_result
#     except Exception as e:
#         # Comprehensive error logging
#         logger.error(f"Unexpected error in NER processing for case_id {case_id}: {e}", exc_info=True)
#         return {
#             'error': str(e), 
#             'case_id': case_id, 
#             'court_title': court_title,
#             'error_type': type(e).__name__
#         }

def automate_ner(case_input, court_title, case_id):
    """
    Automates NER tasks using an API call with enhanced JSON handling.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Starting NER processing for case_id: {case_id}")

    prompt_messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": case_input}
    ]

    try:
        # Make the API call
        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=prompt_messages,
            temperature=0.05,
            n=1,
            stop=None
        )
        
        result = completion.choices[0].message.content
        logger.debug(f"Raw API response:\n{result}")

        if not result:
            return {'error': 'Empty API response', 'case_id': case_id}

        try:
            # Step 1: Remove any markdown code block indicators
            cleaned = re.sub(r'```json\s*|```\s*', '', result)
            
            # Step 2: Ensure proper JSON structure
            if not cleaned.strip().startswith('{'):
                cleaned = '{' + cleaned
            if not cleaned.strip().endswith('}'):
                cleaned = cleaned + '}'
            
            # Step 3: Replace single quotes with double quotes
            cleaned = re.sub(r"'", '"', cleaned)
            
            # Step 4: Ensure property names are properly quoted
            cleaned = re.sub(r'([{,])\s*(\w+):', r'\1"\2":', cleaned)
            
            logger.debug(f"Cleaned JSON:\n{cleaned}")
            
            # Parse the JSON
            ner_result = json.loads(cleaned)
            
            # Add person_ids
            for idx, person in enumerate(ner_result.get('persons', []), start=1):
                person['person_id'] = f"{court_title}_{case_id}_{idx}"
            
            return ner_result

        except json.JSONDecodeError as jde:
            logger.error(f"JSON decoding error for case_id={case_id}: {jde}")
            logger.error(f"Raw response: {result}")
            logger.error(f"Cleaned response: {cleaned}")
            return {
                'error': 'Invalid JSON in API response',
                'case_id': case_id,
                'raw_response': result
            }
            
    except Exception as e:
        logger.error(f"Error in automate_ner for case_id {case_id}: {e}", exc_info=True)
        return {
            'error': str(e),
            'case_id': case_id,
            'court_title': court_title,
            'error_type': type(e).__name__
        }

def process_dataframe_ner(
    df, 
    summary_column_name, 
    court_title_column_name, 
    case_id_column_name, 
    output_file,
    batch_size=10,  # Added batch size parameter
    resume_from_index=None  # Added resume parameter
):
    """
    Processes a DataFrame for NER with batching and resume capabilities.
    
    Parameters:
        df (DataFrame): Input DataFrame with case data
        summary_column_name (str): Column name for case summaries
        court_title_column_name (str): Column name for court titles
        case_id_column_name (str): Column name for case IDs
        output_file (str): Path to the output CSV file
        batch_size (int): Number of rows to process in each batch
        resume_from_index (int): Index to resume processing from
    """
    headers = ['person_id', 'name', 'gender', 'religion_ethnicity', 
               'social_status_job', 'role_in_case', 'titles',
               'date', 'calendar', 'place_name', 'place_type', 
               'legal_case_type', 'case_result', 'case_result_type',
               'row_index', 'case_unique_id']
    
    # Initialize output file with headers if it doesn't exist
    if not os.path.exists(output_file):
        write_to_csv(output_file, [], headers=headers, mode='w')
    
    # Load existing results if any
    try:
        existing_results = pd.read_csv(output_file)
        ner_results = existing_results.to_dict('records')
        if resume_from_index is None and not existing_results.empty:
            resume_from_index = existing_results['row_index'].max() + 1
    except FileNotFoundError:
        ner_results = []
    
    # Resume from specified index if provided
    if resume_from_index is not None:
        print(f"Resuming from index {resume_from_index}")
        df = df.loc[resume_from_index:]
    
    total_rows = len(df)
    print(f"Processing {total_rows} rows")
    
    # Process in batches
    for batch_start in range(0, total_rows, batch_size):
        batch_end = min(batch_start + batch_size, total_rows)
        batch_df = df.iloc[batch_start:batch_end]
        
        print(f"Processing batch {batch_start//batch_size + 1}: rows {batch_start} to {batch_end-1}")
        
        batch_results = []
        for index, row in batch_df.iterrows():
            try:
                case_input = row[summary_column_name]
                court_title = row[court_title_column_name]
                case_id = row[case_id_column_name]
                
                # Using your original automate_ner function
                ner_result = automate_ner(case_input, court_title, case_id)
                
                if 'error' not in ner_result:
                    # Process extracted entities
                    base_row = {
                        'person_id': 'N/A', 'name': 'N/A', 'gender': 'N/A', 
                        'religion_ethnicity': 'N/A', 'social_status_job': 'N/A', 
                        'role_in_case': 'N/A', 'titles': 'N/A',
                        'date': 'N/A', 'calendar': 'N/A',
                        'place_name': 'N/A', 'place_type': 'N/A',
                        'legal_case_type': 'N/A', 'case_result': 'N/A', 
                        'case_result_type': 'N/A',
                        'row_index': index, 'case_unique_id': case_id
                    }
                    
                    # Process persons
                    for person in ner_result.get('persons', []):
                        person_row = base_row.copy()
                        person_row.update({
                            'person_id': person.get('person_id', 'N/A'),
                            'name': person.get('name', 'N/A'),
                            'gender': person.get('gender', 'N/A'),
                            'religion_ethnicity': person.get('religion_ethnicity', 'N/A'),
                            'social_status_job': person.get('social_status_job', 'N/A'),
                            'role_in_case': person.get('role_in_case', 'N/A'),
                            'titles': person.get('titles', 'N/A')
                        })
                        batch_results.append(person_row)
                    
                    # Process hijri dates
                    for date in ner_result.get('hijri_dates', []):
                        date_row = base_row.copy()
                        date_row.update({
                            'date': date,
                            'calendar': 'Hijri',
                            'row_index': index,
                            'case_unique_id': case_id
                        })
                        batch_results.append(date_row)
                    
                    # Process miladi dates
                    for date in ner_result.get('miladi_dates', []):
                        date_row = base_row.copy()
                        date_row.update({
                            'date': date,
                            'calendar': 'Miladi',
                            'row_index': index,
                            'case_unique_id': case_id
                        })
                        batch_results.append(date_row)

                    # Process places
                    for place in ner_result.get('places', []):
                        place_row = base_row.copy()
                        place_row.update({
                            'place_name': place.get('place_name', 'N/A'),
                            'place_type': place.get('place_type', 'N/A'),
                            'row_index': index,
                            'case_unique_id': case_id
                        })
                        batch_results.append(place_row)

                    # Process legal case type    
                    legal_type = ner_result.get('legal_case_type', 'N/A')
                    legal_type_row = base_row.copy()
                    legal_type_row.update({
                        'legal_case_type': legal_type,
                        'row_index': index,
                        'case_unique_id': case_id
                    })
                    batch_results.append(legal_type_row)

                    # Process case result
                    case_result = ner_result.get('case_result', 'N/A')
                    case_result_row = base_row.copy()
                    case_result_row.update({
                        'case_result': case_result,
                        'row_index': index,
                        'case_unique_id': case_id
                    })
                    batch_results.append(case_result_row)

                    # Process case result type
                    case_result_type = ner_result.get('case_result_type', 'N/A')
                    case_result_type_row = base_row.copy()
                    case_result_type_row.update({
                        'case_result_type': case_result_type,
                        'row_index': index,
                        'case_unique_id': case_id
                    })
                    batch_results.append(case_result_type_row)
                    
            except Exception as e:
                print(f"Error processing row {index}: {e}")
        
        # Write batch results to CSV
        if batch_results:
            write_to_csv(output_file, batch_results, headers=headers, mode='a')
            ner_results.extend(batch_results)
            
        print(f"Completed batch {batch_start//batch_size + 1}")
    
    print("Processing completed")
    return pd.DataFrame(ner_results)