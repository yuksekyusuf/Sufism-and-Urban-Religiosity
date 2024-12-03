import json
import pandas as pd
import sys
import os
import csv
import logging
from utils.openai_client import client
from scripts.prompt import prompt


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))



logging.basicConfig(level=logging.INFO, filename='ner_automation.log', filemode='a',
                    format='%(asctime)s - %(levelname)s - %(message)s')


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



def automate_ner(case_input, court_title, case_id):
    """
    Automates NER tasks using an API call.
    Parameters:
        case_input (str): The text input for NER processing.
        court_title (str): The court title for generating person_id.
        case_id (str): The unique case identifier.
        model (str): Model name for the API call. Defaults to "gpt-4o".
        temperature (float): Temperature for the API call. Defaults to 0.05.
        n (int): Number of completions to generate. Defaults to 1.
        stop (list): Stop sequences for the API call. Defaults to None.
        add_person_id (bool): Whether to add unique person_id to each person. Defaults to True.

    Returns:
        dict: Parsed NER result or error details.
    """
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
        # Clean and parse the result
        result = completion.choices[0].message.content
        cleaned_output_str = re.sub(r'```json\n|\n```', '', result).strip()        
        try:
            ner_result = json.loads(cleaned_output_str)
        except json.JSONDecodeError as jde:
            print(f"JSON decoding error for case_id={case_id}: {jde}")
            return {'error': 'Invalid JSON in API response'}
        # Add unique person_id if required
        for idx, person in enumerate(ner_result.get('persons', []), start=1):
            person_id = f"{court_title}_{case_id}_{idx}"
            person['person_id'] = person_id
        return ner_result
    except Exception as e:
        print(f"An error occured: {e}")
        logging.error(f"Error in automate_ner for case_id={case_id}: {e}")
        return {'error': str(e), 'case_id': case_id, 'court_title': court_title}


def process_dataframe_ner(df, summary_column_name, court_title_column_name, case_id_column_name, output_file):
    """
    Processes a DataFrame for NER and writes results to a CSV file.

    Parameters:
        df (DataFrame): Input DataFrame with case data.
        summary_column_name (str): Column name for case summaries.
        court_title_column_name (str): Column name for court titles.
        case_id_column_name (str): Column name for case IDs.
        output_file (str): Path to the output CSV file.

    Returns:
        DataFrame: DataFrame containing all processed NER results.
    """
    # Ensure headers exist in the output file
    headers = ['person_id', 'name', 'gender', 'religion_ethnicity', 'social_status_job', 'role_in_case', 'titles',
               'date', 'calendar', 'place_name', 'place_type', 'legal_case_type', 'case_result', 'case_result_type',
               'row_index', 'case_unique_id']
    
    # Ensure the output file exists with headers
    if not os.path.exists(output_file):
        write_to_csv(output_file, [], headers=headers, mode='w')
    
    try:
        existing_results = pd.read_csv(output_file)
        ner_results = existing_results.to_dict('records')
    except FileNotFoundError:
        # If file doesn't exist, it will be created above, so just initialize ner_results
        ner_results = []
    
    # A helper function to append rows to the CSV using DictWrite
    def append_rows_to_csv(output_path, rows_to_append, headers):
        if not rows_to_append:
            return
        with open(output_path, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            # Headers already written, just write rows
            writer.writerows(rows_to_append)
    
    #Base template for a row
    base_row = {
        'person_id': 'N/A', 'name': 'N/A', 'gender': 'N/A', 'religion_ethnicity': 'N/A',
        'social_status_job': 'N/A', 'role_in_case': 'N/A', 'titles': 'N/A',
        'date': 'N/A', 'calendar': 'N/A',
        'place_name': 'N/A', 'place_type': 'N/A',
        'legal_case_type': 'N/A', 'case_result': 'N/A', 'case_result_type': 'N/A',
        'row_index': 'N/A', 'case_unique_id': 'N/A'
    }
    for index, row in df.iterrows():
        rows_to_write = []
        case_input = row[summary_column_name]
        court_title = row[court_title_column_name]
        case_id = row[case_id_column_name]
        ner_result = automate_ner(case_input, court_title, case_id)

        if 'error' not in ner_result:
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
                    'titles': person.get('titles', 'N/A'),
                    'row_index': index,
                    'case_unique_id': case_id
                })
                rows_to_write.append(person_row)

            # Process hijri dates
            for date in ner_result.get('hijri_dates', []):
                date_row = base_row.copy()
                date_row.update({
                    'date': date,
                    'calendar': 'Hijri',
                    'row_index': index,
                    'case_unique_id': case_id
                })
                rows_to_write.append(date_row)
            # Process miladi dates
            for date in ner_result.get('miladi_dates', []):
                date_row = base_row.copy()
                date_row.update({
                    'date': date,
                    'calendar': 'Miladi',
                    'row_index': index,
                    'case_unique_id': case_id
                })
                rows_to_write.append(date_row)

            # Process places
            for place in ner_result.get('places', []):
                place_row = base_row.copy()
                place_row.update({
                    'place_name': place.get('place_name', 'N/A'),
                    'place_type': place.get('place_type', 'N/A'),
                    'row_index': index,
                    'case_unique_id': case_id
                })
                rows_to_write.append(place_row)

            # Process legal case type    
            legal_type = ner_result.get('legal_case_type', 'N/A')
            legal_type_row = base_row.copy()
            legal_type_row.update({
                'legal_case_type': legal_type,
                'row_index': index,
                'case_unique_id': case_id
            })
            rows_to_write.append(legal_type_row)

            # Process case result
            case_result = ner_result.get('case_result', 'N/A')
            case_result_row = base_row.copy()
            case_result_row.update({
                'case_result': case_result,
                'row_index': index,
                'case_unique_id': case_id
            })
            rows_to_write.append(case_result_row)

            # Process case result type
            case_result_type = ner_result.get('case_result_type', 'N/A')
            case_result_type_row = base_row.copy()
            case_result_type_row.update({
                'case_result_type': case_result_type,
                'row_index': index,
                'case_unique_id': case_id
            })
            rows_to_write.append(case_result_type_row)

            # Append to main results and write to CSV once per row
            ner_results.extend(rows_to_write)
            append_rows_to_csv(output_file, rows_to_write, headers)
        else:
            print(f"Error processing row {index}: {ner_result['error']}")
    ner_df = pd.DataFrame(ner_results)
    return ner_df