import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.constants import social_status_job_roles, legal_case_types, sufi_orders, sufi_titles

social_status_job_roles_str = ", ".join(social_status_job_roles)
legal_case_types_str = ", ".join(legal_case_types)
sufi_orders_str = ", ".join(sufi_orders)
sufi_titles_str = ", ".join(sufi_titles)

prompt_template = (
    "You are tasked with extracting named entities from an Ottoman Turkish legal document, "
    "paying close attention to the structure and details within. Each case begins with a summary, "
    "providing an overview that includes key details and individuals involved. When examining these documents, "
    "directly provide structured information about all named individuals, places, and dates without introductory text.\n\n"
    "Keep in mind the following naming conventions:\n"
    "- 'X v. Y v. Z' and 'X b. Y' indicate lineage, with 'v.' meaning 'son of' and 'b.' meaning 'bin'. "
    "'bt.' is used for females and means 'bint', indicating 'daughter of'. For example, "
    "'Esteban v. Kirkor v. Yanos' refers to a single individual, Esteban, son of Kirkor, son of Yanos; "
    "'Mustafa b. Abdülhamid' refers to Mustafa, son of Abdülhamid. Based on this, provide the full name of an individual.\n"
    "- Pay special attention to titles and designators that appear before or after names, such as 'es-Seyyid' "
    "or 'Ağa', and include these in a separate 'titles' column, presented as a string array format for each individual.\n"
    "- Additionally, endeavor to identify non-Muslims' ethnicities based on the names provided.\n\n"
    "Social Status/Job Roles to consider include:\n"
    "{}\n\n"
    "Legal Case Types to categorize:\n"
    "{}\n\n"
    "For each individual, extract their presumed gender (Man or Woman), religion/ethnicity (Muslim, Armenian, Greek, Jewish, "
    "European, etc.), social status or job, and their role in the legal case. Also, determine the type of legal case, "
    "categorizing it accordingly. Note any specific titles or affiliations to Sufi orders for individuals associated with Sufi practices, "
    "such as 'şeyh', 'derviş', 'dede', 'baba', or order names like 'mevlevi', 'bektaşi', 'nakşibendi', 'halveti', and others from the following list:\n"
    "{}\n"
    "Include Sufi-specific titles from the following list:\n"
    "{}\n\n"
    "If it is indicated a person lives at a Sufi lodge or a place related to Sufism with an expresion like: "
    "'bir tekkede, tekayada meskun', idenitfy these individuals as Sufi."
    "For places, provide the name and type (e.g., city, village, region, or Sufi-specific places like âsitane, dergâh, hankâh, tekke, zaviye, tekye) "
    "in a separate 'places' field. Also include specific Sufi places associated with particular orders such as Gülşenîhane, Kalenderhane, Kadirîhane, Mevlevîhane.\n"
    "For dates, provide the date in separate 'hijri_dates' and 'miladi_dates' fields.\n\n"
    "For the 'person_id', use the format 'court_title_case_id_number', where 'court_title' is the title of the court, "
    "'case_id' is the unique identifier for the case, and 'number' is a sequential identifier starting from 1 for each person in the case.\n\n"
    "If it is a dispute between a plaintiff and a defendant, or resulted with a settlement, extract the result of the legal case and specify who won the case in a 'case_result' field.\n\n"
    "Disputes settled in court were typically entered into the record generically as the result of intervention by mediators (muslihûn), without further details regarding the process of reaching an agreement.\n\n"
    "The result type can be also categorized as settlement, trial by evidence, and resolution by oath.\n\n"
    "Present your findings starting immediately with a structured format, as follows:\n"
    "{{\n"
    "  'persons': [\n"
    "    {{\n"
    "      'person_id': 'court_title_case_id_1',\n"
    "      'name': '...',\n"
    "      'gender': '...',\n"
    "      'religion_ethnicity': '...',\n"
    "      'social_status_job': '...',\n"
    "      'role_in_case': '...',\n"
    "      'titles': ['...']\n"
    "    }},\n"
    "    {{\n"
    "      'person_id': 'court_title_case_id_2',\n"
    "      'name': '...',\n"
    "      'gender': '...',\n"
    "      'religion_ethnicity': '...',\n"
    "      'social_status_job': '...',\n"
    "      'role_in_case': '...',\n"
    "      'titles': ['...']\n"
    "    }}\n"
    "    ...\n"
    "  ],\n"
    "  'places': [\n"
    "    {{\n"
    "      'place_name': '...',\n"
    "      'place_type': '...'\n"
    "    }}\n"
    "    ...\n"
    "  ],\n"
    "  'hijri_dates': [\n"
    "    'month year'\n"
    "    ...\n"
    "  ],\n"
    "  'miladi_dates': [\n"
    "    'month year'\n"
    "    ...\n"
    "  ],\n"
    "  'legal_case_type': '...'\n"
    "  'case_result': '...'\n"
    "  'case_result_type': '...'\n"
    "}}\n"
    "Use 'N/A' for any unspecified information and ensure that each person mentioned is captured with detailed "
    "categorization according to the specified labels. Start directly with the structured dictionary information."
)


prompt = prompt_template.format(
    social_status_job_roles_str,
    legal_case_types_str,
    sufi_orders_str,
    sufi_titles_str
)