import json
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.prompt import prompt
from utils.openai_client import client


def automate_ner(case_input, court_title, case_id):
    prompt_messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": case_input}
    ]

    try:
        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=prompt_messages,
            temperature=0.05,
            n=1,
            stop=None
        )

        result = completion.choices[0].message.content
        cleaned_output_str = result.replace("```json\n", "").replace("\n```", "").strip()
        ner_result = eval(cleaned_output_str)
        for idx, person in enumerate(ner_result.get('persons', []), start=1):
            person_id = f"{court_title}_{case_id}_{idx}"
            person['person_id'] = person_id
        return ner_result
    except Exception as e:
        print(f"An error occured: {e}")
        return {'error': str(e)}


def process_dataframe_ner(df, summary_column_name, court_title_column_name, case_id_column_name):
    ner_results = []

    for index, row in df.iterrows():
        case_input = row[summary_column_name]
        court_title = row[court_title_column_name]
        case_id = row[case_id_column_name]
        ner_result = automate_ner(case_input, court_title, case_id)

        if 'error' not in ner_result:
            for person in ner_result.get('persons', []):
                person['legal_case_type'] = ner_result.get('legal_case_type', 'N/A')
                person['row_index'] = index
                person['case_unique_id'] = case_id
                ner_results.append(person)

            for date in ner_result.get('dates', []):
                date_info = {
                    'date': date,
                    'row_index': index,
                    'case_unique_id': case_id
                }
                ner_results.append(date_info)
            for place in ner_result.get('places', []):
                place_info = {
                    'place_name': place['place_name'],
                    'place_type': place['place_type'],
                    'row_index': index,
                    'case_unique_id': case_id
                }
                ner_results.append(place_info)
        else:
            print(f"Error processing row {index}: {ner_result['error']}")
    ner_df = pd.DataFrame(ner_results)
    return ner_df

# file_path = 'data/raw.csv'
# df = pd.read_csv(file_path)
# df['case_text_summary'] = "Summary: " + df['case_summary'].astype(str) + "\n" + "Case: " + df['case_text'].astype(str)
# df_copy = df.copy()
# df_copy['case_id'] = (df_copy['court_title'] + '_' + 
#                       df_copy['sicil_number'].astype(str) + '_' + 
#                       df_copy['case_number']).str.lower()
# sample = df_copy.iloc[85:86]
# ner_df = process_dataframe_ner(sample, 'case_text_summary', 'court_title', 'case_id')
# text = "Summary: Andon v. Panayot’un evini Apostol v. Yorgaki’ye sattığı\nCase: ['Pendik karyesinde mütemekkin Andon v. Panayot nâm zimmî meclis-i şer‘-i hatîrde işbu râfi‘ü’r-rakīm Apostol v. Yorgaki zimmî muvâcehesinde ikrâr ve takrîr-i kelâm edüp akd-i âti’z-zikrin sudûruna değin yedimde mülk-i mevrûsum olup karye-i mezbûrede vâki‘ etrâf-ı erba‘ası Kiryakoz? zimmî avlusu ve Pandezi zimmî mahzeni ve Simidçi Vasil zimmî Menzili ve tarîk-i âm ile mahdûd fevkānî iki bâb oda ve tahtında avlu ve bi’r-i mâ ve sokak kapısını müştemil mülk menzilimi bi-cümleti’t-tevâbi‘ ve’l-levâhık ve kâffeti’l-hukūk ve’l-merâfık tarafeynden îcâb ve kabûlü hâvî şurût-ı müfsideden ârî bey‘-i bâtt-ı sahîh-i şer‘î ile müşterî-i merkūm Apostol zimmîye yüz yirmi guruşa bey‘ u teslîm ve temlîk eylediğimde ol dahi ber-vech-i muharrer iştirâ ve tesellüm ve temellük ve kabz u kabûl eyledikden sonra semeni olan mezkûr yüz yirmi guruşu müşterî-i merkūm yedinden bi’t-tamâm ve’l-kemâl ahz ü kabz edüp menzil-i mahdûd-ı mezkûrun gabn ü tağrîrine müte‘allıka âmme-i da‘vâdan zimmetini ibrâ-i âmm-ı kātı‘ü’n-nizâ‘la ibrâ ve iskāt eyledim fîmâ-ba‘d menzil-i mahdûd-ı mezkûrda kat‘â ve kātıbeten benim alâka ve medhalim kalmayup müşterî-i mersûmun mülk-i müşterâsı ve hakk-ı sırfı olmuşdur keyfe mâ yeşâ ve yehtâr mutasarrıf olsun dedikde gıbbe’t-tasdîkı’ş-şer‘î mâ-vaka‘a bi’t-taleb ketb olundu.', 'Tahrîren fi’l-yevmi’s-sâmin ve’l-ışrîn min-Şa‘bâni’l-mu‘azzam li-sene ihdâ ve semânîn ve mi’ete ve elf.', 'Şuhûdü’l-hâl: Osman Ağa, Ömer Yazıcı, Ahmed Efendi, Nalband Halil Beşe.', '...?, Aci Sava, ...? Kostandi v. Aci Kanilo zimmî?']"
# automate_ner(text, "ay", 100)