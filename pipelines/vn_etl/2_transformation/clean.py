import pymongo
import re


def no_accent_vietnamese(s):
    s = re.sub(r'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
    s = re.sub(r'[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', s)
    s = re.sub(r'[èéẹẻẽêềếệểễ]', 'e', s)
    s = re.sub(r'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', s)
    s = re.sub(r'[òóọỏõôồốộổỗơờớợởỡ]', 'o', s)
    s = re.sub(r'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', s)
    s = re.sub(r'[ìíịỉĩ]', 'i', s)
    s = re.sub(r'[ÌÍỊỈĨ]', 'I', s)
    s = re.sub(r'[ùúụủũưừứựửữ]', 'u', s)
    s = re.sub(r'[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', s)
    s = re.sub(r'[ỳýỵỷỹ]', 'y', s)
    s = re.sub(r'[ỲÝỴỶỸ]', 'Y', s)
    s = re.sub(r'[Đ]', 'D', s)
    s = re.sub(r'[đ]', 'd', s)

    marks_list = [u'\u0300', u'\u0301', u'\u0302', u'\u0303', u'\u0306',u'\u0309', u'\u0323']

    for mark in marks_list:
        s = s.replace(mark, '')

    return s

def str2int(string):
    # example: 12.324 is converted to int
    # split it to two part then concatinating them
    num = -1
    if '.' in string:
        num = int(''.join(string.split('.')))
    else: 
        num = int(string)
    return num
    
# extract cases of provinces/cities
def extract_by_province(text):
    provinces = no_accent_vietnamese(text).split('),')
    result = []
    for prov in provinces:

        covid = '' # store a string that contains name and cases of a provinces
        name = ''
        num = -1

        # in case it can not extract
        try:
            covid = re.findall('\s*[A-Z].+\s[A-Z].+\s\(\d+\.*\d*\s*', prov)[0]
            name = re.findall('[A-Z][a-z]+\s[A-Z][a-z]+\s',covid)[0].strip(' ')
            num = str2int(re.findall('\d+\.*\d*', covid)[0])
        except:
                result.append({'[error]': prov})

        # name can be incomplete
        if 'Ho' in name or  'Chi' in name or 'Minh' in name:
            name = 'hochiminhcity'
        name = name.lower().replace(' ','')
        result.append({
            'province': name,
            'new_cases': num
        })
    return result

from variables import MONGO_URI, MONGO_DATABASE

def clean_data(scraped_collection_name: str, clean_collection_name: str):
    client = pymongo.MongoClient(MONGO_URI)
    db = client[MONGO_DATABASE]
    items = db[scraped_collection_name].find()
    cleaned_items = []
    # print(type(items))
    for item in items:
        if not isinstance(item['content'], str):
            continue
    #     assert type(item['content']) is str, 'content is not a string'
        item['date'] = re.search('\d{2}\/\d{2}\/\d{4}', item['date']).group()
        item['content'] = extract_by_province(item['content'])
        cleaned_items.append(item)
    
    # drop the clean collection and write a new one
    db[clean_collection_name].drop()
    db.create_collection(clean_collection_name)
    if len(cleaned_items) > 0: # must be an non-empty list
        db[clean_collection_name].insert_many(cleaned_items)
    
    client.close()

from variables import SCRAPED_COLLECTION_NAME, CLEANED_COLLECTION_NAME
clean_data(SCRAPED_COLLECTION_NAME, CLEANED_COLLECTION_NAME)

