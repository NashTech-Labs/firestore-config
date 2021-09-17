import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import csv
import pandas as pd
from pathlib import Path
import json

import sys
filename = sys.argv[1]
# Use a service account

cred = credentials.Certificate(filename)
firebase_admin.initialize_app(cred)

db = firestore.client()

# Opening JSON file and loading the data
# into the variable data

# absolute path to json file
jsonpath = Path('./sample-project/dev/vars.json')

# reading the json file
with jsonpath.open('r', encoding='utf-8') as dat_f:
    dat = json.loads(dat_f.read())

# creating the dataframe
df = pd.json_normalize(dat)

# converted a file to csv
df.to_csv('./sample-project/dev/vars.csv', encoding='utf-8', index=False)

file_path = "./sample-project/dev/vars.csv"
collection_name = "dev"

def batch_data(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


data = []
headers = []
with open(file_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for row in csv_reader:
        if line_count == 0:
            for header in row:
                headers.append(header)
            line_count += 1
        else:
            obj = {}
            for idx, item in enumerate(row):
                obj[headers[idx]] = item
            data.append(obj)
            line_count += 1
    print(f'Processed {line_count} lines.')

for batched_data in batch_data(data, 499):
    batch = db.batch()
    for data_item in batched_data:
        doc_ref = db.collection(collection_name).document()
        batch.set(doc_ref, data_item)
    batch.commit()

print('Done')