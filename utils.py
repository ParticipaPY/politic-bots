import csv
import json

# Get configuration from file
def get_config(config_file):
    with open(config_file) as f:
        config = json.loads(f.read())
    return config


# Get keywords and metadata from csv file
def parse_metadata(kfile):
    keyword = []
    k_metadata = []
    with open(kfile, 'r', encoding='utf-8') as f:
        kfile = csv.DictReader(f)
        for line in kfile:
            keyword.append(line['keyword'])
            k_metadata.append({
                'partido_politico': line['partido_politico'],
                'movimiento': line['movimiento'],
                'lider_movimiento': line['lider_movimiento'],
                'candidatura': line['candidatura']
            })
    return keyword, k_metadata