import configparser
import re
import pickle
from unidecode import unidecode
from db_fetcher import fetch_data

FIELDS = [
    'CENTER', 'NODE', 'NAME_FIRST', 'EMAIL',
    'MOB_NUMBER', 'ADDRESS', 'POI_DOC_ID', 'POA_DOC_ID'
]
GAZETTEER_SETTINGS_PATH = 'gazetteer_settings'
TRAINING_JSON = 'gazetteer_training.json'
DATA_PICKLE_PATH = 'data_d.pkl'

def preProcess(value):
    if not value or str(value).strip() == '':
        return 'missing'
    value = unidecode(str(value))
    value = re.sub(r"[\n:,/'\"-]", " ", value)
    value = re.sub(r"\s+", " ", value).strip().lower()
    return value if value else 'missing'

config = configparser.ConfigParser()
config.read('config.ini')
db_cfg = config['database']
DEFAULT_DB = {
    'db_type': db_cfg.get('db_type'),
    'host': db_cfg.get('host'),
    'port': str(db_cfg.get('port')),
    'user': db_cfg.get('user'),
    'password': db_cfg.get('password'),
    'database': db_cfg.get('database'),
    'query': db_cfg.get('query'),
}

df = fetch_data(**DEFAULT_DB)
if df is None or df.empty:
    raise Exception("No data fetched from the database.")

data_d = {}
for idx, row in df.iterrows():
    record = {col: preProcess(row[col]) for col in FIELDS}
    if any(val != 'missing' for val in record.values()):
        data_d[str(idx)] = record

print(f" {len(data_d)} usable records for dedupe training.")

with open(DATA_PICKLE_PATH, 'wb') as f:
    pickle.dump(data_d, f)
print(f"Saved dedupe data to '{DATA_PICKLE_PATH}'")
