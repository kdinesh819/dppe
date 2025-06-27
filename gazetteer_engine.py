import pickle
import re
from unidecode import unidecode
from dedupe import StaticGazetteer
from scoring import calculate_scores
FIELDS = ['CENTER', 'NODE', 'NAME_FIRST', 'EMAIL', 'MOB_NUMBER', 'ADDRESS', 'POI_DOC_ID', 'POA_DOC_ID']

def preProcess(value):
    if not value or str(value).strip() == '':
        return 'missing'
    value = unidecode(str(value))
    value = re.sub(r"[\n:,/'\"-]", " ", value)
    value = re.sub(r"\s+", " ", value).strip().lower()
    return value if value else 'missing'

def load_gazetteer(model_path, data_path):
    with open(model_path, 'rb') as f:
        gazetteer = StaticGazetteer(f, num_cores=4)
    with open(data_path, 'rb') as f:
        data_d = pickle.load(f)
    if not isinstance(data_d, dict):
        raise ValueError("❌ Loaded data is not a dictionary.")
    for key, value in data_d.items():
        if not isinstance(value, dict):
            raise ValueError(f"❌ Record {key} is not a dictionary: {value}")
    gazetteer.index(data_d)
    return gazetteer, data_d

def print_block_keys(gazetteer, input_record):
    if not hasattr(gazetteer, 'fingerprinter') or gazetteer.fingerprinter is None:
        return []
    keys = []
    for (block_key, record_id) in gazetteer.fingerprinter([('__INPUT__', input_record)], target=False):
        keys.append({"block_key": block_key, "record_id": record_id})
    return keys

def try_print_blocking_predicates(gazetteer):
    found = False

    # FINGERPRINTER PREDICATES
    if hasattr(gazetteer, 'fingerprinter'):
        fingerprinter = getattr(gazetteer, 'fingerprinter')
        if hasattr(fingerprinter, 'predicates'):
            print("\n--- FINGERPRINTER PREDICATES (gazetteer.fingerprinter.predicates) ---")
            preds = fingerprinter.predicates
            if isinstance(preds, dict):
                for field, pred_list in preds.items():
                    print(f"{field}:")
                    for idx, pred in enumerate(pred_list):
                        print(f"  [{idx}] {pred}")
            elif isinstance(preds, (list, tuple)):
                for idx, pred in enumerate(preds):
                    print(f"  [{idx}] {pred}")
            else:
                print(f"Unknown predicate structure: {type(preds)}")
            found = True

    # PREDICATES ON THE GAZETTEER OBJECT ITSELF
    if hasattr(gazetteer, 'predicates'):
        preds = getattr(gazetteer, 'predicates')
        print("\n--- PREDICATES (gazetteer.predicates) ---")
        if isinstance(preds, dict):
            for field, pred_list in preds.items():
                print(f"{field}:")
                for idx, pred in enumerate(pred_list):
                    print(f"  [{idx}] {pred}")
        elif isinstance(preds, (list, tuple)):
            for idx, pred in enumerate(preds):
                print(f"  [{idx}] {pred}")
        else:
            print(f"Unknown predicate structure: {type(preds)}")
        found = True

    if not found:
        print("\n❌ No blocking predicates found in the model attributes (fingerprinter, predicates).")
        print("You may need to retrain or check dedupe version/model serialization.")


def match_record(input_record, gazetteer, data_d, fields=FIELDS, threshold=0.0, n_matches=15, use_fast_match=True):
    if not isinstance(input_record, dict):
        raise TypeError("Input record must be a dictionary.")

    cleaned_input = {k: preProcess(input_record.get(k, "")) for k in fields}
    wrapped_input = {"__INPUT__": cleaned_input}
    results = []

    try:
        if use_fast_match:
            # Fast method using gazetteer.search (output shape: [("input_id", [(match_id, score), ...]), ...])
            matches = gazetteer.search(wrapped_input, threshold=threshold, n_matches=n_matches)
            top_matches = []
            for input_id, match_list in matches:
                if str(input_id) != '__INPUT__':
                    continue
                for match_tuple in match_list:
                    try:
                        match_id_raw, raw_score = match_tuple
                        match_id = str(match_id_raw)
                        score = float(raw_score)
                    except Exception as e:
                        raise ValueError(f"❌ Could not extract match_id/score from: {match_tuple}") from e

                    match_record = data_d.get(match_id, {})
                    percent, category = calculate_scores(score)
                    results.append({
                        "matching_record_id": int(match_id),
                        "dedupe_Score": percent,
                        "confidence_percent": percent,
                        "dedupe_category": category,
                        "record_data": match_record
                    })

        else:
            # Manual explainable method (output shape: [((input_id, match_id), score), ...])
            blocks = gazetteer.blocks(wrapped_input)
            block_list = list(blocks)
            scored_blocks = gazetteer.score(iter(block_list))
            matches = gazetteer.many_to_n(scored_blocks, threshold=threshold, n_matches=n_matches)
            top_matches = []
            for match_group in matches:
                match_group = match_group.tolist() if hasattr(match_group, "tolist") else match_group
                top_matches.extend(match_group)

            for match in top_matches:
                try:
                    (input_id, match_id), score = match
                except Exception:
                    continue
                if str(input_id) != '__INPUT__':
                    continue

                match_record = data_d.get(str(match_id), {})
                percent, category = calculate_scores(score)
                results.append({
                    "matching_record_id": int(match_id),
                    "dedupe_Score": percent,
                    "confidence_percent": percent,
                    "dedupe_category": category,
                    "record_data": match_record
                })

    except Exception:
        return {"matching_count": 0, "matching_score_percent": 0, "matches": []}

    return {
        "matching_count": len(results),
        "matching_score_percent": round(len(results) / len(data_d) * 100, 2),
        "matches": results
    }
