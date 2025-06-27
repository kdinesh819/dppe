from flask import Flask, request, jsonify
import datetime
import time
from gazetteer_engine import (
    load_gazetteer,
    match_record,
    FIELDS,
    preProcess,
    print_block_keys,
    try_print_blocking_predicates
)

MODEL_PATH = 'gazetteer_settings'
DATA_PATH = 'data_d.pkl'
GAZETTEER = None
DATA_D = None

def get_gazetteer_data():
    global GAZETTEER, DATA_D
    if GAZETTEER is None or DATA_D is None:
        GAZETTEER, DATA_D = load_gazetteer(MODEL_PATH, DATA_PATH)
    return GAZETTEER, DATA_D

app = Flask(__name__)
app.config['SECRET_KEY'] = 'dedupe'

def error_response(message, error_type="BadRequest", status_code=400):
    return jsonify({
        "status": "error",
        "error": {
            "type": error_type,
            "message": message
        },
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z"
    }), status_code

REQUIRED_FIELDS = [
    'CENTER', 'NODE', 'NAME_FIRST', 'EMAIL',
    'MOB_NUMBER', 'ADDRESS', 'POA_DOC_ID'
]
OPTIONAL_FIELDS = ['POI_DOC_ID']

def validate_record_fields(record, required_fields=REQUIRED_FIELDS):
    missing = [
        field for field in required_fields
        if field not in record or record[field] in (None, '', 'missing')
    ]
    return missing

@app.route('/api/v1/dedupe', methods=['POST'])
def dedupe_api():
    t0 = time.time()
    payload = request.get_json(force=True)

    if payload.get('mode') != 'record-match':
        return error_response("Only 'record-match' mode is supported.", "ValidationError", 400)

    try:
        threshold = float(payload.get('threshold', 0.2))
        assert 0 <= threshold <= 1
    except (ValueError, AssertionError):
        return error_response("Invalid threshold; must be a float between 0 and 1", "ValidationError", 400)

    input_record = payload.get('record')
    input_records = payload.get('records')
    gazetteer, data_d = get_gazetteer_data()

    response_data = {
        "status": "success",
        "mode": "record-match",
        "timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "processing_time_seconds": None,
        "request": {"threshold": threshold}
    }

    if input_record:
        missing = validate_record_fields(input_record)
        if missing:
            return error_response(
                f"Missing required fields: {', '.join(missing)} in 'record'", "ValidationError", 400
            )
        #cleaned_input = {k: preProcess(input_record.get(k, "")) for k in FIELDS}
        #block_keys = print_block_keys(gazetteer, cleaned_input)
        #print("Block keys for this input:", block_keys)
        #try_print_blocking_predicates(gazetteer)

        matches = match_record(input_record, gazetteer, data_d, threshold=threshold)
        response_data.update({
            "input_record": input_record,
            "possible_duplicates": matches,
            "duplicates_found": bool(matches["matching_count"]),
            #"block_keys": block_keys
        })

    elif input_records:
        bulk_results = []
        for idx, rec in enumerate(input_records):
            missing = validate_record_fields(rec)
            if missing:
                return error_response(
                    f"Record #{idx+1} is missing required fields: {', '.join(missing)}",
                    "ValidationError",
                    400
                )
            #cleaned_input = {k: preProcess(rec.get(k, "")) for k in FIELDS}
            #block_keys = print_block_keys(gazetteer, cleaned_input)
            #print("Block keys for input:", block_keys)
            #try_print_blocking_predicates(gazetteer)

            result = match_record(rec, gazetteer, data_d, threshold=threshold)
            bulk_results.append({
                "input_record": rec,
                "possible_duplicates": result,
                "duplicates_found": bool(result["matching_count"]),
                #"block_keys": block_keys
            })
        response_data["bulk_results"] = bulk_results
        response_data["total_records_processed"] = len(input_records)

    else:
        return error_response("Missing 'record' or 'records'", "ValidationError", 400)

    response_data["processing_time_seconds"] = round(time.time() - t0, 3)
    return jsonify(response_data), 200

@app.route('/api/v1/blocking_info', methods=['POST'])
def blocking_info():
    payload = request.get_json(force=True)
    input_record = payload.get('record')
    if not input_record:
        return error_response("Missing 'record' in request", "ValidationError", 400)
    gazetteer, _ = get_gazetteer_data()
    cleaned_input = {k: preProcess(input_record.get(k, "")) for k in FIELDS}
    block_keys = print_block_keys(gazetteer, cleaned_input)
    return jsonify({
        "input": cleaned_input,
        "block_keys": block_keys
    }), 200

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=7002, debug=False, use_reloader=False)
