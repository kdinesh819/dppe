# deduper.py

import dedupe
from dedupe import variables, StaticDedupe
import pandas as pd
import os
from scoring import calculate_scores

def run_dedupe(df, dedupe_fields, output_file='DeDupCD_sorted_output.xlsx'):
    """
    Perform bulk deduplication on the entire DataFrame `df`.
    Trains (or loads) a Dedupe model, partitions into clusters,
    annotates with IDs, confidences and categories, saves Excel, and returns the annotated df.
    """
    # 1) Build cleaned dict of existing data
    data_d = {
        str(idx): {
            col: str(row[col]) if pd.notnull(row[col]) else ''
            for col in dedupe_fields
        }
        for idx, row in df.iterrows()
    }

    # 2) Define fields for dedupe
    fields = [variables.String(field) for field in dedupe_fields]

    # 3) Load existing model or train a new one
    if os.path.exists('dedupe_settings'):
        with open('dedupe_settings', 'rb') as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        deduper = dedupe.Gazetteer(fields)
        deduper.prepare_training(data_d)
        dedupe.console_label(deduper)
        deduper.train()

        with open('dedupe_settings', 'wb') as f:
            deduper.write_settings(f)
        with open('training.json', 'w') as f:
            deduper.write_training(f)

    # 4) Partition into clusters
    clustered_dupes = deduper.partition(data_d)

    # 5) Map each record to its cluster and score
    cluster_membership = {}
    for cid, (records, scores) in enumerate(clustered_dupes):
        for rid, score in zip(records, scores):
            cluster_membership[rid] = {
                'Cluster ID': cid,
                'Confidence': round(score, 2)
            }

    # 6) Annotate the DataFrame
    df['Cluster ID'] = df.index.map(lambda x: cluster_membership.get(str(x), {}).get('Cluster ID', -1))
    df['Confidence'] = df.index.map(lambda x: cluster_membership.get(str(x), {}).get('Confidence', 0.0))
    df['Is Duplicate'] = 'Not Duplicate'
    df['Matching Score %'], df['Dedupe Category'] = zip(*df['Confidence'].apply(calculate_scores))

    # 7) Mark Originals vs Duplicates within each cluster
    clustered = df[df['Cluster ID'] != -1].copy()
    clustered.sort_values(['Cluster ID', 'Confidence'], ascending=[True, False], inplace=True)
    for cid, group in clustered.groupby('Cluster ID'):
        top_idx = group.index[0]
        df.loc[group.index, 'Is Duplicate'] = 'Duplicate'
        df.loc[top_idx, 'Is Duplicate'] = 'Original'

    # 8) Sort and save to Excel
    df['Sort Order'] = df['Is Duplicate'].map({'Original': 0, 'Duplicate': 1, 'Not Duplicate': 2})
    df_sorted = df.sort_values(['Cluster ID', 'Sort Order', 'Confidence']).drop(columns='Sort Order')
    df_sorted.to_excel(output_file, index=False)

    return df_sorted

def find_duplicates_for_record(df, input_record, dedupe_fields, threshold=0.5):
    """
    Inject input_record into the dataset, run partition at `threshold`,
    and return any peers clustered with the input along with total case count.
    """
    # 1) Build cleaned dict of existing data
    data_d = {
        str(idx): {
            col: str(row[col]) if pd.notnull(row[col]) else ''
            for col in dedupe_fields
        }
        for idx, row in df.iterrows()
    }

    # 2) Clean and insert the incoming record under a special key
    input_key = '__INPUT__'
    data_d[input_key] = {
        col: str(input_record.get(col, ''))
        for col in dedupe_fields
    }

    # 3) Load trained model
    if not os.path.exists('dedupe_settings'):
        raise FileNotFoundError("dedupe_settings not found. Train via run_dedupe first.")
    with open('dedupe_settings', 'rb') as f:
        deduper = StaticDedupe(f)

    # 4) Partition everything at the provided threshold
    clusters = deduper.partition(data_d, threshold)

    # 5) Collect matches in the same cluster as our input
    matches = []
    for record_ids, scores in clusters:
        if input_key in record_ids and len(record_ids) > 1:
            for rid, score in zip(record_ids, scores):
                if rid != input_key:
                    matching_pct, category = calculate_scores(score)
                    matches.append({
                        "matching_record_id": int(rid),
                        "dedupe_Score": float(matching_pct),
                        "confidence_percent": float(round(score * 100, 2)),
                        "dedupe_category": category,
                        "record_data": data_d[rid]
                    })
            break

    # 6) Sort descending by confidence
    matches.sort(key=lambda x: x["confidence_percent"], reverse=True)

    # 7) Build totals
    total_cases = len(data_d) - 1
    matching_count = len(matches)

    # now compute:
    matching_score_percent = round(matching_count / total_cases * 100, 2)

    # 7) Return matches with total case count
    return {
        "total_cases": total_cases,
        "matching_count": matching_count,
        "matching_score_percent": matching_score_percent,
        "matches": matches
    }
    return result
