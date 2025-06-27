def calculate_scores(confidence: float):
    confidence_percent = confidence * 100

    if confidence_percent >= 90:
        category = 'Duplicate'
    elif confidence_percent >= 70:
        category = 'Moderate'
    else:
        category = 'Tentative'

    return round(confidence_percent, 2), category
