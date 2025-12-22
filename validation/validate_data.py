import csv

REQUIRED_FIELDS = ["track_id", "track_name", "artist_name", "track_popularity"]

def is_valid(row):
    # required must not be empty
    for field in REQUIRED_FIELDS:
        if field not in row or not row[field]:
            return False

    # track_popularity must be numeric
    try:
        int(row["track_popularity"])
    except:
        return False

    return True
def validate_csv(file_path):
    valid_records = []
    invalid_records = []

    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            if is_valid(row):
                valid_records.append(row)
            else:
                invalid_records.append(row)

    return valid_records, invalid_records
