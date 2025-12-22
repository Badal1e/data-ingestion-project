REQUIRED_FIELDS = [
    "track_id",
    "track_name",
    "artist_name",
    "track_popularity"
]

def is_valid(row):
    for field in REQUIRED_FIELDS:
        if not row.get(field):
            return False

    try:
        int(row["track_popularity"])
    except:
        return False

    return True