def is_valid(row):
    """
    Universal validation for:
    - Spotify CSV
    - CoinGecko API JSON
    """

    # 1️⃣ Spotify CSV validation
    if "track_id" in row and "track_popularity" in row:
        try:
            int(row["track_popularity"])
        except:
            return False
        return True

    # 2️⃣ CoinGecko JSON validation
    if "id" in row and "current_price" in row:
        try:
            float(row["current_price"])
        except:
            return False
        return True

    # 3️⃣ If no known schema matched
    return False
