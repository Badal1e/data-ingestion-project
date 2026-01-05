import pandas as pd

def clean_dataframe(df: pd.DataFrame):
    original_df = df.copy()

    if "track_id" in df.columns and "track_popularity" in df.columns:
        required_cols = ["track_id", "track_name", "artist_name", "track_popularity"]

        df["track_popularity"] = pd.to_numeric(df["track_popularity"], errors="coerce")

        clean_df = df.dropna(subset=required_cols)
        error_df = original_df.loc[~original_df.index.isin(clean_df.index)]

        return clean_df, error_df

    if "id" in df.columns and "current_price" in df.columns:
        df["current_price"] = pd.to_numeric(df["current_price"], errors="coerce")

        clean_df = df.dropna(subset=["id", "current_price"])
        error_df = original_df.loc[~original_df.index.isin(clean_df.index)]

        return clean_df, error_df

    return pd.DataFrame(), original_df
