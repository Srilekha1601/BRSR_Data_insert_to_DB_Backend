
import pandas as pd
def conditon_on_df(df):

    condition_1 = (df["Xform Flag"] == "Y") & (df["XFrom Transformation"] == "/10000000")
    condition_2 = (df["Xform Flag"] == "Y") & (df["XFrom Transformation"] == "only year")
    condition_3 = (df["Xform Flag"] == "Y") & (df["XFrom Transformation"] == "*100")

    df.loc[condition_1, "Extracted Value"] = (
        df.loc[condition_1, "Extracted Value"].astype(float) / 10_000_000
    )

    df.loc[condition_2, "Extracted Value"] = pd.to_datetime(
        df.loc[condition_2, "Extracted Value"], errors="coerce"
    ).dt.year

    df.loc[condition_3, "Extracted Value"] = (
        df.loc[condition_3, "Extracted Value"].astype(float) * 100
    )
    
    return df