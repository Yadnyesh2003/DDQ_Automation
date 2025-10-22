import pandas as pd
from datetime import datetime

def calculate(df_fol_duedate: pd.DataFrame, env:dict) -> pd.DataFrame:
    df = df_fol_duedate.copy()
    today_str = pd.to_datetime(env["TODAY"])
    total_days = (
        df['procurement_buffer']+
        df['production_buffer']
    )
    df['buffer_duedate'] = (today_str + pd.to_timedelta(total_days, unit='D')).dt.strftime('%d-%m-%Y')
    return df