import pandas as pd

def calculate(df_buffer_duedate: pd.DataFrame) -> pd.DataFrame:
    df = df_buffer_duedate.copy()
    for col in ['cr_duedate', 'fol_duedate', 'buffer_duedate']:
        df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
    max_per_column = df.groupby(['order_id', 'item_id'])[['cr_duedate', 'fol_duedate', 'buffer_duedate']].transform('max')
    df['due_date'] = max_per_column.max(axis=1)
    df['due_date'] = df['due_date'].dt.strftime('%d-%m-%Y')
    return df
