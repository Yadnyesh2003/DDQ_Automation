import pandas as pd
from datetime import datetime

def calculate(df_orderload: pd.DataFrame, cleaned_dfs: dict) -> pd.DataFrame:
    df = df_orderload
    df_previous_orderload = cleaned_dfs["FOL_Data"]
    # df_master_ccr = cleaned_dfs["Master_CCR"]

    # df_previous_orderload = pd.merge(
    #     df_previous_orderload,
    #     df_master_ccr[['ccr_id', 'ccr_code']],
    #     on='ccr_id',
    #     how='left'
    # )
    df = df.merge(
        df_previous_orderload[['ccr_id', 'fol_in_days']],
        on='ccr_id',
        how='left'
    )
    df['fol_on_ccr'] = 0.0
    last_fol = {}
    for idx, row in df.iterrows():
        ccr_id = row['ccr_id']
        if ccr_id not in last_fol:
            # First occurrence: use fol_in_days * 24 * 60
            initial_fol = (row['fol_in_days'] or 0) * 24 * 60
            df.at[idx, 'fol_on_ccr'] = initial_fol
            last_fol[ccr_id] = initial_fol
        else:
            # Subsequent occurrence: add previous fol + previous orderload
            new_fol = last_fol[ccr_id] + row['orderload_on_ccr']
            df.at[idx, 'fol_on_ccr'] = new_fol
            last_fol[ccr_id] = new_fol
    df[['max_orderload_on_ccr', 'max_fol_on_ccr']] = df.groupby(['order_id', 'item_id'])[['orderload_on_ccr', 'fol_on_ccr']].transform('max')
    # Find rows with max fol_on_ccr per group
    max_fol_rows = df.loc[
        df.groupby(['order_id', 'item_id'])['fol_on_ccr'].idxmax(),
        ['order_id', 'item_id', 'ccr_code']
    ].rename(columns={'ccr_code': 'ccr_code_with_max_fol'})

    # idx = df.groupby(['order_id', 'item_id'])['fol_on_ccr'].idxmax()
    # valid_idx = [i for i in idx if pd.notna(i) and i in df.index]
    # max_fol_rows = (
    #     df.loc[valid_idx, ['order_id', 'item_id', 'ccr_code']]
    #     .rename(columns={'ccr_code': 'ccr_code_with_max_fol'})
    # )

    # Merge it to original dataframe
    df = df.merge(max_fol_rows, on=['order_id', 'item_id'], how='left')
    return df



    # PLACE THIS CODE IN THE ABOVE FUNCTION IF PREVIOUS FOL IS NOT PROVIDED
    # CODE 1
    # ccr_load = {}
    # fol_on_ccr = []
    # for _, row in df.iterrows():
    #     code = row['ccr_code']
    #     fol = ccr_load.get(code, 0)
    #     fol_on_ccr.append(fol)
    #     # Update cumulative load
    #     ccr_load[code] = fol + row['orderload_on_ccr']
    # df['fol_on_ccr'] = fol_on_ccr

    # CODE 2 (SAME AS CODE 1, BUT BETTER APPROACH)
    # df['fol_on_ccr'] = (
    #     df.groupby('ccr_code')['orderload_on_ccr']
    #     .transform(lambda x: x.shift().expanding().sum().fillna(0))
    # )



def calculate_due_date(df_fol_data: pd.DataFrame, cleaned_dfs: dict, env: dict) -> pd.DataFrame:
    df = df_fol_data
    today_str = pd.to_datetime(env["TODAY"])
    # Sort residual buffer by desc to get max residual_buffer on top for the groupby
    df = df.groupby(['order_id', 'item_id', 'ccr_code_with_max_fol']).apply(
        lambda group: group.sort_values('residual_buffer', ascending=False)
    )
    df = df.reset_index(drop=True)
    # Keeps only the first occurence of the duplicate values. Hence sorting was essiential. To keep max_residual_buffer
    df = df.drop_duplicates(subset=['order_id', 'item_id', 'ccr_code_with_max_fol'])
    df_item_buffer = cleaned_dfs["Item_Master_Plant"]
    df = pd.merge(
        df,
        df_item_buffer[["org_code", "item_number", "procurement_buffer","production_buffer"]],
        how="left",
        left_on = ["plant_code", "item_id"],
        right_on=["org_code", "item_number"]
        )
    total_days = (
        df['max_orderload_on_ccr'] / (24*60) +
        df['max_fol_on_ccr'] / (24*60) +
        (df['residual_buffer'] * df['production_buffer']) + 
        1
    )
    df['fol_duedate'] = (today_str + pd.to_timedelta(total_days, unit='D')).dt.strftime('%d-%m-%Y')
    df = df[["order_id", "item_id", "cr_duedate", "seq", "residual_buffer", "procurement_buffer", "production_buffer", "ccr_code_with_max_fol", "fol_duedate"]]
    df = df.sort_values('seq', ascending=True)
    return df