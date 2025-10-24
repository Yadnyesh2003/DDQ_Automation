from src.file_utils import export_dataframe
from src.calculations.bom_utils import calculate_with_bom, calculate_without_bom
import pandas as pd

def calculate(cleaned_dfs: dict, df_sequenced: pd.DataFrame, config: dict, env: dict) -> pd.DataFrame:
    df_master_item = cleaned_dfs["Master_Item"]
    df_master_ccr = cleaned_dfs["Master_CCR"]
    df_map_ccr_itemtype = cleaned_dfs["Map_CCR_ItemType"]
    df_sequenced = cleaned_dfs["Sequenced_Orders"]
    df_master_plant = cleaned_dfs["Master_Plant"]
    df_route_details = cleaned_dfs["Route_Details"]
    df_orderdata = cleaned_dfs["OrderData"]

    df_join_mi_mcit = df_master_item.merge(
        df_map_ccr_itemtype,
        on="item_type_id",
        how="inner"
    )
    df_join_mi_mcit_mc = df_join_mi_mcit.merge(
        df_master_ccr,
        on="ccr_id",
        how="inner"
    )
    df_item_ccr_touchtime_mapping = df_join_mi_mcit_mc[[
            "item_code",
            "ccr_code",
            "ccr_name",
            "ccr_id",
            "touch_time_in_mins",
            "capacity_per_day",
            "working_hours_per_day",
            "schedule_horizon",
            "fol_horizon",
            "residual_buffer"
    ]]
    export_dataframe(df_item_ccr_touchtime_mapping, env["INTERMEDIATE_PATH"], "df_item_ccr_touchtime_mapping.csv")


    df_joined = (
        df_orderdata
        .merge(df_route_details, on='route_id', how='inner', suffixes=('_od', '_rd'))
        .merge(df_master_ccr, left_on='ccr_id_rd', right_on='ccr_id', how='inner')
        [['order_id', 'item_id', 'ccr_code', 'route_id', 'ccr_id']]
        .sort_values('order_id')
    )
    export_dataframe(df_joined, env["INTERMEDIATE_PATH"], "df_joined.csv")

    df_orderload = pd.merge(
        df_sequenced,
        df_joined[["order_id", "ccr_code", "route_id", "ccr_id"]],
        on='order_id',
        how='left'
    )
    export_dataframe(df_orderload, env["INTERMEDIATE_PATH"], "df_orderload_with_ccr_code_only.csv")

    df_orderload = pd.merge(
        df_orderload,
        df_master_ccr[[
            # "ccr_code",
            # "ccr_name",
            "ccr_id",
            "capacity_per_day",
            "working_hours_per_day",
            "schedule_horizon",
            "fol_horizon",
            "residual_buffer"
            ]],
        on='ccr_id'
    )
    export_dataframe(df_orderload, env["INTERMEDIATE_PATH"], "df_orderload_with_master_ccr.csv")

    df_orderload = pd.merge(
        df_orderload,
        df_master_item[['item_code', 'item_type_id']],
        left_on='item_id',
        right_on='item_code',
        how='left'
    )
    export_dataframe(df_orderload, env["INTERMEDIATE_PATH"], "df_orderload_with_master_item.csv")

    df_orderload = pd.merge(
        df_orderload,
        df_map_ccr_itemtype[['item_type_id', 'touch_time_in_mins']],
        on='item_type_id',
        how='left'
    )
    export_dataframe(df_orderload, env["INTERMEDIATE_PATH"], "df_orderload_with_touch_time.csv")

    



    # df_orderload = pd.merge(
    #     df_orderload,
    #     df_item_ccr_touchtime_mapping[['item_code', 'ccr_id', 'touch_time_in_mins','capacity_per_day', 'working_hours_per_day', 'schedule_horizon','fol_horizon', 'residual_buffer']],
    #     how="left",
    #     left_on="item_id",
    #     right_on="item_code"
    # )
    # export_dataframe(df_orderload, env["INTERMEDIATE_PATH"], "df_orderload_with_Null_itemcodes.csv")
    # df_orderload_invalid = df_orderload[df_orderload["item_code"].isnull()]
    # export_dataframe(df_orderload_invalid, env["OUTPUT_PATH"], "df_orderload_invalid.csv")

    df_orderload = df_orderload.merge(
        df_master_plant[["plant_id", "plant_code", "plant_name"]],
        on="plant_id",
        how="left"
    )

    # df_orderload = df_orderload.dropna(subset=["item_code"]).reset_index(drop=True)
    # df_orderload = df_orderload.drop(columns=["ismt_sch_end_date", "item_id"], errors="ignore")

    if config.get("bom_active", False):
        df_orderload = calculate_with_bom(df_orderload, df_item_ccr_touchtime_mapping, cleaned_dfs, config, env)
    else:
        df_orderload = calculate_without_bom(df_orderload, df_item_ccr_touchtime_mapping, cleaned_dfs, config, env)
    
    return df_orderload