from src.file_utils import export_dataframe
from src.calculations.bom_utils import calculate_with_bom, calculate_without_bom
import pandas as pd

def calculate(cleaned_dfs: dict, df_sequenced: pd.DataFrame, config: dict, env: dict) -> pd.DataFrame:
    df_master_item = cleaned_dfs["Master_Item"]
    df_master_ccr = cleaned_dfs["Master_CCR"]
    df_map_ccr_itemtype = cleaned_dfs["Map_CCR_ItemType"]
    df_sequenced = cleaned_dfs["Sequenced_Orders"]
    df_master_plant = cleaned_dfs["Master_Plant"]

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

    df_orderload = pd.merge(
        df_sequenced,
        df_item_ccr_touchtime_mapping,
        how="left",
        left_on="item_id",
        right_on="item_code"
    )

    df_orderload_invalid = df_orderload[df_orderload["item_code"].isnull()]
    export_dataframe(df_orderload_invalid, env["OUTPUT_PATH"], "df_orderload_invalid.csv")

    df_orderload = df_orderload.merge(
        df_master_plant[["plant_id", "plant_code", "plant_name"]],
        on="plant_id",
        how="left"
    )

    df_orderload = df_orderload.dropna(subset=["item_code"]).reset_index(drop=True)
    # df_orderload = df_orderload.drop(columns=["ismt_sch_end_date", "item_id"], errors="ignore")

    if config.get("bom_active", False):
        df_orderload = calculate_with_bom(df_orderload, df_item_ccr_touchtime_mapping, cleaned_dfs, config, env)
    else:
        df_orderload = calculate_without_bom(df_orderload, df_item_ccr_touchtime_mapping, cleaned_dfs, config, env)
    
    return df_orderload