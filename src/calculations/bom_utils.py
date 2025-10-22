import pandas as pd
from src.file_utils import export_dataframe

def calculate_with_bom( df_orderload: pd.DataFrame, df_item_ccr_touchtime_mapping: pd.DataFrame, cleaned_dfs: dict, config: dict, env: dict) -> pd.DataFrame:
    df_order_allocation = cleaned_dfs["Batchwise_OrderAllocation"]
    df_component_allocation = cleaned_dfs["Batchwise_ComponentAllocation"]
    df_merged = pd.merge(
        df_orderload,
        df_order_allocation[["order_id", "item_code", "plant_code", "order_remaining"]],
        on=["order_id", "item_code", "plant_code"],
        how="left"
    )
    df_merged["orderload_on_ccr_due_to_fg"] = df_merged["order_remaining"] / df_merged["touch_time_in_mins"]
    # df_merged = df_merged.drop(columns=["order_remaining"])
    df_orderload = df_merged
    # SFG load calculation
    # Merge SFGs with touchtime mapping
    df_sfg = pd.merge(
        df_component_allocation,
        df_item_ccr_touchtime_mapping,
        on="item_code",
        how="inner"
    )
    # Compute orderload for SFGs
    df_sfg = df_sfg[df_sfg["touch_time_in_mins"] != 0]
    df_sfg["orderload_sfg"] = df_sfg["order_remaining"] / df_sfg["touch_time_in_mins"]
    export_dataframe(df_sfg, env["OUTPUT_PATH"], "df_sfg.csv")

    # Group by order_id and ccr_code to get total SFG orderload per CCR
    sfg_grouped = df_sfg.groupby(["order_id", "ccr_code", "plant_code"], as_index=False)["orderload_sfg"].sum()
    sfg_grouped = sfg_grouped.rename(columns={"orderload_sfg": "orderload_on_ccr_due_to_sfg"})
    export_dataframe(sfg_grouped, env["OUTPUT_PATH"], "sfg_grouped.csv")
    # Merge back with df_merged
    df_result = pd.merge(
        df_merged,
        sfg_grouped,
        on=["order_id", "ccr_code", "plant_code"],
        how="left"
    )
    # Fill NaN (in case no SFGs for the CCR) with 0
    df_result["orderload_on_ccr_due_to_sfg"] = df_result["orderload_on_ccr_due_to_sfg"].fillna(0)
    # Compute total orderload
    df_result["orderload_on_ccr"] = (
        df_result["orderload_on_ccr_due_to_fg"] + df_result["orderload_on_ccr_due_to_sfg"]
    )
    df_orderload = df_result
    # df_orderload = check_parent_child_route(df_orderload, df_component_allocation, df_item_ccr_touchtime_mapping, env)
    return df_orderload


# def check_parent_child_route(df_orderload, df_component_allocation, df_item_ccr_touchtime_mapping, env):
#     # Step 1: Create a set of parent CCRs for each order_id and item_code
#     parent_ccrs = df_orderload.groupby(['order_id', 'item_code'])['ccr_code'].apply(set).reset_index()
#     # Step 2: Extract order_id and item_code from component allocation
#     sfgs = df_component_allocation[['order_id', 'item_code']]
#     # Step 3: Merge SFGs with CCR mappings to get each SFG's CCRs
#     sfgs_with_ccr = pd.merge(
#         sfgs,
#         df_item_ccr_touchtime_mapping[['item_code', 'ccr_code']],
#         on='item_code',
#         how='left'
#     )
#     # Step 4: Drop rows where ccr_code is null
#     sfgs_with_ccr = sfgs_with_ccr.dropna(subset=['ccr_code'])
#     # Convert to string if ccr_code might be numeric (to ensure comparison works)
#     sfgs_with_ccr['ccr_code'] = sfgs_with_ccr['ccr_code'].astype(str)
#     # Step 5: Create a mapping of available CCRs per order_id in sfgs_with_ccr
#     child_ccrs = sfgs_with_ccr.groupby('order_id')['ccr_code'].apply(set).reset_index()
#     child_ccrs = child_ccrs.rename(columns={'ccr_code': 'child_ccrs'})
#     # Merge parent and child CCRs for comparison
#     parent_child_merged = pd.merge(
#         parent_ccrs.groupby('order_id')['ccr_code'].apply(set).reset_index(name='parent_ccrs'),
#         child_ccrs,
#         on='order_id',
#         how='left'
#     )
#     # Step 6: Identify invalid order_ids where not all parent CCRs are in child CCRs
#     def is_valid(row):
#         return row['parent_ccrs'].issubset(row['child_ccrs'] if isinstance(row['child_ccrs'], set) else set())
#     parent_child_merged['is_valid'] = parent_child_merged.apply(is_valid, axis=1)
#     # Extract valid and invalid order_ids
#     valid_order_ids = parent_child_merged[parent_child_merged['is_valid']]['order_id']
#     invalid_order_ids = parent_child_merged[~parent_child_merged['is_valid']]['order_id']
#     # Create invalid dataframe
#     df_invalid = df_orderload[df_orderload['order_id'].isin(invalid_order_ids)]
#     # Export invalid data
#     export_dataframe(df_invalid, env["OUTPUT_PATH"], "df_invalid.csv")
#     # Return valid records only
#     df_valid = df_orderload[df_orderload['order_id'].isin(valid_order_ids)]
#     return df_valid


def check_parent_child_route(df_orderload, df_component_allocation, df_item_ccr_touchtime_mapping, env):
    # Create parent CCRs set per order_id and item_code
    parent_ccrs = (
        df_orderload
        .groupby(['order_id', 'item_code'])['ccr_code']
        .apply(set)
        .reset_index(name='parent_ccrs')
    )
    # Extract order_id and item_code from component allocation (SFGs)
    sfgs = df_component_allocation[['order_id', 'item_code']].drop_duplicates()
    # Merge SFGs with CCR mappings to get CCRs for each SFG item_code
    sfgs_with_ccr = pd.merge(
        sfgs,
        df_item_ccr_touchtime_mapping[['item_code', 'ccr_code']],
        on='item_code',
        how='left'
    )
    # Drop rows where ccr_code is null (match not found)
    sfgs_with_ccr = sfgs_with_ccr.dropna(subset=['ccr_code'])
    # Convert ccr_code to string for consistent comparison
    sfgs_with_ccr['ccr_code'] = sfgs_with_ccr['ccr_code'].astype(str)
    # Group SFGs' CCRs by order_id to get a set of CCRs per order
    sfgs_ccr_map = (
        sfgs_with_ccr
        .groupby('order_id')['ccr_code']
        .apply(set)
        .reset_index(name='sfg_ccrs')
    )
    # Merge parent CCRs with SFG CCR map
    validation_df = pd.merge(parent_ccrs, sfgs_ccr_map, on='order_id', how='left')
    # Identify invalid orders where parent CCRs are not a subset of SFG CCRs
    validation_df['is_valid'] = validation_df.apply(
        lambda row: row['parent_ccrs'].issubset(row['sfg_ccrs']) if isinstance(row['sfg_ccrs'], set) else False,
        axis=1
    )
    # Get valid and invalid order_ids
    valid_order_ids = validation_df.loc[validation_df['is_valid'], 'order_id'].unique()
    invalid_order_ids = validation_df.loc[~validation_df['is_valid'], 'order_id'].unique()
    # Separate valid and invalid orderload records
    df_valid = df_orderload[df_orderload['order_id'].isin(valid_order_ids)].copy()
    df_invalid = df_orderload[df_orderload['order_id'].isin(invalid_order_ids)].copy()
    df_invalid['remarks'] = "Parent Child route mismatch"
    export_dataframe(df_invalid, env["OUTPUT_PATH"], "df_route_mismatch.csv")
    return df_valid


def calculate_without_bom(df_orderload: pd.DataFrame, df_item_ccr_touchtime_mapping: pd.DataFrame, cleaned_dfs: dict, config: dict, env: dict) -> pd.DataFrame:
    df_orderdata = cleaned_dfs["OrderData"]
    df_merged = pd.merge(
        df_orderload,
        df_orderdata[["order_id", "item_id", "pending_ccr_qty"]],
        on=["order_id", "item_id"],
        how="left"
    )
    df_merged["orderload_on_ccr"] = df_merged["pending_ccr_qty"] / df_merged["touch_time_in_mins"]
    df_orderload = df_merged
    return df_orderload
