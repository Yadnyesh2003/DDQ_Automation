import os
import pandas as pd
from src.config_loader import load_config
from src.file_utils import read_input_files, export_dataframe
from src.preprocessing.data_cleaner import clean_dataframes
from src.sequencing import base_sequencer
from src.calculations import (
    duedate_calculator, orderload_calculator, fol_calculator,
    buffer_duedate_calculator
)

def run_pipeline():
    # config, client_config, env = load_config()
    config, env = load_config()

    # Step 1: Load Input Files
    input_dfs = read_input_files(env["DATA_PATH"], config["required_files"])
    cleaned_dfs = clean_dataframes(input_dfs)

    # Step 2: Sequence Orders (client-specific)
    # sequencer = base_sequencer.load_client_sequencer(env["CLIENT_NAME"], client_config)
    # df_sequenced = sequencer.sequence_orders(cleaned_dfs)
    df_sequenced = cleaned_dfs["Sequenced_Orders"]
    export_dataframe(df_sequenced, env["INTERMEDIATE_PATH"], "df_sequenced.csv")

    # Step 3: Calculate Order Loads
    df_orderload = orderload_calculator.calculate(cleaned_dfs, df_sequenced, config, env)
    export_dataframe(df_orderload, env["INTERMEDIATE_PATH"], "df_orderload.csv")

    # Step 4: Calculate FOL + Due Dates
    df_fol_data = fol_calculator.calculate(df_orderload, cleaned_dfs)
    export_dataframe(df_fol_data, env["INTERMEDIATE_PATH"], "df_fol_data.csv")
    df_fol_duedate = fol_calculator.calculate_due_date(df_fol_data, cleaned_dfs, env)
    
    export_dataframe(df_fol_duedate, env["INTERMEDIATE_PATH"], "df_fol_duedate.csv")

    # Step 5: Calculate Buffer Due Date
    # df_buffer_duedate = buffer_duedate_calculator.calculate(cleaned_dfs, df_sequenced)
    df_buffer_duedate = buffer_duedate_calculator.calculate(df_fol_duedate, env)
    export_dataframe(df_buffer_duedate, env["INTERMEDIATE_PATH"], "df_buffer_duedate.csv")

    # Step 6: Merge and Compute Final Due Date
    # df_duedate = duedate_calculator.calculate(df_fol_duedate, df_buffer_duedate)
    df_duedate = duedate_calculator.calculate(df_buffer_duedate)
    export_dataframe(df_duedate, env["OUTPUT_PATH"], "df_duedate.csv")

    print("✅ Pipeline executed successfully! Output stored in data/output/df_duedate.csv")
