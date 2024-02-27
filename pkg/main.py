from pkg.extract_data import ExtractData
from pkg.transform_data import DataframeOperations
from pkg.load_to_snowflake import SnowflakeLoad
from pkg.connectivity import snowflakeConnection as sc
from pkg.validations import ValidateData
from pkg.params import *
from pkg.utils import log, send_slack_notification


class ClickstreamLoad:

    def execute(self) -> None:
        """
        All functions for pipeline execution are called here.
        """
        try:

            extract = ExtractData()  # Step 1 - Extract data

            # extract.unzip_data_file(filename, targetfolder)
            data_df = extract.map_col_headers(col_file, data_file)

            # If dataframe is not empty proceed further

            if not data_df.empty:

                # Step 2 - Transformation and Standardizations
                dfop = DataframeOperations(data_df)
                # dfop.display_data()
                dfop.standardize()
                dfop.transformations()

                # Step 3 - Load to snowflake
                snowflakeObj = SnowflakeLoad(
                    sc
                )  # Snowflake object takes snowflake connector as parameter

                snowflakeObj.convert_to_json(data_df, clickstream_json_file)
                snowflakeObj.put_to_stage()
                latest_file = snowflakeObj.get_latest_file()
                snowflakeObj.delete_from_landing()
                snowflakeObj.copy_to_landing(latest_file)
                rows_inserted = snowflakeObj.insert_to_final()

                # Step 4 - Validate data
                val = ValidateData(sc)

                val.validate_null()
                val.validate_zipcode()
                val.validate_date_id()
                val.validate_hour_id()
                val.validate_snake_case()

                # Step 5 - Send slack success notification, with {rows_inserted} rows
                send_slack_notification(
                    notifs_channel,
                    f"Clickstream data successfully extracted, transformed and loaded to final table in snowflake",
                )

            else:
                log.info("Empty dataframe - no records found.")
                send_slack_notification(
                    notifs_channel, "Empty data - no records found."
                )

        except Exception as e:
            log.exception(f"Error occured: {e}")
            send_slack_notification(alert_channel, f"Error occured: {e}")
