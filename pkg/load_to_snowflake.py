from pkg.params import *
import datetime


class SnowflakeLoad:

    def __init__(self, sc):
        self.sc = sc
        self.cursor = self.sc.cursor()

    def convert_to_json(self, data, file_name) -> None:
        """
        Convert the given data to JSON format and save it to a file.

        Args:
            data (pandas.DataFrame): The data to be converted to JSON.
        """
        data.to_json(
            file_name, orient="records"
        )  # Converting to json by keeping orient as records to fetch separate rows.

    def get_latest_file(self):

        self.cursor.execute(f"ls @{json_stage}")

        result = self.cursor.fetchall()
        file_names = [row[0] for row in result]

        latest_file = max(
            file_names,
            key=lambda file_name: int(
                file_name.split("/")[1].split("_")[2]
                + file_name.split("/")[1].split("_")[3]
            ),
        )

        print(latest_file)
        return latest_file

    def put_to_stage(self) -> None:
        """
        Put file from local storage system to snowflake stage.
        """
        new_file_name = "clickstream_load_" + str(
            datetime.datetime.now().strftime(r"%Y%m%d_%H%M%S")
        )
        print(new_file_name)
        self.cursor.execute(
            f"""
                PUT 'file://{clickstream_json_file}' @{json_stage}/{new_file_name}
            """
        )

    def copy_to_landing(self, latest_file) -> None:
        self.cursor.execute(
            f"""COPY INTO {landing_table}
                FROM @{latest_file}
                FILE_FORMAT = (FORMAT_NAME = {json_file_format})
                ON_ERROR = 'continue'
                FORCE = true;"""
        )

    def insert_to_final(self) -> int:
        self.cursor.execute(
            f"""
                INSERT INTO {final_table} (date_id, hour_id, payload)
                SELECT
                    src:date_id::number,
                    src:hour_id::number,
                    src AS payload
                FROM {landing_table} AS src
                WHERE payload NOT IN (
                    SELECT payload
                    FROM {final_table}
                );
            """
        )

        self.cursor.execute(
            f""" SELECT COUNT(*) FROM {final_table}
            """
        )

        row_count = self.cursor.fetchone()[0]
        return row_count

    def delete_from_landing(self):
        # Clean up previous data
        self.cursor.execute(
            f"""
                DELETE FROM {landing_table}; 
            """
        )
