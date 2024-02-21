import pandas as pd
import numpy as np


class DataframeOperations:

    def __init__(self, data) -> None:
        self.data = data

    def standardize(self) -> None:
        """
        All standardizations are applied here
        """

        def standardize_null():
            missing_values = ["", " ", "NA", "N/A", "nan", "NaN", "NULL"]
            self.data.replace(missing_values, np.nan, inplace=True)

        def standardize_zip_code():
            """
            Zipcode column had inconsistent formats. It is converted to standard 5 digit format.

            valid zip codes are converted to 5 digit format - by slicing if greater than 5 digit or appending 0's at beginning if less than 5 digit.
            Invalid zip codes are replaced to null.
            """

            # Clean the zip code column by replacing invalid zipcodes with np.nan
            pattern = r"^\d+$"
            invalid_zip_codes = ~self.data["geo_zip"].str.contains(
                pattern, regex=True, na=False
            )
            self.data.loc[invalid_zip_codes, "geo_zip"] = np.NaN

            # Standardize the remaining zip code to 5 digit zipcode.
            def standardize_zip(zip_code):
                if pd.isna(zip_code):
                    return zip_code

                elif len(zip_code) < 5:
                    return zip_code.zfill(5)

                elif len(zip_code) >= 5:
                    return zip_code[:5]
                else:
                    return zip_code

            self.data["geo_zip"] = self.data["geo_zip"].apply(
                standardize_zip
            )  # Apply the function to all values in the column

        standardize_null()
        standardize_zip_code()

    def transformations(self) -> None:

        def create_date_id():
            self.data["date_id"] = pd.to_datetime(self.data["date_time"]).dt.strftime(
                r"%Y%m%d"
            )  # Create date id by extracting date format from date_time column by using datetime functions

        def create_hour_id():
            self.data["hour_id"] = (
                pd.to_datetime(self.data["date_time"])
                .dt.strftime("%H")
                .apply(lambda x: x.lstrip("0") or "0")
                + "0000"
            )  # Create hour id by extracting date format from date_time column by using datetime functions

        create_date_id()
        create_hour_id()
