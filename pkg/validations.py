from .utils import log


class ValidateData:
    def __init__(self, sc):
        self.sc = sc
        self.cursor = self.sc.cursor()

    def validate_null(self):
        """
        The function is to validate if all the null values have been standardized. i.e
        There should be no empty strings or invalid missing values.
        """
        log.info("Validating null values")
        self.cursor.execute(
            """
            SELECT COUNT(*) as count 
            FROM clickstream_final_data a, LATERAL FLATTEN(input => payload) b
            WHERE b.value::string   = 'NULL' or b.value::string   = 'NA' or b.value::string   = 'N/A' or b.value::string   = 'NaN' or b.value::string   = 'nan' or b.value::string = '' or b.value::string = ' ';
            """
        )

        count_res = self.cursor.fetchall()
        if count_res[0][0] == 0:
            log.info(
                "Null validation successful.No empty strings or invalid missing values found.\n"
            )
        else:
            log.info(
                "Null validation failed.Empty strings or invalid missing values detected.\n"
            )

    def validate_zipcode(self):
        """
        This function validates if all zipcodes have been standardized to 5 digit format
        and invalid zipcodes have been converted to null
        """
        log.info("Validating zipcode")
        self.cursor.execute(
            """
            SELECT
                CASE
                    WHEN LENGTH(payload:geo_zip::string) = 5 AND payload:geo_zip::string REGEXP '^[0-9]+$' THEN 1
                    ELSE 0
                END AS is_valid,
                COUNT(*) AS count
            FROM
                clickstream_final_data
            WHERE payload:geo_zip::string IS NOT NULL 
            GROUP BY
                is_valid;
            """
        )

        result_set = self.cursor.fetchall()

        has_invalid_rows = False  # Flag to check if result has invalid zip code count.

        for row in result_set:
            is_valid = row[0]
            if is_valid == 0:
                has_invalid_rows = True
                break

        if not has_invalid_rows:
            log.info("Validation successful. All zipcodes are 5 digits.\n")
        else:
            log.info("Validation failed. Invalid zipcodes found.\n")

    def validate_date_id(self):
        """
        Validating Date id column to check if it has been populated in proper format
        For Date id - YYYYMMDD
        """
        log.info("Validating Date id column")
        self.cursor.execute(
            """
            SELECT TO_CHAR(payload:date_time::TIMESTAMP, 'YYYYMMDD') AS date_id_sf, date_id_sf=date_id as date_id_validation from clickstream_final_data
            where date_id_validation = FALSE;
            """
        )

        rows_count = self.cursor.rowcount
        if rows_count == 0:
            log.info("Date Id Validation Successful\n")

        else:
            log.info("Date id did not pass validation\n")

    def validate_hour_id(self):
        """
        Validating Hour id column to check if it has been populated in prper format
        For Hour id - HH0000
        """
        log.info("Validating hour id column")
        self.cursor.execute(
            """
            SELECT RPAD(TO_CHAR(payload:date_time::TIMESTAMP, 'HH24'), 6, '0') AS hour_id_sf,
            hour_id_sf = hour_id AS hour_id_validation
            FROM clickstream_final_data
            WHERE hour_id_validation = FALSE;

            """
        )
        rows_count = self.cursor.rowcount
        if rows_count == 0:
            log.info("Hour id validation successful\n")

        else:
            log.info("Did not pass validation for hour id.\n")

    def validate_snake_case(self):
        """
        This function validates if all the column names inside payload are in snake case format matching with regular expression.
        """
        log.info("Validating snake_case")
        self.cursor.execute(
            """
                            SELECT CASE
                                    WHEN COUNT(*) = 0 THEN 1
                                    ELSE 0
                                    END AS snake_case_validation
                            FROM clickstream_final_data,
                                    LATERAL FLATTEN(input => OBJECT_KEYS(PARSE_JSON(payload))) f
                            WHERE NOT REGEXP_LIKE(f.value, '^[a-z][a-z0-9_]*$');
                            """
        )

        result = self.cursor.fetchone()[0]
        if result == 1:
            log.info("Snake case validation successful\n")

        else:
            log.info("Did not pass validation for snake case.\n")
