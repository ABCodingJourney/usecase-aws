# Snowflake connection configs

# user = "adminAB"
# password = "AdminWelcome@1"
# account_identifier = "lwujfwh-wr47734"
# database = "SNOWFLAKETRAINING"
# schema = "LANDING_ZONE"

"""File names and constant variables"""

# File names for unzipping

filename = "C:\\Users\\ananya.bhat\\PythonPractice\\Pandas\\data\\Clickstream\\01-hgprod_20231217-160000.tsv.gz"
targetfolder = (
    "C:\\Users\\ananya.bhat\\PythonPractice\\Phase1_UseCase\\data\\clickstream-data.tsv"
)

# File names for mapping

col_file = "C:/Users/ananya.bhat/PythonPractice/Phase1_UseCase/data/column_headers.tsv"
data_file = (
    "C:/Users/ananya.bhat/PythonPractice/Phase1_UseCase/data/clickstreamdata.tsv"
)

# Json loading variables

json_file_format = "json_file_format"
json_stage = "my_json_stage"
clickstream_json_file = (
    "C:/Users/ananya.bhat/PythonPractice/Phase1_UseCase/data/clickstream.json"
)


# Table names

landing_table = "clickstream_landing_table"
final_table = "clickstream_final_table"

# slack channels

alert_channel = "clickstream-slack-alerts"
notifs_channel = "clickstream-slack-notifs"
