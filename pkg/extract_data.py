import gzip
import shutil
import pandas as pd

from .params import *


class ExtractData:
    """
    This class has functions required to extract the source data.
    """

    def unzip_data_file(self, filename, targetfolder) -> None:
        """
        Uncompresses a gzip file and saves it to a target folder.

        Args:
            filename (str): The path to the gzip file.
            targetfolder (str): The path to the target folder where the uncompressed file will be saved.
        """

        with gzip.open(filename, "rb") as file_in:
            with open(targetfolder, "wb") as file_out:
                shutil.copyfileobj(file_in, file_out)

    def map_col_headers(self, col_file, data_file) -> pd.DataFrame:
        """
        Reads column headers from a file and maps them to a DataFrame.

        Returns:
            pandas.DataFrame: The DataFrame with column headers mapped from the file.
        """
        colDf = pd.read_csv(col_file, sep="\t")  # Read column headers file
        colList = list(colDf.columns)

        data_df = pd.read_csv(
            data_file,
            sep="\t",
            header=None,
            names=colList,
            low_memory=False,
        )  # Read data file and pass the list of column headers extracted from column headers file

        return data_df
