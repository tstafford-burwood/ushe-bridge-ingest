# import io
import os

import pandas as pd

from dga_dataproc_package.dataframe_factories.dataframe_readers.BaseDataframeReader import (
    BaseDataframeReader,
)

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


class ExcelDataframeReader(BaseDataframeReader):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def read_dataframe_from_file(self, file_path: str):
        # df = pd.read_excel(f"{file_path}", dtype=object, na_filter=False)
        excel_df = pd.read_excel(
            f"{file_path}",
            # dtype={"U_INST": int, "U_YEAR": int, "U_RPT_NUM": int},
            na_filter=False,
            engine="openpyxl",
        )

        # df = self.spark.createDataFrame(excel_df.astype(str), self.schema)
        df = self.spark.createDataFrame(excel_df, self.schema)

        if not self.schema:
            raise Exception("A schema must be set before reading a dataframe.")

        # return self.spark.createDataFrame(df, self.schema)
        return df
