import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dga_dataproc_package.verify.BaseQualityChecker import BaseQualityChecker


class TestQualityChecker(BaseQualityChecker):
    def __init__(
        self,
        building_df: DataFrame,
        rooms_df: DataFrame,
        building_pk: str,
        *args,
        **kwargs,
    ):
        super().__init__(building_df, *args, **kwargs)
        self.building_df = self.df
        self.rooms_df = rooms_df
        self.building_pk = building_pk

    def b06b_duplicate_records(self) -> DataFrame:
        """
        B-06B Duplicate Records

        Returns error dataframe if duplicates are found

        """
        cols_to_check = ["B_INST", "B_NUMBER"]
        error_code = "b06b"

        return self.check_duplicates(error_code, cols_to_check)

    def b06a_blank_records(self) -> DataFrame:
        """
        B-06A Blank Records

        Returns error dataframe if blanks are found


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NAME",
            "B_NUMBER",
        ]
        error_code = "b06a"
        where_clause = "B_NUMBER IS NULL OR B_NUMBER=''"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b02a_blank_records(self) -> DataFrame:
        """
        B-02A Blank Records

        Returns error dataframe if blanks are found


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_YEAR",
        ]
        error_code = "b02a"
        where_clause = "B_LOCATION IS NULL OR B_LOCATION=''"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b03a_blank_records(self) -> DataFrame:
        """
        B-03A Blank Records

        Returns error dataframe if blanks are found


        """
        cols_to_check = [
            "B_INST",
            "B_OWNERSHIP",
            "B_NUMBER",
            "B_YEAR",
        ]
        error_code = "b03a"
        where_clause = "B_OWNERSHIP IS NULL OR B_OWNERSHIP=''"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b04a_blank_records(self) -> DataFrame:
        """
        B-04A Blank Records

        Returns error dataframe if blanks are found

        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_YEAR",
        ]
        error_code = "b04a"
        where_clause = "B_YEAR IS NULL OR B_YEAR=''"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b05a_blank_records(self) -> DataFrame:
        """
        B-05A Blank Records

        Returns error dataframe if blanks are found


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
        ]
        error_code = "b05a"
        where_clause = "B_NAME IS NULL OR B_NAME=''"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b07a_blank_records(self) -> DataFrame:
        """
        B-07A Blank Records

        Returns error dataframe if blanks are found


        """
        cols_to_check = ["B_INST", "B_LOCATION", "B_NUMBER", "B_NAME", "B_SNAME"]
        error_code = "b07a"
        where_clause = "B_SNAME IS NULL OR B_SNAME=''"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b10a_blank_records(self) -> DataFrame:
        """
        B-10A Blank Records

        Returns error dataframe if blanks are found


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_YEAR_CONS",
            "B_AUX",
            "B_REPLACE_COST",
        ]
        error_code = "b10a"
        where_clause = "B_REPLACE_COST IS NULL AND B_OWNERSHIP = 'O' AND B_AUX='N'"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b01a_missing_institution_code(self) -> DataFrame:
        """
        B-01A missing inst code

        Returns error dataframe if inst code value not found in lookup

        """
        error_code = "b01a"

        distinct_inst_id_list = self.get_distinct_inst_id_list()

        error_df = self.df.select("B_INST", "B_LOCATION", "B_NUMBER", "B_YEAR").filter(
            ~self.df.B_INST.isin(distinct_inst_id_list)
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def b02b_missing_b_location(self) -> DataFrame:
        """
        B-02b missing location

        Returns error dataframe if location value not found in lookup

        """
        error_code = "b02b"

        distinct_buildings_loc_list = (
            self.reference_dataframes["buildings_loc"]
            .rdd.map(lambda x: x.B_Loc_Code)
            .collect()
        )

        error_df = self.df.select("B_INST", "B_LOCATION", "B_NUMBER", "B_YEAR").filter(
            (~self.df.B_LOCATION.isin(distinct_buildings_loc_list))
            & (~self.df.B_LOCATION.isNull())
            & (self.df.B_LOCATION != "")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def b03b_missing_b_ownership(self) -> DataFrame:
        """
        B-03b missing ownership

        Returns error dataframe if location value not found in lookup

        """
        error_code = "b03b"

        distinct_buildings_own_list = (
            self.reference_dataframes["buildings_own"]
            .rdd.map(lambda x: x.B_Own_Code)
            .collect()
        )

        error_df = self.df.select("B_INST", "B_OWNERSHIP", "B_NUMBER", "B_YEAR").filter(
            (~self.df.B_OWNERSHIP.isin(distinct_buildings_own_list))
            & (~self.df.B_OWNERSHIP.isNull())
            & (self.df.B_OWNERSHIP != "")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def b03d_lease_by_location(self) -> DataFrame:
        """
        B-03D Blank Records

        Returns dataframe with lease space by location


        """
        cols_to_check = [
            "B_INST",
            "B_OWNERSHIP",
            "B_LOCATION",
            "B_NUMBER",
        ]
        error_code = "b03d"
        where_clause = "B_OWNERSHIP = 'L'"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b08a_blank_records(self) -> DataFrame:
        """
        B-08A Blank Records

        Returns error dataframe if year cons is blank/null and replace cost > 3500000


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_AUX",
            "B_YEAR_CONS",
        ]
        error_code = "b08a"
        where_clause = "(B_YEAR_CONS IS NULL OR B_YEAR_CONS ='') AND B_OWNERSHIP = 'O' AND B_AUX = 'N' AND (CAST(B_REPLACE_COST AS FLOAT) >'3500000' OR B_REPLACE_COST IS NULL)"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b08b_blank_records(self) -> DataFrame:
        """
        B-08b Blank Records

        Returns error dataframe if year cons is blank/null and replace cost <= 3500000


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_AUX",
            "B_YEAR_CONS",
        ]
        error_code = "b08b"
        where_clause = "(B_YEAR_CONS IS NULL OR B_YEAR_CONS ='') AND B_OWNERSHIP = 'O' AND B_AUX = 'N' AND (CAST(B_REPLACE_COST AS FLOAT) <= '3500000' OR B_REPLACE_COST IS NULL)"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b11a_blank_records(self) -> DataFrame:
        """
        B-11a Blank Records

        Returns error dataframe if b_condition is blank/null and replace cost > 3500000


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_AUX",
            "B_CONDITION",
        ]
        error_code = "b11a"
        where_clause = "(B_CONDITION IS NULL OR B_CONDITION ='') AND B_OWNERSHIP = 'O' AND B_AUX= 'N' AND (CAST(B_REPLACE_COST AS FLOAT) > '3500000.00' OR B_REPLACE_COST IS NULL)"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b12a_blank_records(self) -> DataFrame:
        """
        B-12a Blank Records

        Returns error dataframe if b_gross is blank/null


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_AUX",
            "B_GROSS",
        ]
        error_code = "b12a"
        where_clause = (
            "(B_GROSS IS NULL OR B_GROSS ='') AND B_OWNERSHIP = 'O' AND B_AUX = 'N'"
        )

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b12b_blank_records(self) -> DataFrame:
        """
        B-12b Blank Records

        Returns error dataframe if b_gross is 0


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_AUX",
            "B_GROSS",
        ]
        error_code = "b12b"
        where_clause = "B_GROSS = 0 AND B_OWNERSHIP = 'O' AND B_AUX = 'N'"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b14a_blank_records(self) -> DataFrame:
        """
        B-14a Blank Records

        Returns error dataframe if b_rsknbr is blank/null


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_OWNERSHIP",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_AUX",
            "B_RSKNBR",
        ]
        error_code = "b14a"
        where_clause = "(B_RSKNBR IS NULL OR B_RSKNBR ='') AND B_OWNERSHIP = 'O' AND B_AUX = 'N' AND (CAST(B_REPLACE_COST AS FLOAT) > '3500000' OR B_REPLACE_COST IS NULL)"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b14b_blank_records(self) -> DataFrame:
        """
        B-14b Blank Records

        Returns error dataframe if b_rsknbr is blank/null


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_OWNERSHIP",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_AUX",
            "B_RSKNBR",
        ]
        error_code = "b14b"
        where_clause = "(B_RSKNBR IS NULL OR B_RSKNBR ='') AND B_OWNERSHIP = 'O' AND B_AUX = 'N' AND (CAST(B_REPLACE_COST AS FLOAT) <= '3500000' OR B_REPLACE_COST IS NULL)"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b15a_blank_records(self) -> DataFrame:
        """
        B-15A Blank Records

        Returns error dataframe if b_aux is blank/null


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_OWNERSHIP",
            "B_NUMBER",
            "B_NAME",
            "B_AUX",
        ]
        error_code = "b15a"
        where_clause = "B_AUX IS NULL OR B_AUX =''"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b15b_blank_records(self) -> DataFrame:
        """
        B-15B Blank Records

        Returns error dataframe if b_aux is not A or N


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_OWNERSHIP",
            "B_NUMBER",
            "B_NAME",
            "B_AUX",
        ]
        error_code = "b15b"
        where_clause = "(B_AUX IS NOT NULL OR B_AUX <>'') AND B_AUX NOT IN ('A','N')"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b15c_blank_records(self) -> DataFrame:
        """
        B-15C Blank Records

        Returns error dataframe if b_aux is A


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_OWNERSHIP",
            "B_NUMBER",
            "B_NAME",
            "B_AUX",
        ]
        error_code = "b15a"
        where_clause = "B_AUX ='A'"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b99a_duplicate_records(self) -> DataFrame:
        """
        B-99A Duplicate Records

        Returns error dataframe if duplicates are found

        """
        new_column = "B_KEY"
        concat_columns = (
            self.df.B_INST.cast("string")
            + self.df.B_YEAR.cast("string")
            + self.df.B_NUMBER.cast("string")
        )
        cols_to_check = [
            "B_INST",
            "B_YEAR",
            "B_NUMBER",
        ]
        error_code = "B99A"
        cols_groupby = [
            "B_INST",
            "B_YEAR",
            "B_NUMBER",
            "B_KEY",
        ]

        return self.check_duplicate_keys(
            error_code, cols_to_check, new_column, concat_columns, cols_groupby
        )

    def b11b_missing_b_condition(self) -> DataFrame:
        """
        B-11b missing condition

        Returns error dataframe if condition value not found in lookup

        """
        error_code = "b11b"

        distinct_buildings_cond_list = (
            self.reference_dataframes["buildings_cond"]
            .rdd.map(lambda x: x.B_Cond_Code)
            .collect()
        )

        error_df = self.df.select(
            "B_INST", "B_NUMBER", "B_NAME", "B_REPLACE_COST", "B_CONDITION"
        ).filter(
            (~self.df.B_CONDITION.isin(distinct_buildings_cond_list))
            & (~self.df.B_CONDITION.isNull())
            & (self.df.B_CONDITION != "")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def b11c_blank_records(self) -> DataFrame:
        """
        B-11C Blank Records

        Returns error dataframe if b_condition is blank/null and replace cost > 3500000


        """
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_NAME",
            "B_REPLACE_COST",
            "B_CONDITION",
        ]
        error_code = "b11c"
        where_clause = "(B_CONDITION IS NULL OR B_CONDITION ='') AND B_OWNERSHIP = 'O' AND B_AUX = 'N' AND (CAST(B_REPLACE_COST AS FLOAT) <= '3500000.00' OR B_REPLACE_COST IS NULL)"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b04b_blank_records(self) -> DataFrame:
        """
        B-04b Invalid Records

        Returns error dataframe if year is not current


        """
        year = str(datetime.datetime.now().year)
        cols_to_check = [
            "B_INST",
            "B_LOCATION",
            "B_NUMBER",
            "B_YEAR",
        ]
        error_code = "b04b"
        where_clause = f"B_YEAR <> '{year}' AND B_YEAR <> '' AND B_YEAR IS NOT NULL"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def b03c_lease_space(self) -> DataFrame:
        """
        B-03C lease space exists at institution
        Returns dataframe of lease spaces for institution

        """
        error_code = "b03c"

        error_df = self.df.select(
            "B_INST",
            F.expr(
                "CASE WHEN B_OWNERSHIP == 'L' THEN 'Lease Space Exists' ELSE 'No Lease Space Exists' END AS LEASE_SPACE"
            ),
        ).distinct()

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def b99b_missing_rooms(self) -> DataFrame:
        """
        B-99B missing rooms

        Returns error dataframe if rooms missing

        """

        cols_to_select = [
            "B_INST",
            "B_LOCATION",
            "B_OWNERSHIP",
            "B_NUMBER",
        ]

        error_code = "b99b"

        rooms_df = self.rooms_df

        cond = [
            self.df.B_INST == rooms_df.R_INST,
            self.df.B_YEAR == rooms_df.R_YEAR,
            self.df.B_NUMBER == rooms_df.R_BUILD_NUMBER,
        ]

        error_df = (
            self.df.join(rooms_df, cond)
            .select("B_INST", "B_LOCATION", "B_OWNERSHIP", "B_NUMBER")
            .where("R_NUMBER IS NULL OR R_NUMBER = ''")
            .select(cols_to_select)
        )

        print(error_df.show())

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def b12c_b_gross(self) -> DataFrame:
        """
        B-12C gross less than sum of room area in building

        Returns error dataframe if gross less than sum of room area in building

        """
        error_code = "b12c"

        room_df_aggregated_area_prorated = (
            self.rooms_df.where("R_PRORATION = 'Y'")
            .groupby("R_INST", "R_YEAR", "R_BUILD_NUMBER")
            .agg(
                F.sum(
                    F.coalesce(F.col("R_PRORATED_AREA").cast("float"), F.lit(0))
                ).alias("Sum_Prorated")
            )
        )

        room_df_aggregated_area_nonpro = (
            self.rooms_df.where("R_PRORATION = 'N'")
            .groupby("R_INST", "R_YEAR", "R_BUILD_NUMBER")
            .agg(
                F.sum(F.coalesce(F.col("R_AREA").cast("float"), F.lit(0))).alias(
                    "Sum_NonPro"
                )
            )
        )

        error_df1 = self.df.join(
            room_df_aggregated_area_prorated,
            (self.df.B_INST == room_df_aggregated_area_prorated.R_INST)
            & (self.df.B_YEAR == room_df_aggregated_area_prorated.R_YEAR)
            & (self.df.B_NUMBER == room_df_aggregated_area_prorated.R_BUILD_NUMBER),
            "left",
        ).select(
            self.df.B_INST,
            self.df.B_YEAR,
            self.df.B_NUMBER,
            self.df.B_NAME,
            self.df.B_GROSS,
            self.df.B_OWNERSHIP,
            room_df_aggregated_area_prorated.Sum_Prorated,
        )
        error_df = (
            error_df1.join(
                room_df_aggregated_area_nonpro,
                (error_df1.B_INST == room_df_aggregated_area_nonpro.R_INST)
                & (error_df1.B_YEAR == room_df_aggregated_area_nonpro.R_YEAR)
                & (error_df1.B_NUMBER == room_df_aggregated_area_nonpro.R_BUILD_NUMBER),
                "left",
            )
            .withColumn(
                "Sum_Prorated_Area", F.coalesce(F.col("Sum_Prorated"), F.lit(0))
            )
            .withColumn("Sum_NonPro_Area", F.coalesce(F.col("Sum_NonPro"), F.lit(0)))
            .withColumn(
                "Sum_Rooms_Area", F.col("Sum_Prorated_Area") + F.col("Sum_NonPro_Area")
            )
            .withColumn(
                "B_Gross_Area", F.coalesce(F.col("B_GROSS").cast("float"), F.lit(0))
            )
            .select(
                "B_INST",
                "B_NUMBER",
                "B_NAME",
                "B_Gross_Area",
                "Sum_Rooms_Area",
                "Sum_Prorated_Area",
                "Sum_NonPro_Area",
            )
            .where(
                "(IFNULL(Sum_Prorated,0) + IFNULL(Sum_NonPro,0)) > IFNULL(B_Gross_area,0) AND B_OWNERSHIP = 'O'"
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def quality_check(self):
        """
        Performs all quality checks and publishes to pubsub with error payload if found
        """

        # Quality checks will populate the self.error_dataframes list if errors are found
        self.b06b_duplicate_records()
        self.b06a_blank_records()
        self.b02a_blank_records()
        self.b03a_blank_records()
        self.b04a_blank_records()
        self.b05a_blank_records()
        self.b07a_blank_records()
        self.b10a_blank_records()
        self.b01a_missing_institution_code()
        self.b02b_missing_b_location()
        self.b03b_missing_b_ownership()
        self.b03d_lease_by_location()
        self.b08a_blank_records()
        self.b08b_blank_records()
        self.b11a_blank_records()
        self.b12a_blank_records()
        self.b12b_blank_records()
        self.b14a_blank_records()
        self.b14b_blank_records()
        self.b15a_blank_records()
        self.b15b_blank_records()
        self.b15c_blank_records()
        self.b99a_duplicate_records()
        self.b11b_missing_b_condition()
        self.b11c_blank_records()
        self.b04b_blank_records()
        self.b03c_lease_space()
        self.b99b_missing_rooms()
        self.b12c_b_gross()

        # Roll up to the base class, which will publish the errors to pubsub if self.error_dataframes is populated
        super().quality_check()
