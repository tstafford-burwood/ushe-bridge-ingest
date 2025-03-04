import json

from google.cloud import pubsub_v1
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from dga_dataproc_package.utils.gcs_pubsub_helpers import (
    publish_to_topic_with_dictionary_payload,
)
from dga_dataproc_package.utils.gcs_reader_helpers import (
    read_inst_id_from_gcs_input_blob_path,
    read_pk_from_gcs_input_blob_path,
)
from dga_dataproc_package.verify.reference_configs.gcs_reference_filepaths import (
    gcs_reference_filepaths,
)
from dga_dataproc_package.verify.reference_configs.local_reference_filepaths import (
    local_reference_filepaths,
)


class BaseQualityChecker:
    def __init__(
        self,
        df: DataFrame,
        project_id: str,
        pubsub_topic_name: str,
        input_blob_path: str,
        ref_file_bucket_id: str,
        validation_year: str = "2024",
        publisher_client=None,
        use_local_filepaths: bool = False,
    ):
        self.spark = self.get_spark_session()
        self.df = df
        self.publisher_client = (
            pubsub_v1.PublisherClient()
            if publisher_client is None
            else publisher_client
        )
        self.validation_year = validation_year
        self.project_id = project_id
        self.ref_file_bucket_id = "dev-0-dataproc-d565"
        # self.ref_file_bucket_id = ref_file_bucket_id
        self.institution_id = read_inst_id_from_gcs_input_blob_path(input_blob_path)
        self.input_event_pk = read_pk_from_gcs_input_blob_path(input_blob_path)
        self.pubsub_topic_name = pubsub_topic_name
        self.error_dataframes = []
        self.use_local_filepaths = use_local_filepaths
        self.set_reference_dataframes(use_local_filepaths)

    @staticmethod
    def get_reference_dataframes(
        spark, ref_file_bucket_id: str, use_local_filepaths: bool = False
    ):
        reference_filepaths = (
            local_reference_filepaths
            if use_local_filepaths
            else gcs_reference_filepaths
        )
        return {
            key: BaseQualityChecker.get_reference_file_dataframe(
                spark, value, ref_file_bucket_id, use_local_filepaths
            )
            for key, value in reference_filepaths.items()
        }

    @staticmethod
    def get_reference_file_dataframe(
        spark,
        reference_file_path: str,
        ref_file_bucket_id: str = "dev-0-dataproc-d565",
        use_local_filepaths: bool = False,
    ) -> DataFrame:
        """
        Reads reference file JSON from GCS and returns a dataframe
        """
        if not use_local_filepaths:
            # reference_file_path = f"gs://{ref_file_bucket_id}/{reference_file_path}"
            final_reference_path = f"gs://{ref_file_bucket_id}/{reference_file_path}"
        else:
            final_reference_path = reference_file_path

        return spark.read.options(header=True, sep="|").csv(final_reference_path)

    def set_reference_dataframes(self, use_local_filepaths: bool):
        self.reference_dataframes = BaseQualityChecker.get_reference_dataframes(
            self.spark,
            "dev-0-dataproc-d565",
            use_local_filepaths
            # self.spark, self.ref_file_bucket_id, use_local_filepaths
        )

    def get_spark_session(self):
        return SparkSession.builder.getOrCreate()

    def push_error_dataframe_if_errors_found(
        self, error_code: str, error_df: tuple[str, DataFrame]
    ):
        """
        Helper that appends an error dataframe to the error_dataframes list
        """
        ########theo test
        # error_df = error_df.fillna(
        #     {col: 0.0 for col in error_df.columns[error_df.dtypes.eq(float)]}
        # )
        error_df = error_df.na.fill(0)
        error_df = error_df.na.fill("")
        # error_df = error_df.na.fill(None)
        ########
        error_tuple = (error_code, error_df)
        if error_tuple[1].count() > 0:
            self.error_dataframes.append(error_tuple)

    def check_missing_by_columns(
        self, error_code: str, cols_to_select: list[str], cols_to_check: list[str]
    ) -> DataFrame:
        """
        Helper that checks for missing values in the dataframe from a list of columns
        """
        error_df = self.df.select(cols_to_select)
        for col in cols_to_check:
            error_df = error_df.where(f"{col} IS NULL OR {col} = ''")
        self.push_error_dataframe_if_errors_found(error_code, error_df)
        return error_df

    def check_duplicates(self, error_code: str, columns: list[str]) -> DataFrame:
        """
        Helper that checks for duplicates in the dataframe given a list of columns to group by
        """
        error_df = (
            self.df.select(columns)
            .groupby(columns)
            .count()
            .where("count > 1")
            .sort("count", ascending=False)
        )
        self.push_error_dataframe_if_errors_found(error_code, error_df)
        return error_df

    def check_invalid_by_column(
        self,
        error_code: str,
        cols_to_select: list[str],
        col_to_check: str,
        valid_values: list[str],
    ):
        """
        Check for invalid values in the dataframe
        """
        error_df = self.df.select(cols_to_select).filter(
            ~self.df[col_to_check].isin(valid_values)
            & ~self.df[col_to_check].isNull()
            & ~(self.df[col_to_check] == "")
        )
        self.push_error_dataframe_if_errors_found(error_code, error_df)
        return error_df

    def check_duplicate_keys(
        self,
        error_code: str,
        columns: list[str],
        new_column: str,
        concat_columns: str,
        cols_groupby: list[str],
    ) -> DataFrame:
        """
        Helper that checks for duplicates in the dataframe given a list of columns to group by
        """
        error_df = (
            self.df.select(columns)
            .withColumn(new_column, concat_columns)
            .groupby(cols_groupby)
            .count()
            .where("count > 1")
            .sort("count", ascending=False)
        )
        self.push_error_dataframe_if_errors_found(error_code, error_df)
        return error_df

    def select_cols_with_where_clause(
        self, error_code: str, columns: list[str], where_clause: str
    ) -> DataFrame:
        """
        Helper that checks for blanks in the dataframe column specified in the where clause
        """
        error_df = self.df.select(columns).where(where_clause)
        self.push_error_dataframe_if_errors_found(error_code, error_df)
        return error_df

    def select_col_with_count_col_renamed(
        self, error_code: str, columns: list[str], rename: str
    ) -> DataFrame:
        """
        Helper that checks for blanks in the dataframe column specified in the where clause
        """
        error_df = (
            self.df.select(columns)
            .groupby(columns)
            .count()
            .withColumnRenamed("count", rename)
        )
        self.push_error_dataframe_if_errors_found(error_code, error_df)
        return error_df

    def get_valid_value_list(
        self, ref_df_key: str, column_name: str, where_clause: str = None
    ) -> list[str]:
        """
        Helper that returns a list of valid values for a given column from a reference dataframe
        """
        df = self.reference_dataframes[ref_df_key]
        if where_clause:
            df = df.where(where_clause)
        return df.select(column_name).distinct().rdd.flatMap(lambda x: x).collect()

    def get_distinct_inst_id_list(self) -> list[str]:
        inst_ref_df = self.reference_dataframes["institution"]
        return (
            inst_ref_df.where(inst_ref_df["Inactive"] != "Y")
            .select("I_ID")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_grade_code_list(self) -> list[str]:
        grade_code_ref_df = self.reference_dataframes["grade"]
        return (
            grade_code_ref_df.where(
                (~grade_code_ref_df.Grade_Code.isNull())
                & (grade_code_ref_df["Inactive"] == "N")
            )
            .select("Grade_Code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_enrl_obj_list(self) -> list[str]:
        inst_ref_df = self.reference_dataframes["u_enrl_obj"]
        return (
            inst_ref_df.where(inst_ref_df["Inactive"] != "Y")
            .select("U_Enrl_Obj")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_ipeds_code(self) -> list[str]:
        ipeds_ref_df = self.reference_dataframes["ipeds_awards"]
        return (
            ipeds_ref_df.where(ipeds_ref_df.Inactive == "N")
            .select("IPEDS_CODE")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_country_no_list(self) -> list[str]:
        country_ref_df = self.reference_dataframes["county"]
        return (
            country_ref_df.select("COUNTY_NO")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_county_no_list(self) -> list[str]:
        county_ref_df = self.reference_dataframes["county"]
        return (
            county_ref_df.select("COUNTY_NO")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_inst_banner(self) -> list[str]:
        banner_ref_df = self.reference_dataframes["inst"]
        return (
            banner_ref_df.where(
                (banner_ref_df.Inactive != "Y") & (banner_ref_df.I_ID == "44")
            )
            .select("I_Banner")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    # Per Theo: Needs to include dynamic inst variable.

    def get_distinct_banner_id_list(self) -> list[str]:
        banner_id_ref_df = self.reference_dataframes["inst"]
        return (
            banner_id_ref_df.join(self.df, banner_id_ref_df.I_ID == self.df.S_INST)
            .select("I_Banner")
            .where(
                (banner_id_ref_df.Inactive != "Y")
                & (self.df.S_INST == banner_id_ref_df.I_ID)
            )
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_banner_list(self) -> list[str]:
        banner_id_ref_df = self.reference_dataframes["inst"]
        return (
            banner_id_ref_df.where(banner_id_ref_df.Inactive == "N")
            .select("I_Banner")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_cip_list(self) -> list[str]:
        cip_ref_df = self.reference_dataframes["cip"]
        return (
            cip_ref_df.where(
                (cip_ref_df.Inactive != "Y")
                & (~cip_ref_df.CIP_Code.isin("", "999999", "000000"))
            )
            .select("CIP_Code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
    
    def get_distinct_cip_code_list(self) -> list[str]:
        cip_ref_df = self.reference_dataframes["cip"]
        return (
            cip_ref_df.where(
                (cip_ref_df.Inactive != "Y")
            )
            .select("CIP_Code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_deg_type_list(self) -> list[str]:
        deg_type_ref_df = self.reference_dataframes["degree_type"]
        return (
            deg_type_ref_df.where(deg_type_ref_df.Inactive == "N")
            .select("G_Deg_Type")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_degree_type_list(self) -> list[str]:
        deg_type_ref_df = self.reference_dataframes["degree_type"]
        return (
            deg_type_ref_df.where(deg_type_ref_df.Inactive == "N")
            .select("G_DEG_TYPE")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_highschools_code(self) -> list[str]:
        highschools_df = self.reference_dataframes["highschools"]
        return (
            highschools_df.where(highschools_df.Inactive == "N")
            .select("HS_ACT_Code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_inst_code_list(self) -> list[str]:
        inst_ref_df = self.reference_dataframes["inst"]
        return (
            inst_ref_df.where(inst_ref_df.Inactive == "N")
            .select("I_ID")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_cr_type_code_list(self) -> list[str]:
        cr_type_ref_df = self.reference_dataframes["credit_hr_type"]
        return (
            cr_type_ref_df.where(cr_type_ref_df.Inactive == "N")
            .select("CR_Type_Code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_hs_act_code_list(self) -> list[str]:
        hs_act_code_ref_df = self.reference_dataframes["highschools"]
        return (
            hs_act_code_ref_df.where(hs_act_code_ref_df.HS_State == "UT")
            .select("HS_ACT_Code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_active_hs_act_code_list(self) -> list[str]:
        hs_act_code_ref_df = self.reference_dataframes["highschools"]
        return (
            hs_act_code_ref_df.where(
                (hs_act_code_ref_df.HS_State == "UT")
                & (hs_act_code_ref_df.Inactive == "N")
                & (hs_act_code_ref_df.HS_School_Type.isin("B", "C", "D"))
            )
            .select("HS_ACT_Code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_deg_type_df(self) -> DataFrame:
        deg_type_df = self.reference_dataframes["degree_type"]
        return deg_type_df

    def get_distinct_county_list(self) -> list[str]:
        county_ref_df = self.reference_dataframes["county"]
        return (
            county_ref_df.select(F.concat(F.lit("UT"), "COUNTY_NO"))
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_grade_level_list(self) -> list[str]:
        grade_level_ref_df = self.reference_dataframes["u_grade_level"]
        return (
            grade_level_ref_df.where(grade_level_ref_df.Inactive == "N")
            .select("u_grade_level")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_delivery_method_list(self) -> list[str]:
        delivery_method_ref_df = self.reference_dataframes["delivery_method"]
        return (
            delivery_method_ref_df.where(delivery_method_ref_df.Inactive != "Y")
            .select("d_method_code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_instruct_type_list(self) -> list[str]:
        instruct_type_ref_df = self.reference_dataframes["instruct_type"]
        return (
            instruct_type_ref_df.where(instruct_type_ref_df.Inactive != "Y")
            .select("Instruct_Type_Code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_budget_code_list(self) -> list[str]:
        budget_code_ref_df = self.reference_dataframes["budget_code"]
        return (
            budget_code_ref_df.where(budget_code_ref_df.Inactive != "Y")
            .select("budget_code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_site_type_code_list(self) -> list[str]:
        site_type_code_ref_df = self.reference_dataframes["site_type"]
        return (
            site_type_code_ref_df.where(
                (site_type_code_ref_df.S_Type_Space_Utilize == "Y")
                & (site_type_code_ref_df.Inactive == "N")
            )
            .select("S_Type_Code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_distinct_site_type_list(self) -> list[str]:
        site_type_code_ref_df = self.reference_dataframes["site_type"]
        return (
            site_type_code_ref_df.where(site_type_code_ref_df.Inactive == "N")
            .select("S_Type_Code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def get_rooms_reference_df(self) -> DataFrame:
        rooms_ref_df = self.reference_dataframes["rooms"]
        return rooms_ref_df

    def get_inst_id_list(self) -> DataFrame:
        inst_ref_df = self.reference_dataframes["inst"]
        return (
            inst_ref_df.where(inst_ref_df["Inactive"] == "N")
            .select("I_ID", "I_Inst_Type", "Inactive")
            .distinct()
        )

    def get_finaid_type_list(self) -> DataFrame:
        finaid_type_ref_df = self.reference_dataframes["finaid_type"]
        return (
            finaid_type_ref_df.where(finaid_type_ref_df["Inactive"] == "N")
            .select("F_Type_Code", "F_Type_Group")
            .distinct()
        )

    def get_finaid_type_list_active(self) -> DataFrame:
        finaid_type_ref_df = self.reference_dataframes["finaid_type"]
        return (
            finaid_type_ref_df.where(finaid_type_ref_df["Inactive"] != "N")
            .select("F_Type_Code")
            .distinct()
        )

    def get_rooms_use_code_reference_df(self) -> DataFrame:
        rooms_use_code_df = self.reference_dataframes["rooms_use_code"]
        return rooms_use_code_df

    def get_inst_reference_df(self) -> DataFrame:
        inst_ref_df = self.reference_dataframes["inst"]
        return inst_ref_df

    def get_distinct_rooms_use_code(self) -> list[str]:
        rooms_use_code_df = self.reference_dataframes["rooms_use_code"]
        return (
            rooms_use_code_df.where(rooms_use_code_df.Inactive == "N")
            .select("R_Use_Code")
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )

    def quality_check(self):
        """
        This function should be overridden by the child class to perform quality checks
        """
        self.execute_quality_check()

    def execute_quality_check(self):
        """
        Performs all quality checks and publishes to pubsub with error payload if found.
        This function should be called at the end of the quality check function in the child class.
        """
        if len(self.error_dataframes) > 0:
            for error_tuple in self.error_dataframes:
                # This loop could be outside of the conditional and the logic would be the same,
                # but it's here to make the code explicit about the fact that
                # the pubsub error messages should only go out when there are errors
                dataframe_payload = [
                    json.loads(row) for row in error_tuple[1].toJSON().collect()
                ]
                dict_payload = {
                    "code": error_tuple[0],
                    "pk": self.input_event_pk,
                    "data": dataframe_payload,
                }
                publish_to_topic_with_dictionary_payload(
                    self.project_id,
                    self.pubsub_topic_name,
                    dict_payload,
                    self.publisher_client,
                )
