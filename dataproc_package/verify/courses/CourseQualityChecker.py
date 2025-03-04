from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, length, substring, when

from dga_dataproc_package.verify.BaseQualityChecker import BaseQualityChecker


class CourseQualityChecker(BaseQualityChecker):
    def __init__(
        self,
        courses_df: DataFrame,
        students_df: DataFrame,
        studentcourses_df: DataFrame,
        buildings_prod_df: DataFrame,
        rooms_prod_df: DataFrame,
        courses_load_prod_df: DataFrame,
        perkins2015_prod_df: DataFrame,
        logan_perkins_prod_df: DataFrame,
        cc_master_list_prod_df: DataFrame,
        courses_pk: str,
        *args,
        **kwargs,
    ):
        super().__init__(courses_df, *args, **kwargs)
        self.courses_df = self.df
        self.studentcourses_df = studentcourses_df
        self.students_df = students_df
        self.buildings_prod_df = buildings_prod_df
        self.rooms_prod_df = rooms_prod_df
        self.courses_load_prod_df = courses_load_prod_df
        self.perkins2015_prod_df = perkins2015_prod_df
        self.logan_perkins_prod_df = logan_perkins_prod_df
        self.cc_master_list_prod_df = cc_master_list_prod_df
        self.courses_pk = courses_pk

    def c00_duplicate_records(self) -> DataFrame:
        """
        C-00 Duplicate Records

        Returns error dataframe if duplicates are found

        """
        cols_to_check = ["C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC"]
        error_code = "c00"

        return self.check_duplicates(error_code, cols_to_check)

    def c00a_duplicate_records(self) -> DataFrame:
        """
        C00a Duplicate Records

        Returns error dataframe if duplicates are found


        """
        cols_to_check = [
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_SEC",
        ]
        error_code = "c00a"

        return self.check_duplicates(error_code, cols_to_check)

    def c01_missing_institution_code(self) -> DataFrame:
        """
        C-01 Missing institution code

        Returns error dataframe if duplicates are found
        """

        error_code = "c01"

        distinct_inst_id_list = self.get_distinct_inst_id_list()

        error_df = self.df.select(
            "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC"
        ).filter(
            ~self.df.C_INST.isin(distinct_inst_id_list)
            | self.df.C_INST.isNull()
            | (self.df.C_INST == "")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c02_blank_records(self) -> DataFrame:
        """
        C-02 blank year term or extract

        Returns error dataframe if blanks are found

        """
        cols_to_check = [
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
        ]
        error_code = "c02"
        where_clause = "C_INST IS NULL OR C_YEAR IS NULL OR C_EXTRACT IS NULL"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def c04a_invalid_course_number(self) -> DataFrame:
        """
        C-04a invalid course number

        the Course Number should have four digits

        """
        cols_to_check = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
        ]
        error_code = "c04a"
        where_clause = (
            "LENGTH(C_CRS_NUM) = 3 or LENGTH(C_CRS_NUM) = 2  or LENGTH(C_CRS_NUM) = 1"
        )

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def c04b_invalid_course_number_grad(self) -> DataFrame:
        """
        C-04b invalid course number grad

        the Course Number should not be grad level courses
        """

        error_code = "c04b"

        error_df = self.df.select(
            "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN"
        ).filter(
            self.df.C_INST.isin("3679", "5220", "5221")
            & ~F.substring(self.df.C_CRS_NUM, 1, 1).isin("", "1", "2", "3", "4", "5")
            | (self.df.C_INST == "")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c04c_invalid_course_number(self) -> DataFrame:
        """
        C-04a invalid course number

        the Course Number Invalid first character

        """

        error_code = "c04c"
        error_df = self.df.select(
            "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN"
        ).filter(~F.substring(self.df.C_CRS_NUM, 0, 1).isin("8", "9"))

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c04d_invalid_course_number(self) -> DataFrame:
        """
        C-04a invalid course number

        the Course Number Invalid first character

        """

        error_code = "c04c"
        error_df = self.df.select(
            "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN"
        ).filter(~F.substring(self.df.C_CRS_NUM, 1, 4).rlike("[a-zA-Z]"))

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c05_invalid_course_section(self) -> DataFrame:
        """
        C-05 invalid course section

        the Section Number should have at least three digits

        """
        cols_to_check = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
        ]
        error_code = "c05"
        where_clause = "LENGTH(C_CRS_SEC) = 2  or LENGTH(C_CRS_SEC) = 1"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def c06a_invalid_min_credits(self) -> DataFrame:
        """
        S-06a Invalid Records

        C_MIN_Credits Invalid

        """

        error_code = "c06a"

        error_df1 = self.df.withColumn(
            "C_MIN_CREDIT_ERROR",
            F.when(self.df.C_MIN_CREDIT.cast("float") == "", "Can not be blank")
            .when(F.length(self.df.C_MIN_CREDIT) > "4", "Too many digits")
            .when(self.df.C_MIN_CREDIT.cast("float") > "25.0", "Exceeds 25.0 Credits")
            .when(self.df.C_MIN_CREDIT.cast("float") < "0", "Can not be negative")
            .when(self.df.C_MIN_CREDIT.rlike("[^.0-9]"), "Contains Non_Numeric")
            .otherwise(None),
        )
        error_df = (
            error_df1.select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_CRN",
                "C_MIN_CREDIT_ERROR",
            )
            .filter(~error_df1.C_MIN_CREDIT.isin("0", "0.0"))
            .filter(~error_df1.C_MIN_CREDIT_ERROR.isNull())
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c07a_invalid_max_credits(self) -> DataFrame:
        """
        S-07a Invalid Records

        C_Max_Credits Invalid

        """

        error_code = "c07a"

        error_df1 = self.df.withColumn(
            "C_MAX_CREDIT_ERROR",
            F.when(self.df.C_MAX_CREDIT.cast("float") == "", "Can not be blank")
            .when(F.length(self.df.C_MAX_CREDIT) > "4", "Too many digits")
            .when(self.df.C_MAX_CREDIT.cast("float") > "25.0", "Exceeds 25.0 Credits")
            .when(self.df.C_MAX_CREDIT.cast("float") < "0", "Can not be negative")
            .when(self.df.C_MAX_CREDIT.rlike("[^.0-9]"), "Contains Non_Numeric")
            .otherwise(None),
        )
        error_df = (
            error_df1.select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_CRN",
                "C_MAX_CREDIT_ERROR",
            )
            .filter(~error_df1.C_MAX_CREDIT.isin("0", "0.0"))
            .filter(~error_df1.C_MAX_CREDIT_ERROR.isNull())
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c07b_invalid_credits(self) -> DataFrame:
        """
        C-07b invalid credits

        C_MAX_Credits less than C_MIN_Credits

        """
        cols_to_check = [
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_MIN_CREDIT",
            "C_MAX_CREDIT",
        ]
        error_code = "c07b"
        where_clause = "ifnull(CAST(C_MAX_CREDIT AS float),'0') < ifnull(CAST(C_MIN_CREDIT as float),'0')"

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def c08a_invalid_contact_hrs(self) -> DataFrame:
        """
        S-08a Invalid Records

        C_Contact_HRS Invalid

        """

        error_code = "c08a"

        error_df1 = self.df.withColumn(
            "C_CONTACT_HRS_ERROR",
            F.when(self.df.C_CONTACT_HRS.cast("float") == "", "Can not be blank")
            .when(F.length(self.df.C_CONTACT_HRS) > "5", "Too many digits")
            .when(self.df.C_CONTACT_HRS.cast("float") > "4000", "Exceeds 4000 HRS")
            .when(self.df.C_CONTACT_HRS.cast("float") < "0", "Can not be negative")
            .when(self.df.C_CONTACT_HRS.rlike("[^.0-9]"), "Contains Non_Numeric")
            .otherwise(None),
        )
        error_df = (
            error_df1.select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_CRN",
                "C_CONTACT_HRS_ERROR",
            )
            .filter(~error_df1.C_CONTACT_HRS.isin("0"))
            .filter(~error_df1.C_CONTACT_HRS.isNull())
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c09_invalid_line_item(self) -> DataFrame:
        """
        C-09 invalid line item

        Line Item. Checking for valid values in c_line_item.

        """

        error_code = "c09"
        error_df = self.df.select(
            "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN", "C_LINE_ITEM"
        ).filter(
            ~self.df.C_LINE_ITEM.isin(
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
                "g",
                "h",
                "i",
                "p",
                "q",
                "r",
                "s",
                "t",
                "x",
            )
            | (self.df.C_LINE_ITEM == "")
            | self.df.C_LINE_ITEM.isNull()
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c09a_invalid_line_item(self) -> DataFrame:
        """
        C-09a invalid line item

        Line Item. Checking for valid institutional values in c_line_item.

        """

        error_code = "c09a"
        error_df = self.df.select(
            "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN", "C_LINE_ITEM"
        ).filter(
            (
                (~self.df.C_LINE_ITEM.isin("A", "B", "C", "D", "P"))
                & (self.df.C_INST == "3675")
            )
            | (
                (~self.df.C_LINE_ITEM.isin("A", "E", "F", "G", "H", "R", "S", "T", "X"))
                & (self.df.C_INST == "3677")
            )
            | (
                (self.df.C_LINE_ITEM != "A")
                & (~self.df.C_INST.isin("3671", "3680", "3678", "4027", "5220"))
            )
            | ((~self.df.C_LINE_ITEM.isin("A", "Q")) & (self.df.C_INST == "3679"))
            | ((self.df.C_LINE_ITEM != "I") & (self.df.C_INST == "5221"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c10_invalid_site_type(self) -> DataFrame:
        """
        C-10 invalid site type

        Site Type. Checking for valid values IN c_site_type.

        """

        error_code = "c10"
        ref_df_key = "site_type"
        column_name = "s_type_code"
        site_type_list = self.get_valid_value_list(ref_df_key, column_name)
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
        ).filter(
            (
                (~self.df.C_SITE_TYPE.isin(site_type_list))
                | (self.df.C_SITE_TYPE == "")
                | (self.df.C_SITE_TYPE.isNull())
            )
            & (self.df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c10a_invalid_site_type(self) -> DataFrame:
        """
        C-10a invalid site type

        Site Type. Checking for valid values IN c_site_type.

        """

        error_code = "c10"
        ref_df_key = "site_type"
        column_name = "s_type_code"
        site_type_list = self.get_valid_value_list(ref_df_key, column_name)
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
        ).filter(
            ~self.df.C_SITE_TYPE.isin(site_type_list)
            & (self.df.C_EXTRACT == "E")
            & ~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y")
            & self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c11_invalid_budget_code(self) -> DataFrame:
        """
        C-11 invalid budget code

        Budget Codes  Checking to ensure that budget codes are not blank or NULL.

        """

        error_code = "c11"

        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_BUDGET_CODE",
        ).filter(self.df.C_BUDGET_CODE.isNull() | (self.df.C_BUDGET_CODE == ""))

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c11a_invalid_budget_code(self) -> DataFrame:
        """
        C-11a invalid budget code

        Budget Codes  Checking to ensure that budget codes are in the reference table

        """

        error_code = "c11a"
        ref_df_key = "budget_code"
        column_name = "Budget_Code"
        where_clause = "Inactive <> 'Y'"
        budget_code_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_BUDGET_CODE",
        ).filter(
            ~self.df.C_BUDGET_CODE.isin(budget_code_list)
            & ((~self.df.C_BUDGET_CODE.isNull()) | (self.df.C_BUDGET_CODE != ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c12_invalid_delivery_method(self) -> DataFrame:
        """
        C-12 invalid delivery method

        Delivery Method.  Checking for valid values IN c_delivery_method.

        """

        error_code = "c12"
        ref_df_key = "delivery_method"
        column_name = "D_Method_Code"
        where_clause = "Inactive <> 'Y'"
        delivery_method_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_DELIVERY_METHOD",
        ).filter(
            (
                (~self.df.C_DELIVERY_METHOD.isin(delivery_method_list))
                | (self.df.C_DELIVERY_METHOD == "")
                | self.df.C_DELIVERY_METHOD.isNull()
            )
            & (self.df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c12a_invalid_delivery_method(self) -> DataFrame:
        """
        C-12a invalid delivery method

        Delivery Method.  Checking for valid values IN c_delivery_method.

        """

        error_code = "c12a"
        ref_df_key = "delivery_method"
        column_name = "D_Method_Code"
        where_clause = "Inactive <> 'Y'"
        delivery_method_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_DELIVERY_METHOD",
        ).filter(
            (
                (~self.df.C_DELIVERY_METHOD.isin(delivery_method_list))
                | (self.df.C_DELIVERY_METHOD == "")
                | self.df.C_DELIVERY_METHOD.isNull()
            )
            & (self.df.C_EXTRACT == "E")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c13_invalid_program_type(self) -> DataFrame:
        """
        C-13 invalid program type

        Program Type.  Checking for invalid program types.

        """

        error_code = "c13"
        ref_df_key = "program_type"
        column_name = "Program_Type_Code"
        where_clause = "Inactive <> 'Y'"
        program_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_PROGRAM_TYPE",
        ).filter(
            (~self.df.C_PROGRAM_TYPE.isin(program_type_list))
            | (self.df.C_PROGRAM_TYPE == "")
            | (self.df.C_PROGRAM_TYPE.isNull())
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c13a_invalid_program_type(self) -> DataFrame:
        """
        C-13a invalid program type

        checks validity of c_program_type against perkins list.

        """
        error_code = "c13a"
        perkins_df = self.perkins2015_prod_df
        condition = [
            self.df.C_INST == perkins_df.p_inst,
            self.df.C_TERM == perkins_df.p_term,
            self.df.C_YEAR == perkins_df.p_year,
        ]
        joined_df = (
            self.df.join(perkins_df, condition, "left")
            .where(perkins_df.Inactive != "Y")
            .withColumn(
                "P_SBJ_NUM", F.concat(perkins_df.p_crs_sbj, perkins_df.p_crs_num)
            )
            .select("P_SBJ_NUM")
        )

        crs_num_list = (
            joined_df.select("P_SBJ_NUM").distinct().rdd.flatMap(lambda x: x).collect()
        )
        error_df = self.df.select(
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ).filter(
            (~F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM).isin(crs_num_list))
            & (self.df.C_PROGRAM_TYPE.isin("V", "P"))
            & (self.df.C_EXTRACT == "3")
            & (~self.df.C_INST.isin("3676", "3677"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c13b_invalid_program_type(self) -> DataFrame:
        """
        C-13b invalid program type

        checks validity of c_program_type against perkins list.

        """
        error_code = "c13b"
        perkins_df = self.perkins2015_prod_df
        condition = [
            self.df.C_INST == perkins_df.p_inst,
            self.df.C_TERM == perkins_df.p_term,
            self.df.C_YEAR == perkins_df.p_year,
        ]
        joined_df = (
            self.df.join(perkins_df, condition, "left")
            .where(perkins_df.Inactive != "Y")
            .withColumn(
                "P_SBJ_NUM", F.concat(perkins_df.p_crs_sbj, perkins_df.p_crs_num)
            )
            .select("P_SBJ_NUM")
        )

        crs_num_list = (
            joined_df.select("P_SBJ_NUM").distinct().rdd.flatMap(lambda x: x).collect()
        )
        error_df = self.df.select(
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ).filter(
            (~F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM).isin(crs_num_list))
            & (self.df.C_PROGRAM_TYPE.isin("V", "P"))
            & (self.df.C_EXTRACT == "e")
            & (~self.df.C_INST.isin("3676", "3677"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c13c_invalid_program_type(self) -> DataFrame:
        """
        C-13c invalid program type

        checks validity of c_program_type against perkins list.

        """
        error_code = "c13c"
        perkins_df = self.perkins2015_prod_df
        condition = [
            self.df.C_INST == perkins_df.p_inst,
            self.df.C_TERM == perkins_df.p_term,
            self.df.C_YEAR == perkins_df.p_year,
        ]
        joined_df = (
            self.df.join(perkins_df, condition, "left")
            .where(perkins_df.Inactive != "Y")
            .withColumn(
                "P_SBJ_NUM", F.concat(perkins_df.p_crs_sbj, perkins_df.p_crs_num)
            )
            .select("P_SBJ_NUM")
        )

        crs_num_list = (
            joined_df.select("P_SBJ_NUM").distinct().rdd.flatMap(lambda x: x).collect()
        )
        error_df = self.df.select(
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ).filter(
            (F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM).isin(crs_num_list))
            & (~self.df.C_PROGRAM_TYPE.isin("V", "P"))
            & (~self.df.C_INST.isin("3676", "3677"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c13e_invalid_vocational(self) -> DataFrame:
        """
        C-13e invalid vocational

        checks for USU (non-Eastern) courses not on the USU Logan list but coded as Vocational

        """
        error_code = "c13e"
        perkins_df = self.logan_perkins_prod_df
        condition = [
            self.df.C_INST == perkins_df.p_inst,
            self.df.C_TERM == perkins_df.p_term,
            self.df.C_YEAR == perkins_df.p_year,
        ]
        joined_df = (
            self.df.join(perkins_df, condition, "left")
            .where(perkins_df.Inactive != "Y")
            .withColumn(
                "P_SBJ_NUM", F.concat(perkins_df.p_crs_sbj, perkins_df.p_crs_num)
            )
            .select("P_SBJ_NUM")
        )

        crs_num_list = (
            joined_df.select("P_SBJ_NUM").distinct().rdd.flatMap(lambda x: x).collect()
        )
        error_df = self.df.select(
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ).filter(
            (~F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM).isin(crs_num_list))
            & (self.df.C_PROGRAM_TYPE.isin("V", "P"))
            & (self.df.C_INST.isin("3676", "3677"))
            & (self.df.C_EXTRACT == "3")
            & (~self.df.C_LINE_ITEM.isin("E", "F", "G", "H", "T", "X"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c13f_invalid_vocational(self) -> DataFrame:
        """
        C-13f invalid vocational

        checks for USU (non-Eastern) courses not on the USU Logan list but coded as Vocational
        Same as c13e but c_extract = e

        """
        error_code = "c13f"
        perkins_df = self.logan_perkins_prod_df
        condition = [
            self.df.C_INST == perkins_df.p_inst,
            self.df.C_TERM == perkins_df.p_term,
            self.df.C_YEAR == perkins_df.p_year,
        ]
        joined_df = (
            self.df.join(perkins_df, condition, "left")
            .where(perkins_df.Inactive != "Y")
            .withColumn(
                "P_SBJ_NUM", F.concat(perkins_df.p_crs_sbj, perkins_df.p_crs_num)
            )
            .select("P_SBJ_NUM")
        )

        crs_num_list = (
            joined_df.select("P_SBJ_NUM").distinct().rdd.flatMap(lambda x: x).collect()
        )
        error_df = self.df.select(
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ).filter(
            (~F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM).isin(crs_num_list))
            & (self.df.C_PROGRAM_TYPE.isin("V", "P"))
            & (self.df.C_INST.isin("3676", "3677"))
            & (self.df.C_EXTRACT == "e")
            & (~self.df.C_LINE_ITEM.isin("E", "F", "G", "H", "T", "X"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c13g_invalid_vocational(self) -> DataFrame:
        """
        C-13g invalid vocational

        checks for USU (non-Eastern) courses on the USU Logan list that are not coded as vocational

        """
        error_code = "c13g"
        perkins_df = self.logan_perkins_prod_df
        condition = [
            self.df.C_INST == perkins_df.p_inst,
            self.df.C_TERM == perkins_df.p_term,
            self.df.C_YEAR == perkins_df.p_year,
        ]
        joined_df = (
            self.df.join(perkins_df, condition, "left")
            .where(perkins_df.Inactive != "Y")
            .withColumn(
                "P_SBJ_NUM", F.concat(perkins_df.p_crs_sbj, perkins_df.p_crs_num)
            )
            .select("P_SBJ_NUM")
        )

        crs_num_list = (
            joined_df.select("P_SBJ_NUM").distinct().rdd.flatMap(lambda x: x).collect()
        )
        error_df = self.df.select(
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ).filter(
            (F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM).isin(crs_num_list))
            & (~self.df.C_PROGRAM_TYPE.isin("V", "P"))
            & (self.df.C_INST.isin("3676", "3677"))
            & (~self.df.C_LINE_ITEM.isin("E", "F", "H", "T", "X"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c13i_invalid_vocational(self) -> DataFrame:
        """
        C-13i invalid vocational

        checks for USU Eastern courses not on the USU Master courses list but coded as Vocational

        """
        error_code = "c13i"
        perkins_df = self.perkins2015_prod_df
        condition = [
            self.df.C_INST == perkins_df.p_inst,
            self.df.C_TERM == perkins_df.p_term,
            self.df.C_YEAR == perkins_df.p_year,
        ]
        joined_df = (
            self.df.join(perkins_df, condition, "left")
            .where(perkins_df.Inactive != "Y")
            .withColumn(
                "P_SBJ_NUM", F.concat(perkins_df.p_crs_sbj, perkins_df.p_crs_num)
            )
            .select("P_SBJ_NUM")
        )

        crs_num_list = (
            joined_df.select("P_SBJ_NUM").distinct().rdd.flatMap(lambda x: x).collect()
        )
        error_df = self.df.select(
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ).filter(
            (~F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM).isin(crs_num_list))
            & (self.df.C_PROGRAM_TYPE.isin("V", "P"))
            & (self.df.C_INST.isin("3676", "3677"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_LINE_ITEM.isin("E", "F", "H", "T", "X"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c13j_invalid_vocational(self) -> DataFrame:
        """
        C-13j invalid vocational

        checks for USU Eastern courses not on the USU Master courses list but coded as Vocational

        """
        error_code = "c13j"
        perkins_df = self.perkins2015_prod_df
        condition = [
            self.df.C_INST == perkins_df.p_inst,
            self.df.C_TERM == perkins_df.p_term,
            self.df.C_YEAR == perkins_df.p_year,
        ]
        joined_df = (
            self.df.join(perkins_df, condition, "left")
            .where(perkins_df.Inactive != "Y")
            .withColumn(
                "P_SBJ_NUM", F.concat(perkins_df.p_crs_sbj, perkins_df.p_crs_num)
            )
            .select("P_SBJ_NUM")
        )

        crs_num_list = (
            joined_df.select("P_SBJ_NUM").distinct().rdd.flatMap(lambda x: x).collect()
        )
        error_df = self.df.select(
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ).filter(
            (~F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM).isin(crs_num_list))
            & (self.df.C_PROGRAM_TYPE.isin("V", "P"))
            & (self.df.C_INST.isin("3676", "3677"))
            & (self.df.C_EXTRACT == "e")
            & (self.df.C_LINE_ITEM.isin("E", "F", "H", "T", "X"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c13k_invalid_vocational(self) -> DataFrame:
        """
        C-13k invalid vocational

        checks for USU Eastern courses not on the USU Master courses list but coded as Vocational

        """
        error_code = "c13k"
        perkins_df = self.perkins2015_prod_df
        condition = [
            self.df.C_INST == perkins_df.p_inst,
            self.df.C_TERM == perkins_df.p_term,
            self.df.C_YEAR == perkins_df.p_year,
        ]
        joined_df = (
            self.df.join(perkins_df, condition, "left")
            .where(perkins_df.Inactive != "Y")
            .withColumn(
                "P_SBJ_NUM", F.concat(perkins_df.p_crs_sbj, perkins_df.p_crs_num)
            )
            .select("P_SBJ_NUM")
        )

        crs_num_list = (
            joined_df.select("P_SBJ_NUM").distinct().rdd.flatMap(lambda x: x).collect()
        )
        error_df = self.df.select(
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_PROGRAM_TYPE",
            "C_LINE_ITEM",
            "C_CRN",
        ).filter(
            (F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM).isin(crs_num_list))
            & (~self.df.C_PROGRAM_TYPE.isin("V", "P"))
            & (self.df.C_INST.isin("3676", "3677"))
            & (self.df.C_LINE_ITEM.isin("E", "F", "H", "T", "X"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c14a_invalid_credit_ind(self) -> DataFrame:
        """
        C-14a invalid credit ind

        Credit Indicator.  Checking for valid values for c_credit_ind.

        """

        error_code = "c14a"
        error_df = self.df.select(
            "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN", "C_CREDIT_IND"
        ).filter(
            (~self.df.C_CREDIT_IND.isin("C", "N")) | (self.df.C_CREDIT_IND.isNull())
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c14b_invalid_credit_ind(self) -> DataFrame:
        """
        C-14b invalid credit ind

        NON-CREDIT COURSES IN 3RD WEEK

        """

        error_code = "c14b"
        error_df = self.df.select(
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_CREDIT_IND",
        ).filter(
            (self.df.C_CREDIT_IND == "N")
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_INSTRUCT_TYPE != "LAB")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c14c_invalid_vocational(self) -> DataFrame:
        """
        C-14c invalid vocational

        C-14c Only Labs and Vocational non-credit classes can be submitted

        """
        error_code = "c14c"
        c_inst_list = self.df.first()["C_INST"]
        # c_inst_list = self.df.select("C_INST").distinct().rdd.flatMap(lambda x: x).collect()
        ref_df_key = "etpl"
        column_name = "ETPL_CIP_USHE"
        where_clause = f"""ETPL_Inst_Code = "{c_inst_list}" AND Inactive = 'N'"""
        etpl_list = self.get_valid_value_list(ref_df_key, column_name, where_clause)

        error_df = self.df.select(
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_TITLE",
            "C_CREDIT_IND",
            "C_INSTRUCT_TYPE",
            "C_PROGRAM_TYPE",
            "C_BUDGET_CODE",
            "C_CONTACT_HRS",
            "C_CRN",
        ).filter(
            (
                ~F.concat(self.df.C_CRS_SBJ, F.lit(" "), self.df.C_CRS_NUM).isin(
                    etpl_list
                )
            )
            & (~self.df.C_PROGRAM_TYPE.isin("V", "P"))
            & (self.df.C_CREDIT_IND == "N")
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_INSTRUCT_TYPE != "LAB")
            & (~self.df.C_BUDGET_CODE.isin("BV", "SQ"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c15a_invalid_start_time(self) -> DataFrame:
        """
        C-15a invalid start time

        Courses Start Time conditionally required

        """
        error_code = "c15a"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_START_TIME",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_START_TIME.isNull()) | (self.df.C_START_TIME == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c15b_invalid_start_time(self) -> DataFrame:
        """
        C-15b invalid start time

        Courses Start Time conditionally required

        """
        error_code = "c15b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_START_TIME",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_START_TIME.isNull()) | (self.df.C_START_TIME == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c15c_invalid_start_time(self) -> DataFrame:
        """
        C-15c invalid start time

        Courses Start Time conditionally required

        """
        error_code = "c15c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_START_TIME",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_START_TIME.isNull()) | (self.df.C_START_TIME == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c16a_invalid_stop_time(self) -> DataFrame:
        """
        C-16a invalid stop time

        Courses Stop Time conditionally required

        """
        error_code = "c16a"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_STOP_TIME",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_STOP_TIME.isNull()) | (self.df.C_STOP_TIME == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c16b_invalid_stop_time(self) -> DataFrame:
        """
        C-16b invalid stop time

        Courses Stop Time conditionally required

        """
        error_code = "c16b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_STOP_TIME",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_STOP_TIME.isNull()) | (self.df.C_STOP_TIME == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c16c_invalid_stop_time(self) -> DataFrame:
        """
        C-16c invalid stop time

        Courses Stop Time conditionally required

        """
        error_code = "c16c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_STOP_TIME",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_STOP_TIME.isNull()) | (self.df.C_STOP_TIME == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c17a_invalid_days(self) -> DataFrame:
        """
        C-17a invalid c days

        Courses Days conditionally required

        """
        error_code = "c17a"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_DAYS.isNull()) | (self.df.C_DAYS == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c17b_invalid_days(self) -> DataFrame:
        """
        C-17b invalid c days

        c_days Stop Time conditionally required

        """
        error_code = "c17b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_DAYS.isNull()) | (self.df.C_DAYS == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c17c_invalid_days(self) -> DataFrame:
        """
        C-17c invalid c days

        c_days Stop Time conditionally required

        """
        error_code = "c17c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_DAYS.isNull()) | (self.df.C_DAYS == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c18_invalid_bldg_name_and_num(self) -> DataFrame:
        """
        C-18 invalid bldg name and num

        BLDG Name and number are the same

        """
        error_code = "c18"

        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
        ).filter(
            (self.df.C_BLDG_SNAME == self.df.C_BLDG_NUM)
            & (~self.df.C_BLDG_NUM.isNull())
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c18a_invalid_bldg_name(self) -> DataFrame:
        """
        C-18a invalid bldg sname

        BLDG SName conditionally Required

        """
        error_code = "c18a"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_SNAME",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_BLDG_SNAME.isNull()) | (self.df.C_BLDG_SNAME == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c18b_invalid_bldg_name(self) -> DataFrame:
        """
        C-18b invalid bldg sname

        BLDG SName conditionally Required

        """
        error_code = "c18b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_SNAME",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_BLDG_SNAME.isNull()) | (self.df.C_BLDG_SNAME == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c18c_invalid_bldg_name(self) -> DataFrame:
        """
        C-18c invalid bldg sname

        BLDG SName conditionally Required

        """
        error_code = "c18b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_SNAME",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_BLDG_SNAME.isNull()) | (self.df.C_BLDG_SNAME == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c19a_invalid_bldg_num(self) -> DataFrame:
        """
        C-19a invalid bldg num

        BLDG num conditionally Required

        """
        error_code = "c19a"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_NUM",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_BLDG_NUM.isNull()) | (self.df.C_BLDG_NUM == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c19b_invalid_bldg_num(self) -> DataFrame:
        """
        C-19b invalid bldg num

        BLDG num conditionally Required

        """
        error_code = "c19b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_NUM",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_BLDG_NUM.isNull()) | (self.df.C_BLDG_NUM == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c19e_invalid_bldg_num(self) -> DataFrame:
        """
        C-19e invalid bldg num

        BLDG num conditionally Required

        """
        error_code = "c19b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_BLDG_NUM",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_BLDG_NUM.isNull()) | (self.df.C_BLDG_NUM == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c20a_invalid_room_num(self) -> DataFrame:
        """
        C-20a invalid room num

        room num conditionally Required

        """
        error_code = "c20a"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_ROOM_NUM",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_ROOM_NUM.isNull()) | (self.df.C_ROOM_NUM == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c20b_invalid_room_num(self) -> DataFrame:
        """
        C-20b invalid room num

        room num conditionally Required

        """
        error_code = "c20b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_ROOM_NUM",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_ROOM_TYPE.isin("110", "210"))
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_ROOM_NUM.isNull()) | (self.df.C_ROOM_NUM == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c20c_invalid_room_num(self) -> DataFrame:
        """
        C-20c invalid room num

        room num conditionally Required

        """
        error_code = "c20c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM",
            "C_ROOM_TYPE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE",
            "C_CRN",
            "C_ROOM_NUM",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_ROOM_NUM.isNull()) | (self.df.C_ROOM_NUM == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c22b_invalid_room_type(self) -> DataFrame:
        """
        C-22b invalid room type

        C_Room_type Missing

        """
        error_code = "c22b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DAYS.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y", "B", "P"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_ROOM_TYPE.isNull()) | (self.df.C_ROOM_TYPE == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c22c_invalid_room_type(self) -> DataFrame:
        """
        C-22c invalid room type

        C_Room_type Missing

        """
        error_code = "c22c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DAYS.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y", "B", "P"))
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_ROOM_TYPE.isNull()) | (self.df.C_ROOM_TYPE == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c23b_invalid_start_time2(self) -> DataFrame:
        """
        C-23b invalid start_time2

        Courses Start Time2 conditionally required

        """
        error_code = "c23b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_TYPE",
            "C_START_TIME2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE2.isin("110", "210"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_START_TIME2.isNull()) | (self.df.C_START_TIME2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c23c_invalid_start_time2(self) -> DataFrame:
        """
        C-23c invalid start_time2

        Courses Start Time2 conditionally required

        """
        error_code = "c23c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_TYPE",
            "C_START_TIME2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE2.isin("110", "210"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_START_TIME2.isNull()) | (self.df.C_START_TIME2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c23d_invalid_start_time2(self) -> DataFrame:
        """
        C-23d invalid start_time2

        Courses Start Time2 conditionally required

        """
        error_code = "c23d"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_TYPE",
            "C_START_TIME2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_START_TIME2.isNull()) | (self.df.C_START_TIME2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c24b_invalid_stop_time2(self) -> DataFrame:
        """
        C-24b invalid stop_time2

        Courses Stop Time2 conditionally required

        """
        error_code = "c24b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_STOP_TIME2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE2.isin("110", "210"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_STOP_TIME2.isNull()) | (self.df.C_STOP_TIME2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c24c_invalid_stop_time2(self) -> DataFrame:
        """
        C-24c invalid stop_time2

        Courses Stop Time2 conditionally required

        """
        error_code = "c24c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_STOP_TIME2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE2.isin("110", "210"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_STOP_TIME2.isNull()) | (self.df.C_STOP_TIME2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c24d_invalid_stop_time2(self) -> DataFrame:
        """
        C-24d invalid stop_time2

        Courses Stop Time2 conditionally required

        """
        error_code = "c24d"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_STOP_TIME2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_STOP_TIME2.isNull()) | (self.df.C_STOP_TIME2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c25b_invalid_days2(self) -> DataFrame:
        """
        C-25b invalid days2

        Courses Days2 conditionally required

        """
        error_code = "c25b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_DAYS2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_ROOM_TYPE2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE2.isin("110", "210"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_DAYS2.isNull()) | (self.df.C_DAYS2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c25c_invalid_days2(self) -> DataFrame:
        """
        C-25c invalid days2

        Courses Days2 conditionally required

        """
        error_code = "c25c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_DAYS2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_ROOM_TYPE2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE2.isin("110", "210"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_DAYS2.isNull()) | (self.df.C_DAYS2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c25d_invalid_days2(self) -> DataFrame:
        """
        C-25d invalid days2

        Courses Days2 conditionally required

        """
        error_code = "c25d"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_DAYS2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_ROOM_TYPE2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_DAYS2.isNull()) | (self.df.C_DAYS2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c26a_invalid_sname2(self) -> DataFrame:
        """
        C-26a invalid SName2

        BLDG SName2 conditionally Required

        """
        error_code = "c26a"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_SNAME2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_EXTRACT == "3")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE2.isin("110", "210"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_BLDG_SNAME2.isNull()) | (self.df.C_BLDG_SNAME2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c26b_invalid_sname2(self) -> DataFrame:
        """
        C-26b invalid SName2

        BLDG SName2 conditionally Required

        """
        error_code = "c26b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_SNAME2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_EXTRACT == "E")
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE2.isin("110", "210"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_BLDG_SNAME2.isNull()) | (self.df.C_BLDG_SNAME2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c26c_invalid_sname2(self) -> DataFrame:
        """
        C-26c invalid SName2

        BLDG SName2 conditionally Required

        """
        error_code = "c26c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_SNAME2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & ((self.df.C_BLDG_SNAME2.isNull()) | (self.df.C_BLDG_SNAME2 == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c11b_invalid_ceml(self) -> DataFrame:
        """
        C-11b invalid ceml match

        C11B FET Not on CEML

        """
        error_code = "c11b"
        cc_master_list_prod_df = self.cc_master_list_prod_df
        condition = [
            self.df.C_INST == cc_master_list_prod_df.inst,
            self.df.C_YEAR == cc_master_list_prod_df.year,
        ]
        joined_df = (
            self.df.join(cc_master_list_prod_df, condition, "left")
            .withColumn(
                "SBJ_NUM",
                F.concat(cc_master_list_prod_df.sbj, cc_master_list_prod_df.num),
            )
            .select("SBJ_NUM")
        )

        crs_num_list = (
            joined_df.select("SBJ_NUM").distinct().rdd.flatMap(lambda x: x).collect()
        )
        error_df = self.df.select(
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_CRN",
        ).filter(
            (~F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM).isin(crs_num_list))
            & (self.df.C_BUDGET_CODE.isin("BC", "SF"))
            & (self.df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c11c_invalid_ceml(self) -> DataFrame:
        """
        C-11c invalid ceml match

        C11C FET Not on CEML

        """
        error_code = "c11c"
        cc_master_list_prod_df = self.cc_master_list_prod_df
        condition = [
            self.df.C_INST == cc_master_list_prod_df.inst,
            self.df.C_YEAR == cc_master_list_prod_df.year,
        ]
        joined_df = (
            self.df.join(cc_master_list_prod_df, condition, "left")
            .withColumn(
                "SBJ_NUM",
                F.concat(cc_master_list_prod_df.sbj, cc_master_list_prod_df.num),
            )
            .select("SBJ_NUM")
        )

        crs_num_list = (
            joined_df.select("SBJ_NUM").distinct().rdd.flatMap(lambda x: x).collect()
        )
        error_df = self.df.select(
            "C_INST",
            "C_TITLE",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_CRN",
        ).filter(
            (~F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM).isin(crs_num_list))
            & (self.df.C_BUDGET_CODE.isin("BC", "SF"))
            & (self.df.C_EXTRACT == "e")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c19c_invalid_bldg_num(self) -> DataFrame:
        """
        C-19c invalid bldg num

        BLDG NUM not in Building Inventory

        """

        error_code = "c19c"
        buildings_prod_df = self.buildings_prod_df
        c_inst_list = self.df.first()["C_INST"]
        where_clause = f"B_INST = '{c_inst_list}' AND B_INST <> '5221'"
        error_df1 = (
            buildings_prod_df.groupby("B_INST", "B_NUMBER")
            .agg(F.max("B_YEAR").alias("B_MAX_YEAR"))
            .select("B_INST", "B_NUMBER")
            .distinct()
            .where(where_clause)
        )
        cond = [
            self.df.C_INST == error_df1.B_INST,
            self.df.C_BLDG_NUM == error_df1.B_NUMBER,
        ]
        error_df = (
            self.df.join(error_df1, cond, "left")
            .select(
                "C_INST",
                "C_BLDG_SNAME",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_DAYS",
                "C_DELIVERY_METHOD",
                "C_INSTRUCT_TYPE",
                "C_SITE_TYPE",
                "C_CRN",
                "C_BLDG_NUM",
            )
            .filter(
                (error_df1.B_NUMBER.isNull())
                & (~self.df.C_BLDG_NUM.isNull())
                & (self.df.C_INST != "5221")
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c19d_invalid_r_bldg_num(self) -> DataFrame:
        """
        C-19d invalid r bldg num

        BLDG NUM not in Rooms Inventory

        """

        error_code = "c19d"
        rooms_prod_df = self.rooms_prod_df
        c_inst_list = self.df.first()["C_INST"]
        where_clause = f"R_INST = '{c_inst_list}' AND R_INST <> '5221'"
        error_df1 = (
            rooms_prod_df.groupby("R_INST", "R_BUILD_NUMBER")
            .agg(F.max("R_YEAR").alias("R_MAX_YEAR"))
            .select("R_INST", "R_BUILD_NUMBER")
            .distinct()
            .where(where_clause)
        )
        cond = [
            self.df.C_INST == error_df1.R_INST,
            self.df.C_BLDG_NUM == error_df1.R_BUILD_NUMBER,
        ]
        error_df = (
            self.df.join(error_df1, cond, "left")
            .select(
                "C_INST",
                "C_BLDG_SNAME",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_DAYS",
                "C_DELIVERY_METHOD",
                "C_INSTRUCT_TYPE",
                "C_SITE_TYPE",
                "C_CRN",
                "C_BLDG_NUM",
            )
            .filter(
                (error_df1.R_BUILD_NUMBER.isNull())
                & (~self.df.C_BLDG_NUM.isNull())
                & (self.df.C_INST != "5221")
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c21a_invalid_room_max(self) -> DataFrame:
        """
        S-21a Invalid Records

        C_Room_Max Invalid

        """

        error_code = "c21a"

        error_df1 = self.df.withColumn(
            "C_ROOM_MAX_ERROR",
            F.when(self.df.C_ROOM_MAX.cast("float") == "", "Can not be blank")
            .when(F.length(self.df.C_ROOM_MAX) > "4", "Too many digits")
            .when(self.df.C_ROOM_MAX.cast("float") > "9999", "Exceeds 99999 HRS")
            .when(self.df.C_ROOM_MAX.cast("float") < "0", "Can not be negative")
            .when(self.df.C_ROOM_MAX.rlike("[^.0-9]"), "Contains Non_Numeric")
            .otherwise(None),
        )
        error_df = (
            error_df1.select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_BLDG_NUM",
                "C_ROOM_NUM",
                "C_CRN",
                "C_ROOM_MAX",
                "C_ROOM_MAX_ERROR",
            )
            .filter(error_df1.C_ROOM_MAX != "0")
            .filter(~error_df1.C_ROOM_MAX_ERROR.isNull())
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c22_room_type_count(self) -> DataFrame:
        """
        C-22 Room Type Count

        """

        error_code = "c22"
        rooms_use_code_df = self.get_rooms_use_code_reference_df()
        error_df1 = self.df.join(
            rooms_use_code_df,
            self.df.C_ROOM_TYPE == rooms_use_code_df.R_Use_Name,
            "left",
        ).select(
            self.df.C_INST,
            self.df.C_ROOM_TYPE,
            rooms_use_code_df.R_Use_Name.alias("DESCRIPTION"),
        )

        error_df = (
            error_df1.select(
                "C_INST",
                "C_ROOM_TYPE",
                "DESCRIPTION",
            )
            .groupby("C_INST", "C_ROOM_TYPE", "DESCRIPTION")
            .count()
            .withColumnRenamed("count", "NUMBER_OF_COURSES")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c22a_invalid_room_type(self) -> DataFrame:
        """
        C-22a invalid room type

        Checking for valid values for c_room_type.

        """

        error_code = "c22a"
        ref_df_key = "rooms_use_code"
        column_name = "R_Use_Code"
        where_clause = "Inactive <> 'Y'"
        use_code_list = self.get_valid_value_list(ref_df_key, column_name, where_clause)
        error_df = self.df.select(
            "C_YEAR",
            "C_INST",
            "C_TERM",
            "C_EXTRACT",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_ROOM_TYPE",
        ).filter(
            ~self.df.C_ROOM_TYPE.isin(use_code_list)
            & ((~self.df.C_ROOM_TYPE.isNull()) | (self.df.C_ROOM_TYPE != ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c22d_invalid_room_type(self) -> DataFrame:
        """
        C-22d invalid room type

        Room type counts that are included in Space Utilization

        """
        error_code = "c22d"
        rooms_use_code_df = self.get_rooms_use_code_reference_df()
        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df1 = self.df.join(
            rooms_use_code_df,
            self.df.C_ROOM_TYPE == rooms_use_code_df.R_Use_Code,
            "left",
        ).filter(
            (self.df.C_SITE_TYPE.isin(site_type_list))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
        )

        error_df = (
            error_df1.select(
                error_df1.C_INST,
                error_df1.C_ROOM_TYPE,
                error_df1.R_Use_Name.alias("DESCRIPTION"),
            )
            .groupby("C_INST", "C_ROOM_TYPE", "DESCRIPTION")
            .count()
            .withColumnRenamed("count", "NUMBER_OF_COURSES")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c22e_invalid_room_type(self) -> DataFrame:
        """
        C-22e invalid room type

        Room Type is missing with Delivery Method = B

        """
        error_code = "c22e"
        courses_load_prod_df = self.courses_load_prod_df
        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = courses_load_prod_df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ).filter(
            (courses_load_prod_df.C_SITE_TYPE.isin(site_type_list))
            & (
                (courses_load_prod_df.C_ROOM_TYPE.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE == "")
            )
            & (
                (courses_load_prod_df.C_ROOM_TYPE2.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE2 == "")
            )
            & (
                (courses_load_prod_df.C_ROOM_TYPE3.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE3 == "")
            )
            & (~courses_load_prod_df.C_DAYS.isNull())
            & (courses_load_prod_df.C_DELIVERY_METHOD == "B")
            & (courses_load_prod_df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (courses_load_prod_df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c38f_room_type_missing_delivery_method_b(self) -> DataFrame:
        """
        C38F Room Type3 is missing with Delivery Method = B

        Returns error dataframe.
        """
        error_code = "c38f"

        distinct_site_type_code_list = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_ROOM_TYPE.isNull() | (self.df.C_ROOM_TYPE == ""))
            & (self.df.C_ROOM_TYPE2.isNull() | (self.df.C_ROOM_TYPE2 == ""))
            & (self.df.C_ROOM_TYPE3.isNull() | (self.df.C_ROOM_TYPE3 == ""))
            & (self.df.C_DAYS3.isNotNull())
            & (self.df.C_DELIVERY_METHOD == "B")
            & (self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"]))
            & (self.df.C_SITE_TYPE3.isin(distinct_site_type_code_list))
            & (self.df.C_EXTRACT == "E")
        ).select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c38g_room_type_missing_delivery_method_p(self) -> DataFrame:
        """
        C38G Room Type3 is missing with Delivery Method = P

        Returns error dataframe.
        """
        error_code = "c38g"

        distinct_site_type_code_list = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_ROOM_TYPE.isNull() | (self.df.C_ROOM_TYPE == ""))
            & (self.df.C_ROOM_TYPE2.isNull() | (self.df.C_ROOM_TYPE2 == ""))
            & (self.df.C_ROOM_TYPE3.isNull() | (self.df.C_ROOM_TYPE3 == ""))
            & (self.df.C_DAYS3.isNotNull())
            & (self.df.C_DELIVERY_METHOD == "P")
            & (self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"]))
            & (self.df.C_SITE_TYPE3.isin(distinct_site_type_code_list))
            & (self.df.C_EXTRACT == "3")
        ).select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c38h_room_type_missing_delivery_method_p(self) -> DataFrame:
        """
        C38H Room Type3 is missing with Delivery Method = P

        Returns error dataframe.
        """
        error_code = "c38h"

        distinct_site_type_code_list = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_ROOM_TYPE.isNull() | (self.df.C_ROOM_TYPE == ""))
            & (self.df.C_ROOM_TYPE2.isNull() | (self.df.C_ROOM_TYPE2 == ""))
            & (self.df.C_ROOM_TYPE3.isNull() | (self.df.C_ROOM_TYPE3 == ""))
            & (self.df.C_DAYS3.isNotNull())
            & (self.df.C_DELIVERY_METHOD == "P")
            & (self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"]))
            & (self.df.C_SITE_TYPE3.isin(distinct_site_type_code_list))
            & (self.df.C_EXTRACT == "E")
        ).select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c39a_start_date_summer(self) -> DataFrame:
        """
        C-39a Start Date-Summer. Checks for valid format for c_start_date to ensure it falls within
        the typical summer term range.

        Returns DataFrame containing records that fall out of the typical summer range.
        """
        error_code = "c39a"

        start_year = substring(col("C_START_DATE"), 1, 4)
        start_month = substring(col("C_START_DATE"), 5, 2)
        start_day = substring(col("C_START_DATE"), 7, 2)
        instance_year = substring(col("c_instance"), 1, 4)

        error_df = (
            self.df.filter(
                (start_year != (instance_year - 1))
                | (~start_month.isin(["05", "06", "07", "08"]))
                | (start_day > 31)
                | (
                    col("C_START_DATE").isNull()
                    | (col("C_START_DATE") == "")
                    | (col("C_START_DATE") == "0")
                )
            )
            .filter(col("C_TERM") == 1)
            .select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_BUDGET_CODE",
                "C_COLLEGE",
                "C_CRN",
                "C_START_DATE",
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c39b_start_date_fall(self) -> DataFrame:
        """
        C-39b Start Date-Fall. Checks for valid format for c_start_date to ensure it falls within
        the typical fall term range.

        Returns DataFrame containing records that fall out of the typical fall range.
        """
        error_code = "c39b"

        start_year = substring(col("C_START_DATE"), 1, 4)
        start_month = substring(col("C_START_DATE"), 5, 2)
        start_day = substring(col("C_START_DATE"), 7, 2)
        instance_year = substring(col("c_instance"), 1, 4)

        error_df = (
            self.df.filter(
                (start_year != (instance_year - 1))
                | (~start_month.isin(["08", "09", "10", "11", "12"]))
                | (start_day > 31)
                | (
                    col("C_START_DATE").isNull()
                    | (col("C_START_DATE") == "")
                    | (col("C_START_DATE") == "0")
                )
            )
            .filter(col("C_TERM") == 2)
            .select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_BUDGET_CODE",
                "C_COLLEGE",
                "C_CRN",
                "C_START_DATE",
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c39c_start_date_spring(self) -> DataFrame:
        """
        C-39c Start Date-Spring. Checks for valid format for c_start_date to ensure it falls within
        the typical spring term range.

        Returns DataFrame containing records that fall out of the typical spring range.
        """
        error_code = "c39c"

        start_year = substring(col("C_START_DATE"), 1, 4)
        start_month = substring(col("C_START_DATE"), 5, 2)
        start_day = substring(col("C_START_DATE"), 7, 2)
        instance_year = substring(col("c_instance"), 1, 4)

        error_df = (
            self.df.filter(
                (start_year != (instance_year - 1))
                | (~start_month.isin(["01", "02", "03", "04", "05"]))
                | (start_day > 31)
                | (
                    col("C_START_DATE").isNull()
                    | (col("C_START_DATE") == "")
                    | (col("C_START_DATE") == "0")
                )
            )
            .filter(col("C_TERM") == 3)
            .select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_BUDGET_CODE",
                "C_COLLEGE",
                "C_CRN",
                "C_START_DATE",
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c40a_end_date_summer(self) -> DataFrame:
        """
        C-40a End Date-Summer. Checks for valid format for c_end_date to ensure it falls within
        the typical summer term range.

        Returns DataFrame containing records that fall out of the typical summer range.
        """
        error_code = "c40a"

        start_year = substring(col("C_END_DATE"), 1, 4)
        start_month = substring(col("C_END_DATE"), 5, 2)
        start_day = substring(col("C_END_DATE"), 7, 2)
        instance_year = substring(col("c_instance"), 1, 4)

        error_df = (
            self.df.filter(
                (start_year != (instance_year - 1))
                | (~start_month.isin(["05", "06", "07", "08"]))
                | (start_day > 31)
                | (
                    col("C_END_DATE").isNull()
                    | (col("C_END_DATE") == "")
                    | (col("C_END_DATE") == "0")
                )
            )
            .filter(col("C_TERM") == 1)
            .select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_BUDGET_CODE",
                "C_COLLEGE",
                "C_CRN",
                "C_START_DATE",
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c40b_end_date_fall(self) -> DataFrame:
        """
        C-40b End Date-Fall. Checks for valid format for c_end_date to ensure it falls within
        the typical fall term range.

        Returns DataFrame containing records that fall out of the typical fall range.
        """
        error_code = "c40b"

        start_year = substring(col("C_END_DATE"), 1, 4)
        start_month = substring(col("C_END_DATE"), 5, 2)
        start_day = substring(col("C_END_DATE"), 7, 2)
        instance_year = substring(col("c_instance"), 1, 4)

        error_df = (
            self.df.filter(
                (start_year != (instance_year - 1))
                | (~start_month.isin(["08", "09", "10", "11", "12"]))
                | (start_day > 31)
                | (
                    col("C_END_DATE").isNull()
                    | (col("C_END_DATE") == "")
                    | (col("C_END_DATE") == "0")
                )
            )
            .filter(col("C_TERM") == 2)
            .select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_BUDGET_CODE",
                "C_COLLEGE",
                "C_CRN",
                "C_START_DATE",
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c40c_end_date_spring(self) -> DataFrame:
        """
        C-40c End Date-Spring. Checks for valid format for c_end_date to ensure it falls within
        the typical spring term range.

        Returns DataFrame containing records that fall out of the typical spring range.
        """
        error_code = "c40c"

        start_year = substring(col("C_END_DATE"), 1, 4)
        start_month = substring(col("C_END_DATE"), 5, 2)
        start_day = substring(col("C_END_DATE"), 7, 2)
        instance_year = substring(col("c_instance"), 1, 4)

        error_df = (
            self.df.filter(
                (start_year != (instance_year - 1))
                | (~start_month.isin(["01", "02", "03", "04", "05"]))
                | (start_day > 31)
                | (
                    col("C_END_DATE").isNull()
                    | (col("C_END_DATE") == "")
                    | (col("C_END_DATE") == "0")
                )
            )
            .filter(col("C_TERM") == 3)
            .select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_BUDGET_CODE",
                "C_COLLEGE",
                "C_CRN",
                "C_START_DATE",
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c41a_title_missing(self) -> DataFrame:
        """
        C41A Checks if the course title is missing or null.

        Returns DataFrame containing records with missing or null course titles.
        """

        error_code = "c41a"

        error_df = (
            self.df.filter(
                col("C_TITLE").isNull()
                | (col("C_TITLE") == "")
                # &(col("C_INST") != "63")
            )
            .select("C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN", "C_TITLE")
            .orderBy("C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c51c_course_level_counts(self) -> DataFrame:
        """
        C51C Course Level Counts. Computes counts of courses at each level for a given institution
        and instance.

        Returns DataFrame with course level counts for specified institution and instance.
        """

        error_code = "c51c"

        error_df = (
            self.df.groupBy("C_INST", "C_YEAR", "C_TERM", "C_EXTRACT", "C_LEVEL")
            .agg(count("*").alias("Levels"))
            .orderBy("C_LEVEL")
            .select("C_INST", "C_YEAR", "C_TERM", "C_EXTRACT", "C_LEVEL", "Levels")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c52a_missing_crn(self) -> DataFrame:
        """
        C-52a Missing Course Reference Number. Identifies courses missing a CRN.

        Returns DataFrame containing records where the course reference number is missing.
        """
        error_code = "c52a"
        error_df = (
            self.df.filter(col("C_CRN").isNull() | (col("C_CRN") == ""))
            .select("C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_DEPT", "C_CRN")
            .orderBy("C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_DEPT")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c52b_invalid_crn(self) -> DataFrame:
        """
        C-52b Invalid Course Reference Number. Identifies courses with invalid CRNs based on several criteria,
        including format, length, and numeric constraints.

        Returns DataFrame with invalid CRN records and the corresponding error reasons.
        """

        error_code = "c52b"

        error_conditions = (
            when(col("C_CRN") == "", "Can not be blank")
            .when(length(col("C_CRN")) > 5, "Too many digits")
            .when(col("C_CRN") == "0", "Cannot be Zero")
            .when(col("C_CRN").cast("float") > 99999, "Exceeds 99999")
            .when(col("C_CRN").cast("float") < 0, "Can not be negative")
            .when(col("C_CRN").rlike("[^0-9]"), "Contains Non_Numeric")
        )

        error_df = (
            self.df.filter(
                (col("C_CRN") == "")
                | (length(col("C_CRN")) > 5)
                | (col("C_CRN") == "0")
                | (col("C_CRN").cast("float") > 99999)
                | (col("C_CRN").cast("float") < 0)
                | (col("C_CRN").rlike("[^0-9]"))
            )
            .withColumn("C_CRN_Error", error_conditions)
            .select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_DEPT",
                "C_CRN",
                "C_CRN_Error",
            )
            .orderBy("C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_DEPT")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c52c_non_unique_crn(self) -> DataFrame:
        """
        C-52c Course Reference Number should be unique. Identifies CRNs that are not unique
        within the specified institution and instance, excluding certain institutions..

        Returns DataFrame with CRNs that are not unique along with their count.
        """
        error_code = "c52c"

        filtered_df = self.df.filter(
            (col("C_INST") != "3675")
            & (col("C_INST") != "3677")  # Exclude UU  # Exclude USU
        )

        # Group by institution and CRN, count occurrences, and filter for counts greater than 1
        error_df = (
            filtered_df.groupBy("C_INST", "C_CRN")
            .agg(count("*").alias("Count"))
            .filter(col("Count") > 1)
            .select("C_INST", "C_CRN", "Count")
            .orderBy("C_CRN")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c53_validate_site_type2(self) -> DataFrame:
        """
        C53 Site Type 2. Checks for valid values in c_site_type2.

        Returns DataFrame with records having invalid or missing c_site_type2 values.
        """
        error_code = "c53"

        distinct_site_type_code_list = self.get_distinct_site_type_list()

        error_df = (
            self.df.filter(
                (
                    ~self.df.C_SITE_TYPE2.isin(distinct_site_type_code_list)
                    | (col("C_SITE_TYPE2") == "")
                )
                & (col("C_EXTRACT") == "3")
            )
            .select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_DELIVERY_METHOD",
                "C_INSTRUCT_TYPE",
                "C_CRN",
                "C_SITE_TYPE2",
            )
            .orderBy("C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c53a_validate_values_site_type2(self) -> DataFrame:
        """
        C53A Site Type2. Checks for valid values in c_site_type2 and flags as fatal error on EOT.

        Returns DataFrame with courses having invalid c_site_type2 values.
        """
        error_code = "c53a"

        distinct_site_type_code_list = self.get_distinct_site_type_list()

        error_df = (
            self.df.filter(
                (~self.df.C_SITE_TYPE2.isin(distinct_site_type_code_list))
                & (~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y"]))
                & (col("C_INSTRUCT_TYPE").isin(["LEC", "LEL", "LAB"]))
                & (col("C_EXTRACT") == "E")
            )
            .select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_DELIVERY_METHOD",
                "C_INSTRUCT_TYPE",
                "C_CRN",
                "C_SITE_TYPE2",
            )
            .orderBy("C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c54_validate_site_type3(self) -> DataFrame:
        """
        C54 Site Type 3. Checks for valid values in c_site_type3.

        Returns DataFrame with records having invalid or missing c_site_type3 values.
        """
        error_code = "c54"

        distinct_site_type_code_list = self.get_distinct_site_type_list()

        error_df = (
            self.df.filter(
                (
                    ~self.df.C_SITE_TYPE3.isin(distinct_site_type_code_list)
                    | (col("C_SITE_TYPE3") == "")
                )
                & (col("C_EXTRACT") == "3")
            )
            .select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_DELIVERY_METHOD",
                "C_INSTRUCT_TYPE",
                "C_CRN",
                "C_SITE_TYPE2",
            )
            .orderBy("C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c54a_validate_values_site_type3(self) -> DataFrame:
        """
        C54A Site Type3. Checks for valid values in c_site_type3 and flags as fatal error on EOT.

        Returns DataFrame with courses having invalid c_site_type3 values.
        """
        error_code = "c54a"

        distinct_site_type_code_list = self.get_distinct_site_type_list()

        error_df = (
            self.df.filter(
                (~self.df.C_SITE_TYPE3.isin(distinct_site_type_code_list))
                & (~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y"]))
                & (col("C_INSTRUCT_TYPE").isin(["LEC", "LEL", "LAB"]))
                & (col("C_EXTRACT") == "E")
            )
            .select(
                "C_INST",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_DELIVERY_METHOD",
                "C_INSTRUCT_TYPE",
                "C_CRN",
                "C_SITE_TYPE2",
            )
            .orderBy("C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c22f_invalid_room_type(self) -> DataFrame:
        """
        C-22f invalid room type
        Room Type is missing with Delivery Method = B
        """
        error_code = "c22f"
        courses_load_prod_df = self.courses_load_prod_df
        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = courses_load_prod_df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ).filter(
            (courses_load_prod_df.C_SITE_TYPE.isin(site_type_list))
            & (
                (courses_load_prod_df.C_ROOM_TYPE.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE == "")
            )
            & (
                (courses_load_prod_df.C_ROOM_TYPE2.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE2 == "")
            )
            & (
                (courses_load_prod_df.C_ROOM_TYPE3.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE3 == "")
            )
            & (~courses_load_prod_df.C_DAYS.isNull())
            & (courses_load_prod_df.C_DELIVERY_METHOD == "B")
            & (courses_load_prod_df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (courses_load_prod_df.C_EXTRACT == "E")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c22g_invalid_room_type(self) -> DataFrame:
        """
        C-22G invalid room type
        Room Type is missing with Delivery Method = P
        """
        error_code = "c22g"
        courses_load_prod_df = self.courses_load_prod_df
        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = courses_load_prod_df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ).filter(
            (courses_load_prod_df.C_SITE_TYPE.isin(site_type_list))
            & (
                (courses_load_prod_df.C_ROOM_TYPE.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE == "")
            )
            & (
                (courses_load_prod_df.C_ROOM_TYPE2.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE2 == "")
            )
            & (
                (courses_load_prod_df.C_ROOM_TYPE3.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE3 == "")
            )
            & (~courses_load_prod_df.C_DAYS.isNull())
            & (courses_load_prod_df.C_DELIVERY_METHOD == "P")
            & (courses_load_prod_df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (courses_load_prod_df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c22h_invalid_room_type(self) -> DataFrame:
        """
        C-22H invalid room type
        Room Type is missing with Delivery Method = P
        """
        error_code = "c22h"
        courses_load_prod_df = self.courses_load_prod_df
        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = courses_load_prod_df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME",
            "C_BLDG_NUM",
            "C_ROOM_NUM",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ).filter(
            (courses_load_prod_df.C_SITE_TYPE.isin(site_type_list))
            & (
                (courses_load_prod_df.C_ROOM_TYPE.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE == "")
            )
            & (
                (courses_load_prod_df.C_ROOM_TYPE2.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE2 == "")
            )
            & (
                (courses_load_prod_df.C_ROOM_TYPE3.isNull())
                | (courses_load_prod_df.C_ROOM_TYPE3 == "")
            )
            & (~courses_load_prod_df.C_DAYS.isNull())
            & (courses_load_prod_df.C_DELIVERY_METHOD == "P")
            & (courses_load_prod_df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (courses_load_prod_df.C_EXTRACT == "E")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c23a_invalid_course_start_time(self) -> DataFrame:
        """
        C-23a invalid course start time 2
        Invalid course start time2
        """
        error_code = "c23a"
        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_START_TIME",
            "C_START_TIME2",
        ).filter(
            ((~self.df.C_START_TIME2.isNull()) | (self.df.C_START_TIME2 != ""))
            & ((self.df.C_START_TIME.isNull()) | (self.df.C_START_TIME == ""))
            & (self.df.C_SITE_TYPE != "V")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c24a_invalid_course_stop_time(self) -> DataFrame:
        """
        C-24a invalid course stop time 2
        Invalid course stop time2
        """
        error_code = "c24a"
        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_STOP_TIME",
            "C_STOP_TIME2",
        ).filter(
            ((~self.df.C_STOP_TIME2.isNull()) | (self.df.C_STOP_TIME2 != ""))
            & ((self.df.C_STOP_TIME.isNull()) | (self.df.C_STOP_TIME == ""))
            & (self.df.C_SITE_TYPE != "V")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c25a_invalid_course_days2(self) -> DataFrame:
        """
        C-25a invalid C_Days2
        Invalid C_days2 value
        """
        error_code = "c24a"
        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_DAYS",
            "C_DAYS2",
        ).filter(
            ((~self.df.C_DAYS2.isNull()) | (self.df.C_DAYS2 != ""))
            & ((self.df.C_DAYS.isNull()) | (self.df.C_DAYS == ""))
            & (self.df.C_SITE_TYPE != "V")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    # def c26a_invalid_building_name(self) -> DataFrame:
    #     """
    #     C-26a invalid BLDG2

    #     BLDG2 name and number are the same.

    #     """
    #     error_code = "c24a"
    #     error_df = self.df.select(
    #         "C_INST",
    #         "C_CRS_SBJ",
    #         "C_CRS_NUM",
    #         "C_CRS_SEC",
    #         "C_CRN",
    #         "C_BLDG_SNAME2",
    #         "C_BLDG_NUM2",
    #     ).filter(
    #         ((self.df.C_DAYS2 == self.df.C_BLDG_NUM2))
    #         & (~self.df.C_BLDG_NUM2.isNull())
    #     )

    #     self.push_error_dataframe_if_errors_found(error_code, error_df)

    #     return error_df

    def c27a_buildingnum2_cond_required(self) -> DataFrame:
        """
        C-27a building type required
        BLDG NUM2 conditionally required
        """
        error_code = "c27a"
        # ref_df_key = "site_type"
        # column_name = "S_Type_Code"
        # where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        # site_type_list = self.get_valid_value_list(
        #     ref_df_key, column_name, where_clause
        # )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_NUM2",
        ).filter((self.df.C_BLDG_NUM2.isNull()) | (self.df.C_BLDG_NUM2 == ""))

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c27b_buildingnum2_cond_required(self) -> DataFrame:
        """
        C-27b building num 2 cond required
        """
        error_code = "c27b"
        # courses_load_prod_df = self.courses_load_prod_df
        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_NUM2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & ((self.df.C_BLDG_NUM2.isNull()) | (self.df.C_BLDG_NUM2 == ""))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE2.isin("110", "120"))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_EXTRACT == "E")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c27e_buildingnum2_cond_required(self) -> DataFrame:
        """
        C-27e building num 2 cond required
        """
        error_code = "c27e"
        # courses_load_prod_df = self.courses_load_prod_df
        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_BLDG_NUM2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & ((self.df.C_BLDG_NUM2.isNull()) | (self.df.C_BLDG_NUM2 == ""))
            & (self.df.C_BUDGET_CODE != "SF")
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c28a_roomnum2_cond_required(self) -> DataFrame:
        """
        C-28a room num 2 cond required
        """
        error_code = "c28a"
        # courses_load_prod_df = self.courses_load_prod_df
        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_NUM2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & ((self.df.C_ROOM_NUM2.isNull()) | (self.df.C_ROOM_NUM2 == ""))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE2.isin("110", "210"))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c28b_roomnum2_cond_required(self) -> DataFrame:
        """
        C-28b room num 2 cond required
        """
        error_code = "c28b"
        # courses_load_prod_df = self.courses_load_prod_df
        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS2",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_NUM2",
        ).filter(
            (self.df.C_SITE_TYPE2.isin(site_type_list))
            & ((self.df.C_ROOM_NUM2.isNull()) | (self.df.C_ROOM_NUM2 == ""))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE2.isin("110", "210"))
            & (~self.df.C_DAYS2.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_EXTRACT == "E")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c27c_building_num2_not_in_building_inventory(self) -> DataFrame:
        """
        C-27c BLDG NUM2 not in Building Inventory
        """
        error_code = "c27c"
        buildings_prod_df = self.buildings_prod_df
        c_inst_list = self.df.first()["C_INST"]
        where_clause = f"B_INST = '{c_inst_list}' AND B_INST <> '5221'"
        error_df1 = (
            buildings_prod_df.groupby("B_INST", "B_NUMBER")
            .agg(F.max("B_YEAR").alias("B_MAX_YEAR"))
            .select("B_INST", "B_NUMBER")
            .distinct()
            .where(where_clause)
        )

        cond = [
            self.df.C_INST == error_df1.B_INST,
            self.df.C_BLDG_NUM2 == error_df1.B_NUMBER,
        ]

        error_df = (
            self.df.join(error_df1, cond, "left")
            .select(
                "C_INST",
                "C_BLDG_SNAME2",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_DAYS2",
                "C_DELIVERY_METHOD",
                "C_INSTRUCT_TYPE",
                "C_SITE_TYPE2",
                "C_CRN",
                "C_BLDG_NUM2",
            )
            .filter(
                (error_df1.B_NUMBER.isNull())
                & (~self.df.C_BLDG_NUM2.isNull())
                & (self.df.C_INST != "5221")
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c27d_building_num2_not_in_room_inventory(self) -> DataFrame:
        """
        C-27d BLDG NUM2 not in room Inventory
        """
        error_code = "c27d"
        rooms_prod_df = self.rooms_prod_df
        c_inst_list = self.df.first()["C_INST"]
        where_clause = f"R_INST = '{c_inst_list}' AND R_INST <> '5221'"
        error_df1 = (
            rooms_prod_df.groupby("R_INST", "R_BUILD_NUMBER")
            .agg(F.max("R_YEAR").alias("R_MAX_YEAR"))
            .select("R_INST", "R_BUILD_NUMBER")
            .distinct()
            .where(where_clause)
        )
        cond = [
            self.df.C_INST == error_df1.R_INST,
            self.df.C_BLDG_NUM == error_df1.R_BUILD_NUMBER,
        ]
        error_df = (
            self.df.join(error_df1, cond, "left")
            .select(
                "C_INST",
                "C_BLDG_SNAME2",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_DAYS2",
                "C_DELIVERY_METHOD",
                "C_INSTRUCT_TYPE",
                "C_SITE_TYPE2",
                "C_CRN",
                "C_BLDG_NUM2",
            )
            .filter(
                (error_df1.R_BUILD_NUMBER.isNull())
                & (~self.df.C_BLDG_NUM2.isNull())
                & (self.df.C_INST != "5221")
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c41b_course_title_invalid(self) -> DataFrame:
        """
        C-41b invalid course number
        Course title needs a minimum of 2 alpha characters together
        """

        error_code = "c41b"
        error_df = self.df.select(
            "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN", "C_TITLE"
        ).filter(
            ((~self.df.C_TITLE.isNull()) | (self.df.C_TITLE != ""))
            & (~self.df.C_TITLE.rlike("[a-z][A-Z]"))
            & (~self.df.C_TITLE.isin("C++", "P90X"))
            & (self.df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c41c_course_title_invalid(self) -> DataFrame:
        """
        C-41c invalid course number
        Course title needs a minimum of 2 alpha characters together
        """

        error_code = "c41c"
        error_df = self.df.select(
            "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN", "C_TITLE"
        ).filter(
            ((~self.df.C_TITLE.isNull()) | (self.df.C_TITLE != ""))
            & (~self.df.C_TITLE.rlike("[a-z][A-Z]"))
            & (~self.df.C_TITLE.isin("P90X"))
            & (self.df.C_EXTRACT == "e")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c41d_course_title_invalid(self) -> DataFrame:
        """
        C-41d invalid course number
        Course title special characters are not allowed
        """

        error_code = "c41d"
        error_df = self.df.select(
            "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN", "C_TITLE"
        ).filter(
            ((~self.df.C_TITLE.isNull()) | (self.df.C_TITLE != ""))
            & (self.df.C_TITLE.rlike(r"^[a-zA-Z0-9 /&()#+;:.\-]+$"))
            & (~self.df.C_TITLE.rlike("''"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c42a_instructor_id_invalid(self) -> DataFrame:
        """
        C-42a invalid instructor number id
        Course title needs a minimum of 2 alpha characters together
        """

        error_code = "c42a"
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_NAME",
            "C_INSTRUCT_ID",
        ).filter(
            (~self.df.C_INSTRUCT_ID.isNull())
            | (self.df.C_INSTRUCT_ID != "")
            # &(F.length(self.df.C_TITLE) == "9")
            # &(~self.df.C_TITLE.isin("P90X"))
            # &(self.df.C_EXTRACT == "e")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c42b_instructor_id_invalid(self) -> DataFrame:
        """
        C-42a invalid instructor number id
        Course title needs 9 alpha num characters together and makes sure it is not an ssn
        """

        error_code = "c42b"
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_NAME",
            "C_INSTRUCT_ID",
        ).filter(
            ((~self.df.C_INSTRUCT_ID.isNull()) | (self.df.C_INSTRUCT_ID != ""))
            & (F.length(self.df.C_INSTRUCT_ID) == "9")
            & (~self.df.C_INSTRUCT_ID.rlike("[a-z]"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c42c_invalid_instruct_id(self) -> DataFrame:
        """
        C-42c invalid instruct type
        C_Instruct_ID is using the wrong institutional assigned alpha characters
        """

        error_code = "c42c"
        ref_df_key = "inst"
        column_name = "I_Banner"
        where_clause = "Inactive <> 'Y' and i_banner is not NULL"
        banner_id_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_NAME",
            "C_INSTRUCT_ID",
        ).filter(
            ((~self.df.C_INSTRUCT_ID.isNull()) | (self.df.C_INSTRUCT_ID != ""))
            & (self.df.C_INSTRUCT_ID.rlike("[a-zA-Z]"))
            & (~F.substring(self.df.C_INSTRUCT_ID, 1, 1).isin(banner_id_list))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c43a_instructor_name_invalid(self) -> DataFrame:
        """
        C-42a invalid instructor name
        Instructor name is null or missing
        """

        error_code = "c43a"
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_NAME",
            "C_INSTRUCT_ID",
        ).filter(
            (self.df.C_INSTRUCT_NAME.isNull())
            | (self.df.C_INSTRUCT_NAME != "")
            # &(self.df.C_INSTRUCT_NAME.rlike("[^ -`a-Z]"))
            # &(~self.df.C_INSTRUCT_NAME.rlike("''"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c43c_instructor_name_invalid(self) -> DataFrame:
        """
        C-43c invalid instructor name
        Instructor name should not contain special characters
        """

        error_code = "c43c"
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_NAME",
            "C_INSTRUCT_ID",
        ).filter((self.df.C_INSTRUCT_NAME.isNull()) | (self.df.C_INSTRUCT_NAME != ""))

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c44_instruction_type_invalid(self) -> DataFrame:
        """
        C-44 instruction type invalid
        Instruction type cannot be null
        """

        error_code = "c44"
        error_df = self.df.select(
            "C_YEAR",
            "C_INST",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_TYPE",
        ).filter((self.df.C_INSTRUCT_TYPE.isNull()) | (self.df.C_INSTRUCT_TYPE != ""))

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c44a_valid_instruct_type(self) -> DataFrame:
        """
        C-44a valid values for instruct type
        Checking for valid values for c_instruct_type.
        """

        error_code = "c44a"
        ref_df_key = "instruct_type"
        column_name = "Instruct_Type_Code"
        where_clause = "Inactive <> 'Y'"
        instruct_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )
        error_df = self.df.select(
            "C_YEAR",
            "C_INST",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_INSTRUCT_TYPE",
        ).filter(
            ((~self.df.C_INSTRUCT_TYPE.isNull()) | (self.df.C_INSTRUCT_TYPE != ""))
            & (~(self.df.C_INSTRUCT_TYPE.isin(instruct_type_list)))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c45_college_exists(self) -> DataFrame:
        """
        C-45 college exists
        Instruction type cannot be null
        """

        error_code = "c45"
        error_df = self.df.select(
            "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN", "C_COLLEGE"
        ).filter(
            (~self.df.C_INST.isin("63"))
            & ((self.df.C_COLLEGE.isNull()) | (self.df.C_COLLEGE == ""))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c45a_invalid_college_name(self) -> DataFrame:
        """
        C-45a invalid college
        C_College should only contain alpha.
        """

        error_code = "c45a"
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_COLLEGE",
        ).filter(
            ((~self.df.C_COLLEGE.isNull()) | (self.df.C_COLLEGE != ""))
            & (self.df.C_COLLEGE.rlike("[a-zA-Z]"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c46_department_exists(self) -> DataFrame:
        """
        C-46 department exists
        C_DEPT should only contain alpha.
        """

        error_code = "c46"
        error_df = (
            self.df.select("C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_DEPT")
            .filter(self.df.C_DEPT.isNull() | (self.df.C_DEPT == ""))
            .groupby("C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_DEPT")
            .count()
            .withColumnRenamed("count", "LEVELS")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c46a_department_invalid(self) -> DataFrame:
        """
        C-46a invalid college
        C_DEPT should only contain alpha.
        """

        error_code = "c46a"
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_COLLEGE",
            "C_DEPT",
        ).filter(
            ((~self.df.C_DEPT.isNull()) | (self.df.C_DEPT != ""))
            & (self.df.C_DEPT.rlike("[a-zA-Z]"))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c47a_compare_c_gen_ed(self) -> DataFrame:
        """
        C-47a c_gen_ed count
        See how many c_gen_ed codes there are compared to the total.
        """

        error_code = "c47a"
        error_df1 = (
            self.df.select(
                "C_INST",
            )
            .groupby("C_INST")
            .count()
            .withColumnRenamed("count", "Total Count")
        )

        error_df2 = (
            self.df.groupby(self.df.C_INST)
            .agg(F.count("C_GEN_ED").alias("Gen Ed Count"))
            .select(self.df.C_INST.alias("INST"), "Gen Ed Count")
        )

        error_df = error_df1.join(
            error_df2, error_df1.C_INST == error_df2.INST, "left"
        ).select("C_INST", "Gen Ed Count", "Total Count")

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c47b_gen_ed_invalid(self) -> DataFrame:
        """
        C-47b gen ed values invalid
        C_GenEd valid values
        """

        error_code = "c47b"
        ref_df_key = "general_ed"
        column_name = "general_ed_code"
        where_clause = "Inactive <> 'Y'"
        gen_ed_code_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )
        error_df = self.df.select(
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_GEN_ED",
        ).filter(~self.df.C_GEN_ED.isin(gen_ed_code_list))

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c48a_dest_site_invalid(self) -> DataFrame:
        """
        C-48a valid values for C_DEST_SITE
        Checking for valid values for C_DEST_SITE.
        """

        error_code = "c48a"
        ref_df_key = "highschools"
        column_name = "hs_act_code"
        # where_clause = "Inactive <> 'Y'"
        dest_site_list = self.get_valid_value_list(ref_df_key, column_name)
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BUDGET_CODE",
            "C_CRN",
            "C_DEST_SITE",
        ).filter(
            ((~self.df.C_DEST_SITE.isNull()) | (self.df.C_DEST_SITE != ""))
            & (~(self.df.C_DEST_SITE.isin(dest_site_list)))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c49a_invalid_students_enrolled(self) -> DataFrame:
        """
        C-49a invalid students enrolled
        C_CLASS_SIZE Number of students enrolled
        """

        error_code = "c49a"
        error_df = self.df.select(
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_CLASS_SIZE",
        ).filter((self.df.C_CLASS_SIZE.isNull()) | (self.df.C_CLASS_SIZE == "0"))

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c49b_invalid_class_size(self) -> DataFrame:
        """
        C-49b invalid class size
        C_CLASS_SIZE Number invalid
        """

        error_code = "c49b"
        error_df = (
            self.df.select(
                "C_INST", "C_CRS_SBJ", "C_CRS_NUM", "C_CRS_SEC", "C_CRN", "C_CLASS_SIZE"
            )
            .withColumn(
                "C_CLASS_SIZE_Error",
                when(col("C_CLASS_SIZE").cast("float").isNull(), "Can not be blank")
                .when(length(col("C_CLASS_SIZE")) > 4, "Too many digits")
                .when(col("C_CLASS_SIZE").cast("float") > 9999, "Exceeds 9999 Students")
                .when(col("C_CLASS_SIZE").cast("float") < 0, "Can not be negative")
                .when(col("C_CLASS_SIZE").rlike("[^0-9]"), "Contains Non_Numeric")
                .otherwise(None),
            )
            .filter(
                (col("C_CLASS_SIZE").cast("float").isNull())
                | (length(col("C_CLASS_SIZE")) > 4)
                | (col("C_CLASS_SIZE").cast("float") > 9999)
                | (col("C_CLASS_SIZE").cast("float") < 0)
                | (col("C_CLASS_SIZE").rlike("[^0-9]")) & (col("C_CLASS_SIZE") != "0")
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c49c_count_c_delivery_model(self) -> DataFrame:
        """
        C49c C_CLASS_SIZE
        """
        studentcourses_df = self.studentcourses_df
        error_code = "c49c"
        error_df1 = studentcourses_df.select("SC_ID", "SC_C_KEY")
        error_df2 = self.df.select(
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_CLASS_SIZE",
            "C_KEY",
        )

        error_df = (
            error_df1.join(error_df2, error_df1.SC_C_KEY == error_df2.C_KEY, "left")
            .select(
                "C_INST",
                "C_YEAR",
                "C_TERM",
                "C_EXTRACT",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_CRN",
                "C_CLASS_SIZE",
            )
            .groupBy(
                "C_INST",
                "C_YEAR",
                "C_TERM",
                "C_EXTRACT",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_CRN",
                "C_CLASS_SIZE",
            )
            .count()
            .withColumnRenamed("count", "COUNT_SC_ID")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c50_count_c_delivery_model(self) -> DataFrame:
        """
        C-50 C_DELIVERY Model
        """

        error_code = "c50"
        error_df = (
            self.df.select(
                "C_INST",
                "C_YEAR",
                "C_TERM",
                "C_EXTRACT",
                "C_DELIVERY_MODEL",
            )
            .groupby("C_INST", "C_YEAR", "C_TERM", "C_EXTRACT", "C_DELIVERY_MODEL")
            .count()
            .withColumnRenamed("count", "COURSES")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c51a_invalid_c_level(self) -> DataFrame:
        """
        C-51a valid values for C_LEVEL
        Checking for valid values for C_LEVEL.
        """

        error_code = "c51a"
        error_df = self.df.select(
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_LEVEL",
        ).filter(
            (~self.df.C_LEVEL.isin("R", "U", "G"))
            | (self.df.C_LEVEL.isNull())
            | (self.df.C_LEVEL == "")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c51b_invalid_c_level_remedial(self) -> DataFrame:
        """
        C-51b C_LEVEL remedial
        C_LEVEL remedial
        """

        error_code = "c51b"
        condition = (
            (self.df.C_LEVEL == "R")
            & (~self.df.C_CRS_SBJ.isin("MATH", "MAT", "ENGL", "RDG", "WRTG", "ESL"))
            & (
                ~F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM, self.df.C_INST).isin(
                    "WDEV002163", "WDEV002263", "WDEV021063", "WDEV025063"
                )
            )
            & (
                ~F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM, self.df.C_INST).isin(
                    "SPED01003677", "EDUC01003677"
                )
            )
            & (~F.concat(self.df.C_CRS_SBJ, self.df.C_INST).isin("IEP3678", "FESL3678"))
            & (
                F.concat(self.df.C_CRS_SBJ, self.df.C_CRS_NUM, self.df.C_INST)
                != "ENGH08904027"
            )
        )

        error_df = self.df.select(
            "C_INST",
            "C_YEAR",
            "C_TERM",
            "C_EXTRACT",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_LEVEL",
        ).filter(condition)

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c28c_missing_room_num2(self) -> DataFrame:
        """
        C-28C missing Room Num2
        Returns error dataframe if Room Num2 is missing
        """
        error_code = "c28c"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_ROOM_NUM2.isNull() | (self.df.C_ROOM_NUM2 == ""))
            & ~self.df.C_DAYS2.isNull()
            & ~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y"])
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & (self.df.C_BUDGET_CODE != "SF")
            & self.df.C_SITE_TYPE2.isin(distinct_site_type_code)
        )

        error_df = error_df.select(
            "C_INST",
            "C_BLDG_NUM2",
            "C_ROOM_TYPE2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE2",
            "C_CRN",
            "C_ROOM_NUM2",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c29a_invalid_room_max2(self) -> DataFrame:
        """
        C-29A invalid Room Max2
        Returns error dataframe if Room Max2 is invalid
        """
        error_code = "c29a"
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRN",
            "C_ROOM_MAX2",
            F.expr(
                """
                    (Case
                        WHEN CAST(C_ROOM_MAX2 AS FLOAT) = "" THEN "Can not be blank"
                        WHEN LENGTH(C_ROOM_MAX2) > "4" THEN "Too many digits"
                        WHEN CAST(C_ROOM_MAX2 AS FLOAT) > "9999" THEN "Exceeds 99999 HRS"
                        WHEN CAST(C_ROOM_MAX2 AS FLOAT) < "0" THEN "Can not be negitive"
                        WHEN C_ROOM_MAX2 LIKE "%[^0-9]%" THEN "Contains Non_Numeric"
                    End) AS C_ROOM_MAX2_error
                """
            ),
        ).where(
            """
                (
                    CAST(C_ROOM_MAX2 AS FLOAT) = ""
                    OR LENGTH(C_ROOM_MAX2) > "4"
                    OR CAST(C_ROOM_MAX2 AS FLOAT) > "9999"
                    OR CAST(C_ROOM_MAX2 AS FLOAT) < "0"
                    OR C_ROOM_MAX2 LIKE "%[^0-9]%"
                )
                AND C_ROOM_MAX2 <> "0"
            """
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c30(self) -> DataFrame:
        """
        C-30
        Returns error dataframe if
        """

        pass

    def c30a_invalid_room_type2(self) -> DataFrame:
        """
        C-30a invalid Room Type2
        Returns error dataframe if Room Type2 is invalid
        """
        error_code = "c30a"

        distinct_rooms_use_code = self.get_distinct_rooms_use_code()

        error_df = self.df.filter(
            ~self.df.C_ROOM_TYPE2.isin(distinct_rooms_use_code)
            & (~self.df.C_ROOM_TYPE2.isNull() | (self.df.C_ROOM_TYPE2 != ""))
        )

        error_df = error_df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_ROOM_TYPE2",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c30b_missing_room_type2(self) -> DataFrame:
        """
        C-30b missing Room Type2
        Returns error dataframe if Room Type2 is missing
        """
        error_code = "c30b"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_ROOM_TYPE2.isNull() | (self.df.C_ROOM_TYPE2 == ""))
            & ~self.df.C_DAYS2.isNull()
            & ~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y", "B", "P"])
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & self.df.C_SITE_TYPE2.isin(distinct_site_type_code)
            & (self.df.C_EXTRACT == "3")
        )

        error_df = error_df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c30c_missing_room_type2(self) -> DataFrame:
        """
        C-30c missing Room Type2
        Returns error dataframe if Room Type2 is missing
        """
        error_code = "c30c"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_ROOM_TYPE2.isNull() | (self.df.C_ROOM_TYPE2 == ""))
            & ~self.df.C_DAYS2.isNull()
            & ~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y", "B", "P"])
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & self.df.C_SITE_TYPE2.isin(distinct_site_type_code)
            & (self.df.C_EXTRACT == "E")
        )

        error_df = error_df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c30d_room_type2_in_space_utilization(self) -> DataFrame:
        """
        C-30d Room type2 that are included in Space Utilization
        Returns error dataframe if room types are included in space utilization
        """

        cols_to_select = [
            "C_INST",
            "C_ROOM_TYPE2",
            "R_Use_Name",
        ]

        error_code = "c30d"

        rooms_use_code_df = self.reference_dataframes["rooms_use_code"]
        distinct_site_type_code = self.get_distinct_site_type_code_list()

        cond = [
            self.df.C_ROOM_TYPE2 == rooms_use_code_df.R_Use_Code,
        ]

        error_df = (
            self.df.join(rooms_use_code_df, cond)
            .filter(
                ~self.df.C_DAYS2.isNull()
                & ~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y"])
                & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
                & (self.df.C_BUDGET_CODE != "SF")
                & self.df.C_SITE_TYPE2.isin(distinct_site_type_code)
            )
            .select(cols_to_select)
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c30e_missing_room_type2(self) -> DataFrame:
        """
        C-30e missing Room Type2 with Delivery Method = B
        Returns error dataframe if Room Type2 is missing with Delivery Method = B
        """
        error_code = "c30e"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_ROOM_TYPE.isNull() | (self.df.C_ROOM_TYPE == ""))
            & (self.df.C_ROOM_TYPE2.isNull() | (self.df.C_ROOM_TYPE2 == ""))
            & ~self.df.C_DAYS2.isNull()
            & (self.df.C_DELIVERY_METHOD == "B")
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & self.df.C_SITE_TYPE2.isin(distinct_site_type_code)
            & (self.df.C_EXTRACT == "3")
        )

        error_df = error_df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c30f_missing_room_type2(self) -> DataFrame:
        """
        C-30f missing Room Type2 with Delivery Method = B
        Returns error dataframe if Room Type2 is missing with Delivery Method = B
        """
        error_code = "c30f"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_ROOM_TYPE.isNull() | (self.df.C_ROOM_TYPE == ""))
            & (self.df.C_ROOM_TYPE2.isNull() | (self.df.C_ROOM_TYPE2 == ""))
            & ~self.df.C_DAYS2.isNull()
            & (self.df.C_DELIVERY_METHOD == "B")
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & self.df.C_SITE_TYPE2.isin(distinct_site_type_code)
            & (self.df.C_EXTRACT == "E")
        )

        error_df = error_df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c30g_missing_room_type2(self) -> DataFrame:
        """
        C-30g missing Room Type2 with Delivery Method = P
        Returns error dataframe if Room Type2 is missing with Delivery Method = P
        """
        error_code = "c30g"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_ROOM_TYPE.isNull() | (self.df.C_ROOM_TYPE == ""))
            & (self.df.C_ROOM_TYPE2.isNull() | (self.df.C_ROOM_TYPE2 == ""))
            & ~self.df.C_DAYS2.isNull()
            & (self.df.C_DELIVERY_METHOD == "P")
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & self.df.C_SITE_TYPE2.isin(distinct_site_type_code)
            & (self.df.C_EXTRACT == "3")
        )

        error_df = error_df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c30h_missing_room_type2(self) -> DataFrame:
        """
        C-30h missing Room Type2 with Delivery Method = P
        Returns error dataframe if Room Type2 is missing with Delivery Method = P
        """
        error_code = "c30h"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_ROOM_TYPE.isNull() | (self.df.C_ROOM_TYPE == ""))
            & (self.df.C_ROOM_TYPE2.isNull() | (self.df.C_ROOM_TYPE2 == ""))
            & ~self.df.C_DAYS2.isNull()
            & (self.df.C_DELIVERY_METHOD == "P")
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & self.df.C_SITE_TYPE2.isin(distinct_site_type_code)
            & (self.df.C_EXTRACT == "E")
        )

        error_df = error_df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME2",
            "C_BLDG_NUM2",
            "C_ROOM_NUM2",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c31a_invalid_start_time3(self) -> DataFrame:
        """
        C-31A Courses Start Time3 with no Start Time or Start time 2
        Returns error dataframe if Courses Start Time3 with no Start Time or Start time 2
        """
        error_code = "c31a"
        cols_to_check = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_SITE_TYPE3",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_START_TIME",
            "C_START_TIME2",
            "C_START_TIME3",
        ]
        where_clause = """
            (C_START_TIME3 IS NOT NULL OR C_START_TIME3 <> "")
            AND (
                C_START_TIME2 IS  NULL OR C_START_TIME2 = ""
                OR C_START_TIME IS  NULL OR C_START_TIME = ""
            )
            AND (C_SITE_TYPE <> "V" OR C_SITE_TYPE2 <> "V")
        """

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def c31b_missing_start_time3(self) -> DataFrame:
        """
        C-31B missing Start Time3
        Returns error dataframe if Start Time3 is missing
        """
        error_code = "c31b"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_START_TIME3.isNull() | (self.df.C_START_TIME3 == ""))
            & ~self.df.C_DAYS3.isNull()
            & ~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y"])
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & (self.df.C_BUDGET_CODE != "SF")
            & self.df.C_ROOM_TYPE3.isin(["110", "210"])
            & self.df.C_SITE_TYPE3.isin(distinct_site_type_code)
            & (self.df.C_EXTRACT == "3")
        )

        error_df = error_df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_START_TIME3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c31c_missing_start_time3(self) -> DataFrame:
        """
        C-31C missing Start Time3
        Returns error dataframe if Start Time3 is missing
        """
        error_code = "c31c"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_START_TIME3.isNull() | (self.df.C_START_TIME3 == ""))
            & ~self.df.C_DAYS3.isNull()
            & ~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y"])
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & (self.df.C_BUDGET_CODE != "SF")
            & self.df.C_ROOM_TYPE3.isin(["110", "210"])
            & self.df.C_SITE_TYPE3.isin(distinct_site_type_code)
            & (self.df.C_EXTRACT == "E")
        )

        error_df = error_df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_START_TIME3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c31d_missing_start_time3(self) -> DataFrame:
        """
        C-31D missing Start Time3
        Returns error dataframe if Start Time3 is missing
        """
        error_code = "c31d"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_START_TIME3.isNull() | (self.df.C_START_TIME3 == ""))
            & ~self.df.C_DAYS3.isNull()
            & ~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y"])
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & (self.df.C_BUDGET_CODE != "SF")
            & self.df.C_SITE_TYPE3.isin(distinct_site_type_code)
        )

        error_df = error_df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_START_TIME3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c32a_invalid_stop_time3(self) -> DataFrame:
        """
        C-32A Courses Stop Time3 with no Stop Time or Stop time 2
        Returns error dataframe if Courses Stop Time3 with no Stop Time or Stop time 2
        """
        error_code = "c32a"
        cols_to_check = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_SITE_TYPE3",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_STOP_TIME",
            "C_STOP_TIME2",
            "C_STOP_TIME3",
        ]
        where_clause = """
            (C_STOP_TIME3 IS NOT NULL OR C_STOP_TIME3 <> "")
            AND (
                C_STOP_TIME2 IS  NULL OR C_STOP_TIME2 = ""
                OR C_STOP_TIME IS  NULL OR C_STOP_TIME = ""
            )
            AND (C_SITE_TYPE <> "V" OR C_SITE_TYPE2 <> "V")
        """

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def c32b_missing_stop_time3(self) -> DataFrame:
        """
        C-32B missing Stop Time3
        Returns error dataframe if Stop Time3 is missing
        """
        error_code = "c32b"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_STOP_TIME3.isNull() | (self.df.C_STOP_TIME3 == ""))
            & ~self.df.C_DAYS3.isNull()
            & ~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y"])
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & (self.df.C_BUDGET_CODE != "SF")
            & self.df.C_ROOM_TYPE3.isin(["110", "210"])
            & self.df.C_SITE_TYPE3.isin(distinct_site_type_code)
            & (self.df.C_EXTRACT == "3")
        )

        error_df = error_df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_STOP_TIME3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c32c_missing_stop_time3(self) -> DataFrame:
        """
        C-32C missing Stop Time3
        Returns error dataframe if Stop Time3 is missing
        """
        error_code = "c32c"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_STOP_TIME3.isNull() | (self.df.C_STOP_TIME3 == ""))
            & ~self.df.C_DAYS3.isNull()
            & ~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y"])
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & (self.df.C_BUDGET_CODE != "SF")
            & self.df.C_ROOM_TYPE3.isin(["110", "210"])
            & self.df.C_SITE_TYPE3.isin(distinct_site_type_code)
            & (self.df.C_EXTRACT == "E")
        )

        error_df = error_df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_STOP_TIME3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c32d_missing_stop_time3(self) -> DataFrame:
        """
        C-32D missing Stop Time3
        Returns error dataframe if Stop Time3 is missing
        """
        error_code = "c32d"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_STOP_TIME3.isNull() | (self.df.C_STOP_TIME3 == ""))
            & ~self.df.C_DAYS3.isNull()
            & ~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y"])
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & (self.df.C_BUDGET_CODE != "SF")
            & self.df.C_SITE_TYPE3.isin(distinct_site_type_code)
        )

        error_df = error_df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_STOP_TIME3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c33a_invalid_days3(self) -> DataFrame:
        """
        C-33A C_Days3 is present, but C_Days and C_Days2 is missing
        Returns error dataframe if C_Days3 is present, but C_Days and C_Days2 is missing
        """
        error_code = "c33a"
        cols_to_check = [
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_SITE_TYPE",
            "C_SITE_TYPE2",
            "C_SITE_TYPE3",
            "C_INSTRUCT_TYPE",
            "C_DAYS",
            "C_DAYS2",
            "C_DAYS3",
        ]
        where_clause = """
            (C_DAYS3 IS NOT NULL OR C_DAYS3 <> "")
            AND (
                C_DAYS2 IS  NULL OR C_DAYS2 = ""
                OR C_DAYS IS  NULL OR C_DAYS = ""
            )
            AND (C_SITE_TYPE <> "V" OR C_SITE_TYPE2 <> "V")
        """

        return self.select_cols_with_where_clause(
            error_code, cols_to_check, where_clause
        )

    def c33b_missing_days3(self) -> DataFrame:
        """
        C-33B missing Days3
        Returns error dataframe if Days3 is missing
        """
        error_code = "c33b"

        distinct_site_type_code = self.get_distinct_site_type_code_list()

        error_df = self.df.filter(
            (self.df.C_DAYS3.isNull() | (self.df.C_DAYS3 == ""))
            & ~self.df.C_ROOM_TYPE3.isNull()
            & ~self.df.C_DELIVERY_METHOD.isin(["C", "I", "V", "Y"])
            & self.df.C_INSTRUCT_TYPE.isin(["LEC", "LEL", "LAB"])
            & (self.df.C_BUDGET_CODE != "SF")
            & self.df.C_ROOM_TYPE3.isin(["110", "210"])
            & self.df.C_SITE_TYPE3.isin(distinct_site_type_code)
            & (self.df.C_EXTRACT == "3")
        )

        error_df = error_df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE3",
            "C_DAYS3",
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c33c_invalid_days3(self) -> DataFrame:
        """
        C-33c Days3 invalid
        Days3 invalid
        """
        error_code = "c33c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_DAYS2",
        ).filter(
            (self.df.C_DAYS3.isNull() | (self.df.C_DAYS3 == ""))
            & (~self.df.C_ROOM_TYPE3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE3.isin("110", "210"))
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
            & (self.df.C_EXTRACT == "e")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c33d_invalid_days3(self) -> DataFrame:
        """
        C-33d Days3 invalid
        Days3 invalid
        """
        error_code = "c33d"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_DAYS3",
        ).filter(
            (self.df.C_DAYS3.isNull() | (self.df.C_DAYS3 == ""))
            & (~self.df.C_ROOM_TYPE3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c34_same_blgd_name_number(self) -> DataFrame:
        """
        C-34 BLDG3 Name and number are the same
        BLDG3 Name and number are the same
        """
        error_code = "c34"

        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_CRN",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
        ).filter(
            (self.df.C_BLDG_SNAME3 == self.df.C_BLDG_NUM3)
            & (~self.df.C_BLDG_NUM3.isNull())
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c34a_invalid_bldg_sname3(self) -> DataFrame:
        """
        C-34a BLDG SName3 invalid
        BLDG SName3 invalid
        """
        error_code = "c34a"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_SNAME3",
        ).filter(
            (self.df.C_BLDG_SNAME3.isNull() | (self.df.C_BLDG_SNAME3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE3.isin("110", "210"))
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
            & (self.df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c34b_invalid_bldg_sname3(self) -> DataFrame:
        """
        C-34b BLDG SName3 invalid
        BLDG SName3 invalid
        """
        error_code = "c34b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_SNAME3",
        ).filter(
            (self.df.C_BLDG_SNAME3.isNull() | (self.df.C_BLDG_SNAME3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE3.isin("110", "210"))
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
            & (self.df.C_EXTRACT == "E")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c34c_invalid_bldg_sname3(self) -> DataFrame:
        """
        C-34c BLDG SName3 invalid
        BLDG SName3 invalid
        """
        error_code = "c34c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_SNAME3",
        ).filter(
            (self.df.C_BLDG_SNAME3.isNull() | (self.df.C_BLDG_SNAME3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c35a_invalid_bldg_snum3(self) -> DataFrame:
        """
        C-35a BLDG SNum3 invalid
        BLDG SNum3 invalid
        """
        error_code = "c35a"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_NUM3",
        ).filter(
            (self.df.C_BLDG_NUM3.isNull() | (self.df.C_BLDG_NUM3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE3.isin("110", "210"))
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
            & (self.df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c35b_invalid_bldg_snum3(self) -> DataFrame:
        """
        C-35b BLDG SNum3 invalid
        BLDG SNum3 invalid
        """
        error_code = "c35b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_NUM3",
        ).filter(
            (self.df.C_BLDG_NUM3.isNull() | (self.df.C_BLDG_NUM3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE3.isin("110", "210"))
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
            & (self.df.C_EXTRACT == "E")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c35c_invalid_bldg_num3(self) -> DataFrame:
        """
        C-35c invalid bldg num
        BLDG NUM3 not in Building Inventory
        """

        error_code = "c35c"
        buildings_prod_df = self.buildings_prod_df
        where_clause = "B_INST <> '5221'"
        error_df1 = (
            buildings_prod_df.groupby("B_INST", "B_NUMBER")
            .agg(F.max("B_YEAR").alias("B_MAX_YEAR"))
            .select("B_INST", "B_NUMBER")
            .distinct()
            .where(where_clause)
        )
        cond = [
            self.df.C_INST == error_df1.B_INST,
            self.df.C_BLDG_NUM3 == error_df1.B_NUMBER,
        ]
        error_df = (
            self.df.join(error_df1, cond, "left")
            .select(
                "C_INST",
                "C_BLDG_SNAME3",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_DAYS3",
                "C_DELIVERY_METHOD",
                "C_INSTRUCT_TYPE",
                "C_SITE_TYPE3",
                "C_CRN",
                "C_BLDG_NUM3",
            )
            .filter(
                (error_df1.B_NUMBER.isNull())
                & (~self.df.C_BLDG_NUM3.isNull())
                & (self.df.C_INST != "5221")
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c35d_invalid_r_bldg_num3(self) -> DataFrame:
        """
        C-35d invalid r bldg num3
        BLDG NUM3 not in Rooms Inventory
        """

        error_code = "c35d"
        rooms_prod_df = self.rooms_prod_df
        where_clause = "R_INST <> '5221'"
        error_df1 = (
            rooms_prod_df.groupby("R_INST", "R_BUILD_NUMBER")
            .agg(F.max("R_YEAR").alias("R_MAX_YEAR"))
            .select("R_INST", "R_BUILD_NUMBER")
            .distinct()
            .where(where_clause)
        )
        cond = [
            self.df.C_INST == error_df1.R_INST,
            self.df.C_BLDG_NUM == error_df1.R_BUILD_NUMBER,
        ]
        error_df = (
            self.df.join(error_df1, cond, "left")
            .select(
                "C_INST",
                "C_BLDG_SNAME3",
                "C_CRS_SBJ",
                "C_CRS_NUM",
                "C_CRS_SEC",
                "C_DAYS3",
                "C_DELIVERY_METHOD",
                "C_INSTRUCT_TYPE",
                "C_SITE_TYPE3",
                "C_CRN",
                "C_BLDG_NUM3",
            )
            .filter(
                (error_df1.R_BUILD_NUMBER.isNull())
                & (~self.df.C_BLDG_NUM3.isNull())
                & (self.df.C_INST != "5221")
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c35e_invalid_bldg_snum3(self) -> DataFrame:
        """
        C-35e BLDG SNum3 invalid
        BLDG SNum3 invalid
        """
        error_code = "c35e"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_SNAME3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_BLDG_NUM3",
        ).filter(
            (self.df.C_BLDG_NUM3.isNull() | (self.df.C_BLDG_NUM3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c36a_invalid_bldg_room3(self) -> DataFrame:
        """
        C-36a BLDG Room3 invalid
        BLDG Room3 invalid
        """
        error_code = "c36a"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_ROOM_NUM3",
        ).filter(
            (self.df.C_ROOM_NUM3.isNull() | (self.df.C_ROOM_NUM3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE3.isin("110", "210"))
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
            & (self.df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c36b_invalid_bldg_room3(self) -> DataFrame:
        """
        C-36b BLDG Room3 invalid
        BLDG Room3 invalid
        """
        error_code = "c36b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_ROOM_NUM3",
        ).filter(
            (self.df.C_ROOM_NUM3.isNull() | (self.df.C_ROOM_NUM3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_ROOM_TYPE3.isin("110", "210"))
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
            & (self.df.C_EXTRACT == "E")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c36c_invalid_bldg_room3(self) -> DataFrame:
        """
        C-36c BLDG Room3 invalid
        BLDG Room3 invalid
        """
        error_code = "c36c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_INST",
            "C_BLDG_NUM3",
            "C_ROOM_TYPE3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DAYS3",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_SITE_TYPE3",
            "C_CRN",
            "C_ROOM_NUM3",
        ).filter(
            (self.df.C_ROOM_NUM3.isNull() | (self.df.C_ROOM_NUM3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_BUDGET_CODE != "SF")
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c37a_invalid_room_max3(self) -> DataFrame:
        """
        C-37A invalid C_Room_Max3
        Returns error dataframe if C_Room_Max3 is invalid
        """
        error_code = "c37a"
        error_df = self.df.select(
            "C_INST",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_BLDG_NUM3",
            "C_CRN",
            "C_ROOM_NUM3",
            "C_ROOM_MAX3",
            F.expr(
                """
                    (Case
                        WHEN CAST(C_ROOM_MAX3 AS FLOAT) = "" THEN "Can not be blank"
                        WHEN LENGTH(C_ROOM_MAX3) > "4" THEN "Too many digits"
                        WHEN CAST(C_ROOM_MAX3 AS FLOAT) > "9999" THEN "Exceeds 9999 HRS"
                        WHEN CAST(C_ROOM_MAX3 AS FLOAT) < "0" THEN "Can not be negitive"
                        WHEN C_ROOM_MAX3 LIKE "%[^0-9]%" THEN "Contains Non_Numeric"
                    End) AS C_ROOM_MAX3_error
                """
            ),
        ).where(
            """
                (
                    CAST(C_ROOM_MAX3 AS FLOAT) = ""
                    OR LENGTH(C_ROOM_MAX3) > "4"
                    OR CAST(C_ROOM_MAX3 AS FLOAT) > "9999"
                    OR CAST(C_ROOM_MAX3 AS FLOAT) < "0"
                    OR C_ROOM_MAX3 LIKE "%[^0-9]%"
                )
                AND C_ROOM_MAX3 <> "0"
            """
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c38_count_courses(self) -> DataFrame:
        """
        C-38 count of All
        Returns error dataframe with count of all
        """
        error_code = "c38"
        rooms_use_code_ref_df = self.get_rooms_use_code_reference_df()

        cond = [
            self.df.C_ROOM_TYPE3 == rooms_use_code_ref_df.R_Use_Code,
        ]

        error_df = (
            self.df.join(rooms_use_code_ref_df, cond)
            .groupBy(["C_INST", "C_ROOM_TYPE3", "R_Use_Code"])
            .agg(F.count("*").alias("Number_of_Courses"))
            .select(
                "C_INST",
                "C_ROOM_TYPE3",
                F.col("R_Use_Code").alias("Description"),
                "Number_of_Courses",
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c38b_invalid_room_type3(self) -> DataFrame:
        """
        C-38B invalid C_Room_type3
        Returns error dataframe if C_Room_type3 is invalid
        """
        error_code = "c38b"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ).filter(
            (self.df.C_ROOM_TYPE3.isNull() | (self.df.C_ROOM_TYPE3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y", "B", "P"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
            & (self.df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c38c_invalid_room_type3(self) -> DataFrame:
        """
        C-38C invalid C_Room_type3
        Returns error dataframe if C_Room_type3 is invalid
        """
        error_code = "c38c"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ).filter(
            (self.df.C_ROOM_TYPE3.isNull() | (self.df.C_ROOM_TYPE3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y", "B", "P"))
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
            & (self.df.C_EXTRACT == "E")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c38d_count_in_space_utilization(self) -> DataFrame:
        """
        C-38D invalid C_Room_type3
        Returns error dataframe if C_Room_type3 is invalid
        """
        error_code = "c38d"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        rooms_use_code_ref_df = self.get_rooms_use_code_reference_df()

        cond = [
            self.df.C_ROOM_TYPE3 == rooms_use_code_ref_df.R_Use_Code,
        ]

        error_df = (
            self.df.join(rooms_use_code_ref_df, cond)
            .filter(
                (~self.df.C_DAYS3.isNull())
                & (~self.df.C_DELIVERY_METHOD.isin("C", "I", "V", "Y"))
                & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
                & (self.df.C_BUDGET_CODE != "SF")
                & (self.df.C_SITE_TYPE3.isin(site_type_list))
            )
            .groupBy(["C_INST", "C_ROOM_TYPE3", "R_Use_Code"])
            .agg(F.count("*").alias("Number_of_Courses"))
            .select(
                "C_INST",
                "C_ROOM_TYPE3",
                F.col("R_Use_Code").alias("Description"),
                "Number_of_Courses",
            )
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def c38e_invalid_room_type3(self) -> DataFrame:
        """
        C-38E invalid C_Room_type3
        Returns error dataframe if C_Room_type3 is invalid
        """
        error_code = "c38e"

        ref_df_key = "site_type"
        column_name = "S_Type_Code"
        where_clause = "S_Type_Space_Utilize = 'Y' and Inactive = 'N'"
        site_type_list = self.get_valid_value_list(
            ref_df_key, column_name, where_clause
        )

        error_df = self.df.select(
            "C_YEAR",
            "C_INST",
            "C_BLDG_SNAME3",
            "C_BLDG_NUM3",
            "C_ROOM_NUM3",
            "C_CRS_SBJ",
            "C_CRS_NUM",
            "C_CRS_SEC",
            "C_DELIVERY_METHOD",
            "C_INSTRUCT_TYPE",
            "C_CRN",
            "C_SITE_TYPE",
            "C_ROOM_TYPE",
            "C_DAYS",
            "C_SITE_TYPE2",
            "C_ROOM_TYPE2",
            "C_DAYS2",
            "C_SITE_TYPE3",
            "C_ROOM_TYPE3",
            "C_DAYS3",
        ).filter(
            (self.df.C_ROOM_TYPE.isNull() | (self.df.C_ROOM_TYPE == ""))
            & (self.df.C_ROOM_TYPE2.isNull() | (self.df.C_ROOM_TYPE2 == ""))
            & (self.df.C_ROOM_TYPE3.isNull() | (self.df.C_ROOM_TYPE3 == ""))
            & (~self.df.C_DAYS3.isNull())
            & (self.df.C_DELIVERY_METHOD == "B")
            & (self.df.C_INSTRUCT_TYPE.isin("LEC", "LEL", "LAB"))
            & (self.df.C_SITE_TYPE3.isin(site_type_list))
            & (self.df.C_EXTRACT == "3")
        )

        self.push_error_dataframe_if_errors_found(error_code, error_df)

        return error_df

    def quality_check(self):
        """
        Performs all quality checks and publishes to pubsub with error payload if found
        """

        # Quality checks will populate the self.error_dataframes list if errors are found
        self.c00_duplicate_records()
        self.c00a_duplicate_records()
        self.c01_missing_institution_code()
        self.c02_blank_records()
        self.c04a_invalid_course_number()
        self.c04b_invalid_course_number_grad()
        self.c04c_invalid_course_number()
        self.c05_invalid_course_section()
        self.c06a_invalid_min_credits()
        self.c07a_invalid_max_credits()
        self.c07b_invalid_credits()
        self.c08a_invalid_contact_hrs()
        self.c09_invalid_line_item()
        self.c09a_invalid_line_item()
        self.c10_invalid_site_type()
        self.c10a_invalid_site_type()
        self.c11_invalid_budget_code()
        self.c11a_invalid_budget_code()
        self.c12_invalid_delivery_method()
        self.c12a_invalid_delivery_method()
        self.c13_invalid_program_type()
        self.c13a_invalid_program_type()
        self.c13b_invalid_program_type()
        self.c13c_invalid_program_type()
        self.c13e_invalid_vocational()
        self.c13f_invalid_vocational()
        self.c13g_invalid_vocational()
        self.c13i_invalid_vocational()
        self.c13j_invalid_vocational()
        self.c13k_invalid_vocational()
        self.c14a_invalid_credit_ind()
        self.c14b_invalid_credit_ind()
        self.c14c_invalid_vocational()
        self.c15a_invalid_start_time()
        self.c15b_invalid_start_time()
        self.c15c_invalid_start_time()
        self.c16a_invalid_stop_time()
        self.c16b_invalid_stop_time()
        self.c16c_invalid_stop_time()
        self.c17a_invalid_days()
        self.c17b_invalid_days()
        self.c17c_invalid_days()
        self.c18_invalid_bldg_name_and_num()
        self.c18a_invalid_bldg_name()
        self.c18b_invalid_bldg_name()
        self.c18c_invalid_bldg_name()
        self.c19a_invalid_bldg_num()
        self.c19b_invalid_bldg_num()
        self.c19e_invalid_bldg_num()
        self.c20a_invalid_room_num()
        self.c20b_invalid_room_num()
        self.c20c_invalid_room_num()
        self.c22b_invalid_room_type()
        self.c22c_invalid_room_type()
        self.c23b_invalid_start_time2()
        self.c23c_invalid_start_time2()
        self.c23d_invalid_start_time2()
        self.c24b_invalid_stop_time2()
        self.c24c_invalid_stop_time2()
        self.c24d_invalid_stop_time2()
        self.c25b_invalid_days2()
        self.c25c_invalid_days2()
        self.c25d_invalid_days2()
        self.c26a_invalid_sname2()
        self.c26b_invalid_sname2()
        self.c26c_invalid_sname2()
        self.c11b_invalid_ceml()
        self.c11c_invalid_ceml()
        self.c04d_invalid_course_number()
        self.c19c_invalid_bldg_num()
        self.c19d_invalid_r_bldg_num()
        self.c21a_invalid_room_max()
        self.c22_room_type_count()
        self.c22a_invalid_room_type()
        self.c22d_invalid_room_type()
        self.c22e_invalid_room_type()
        self.c38f_room_type_missing_delivery_method_b()
        self.c38g_room_type_missing_delivery_method_p()
        self.c38h_room_type_missing_delivery_method_p()
        self.c39a_start_date_summer()
        self.c39b_start_date_fall()
        self.c39c_start_date_spring()
        self.c40a_end_date_summer()
        self.c40b_end_date_fall()
        self.c40c_end_date_spring()
        self.c41a_title_missing()
        self.c51c_course_level_counts()
        self.c52a_missing_crn()
        self.c52b_invalid_crn()
        self.c52c_non_unique_crn()
        self.c53_validate_site_type2()
        self.c53a_validate_values_site_type2()
        self.c54_validate_site_type3()
        self.c54a_validate_values_site_type3()
        self.c22f_invalid_room_type()
        self.c22g_invalid_room_type()
        self.c22h_invalid_room_type()
        self.c23a_invalid_course_start_time()
        self.c24a_invalid_course_stop_time()
        self.c25a_invalid_course_days2()
        self.c27a_buildingnum2_cond_required()
        self.c27b_buildingnum2_cond_required()
        self.c27e_buildingnum2_cond_required()
        self.c28a_roomnum2_cond_required()
        self.c28b_roomnum2_cond_required()
        self.c27c_building_num2_not_in_building_inventory()
        self.c27d_building_num2_not_in_room_inventory()
        self.c41b_course_title_invalid()
        self.c41c_course_title_invalid()
        self.c41d_course_title_invalid()  # not passed yet.
        self.c42a_instructor_id_invalid()
        self.c42b_instructor_id_invalid()
        self.c42c_invalid_instruct_id()
        self.c43a_instructor_name_invalid()
        self.c43c_instructor_name_invalid()
        self.c44_instruction_type_invalid()
        self.c45_college_exists()
        self.c44a_valid_instruct_type()
        self.c45a_invalid_college_name()
        self.c46_department_exists()
        self.c46a_department_invalid()
        self.c48a_dest_site_invalid()
        self.c49a_invalid_students_enrolled()
        self.c49b_invalid_class_size()
        self.c51a_invalid_c_level()
        self.c47a_compare_c_gen_ed()
        self.c50_count_c_delivery_model()
        self.c51b_invalid_c_level_remedial()
        self.c49c_count_c_delivery_model()
        self.c28c_missing_room_num2()
        self.c29a_invalid_room_max2()
        self.c30()
        self.c30a_invalid_room_type2()
        self.c30b_missing_room_type2()
        self.c30c_missing_room_type2()
        self.c30d_room_type2_in_space_utilization()
        self.c30e_missing_room_type2()
        self.c30f_missing_room_type2()
        self.c30g_missing_room_type2()
        self.c30h_missing_room_type2()
        self.c31a_invalid_start_time3()
        self.c31b_missing_start_time3()
        self.c31c_missing_start_time3()
        self.c31d_missing_start_time3()
        self.c32a_invalid_stop_time3()
        self.c32b_missing_stop_time3()
        self.c32c_missing_stop_time3()
        self.c32d_missing_stop_time3()
        self.c33a_invalid_days3()
        self.c33b_missing_days3()
        self.c33c_invalid_days3()
        self.c33d_invalid_days3()
        self.c34_same_blgd_name_number()
        self.c34a_invalid_bldg_sname3()
        self.c34b_invalid_bldg_sname3()
        self.c34c_invalid_bldg_sname3()
        self.c35a_invalid_bldg_snum3()
        self.c35b_invalid_bldg_snum3()
        self.c35c_invalid_bldg_num3()
        self.c35d_invalid_r_bldg_num3()
        self.c35e_invalid_bldg_snum3()
        self.c36a_invalid_bldg_room3()
        self.c36b_invalid_bldg_room3()
        self.c36c_invalid_bldg_room3()
        self.c37a_invalid_room_max3()
        self.c38_count_courses()
        self.c38b_invalid_room_type3()
        self.c38c_invalid_room_type3()
        self.c38d_count_in_space_utilization()
        self.c38e_invalid_room_type3()

        # Roll up to the base class, which will publish the errors to pubsub if self.error_dataframes is populatedse
        super().quality_check()
