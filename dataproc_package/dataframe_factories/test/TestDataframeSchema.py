from pyspark.sql.types import FloatType, StringType, StructField, StructType

test_schema = StructType(
    [
        StructField("First Name", StringType()),
        StructField("Last Name _Surname_", StringType()),
        StructField("Date of birth", StringType()),
        StructField("Email Address", StringType()),
        StructField("CCN Visa _ Master Card _ AMEX", StringType()),
        StructField("CVV", StringType()),
        StructField("Expiration Date", StringType()),
        StructField("SSN", StringType()),
        StructField("Address", StringType()),
        StructField("IBAN", StringType()),
        StructField("Bank Account Number", StringType()),
        StructField("Routing Number", StringType()),
        StructField("Patients MRN", StringType()),
        StructField("Blood Type", StringType()),
        StructField("Weight", StringType()),
        StructField("Height", StringType()),
    ]
)


if __name__ == "__main__":
    # Output the current struct type as a json schema.
    # Use this to generate a new schema JSON file.
    test_schema_json = test_schema.json()
    with open("test_schema.json", "w") as f:
        f.write(test_schema_json)
