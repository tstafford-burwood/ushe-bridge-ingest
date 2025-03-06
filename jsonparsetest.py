import json

# Input JSON data
with open('context.json', 'r') as file:
            input_json = json.load(file)
            #return input_json
# input_json = {
#     "sourceTable": "synthetic_data.parquet",
#     "guid": "97306d24-1688-4cd6-8e2c-d4be77a70eb0",
#     "partner": "ADHOC",
#     "operation": "new",
#     "destination": "synthetic_data.parquet",
#     "mpiStrategy": "CreateAndMatch",
#     "columns": [
#         {"name": "First Name", "outputs": {"MPI": {"name": "first_name"}}},
#         {"name": "Last Name (Surname)", "outputs": {"MPI": {"name": "last_name"}}},
#         {"name": "Date of birth", "outputs": {"MPI": {"name": "birth_date"}}},
#         {"name": "Email Address", "outputs": {"DI": {"name": "Email Address"}}},
#         {"name": "CCN Visa / Master Card / AMEX", "outputs": {"DI": {"name": "CCN Visa / Master Card / AMEX"}}},
#         {"name": "CVV", "outputs": {"DI": {"name": "CVV"}}},
#         {"name": "Expiration Date", "outputs": {"DI": {"name": "Expiration Date"}}},
#         {"name": "SSN", "outputs": {"MPI": {"name": "ssn"}}},
#         {"name": "Address", "outputs": {"DI": {"name": "Address"}}},
#         {"name": "IBAN", "outputs": {"DI": {"name": "IBAN"}}},
#         {"name": "Bank Account Number", "outputs": {"DI": {"name": "Bank Account Number"}}},
#         {"name": "Routing Number", "outputs": {"DI": {"name": "Routing Number"}}},
#         {"name": "Patients MRN", "outputs": {"DI": {"name": "Patients MRN"}}},
#         {"name": "Blood Type", "outputs": {"DI": {"name": "Blood Type"}}},
#         {"name": "Weight", "outputs": {"DI": {"name": "Weight"}}},
#         {"name": "Height", "outputs": {"DI": {"name": "Height"}}}
#     ]
# }

# Split the columns into two categories based on "MPI" or "DI"
mpi_columns = []
di_columns = []

for column in input_json['columns']:
    if "MPI" in column['outputs']:
        mpi_columns.append(column['name'])
    elif "DI" in column['outputs']:
        di_columns.append(column['name'])

mpi_columns_list = [s.replace('(', '_') for s in mpi_columns]
mpi_columns_list2 = [s.replace(')', '_') for s in mpi_columns_list]
mpi_columns_list3 = [s.replace('/', '_') for s in mpi_columns_list2]
print(mpi_columns_list3)

di_columns_list = [s.replace('(', '_') for s in di_columns]
di_columns_list2 = [s.replace(')', '_') for s in di_columns_list]
di_columns_list3 = [s.replace('/', '_') for s in di_columns_list2]
print(di_columns_list3)
# Prepare two new JSON structures based on the separation
# mpi_json = input_json.copy()
# di_json = input_json.copy()

# # Update the "columns" list to contain only MPI or DI columns for each new JSON
# mpi_json['columns'] = mpi_columns
# di_json['columns'] = di_columns

# # Save the two new JSON files
# with open('mpi_columns.json', 'w') as mpi_file:
#     json.dump(mpi_json, mpi_file, indent=4)

# with open('di_columns.json', 'w') as di_file:
#     json.dump(di_json, di_file, indent=4)

# print("JSON files split and saved as 'mpi_columns.json' and 'di_columns.json'")