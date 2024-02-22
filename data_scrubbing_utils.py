import pandas as pd
import string
import logging
import sys

# Read input csv file and create a pandads DataFrame and remove leading and trailing white spaces from all column names
def read_csv(input_file_path):
    try:
        # Get the file name from input_file_path to use in logging.
        file_name = input_file_path.split('/')[1]

        # Attempt to read the CSV file
        df = pd.read_csv(input_file_path, encoding='latin-1', low_memory=False)

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        logging.info(f'Successfully read the input CSV file {file_name} and loaded it into the DataFrame.')

        return df

    except FileNotFoundError:
        logging.error(f"Error: CSV File {file_name} not found.")
        sys.exit(f"Exiting the job as file {file_name} not found.")

    except pd.errors.EmptyDataError:
        logging.error(f"Error: CSV file {file_name} is empty.")
        sys.exit(f"Exiting the job as file {file_name} is empty.")

    except pd.errors.ParserError:
        logging.error(f"Error: CSV file {file_name} is Malformed.")
        sys.exit(f"Exiting the job as {file_name} is a malformed CSV file.")


# Create a function to map the parentName values
def map_parent_name(name, parent_name_mapping_dict):
    for key in parent_name_mapping_dict :
        if name.lower().find(key.lower()) == 0:
            return parent_name_mapping_dict[key]
    return name


# Create function to apply mapping on parentName (using map_parent_name function) and masterName Column.
def map_parent_master_name(df, parent_name_mapping_dict, parentName, masterName, additionalTransformedColumn):
    # Check if 'parentName' and 'masterName' both columns are present in the DataFrame/CSV File.
    if parentName in df.columns and masterName in df.columns:
        # Apply mapping on parentName using map_parent_name function
        df[parentName] = df[parentName].apply(lambda x: map_parent_name(x, parent_name_mapping_dict))

        # Use Dense Ranking to assign unique numbers to 'masterName' within each 'parentName' group
        df[masterName] = df.groupby(parentName)[masterName].rank(method='dense', ascending=True).astype(int)

        # Add a suffix to 'masterName' according to its parentName.
        df[masterName] = df[parentName] + ' - ' + 'Client ' + df[masterName].astype(str)
        logging.info(f"{parentName} and {masterName} columns are present in the CSV file to apply transformation on it.")
        logging.info(f'Data Scrubbing for {parentName} and {masterName} is done successfully.')

        # Check if only 'parentName' columns is present in the DataFrame/CSV File.
    elif parentName in df.columns:
        df[parentName] = df[parentName].apply(lambda x: map_parent_name(x, parent_name_mapping_dict))
        logging.info(f"{parentName} column is present in CSV file to apply transformation on it.")
        logging.info(f'Data Scrubbing for {parentName} is done successfully.')

        # Check if any another column as 'additionalTransformedColumn' has been provided in the config file to apply this transformation.
    elif additionalTransformedColumn in df.columns:
        df[additionalTransformedColumn] = df[additionalTransformedColumn].apply(lambda x: map_parent_name(x, parent_name_mapping_dict))
        logging.info(f"{additionalTransformedColumn} column is present in CSV file to apply transformation on it.")
        logging.info(f'Data Scrubbing for {additionalTransformedColumn} is done successfully.')

    else:
        logging.info(f"{parentName}, {masterName} or any {additionalTransformedColumn} is not present in the CSV file to apply transformation on it.")

    return df


# Starting Scrambling to all the given columns
# Scrambling logic: substitute letters and numbers with other letters and numbers
def scrub(value):
    # Convert to string if the value is an integer
    value_str = str(value)
    # Scrambling logic: substitute letters and numbers with other letters and numbers
    letters = string.ascii_letters
    numbers = string.digits
    mapping = str.maketrans(letters + numbers, letters[::-1] + numbers[::-1])
    return value_str.translate(mapping)


# Function to apply scrambling
def apply_scrubbing(df, scrubbingColumns):
    scrubbing_columns = scrubbingColumns.split(',')
    scrubbing_columns = [col.strip() for col in scrubbing_columns]

    for col in scrubbing_columns:
        if col in df.columns:
            df[col] = df[col].apply(scrub)
        else:
            message = f"Column '{col}' not found in the CSV file. Skipping."
            logging.warning(message)

    return df


# Apply scrambling to get final transformed data.
def final_scrubbing(df, scrubbing_columns) :
    df_scrub = apply_scrubbing(df.copy(), scrubbing_columns)
    logging.info("Data Scrubbing for given columns are done.")

    return df_scrub


# Create Output File Name and Write the Transformed DataFrame to a CSV file.
def save_output_csv_file(final_scrubbing_df, input_file_path, output_file_path) :
    file_name = input_file_path.split("/")[1]
    base_name, extension = file_name.rsplit('.', 1)
    # Appending "TRANSFORMED" to the input file name
    output_file_name = f"TRANSFORMED_{base_name}.{extension}"
    output_file_path_with_name = output_file_path+"/"+output_file_name

    logging.info(f"Final Data saved successfully to location : {output_file_path_with_name}")

    final_scrubbing_df.to_csv(output_file_path_with_name, index=False)


# Creating a function to create Log File name to use for logging.
def log_file_name(log_file_path, input_file_path):
    file_name = input_file_path.split("/")[1].split(".")[0]
    # Appending "LOG" to the input file name
    log_file_name = f"LOG_{file_name}.log"
    log_file = log_file_path + "/" + log_file_name
    #logging.info(f"Log file name created to use for log file : {log_file}")
    return log_file



