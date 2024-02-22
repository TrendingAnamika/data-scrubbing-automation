from data_scrubbing_utils import *
from read_config import *


def automation_main():

    # Getting Log file name
    log_file = log_file_name(log_file_path, input_file_path)

    # Configure logging
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# Read Source Data (CSV File)
    df = read_csv(input_file_path)
    logging.info("CSV file loaded successfully.")

# Mapping parentName and masterName columns as per business requirement.
    map_df = map_parent_master_name(df, parent_name_mapping_dict, parentName, masterName, additionalTransformedColumn)
    logging.info("Data Transformation for the given columns are done successfully.")

# Scrubbing All the provided columns.
    final_scrubbing_df = final_scrubbing(map_df, scrubbingColumns)
    logging.info(f"Data Scrubbing for all columns {scrubbingColumns} are done successfully.")

# Write the Transformed DataFrame to a CSV file.
    #final_scrubbing_df.to_csv(output_file_path, index=False)
    save_output_csv_file(final_scrubbing_df, input_file_path, output_file_path)
    logging.info(f"Final Data saved successfully to location : {output_file_path}")


# Execute automation_data_scrubbing.py
if __name__ == "__main__":
    # Get config file as args[1]
    if len(sys.argv) != 2:
        print("Usage: python automation_data_scrubbing.py <horizon_mdg_customer_sku_config.ini>")
        sys.exit(1)

    config_file = sys.argv[1]
    try:
        # Loading Config File
        config = read_config(config_file)
        # Retrieve config details
        # Extract the input csv file name along with its path and path to store output csv file.
        input_file_path = config["inputCsvFile"]["input_csv_file"]
        output_file_path = config["outputCsvFile"]['output_csv_file_path']
        log_file_path = config["logFile"]['log_file_path']

        # Extract the [NameMapping] section
        parent_name_mapping_dict = dict(config['parentNameMapping'])

        # Extract Columns to be transformed according to business requirements.
        parentName = config['TransformedColumn']['ParentName']
        masterName = config['TransformedColumn']['MasterName']
        additionalTransformedColumn = config['TransformedColumn']['AdditionalTransformedColumn']

        # Extract all the columns on which Scrambling needs to be apply.
        scrubbingColumns = config['ScrubbingColumn']['columns']

    except FileNotFoundError:
        print(f"Error: Config file '{config_file}' not found.")
    except configparser.Error as e:
        print(f"Error reading config file: {e}")

    # Executing Main Script : automation_main()
    automation_main()

logging.info("Automation Data Scrubbing completed successfully.")