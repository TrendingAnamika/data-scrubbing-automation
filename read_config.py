import configparser

# Function to Read config file.
def read_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

