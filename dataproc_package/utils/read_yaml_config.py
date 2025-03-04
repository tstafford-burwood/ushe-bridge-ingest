import yaml 

def read_yaml_config(yaml_path):
    """
    Read the YAML configuration file from local path and return the dictionary of the configuration.
    """
    with open(yaml_path) as f:
        yaml_config = yaml.safe_load(f)
    return yaml_config
