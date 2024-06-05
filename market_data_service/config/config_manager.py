import configparser
import os

class ConfigManager:
    def __init__(self, config_file=None):
        self.config = configparser.ConfigParser()
        # If no specific config file path is provided, use the default path
        if config_file is None:
            # Navigate up one directory from `config` and then to the `market_data_service` directory
            self.config_file = os.path.join(os.path.dirname(__file__), '..', 'config.ini')
        else:
            self.config_file = config_file
        self.load_config()

    def load_config(self):
        if not os.path.exists(self.config_file):
            raise FileNotFoundError(f"The configuration file {self.config_file} was not found.")
        self.config.read(self.config_file)

    def get(self, section, option):
        return self.config.get(section, option)

    def getint(self, section, option):
        return self.config.getint(section, option)

    def getfloat(self, section, option):
        return self.config.getfloat(section, option)

    def getboolean(self, section, option):
        return self.config.getboolean(section, option)
