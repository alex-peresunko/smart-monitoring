import json
import os
from src.argparser import ArgParser


def get_source_directory():
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)


def get_env_variable(var_name):
    return os.environ.get(var_name)


class ConfigParser:
    def __init__(self):
        self.args = ArgParser().parse()
        self.config = None
        os.chdir(get_source_directory())

        if os.path.isabs(self.args['config_file']):
            self.config_file = self.args['config_file']
        else:
            self.config_file = os.path.join(get_source_directory(), 'config', self.args['config_file'])

        with open(self.config_file, 'r') as file:
            self.config = json.load(file)

        if not os.path.isabs(self.config['logging']['log_folder']):
            self.config['logging']['log_folder'] = os.path.join(get_source_directory(), self.config['logging']['log_folder'])

        self.config["newrelic"]["ingesting_key"] = get_env_variable(self.config["newrelic"]["nr_ingesting_key_env_var_prefix"] +
                                                                   "_" + str(self.config["newrelic"]["nr_ingest_account_id"]))


CONFIG = ConfigParser().config
