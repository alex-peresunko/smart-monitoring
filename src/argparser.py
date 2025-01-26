import argparse


class ArgParser:
    def __init__(self):
        self.parser = argparse.ArgumentParser()

        # Mandatory arguments
        self.parser.add_argument('--config-file', type=str, required=True,
                                 help='Main configuration file')

        self.args = None

    def parse(self):
        self.args = vars(self.parser.parse_args())
        self.args['prog'] = self.parser.prog

        return self.args
