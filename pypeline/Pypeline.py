import datetime
import mysql.connector
import psycopg2
import json
import importlib
import yaml
from pypeline import Pype


class Pypeline:
    def __init__(
        self, configuration_file, conn_from, conn_to, placeholders={}, debug=False
    ):

        with open(configuration_file, "r") as stream:
            try:
                self.config = yaml.load(stream)
            except yaml.YAMLError as e:
                raise e

        self.placeholders = placeholders
        self.conn_from = conn_from
        self.conn_to = conn_to
        self.debug = debug

    def run(self, pypeline, placeholders={}):
        if len(placeholders):
            self.placeholders = placeholders

        pype_configs = self.get_pypes(pypeline)

        for config in pype_configs:
            Pype.Pype(config, placeholders=self.placeholders).run(
                self.conn_from, self.conn_to
            )

    def get_pypes(self, pypeline):
        if pypeline not in self.config["pypelines"]:
            raise Exception("No pypeline named " + pypeline)

        configs = []
        for pype in self.config["pypelines"][pypeline]:
            if pype in self.config["pypes"][pype]:
                raise Exception("No pype named " + pype)

            self.config["pypes"][pype]["name"] = pype
            self.config["pypes"][pype]["debug"] = self.debug
            configs.append(self.config["pypes"][pype])

        return configs
