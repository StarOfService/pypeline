import datetime
import mysql.connector
import psycopg2
import json
import string
from importlib import import_module

class Pype:
    def __init__(self, config, placeholders={}):
        self.extract_query = config['extract_query']
        self.target_table = config['target_table']
        self.transformers = self.load_transformers(config['transformers'])
        self.placeholders = placeholders
        self.post_query = 0

        if('post_query' in config):
            self.post_query = config['post_query']

        if('bulk_size' in config):
            self.bulk_size = config['bulk_size']

    def run(self, conn_from, conn_to):
        headers = []
        insert_query = ''
        cursor_from = conn_from.cursor(dictionary=True)
        cursor_to = conn_to.cursor()
        cursor_from.execute(self.extract_query)

        while True:
            # Extract
            results = cursor_from.fetchmany(self.bulk_size)
            results_count = len(results)
            if 0 == results_count:
                break

            # Transform
            for transformer in self.transformers:
                results = list(map(transformer.filter, results))

            if 0 == len(headers):
                headers = list(results[0].keys())
                insert_query = self.build_load_query(self.target_table, headers)

            # Load
            self.upsert_data(conn_to, insert_query, results)

            if results_count < self.bulk_size:
                break

        self.execute_post_query(conn_to, cursor_to)

    def build_load_query(self, table_name, headers):
        query = "%s %s"%(self.build_load_query_insert(table_name, headers), self.build_load_query_on_conflict(headers))
        return self.hydrate_query(query)

    def build_load_query_insert(self, table_name, headers):
        return "INSERT INTO %s (SELECT * FROM json_populate_recordset(null::%s, %%s))"%(table_name, table_name)

    def build_load_query_on_conflict(self, fields):
        # removing ID from the list of fields to update in case of conflict
        fields = filter(lambda field: field != 'id', fields)
        fields_to_update = map(lambda field: "%s = excluded.%s"%(field, field), fields)
        return "ON CONFLICT (id) DO UPDATE SET %s"%(','.join(fields_to_update));

    def upsert_data(self, conn, query, data):
        c = conn.cursor()
        c.execute(query, [(json.dumps(data))])
        conn.commit()

    def dynamic_import(self, abs_module_path, class_name):
        module_object = import_module(abs_module_path)
        target_class = getattr(module_object, class_name)
        return target_class

    def load_transformers(self, transformer_names):
        transformers = []

        for transformer_name in transformer_names:
            class_name = transformer_name.split('.')[-1]
            transformers.append(self.dynamic_import(transformer_name, class_name)())

        return transformers

    def execute_post_query(self, conn, cursor):
        if not self.post_query:
            return
        cursor.execute(self.hydrate_query(self.post_query))
        conn.commit()

    def hydrate_query(sefl, query):
        for key, value in self.placeholders:
            query = query.replace(key, value)
        return query