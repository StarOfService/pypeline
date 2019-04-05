import mysql.connector
import psycopg2
import json
import string
import time
import logging
import os
import psutil
from importlib import import_module

class Pype:

    default_config = {
        'fields_excluded_from_update': [],
        'post_query': 0,
        'bulk_size': 2000,
        'debug': False,
        'name': 'pype',
        'is_delete': False,
    }

    def __init__(self, config, placeholders={}):
        config = {**self.default_config, **config}
        for config_field in config:
            setattr(self, config_field, config[config_field])

        self.transformers = self.load_transformers(config['transformers'])
        self.placeholders = placeholders

    def run(self, conn_from, conn_to):
        total_count = 0
        conn_from.ping(True)
        cursor_from = conn_from.cursor(dictionary=True)
        cursor_to = conn_to.cursor()
        cursor_from.execute(self.hydrate_query(self.extract_query))

        while True:
            # Extract
            start_time = time.time()
            results = cursor_from.fetchmany(self.bulk_size)
            results_count = len(results)
            total_count += results_count
            extract_duration = time.time() - start_time

            if 0 == results_count:
                break

            # Transform
            start_time = time.time()
            for transformer in self.transformers:
                results = list(map(transformer.filter, results))

            transform_duration = time.time() - start_time

            start_time = time.time()

            if self.is_delete is not None and self.is_delete:
                self.delete(conn_to, results)
            else:
                self.load(conn_to, results)

            update_duration = time.time() - start_time

            if self.debug:
                logging.info('Pype: {0}, {1} items; ETL: {2:.2f} s., {3:.2f} s., {4:.2f} s.; Mem: {5:.2f} Mb.'.format(
                    self.name,
                    total_count,
                    extract_duration,
                    transform_duration,
                    update_duration,
                    psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
                ))

            if results_count < self.bulk_size:
                break

        self.execute_post_query(conn_to, cursor_to)

    def delete(self, conn_to, results):
        delete_query = self.build_delete_query(self.target_table, self.identifier)
        self.delete_data(conn_to, delete_query, results)

    def load(self, conn_to, results):
        # insert_query = ''
        headers = []

        if 0 == len(headers):
            headers = list(results[0].keys())
            # insert_query = self.build_load_query(self.target_table, headers)

        # Load
        # self.upsert_data(conn_to, insert_query, results)
        self.upsert_data_v2(conn_to, headers, results)

    def upsert_data_v2(self, conn,  headers, results):
        values = []
        replacements = []

        if self.fields_excluded_from_update:
            headers = list(filter(lambda field: field not in self.fields_excluded_from_update, headers))

        fields_to_update = list(map(lambda field: "%s = excluded.%s" % (field, field), headers))
        values_placeholder = ','.join(['%s'] * len(headers))

        for row in results:
            values.append('(' + values_placeholder + ')')

            for field in headers:
                replacements.append(row[field])

        query = '''
            INSERT INTO {table_name} ({fields}) 
            VALUES {values}
            ON CONFLICT (id) 
            DO UPDATE SET {fields_to_update}
        '''.format(
            values=','.join(values),
            fields=','.join(headers),
            table_name=self.target_table,
            fields_to_update=','.join(fields_to_update)
        )

        c = conn.cursor()
        c.execute(query, replacements)
        conn.commit()

    def build_load_query(self, table_name, headers):
        query = "%s %s"%(self.build_load_query_insert(table_name, headers), self.build_load_query_on_conflict(headers))
        return self.hydrate_query(query)

    @staticmethod
    def build_load_query_insert(table_name, headers):
        return "INSERT INTO %s (SELECT * FROM json_populate_recordset(null::%s, %%s))"%(table_name, table_name)

    def build_load_query_on_conflict(self, fields):
        # removing ID from the list of fields to update in case of conflict
        fields = list(filter(lambda field: field != 'id', fields))

        if self.fields_excluded_from_update:
            fields = list(filter(lambda field: field not in self.fields_excluded_from_update, fields))

        fields_to_update = list(map(lambda field: "%s = excluded.%s"%(field, field), fields))
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

    def hydrate_query(self, query, offset=False):
        for key in self.placeholders:
            query = query.replace(key, self.placeholders[key])

        if offset:
            query += ' OFFSET %s' % offset

        return query

    def build_delete_query(self, table_name, identifier):
        return "DELETE FROM %s WHERE %s = ANY(%%s::uuid[])"%(table_name, identifier)

    def delete_data(self, conn, query, data):
        c = conn.cursor()
        ids = list({str(item[self.identifier]) for item in data})
        c.execute(query, (ids, ))
        conn.commit()
