import os

from promg import DatabaseConnection
from promg import Performance
from promg_ocel.queries.oced_import import OcedImportQueryLibrary as ql
from promg.cypher_queries.data_importer_ql import DataImporterQueryLibrary as di_ql

import pandas as pd
from zipfile import ZipFile
import json


class OcedImport:
    K_OBJECTS = "objectinstances"
    K_EVENTS = "events"

    def __init__(self, db_connection):
        self.connection = db_connection
        self.data = dict()
        self._import_directory = None

    def extract_event_stream(self, data):
        event_stream = data["EventStream"]
        object_attributes = set()
        for event in event_stream:
            event_attributes = json.loads(event["eventattributes"])
            event["eventattributes"] = event_attributes

        self.data["EventStream"] = event_stream

    def extract_object_instances(self, data):
        object_instances = data["ObjectInstances"]
        for instance in object_instances:
            attributes = json.loads(instance["attributes"])
            instance["attributes"] = attributes
            # convert to camel case
            instance["name"] = instance["name"].title().replace(" ", "")

        self.data["ObjectInstances"] = object_instances

    def extract_object_instance_relationships(self, data):
        object_instances_relationships = data["ObjectInstancesToObjectInstances"]
        self.data["ObjectInstanceRelationships"] = object_instances_relationships

    @Performance.track()
    def readJsonOcel(self, file: os.path):
        print(f"Reading {file}.")
        # get JSON file from ZIP
        with open(file) as json_file:
            # read JSON
            data = json.load(json_file)
            self.extract_event_stream(data)
            self.extract_object_instances(data)
            self.extract_object_instance_relationships(data)

    def get_import_directory(self):
        if self._import_directory is None:
            self.retrieve_import_directory()
        return self._import_directory

    def retrieve_import_directory(self):
        result = self.connection.exec_query(di_ql.get_import_directory_query)
        # get the correct value from the result
        self._import_directory = result[0]['directory']

    def store_data(self, data, file_name):
        import_directory = self.get_import_directory()
        with open(os.path.join(import_directory, file_name), 'w') as file:
            json.dump(data, file)

    @Performance.track()
    def import_objects(self):
        file_name = f"object_instances.json"
        self.store_data(data=self.data["ObjectInstances"], file_name=file_name)
        self.connection.exec_query(ql.get_import_object_nodes_query,
                                   **{"file_name": file_name})

    @Performance.track()
    def import_events(self):
        file_name = f"eventstream.json"
        self.store_data(data=self.data["EventStream"], file_name=file_name)
        self.connection.exec_query(ql.get_import_event_nodes_query,
                                   **{"file_name": file_name})

    def connect_events_to_objects(self, column_ids, labels=None, keys=None):
        self.connection.exec_query(ql.get_connect_event_nodes_to_objects_query,
                                   **{
                                       "column_ids": column_ids,
                                       "labels": labels,
                                       "keys": keys
                                   })

    def remove_key_props_from_events(self, keys):
        if len(keys) > 0:
            self.connection.exec_query(ql.get_remove_key_properties_from_event_nodes_query,
                                       **{
                                           "keys": keys
                                       })

    def merge_similar_states(self, similar_keys):
        if len(similar_keys) > 0:
            self.connection.exec_query(ql.get_merge_similar_states_query,
                                       **{
                                           "keys": similar_keys
                                       })

    def create_rels(self, _from, _to, _relation):
        self.connection.exec_query(ql.get_create_relations_query,
                                   **{
                                       "_from": _from,
                                       "_to": _to,
                                       "_relation": _relation
                                   })

    @Performance.track()
    def analyze_delays(self):
        result = self.connection.exec_query(ql.q_summarize_delay_entities)
        for r in result:
            print(f"{r['delay_by']} delayed an activity {r['frequency']} times.")

    @Performance.track()
    def visualize_delays(self, threshold: int = 0):
        self.connection.exec_query(ql.visualize_delays,
                                   **{
                                       "threshold": threshold
                                   })
