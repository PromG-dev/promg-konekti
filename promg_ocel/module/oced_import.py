import os
from typing import List, Dict

from promg import DatabaseConnection
from promg import Performance
from promg_ocel.queries.oced_import import OcedImportQueryLibrary as ql
from promg.cypher_queries.data_importer_ql import DataImporterQueryLibrary as di_ql

import pandas as pd
from zipfile import ZipFile
import json


def make_label(label):
    return label.title().replace(" ", "")


class OcedImport:
    K_OBJECTS = "objectinstances"
    K_EVENTS = "events"

    def __init__(self, db_connection):
        self.connection = db_connection
        self.data = dict()
        self._import_directory = None

    def extract_event_stream(self, data):
        event_stream = data["EventStream"]
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
            instance["name"] = make_label(instance["name"])

        self.data["ObjectInstances"] = object_instances

    def relationship_has_correct_direction(self, from_name: str, to_name: str):
        for relationship_type in self.data["RelationshipTypes"]:
            if relationship_type["fromname"] == from_name and relationship_type["toname"] == to_name:
                return True
            if relationship_type["fromname"] == to_name and relationship_type["toname"] == from_name:
                return False
        return None

    def extract_object_instance_relationships(self, data):
        object_instances_relationships = data["ObjectInstancesToObjectInstances"]

        filtered_relationships = []
        seen_relationships = set()
        # update the direction of the relations using the relationship types
        for relationship in object_instances_relationships:
            # update labels to be in correct formatting
            relationship["fromname"] = make_label(relationship["fromname"])
            relationship["toname"] = make_label(relationship["toname"])

            has_correct_direction = self.relationship_has_correct_direction(from_name=relationship["fromname"],
                                                                            to_name=relationship["toname"])

            if has_correct_direction:
                edge_pair = (relationship["fromid"], relationship["toid"])
                type_pair = (relationship["fromname"], relationship["toname"])
            else:
                edge_pair = (relationship["toid"], relationship["fromid"])
                type_pair = (relationship["toname"], relationship["fromname"])

            new_relationship = {
                "edge_pair": edge_pair,
                "type_pair": type_pair
            }

            t = tuple(new_relationship.items())
            if t not in seen_relationships:
                seen_relationships.add(t)
                filtered_relationships.append(new_relationship)

        self.data["ObjectInstanceRelationships"] = filtered_relationships

    def extract_relationship_types(self, data):
        object_relationship_types = data["ObjectsToObjects"]

        # make sure that the direction of each relationship type is correct displayed using from and to
        # so all 1:N relationships are switched to N:1 relationships
        for relationship_type in object_relationship_types:
            relationship_type["toname"] = make_label(relationship_type["toname"])
            relationship_type["fromname"] = make_label(relationship_type["fromname"])

            if relationship_type["Cardinality"] == "1:N":
                from_name = relationship_type["toname"]
                to_name = relationship_type["fromname"]
                relationship_type["toname"] = to_name
                relationship_type["fromname"] = from_name
                relationship_type["Cardinality"] = "N:1"

        self.data["RelationshipTypes"] = object_relationship_types

    @Performance.track()
    def read_json_ocel(self, file: os.path):
        print(f"Reading {file}.")
        # get JSON file from ZIP
        with open(file) as json_file:
            # read JSON
            data = json.load(json_file)
            self.extract_event_stream(data)
            self.extract_object_instances(data)
            self.extract_relationship_types(data)
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
        file_name = "object_instances.json"
        self.store_data(data=self.data["ObjectInstances"], file_name=file_name)
        self.connection.exec_query(ql.get_import_object_nodes_query,
                                   **{"file_name": file_name})

    @Performance.track()
    def import_events(self):
        file_name = "eventstream.json"
        self.store_data(data=self.data["EventStream"], file_name=file_name)
        self.connection.exec_query(ql.get_import_event_nodes_query,
                                   **{"file_name": file_name})

    @Performance.track()
    def import_relationships(self):
        file_name = "object_relationships.json"
        self.store_data(data=self.data["ObjectInstanceRelationships"], file_name=file_name)
        self.connection.exec_query(ql.get_create_relations_between_objects_query,
                                   **{"file_name": file_name})

