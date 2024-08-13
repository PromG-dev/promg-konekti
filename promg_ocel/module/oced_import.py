import os

from promg import DatabaseConnection
from promg import Performance
from promg_ocel.queries.oced_import import OcedImportQueryLibrary as ql
from promg.cypher_queries.data_importer_ql import DataImporterQueryLibrary as di_ql

from zipfile import ZipFile
import json


class OcedImport:
    K_OBJECTS = "objectinstances"
    K_EVENTS = "events"

    def __init__(self, db_connection):
        self.connection = db_connection
        self.ocelData = dict()
        self._import_directory = None

    # "objectinstances": [
    # {
    #   "Id": 138656,
    #   "Name": "Paymentline",
    #   "Attributes": {
    #     "record_id": "13515",
    #     "payment_id": "10285",
    #     "line_id": "3"
    #   }
    # },

    # {
    #   "Id": 120246,
    #   "Name": "Create goodsreceipts",
    #   "TimeStamp": "2020-01-14T20:15:00Z",
    #   "Attributes": {
    #     "record_id": "73",
    #     "goods_receipt_id": "73",
    #     "created_date_time": "01/14/2020 20:15:00"
    #   },
    #   "ObjectInstances": [
    #     74
    #   ]
    # },

    @Performance.track()
    def readJsonOcel(self, dataset: os.path):
        # dataset is a file with 'jsconocel.zip' extension
        # read zip
        with ZipFile(dataset, 'r') as zip:
            for jsonOcelFile in zip.namelist():
                print(f"Reading {jsonOcelFile}.")
                # get JSON file from ZIP
                with zip.open(jsonOcelFile) as jsonOcel:
                    # read JSON
                    data = jsonOcel.read()
                    d = json.loads(data.decode("utf-8"))
                    if isinstance(d, dict):
                        self.ocelData[jsonOcelFile] = d

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
        for key, value in self.ocelData.items():
            data = value[OcedImport.K_OBJECTS]
            file_name = f"{key}_{OcedImport.K_OBJECTS}.json"
            self.store_data(data=data, file_name=file_name)
            self.connection.exec_query(ql.get_import_object_nodes_query,
                                       **{"file_name": file_name})

    @Performance.track()
    def import_events(self):
        for key, value in self.ocelData.items():
            data = value[OcedImport.K_EVENTS]
            file_name = f"{key}_{OcedImport.K_EVENTS}.json"
            self.store_data(data=data, file_name=file_name)
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
