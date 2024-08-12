from dataclasses import dataclass
from typing import Dict, Optional

from string import Template

from promg import Query


class OcedImportQueryLibrary:

    @staticmethod
    def get_import_object_nodes_query(file_name):
        query_str = '''
        CALL apoc.periodic.iterate('
            CALL apoc.load.json("file://$file_name")
            YIELD value
            RETURN value',
            'MERGE (n:Entity {sysId: toString(value.Id)})
            WITH n, value
            CALL apoc.create.addLabels(n, [value.Name]) YIELD node AS n_labeled
            SET n_labeled += COALESCE(value.Attributes, {})',
            {batchSize: 10000})            
        '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "file_name": file_name
                     })

    @staticmethod
    def get_import_event_nodes_query(file_name):
        query_str = '''
        CALL apoc.periodic.iterate('
            CALL apoc.load.json("file://$file_name")
            YIELD value
            RETURN value',
            'MERGE (a:Activity {activity: value.Name})
             CREATE (e:Event {timestamp: datetime(value.TimeStamp)})
             MERGE (a) - [:OBSERVED] -> (e)
             SET e += COALESCE(value.Attributes, {})',
            {batchSize: 10000})            
        '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "file_name": file_name
                     })

    @staticmethod
    def get_connect_event_nodes_to_objects_with_properties_query(column_id, keys, labels=None):
        query_str = '''
           CALL apoc.periodic.iterate('
               MATCH (e:Event)
               WHERE e[$id] is not null
               RETURN e
           ',
               'MERGE (n:Entity:$label {sysId: e[$id]})
                WITH n, e, apoc.map.submap(properties(e), $keys, null, false) as properties
                CALL apoc.create.removeProperties(e, $keys) YIELD node AS e_removed
                CALL apoc.create.node(["State"], properties) YIELD node as st
                MERGE (e_removed) - [:CORR] -> (n)
                MERGE (n) - [:HAS_STATE] -> (st)
                ',
               {batchSize: 10000, params: {id: $id, keys:$keys}})            
           '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "label": labels[0]
                     },
                     parameters={
                         "id": column_id,
                         "keys": keys,
                         "labels": labels if labels is not None else []
                     })

    @staticmethod
    def get_connect_event_nodes_to_objects_without_properties_query(column_id, labels=None):
        query_str = '''
           CALL apoc.periodic.iterate('
               MATCH (e:Event)
               WHERE e[$id] is not null
               RETURN e
           ',
               'MERGE (n:Entity {sysId: e[$id]})
                WITH n, e
                CALL apoc.create.addLabels(n, $labels) YIELD node AS n_labeled
                MERGE (e) - [:CORR] -> (n_labeled)
                ',
               {batchSize: 10000, params: {id: $id, keys:$keys, labels: $labels}})            
           '''

        return Query(query_str=query_str,
                     parameters={
                         "id": column_id,
                         "labels": labels if labels is not None else []
                     })

    @staticmethod
    def get_connect_event_nodes_to_objects_query(column_id, labels=None, keys=None):
        if keys is not None:
            return OcedImportQueryLibrary.get_connect_event_nodes_to_objects_with_properties_query(column_id, keys,
                                                                                                   labels)
        else:
            return OcedImportQueryLibrary.get_connect_event_nodes_to_objects_without_properties_query(column_id, labels)
