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
    def get_remove_key_properties_from_event_nodes_query(keys):
        query_str = '''
           CALL apoc.periodic.iterate('
               MATCH (e:Event)
               RETURN e, apoc.map.submap(properties(e), $keys, null, false) as properties
           ',
               'CALL apoc.create.removeProperties(e, $keys) YIELD node AS e_removed
                RETURN null
                ',
               {batchSize: 10000, params: {keys:$keys}})            
           '''

        return Query(query_str=query_str,
                     parameters={
                         "keys": keys,
                     })

    @staticmethod
    def get_merge_similar_states_query(keys):
        query_str = '''
           CALL apoc.periodic.iterate('
               MATCH (n:Entity)- [:HAS_STATE] -> (st:State)
               WITH n, [x in $keys| st[x]] as same_keys, collect(st) as states
               WHERE size(states) > 1
               RETURN n
           ',
               'MATCH (n) - [:HAS_STATE] -> (st:State)
               WITH n, [x in $keys| st[x]] as same_keys, collect(st) as states
               CALL apoc.refactor.mergeNodes(states, {properties: "combine", mergeRels: true})
               YIELD node
               RETURN null
                ',
               {batchSize: 10000, params: {keys:$keys}})            
           '''

        return Query(query_str=query_str,
                     parameters={
                         "keys": keys,
                     })

    @staticmethod
    def get_create_relations_query(_from, _to, _relation):
        query_str = '''
           CALL apoc.periodic.iterate('
               MATCH (_from:$from_labels) <- [:CORR] - (e:Event) - [:CORR] -> (_to:$to_labels)
               RETURN _from, _to
           ',
               'MERGE (_from) -  [:$rel_type] -> (_to)
                ',
               {batchSize: 10000})            
           '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "from_labels": ":".join(_from),
                         "to_labels": ":".join(_to),
                         "rel_type": _relation
                     })

    @staticmethod
    def get_connect_event_nodes_to_objects_query(column_ids, labels=None, keys=None):
        query_str = '''
                   CALL apoc.periodic.iterate('
                       MATCH (e:Event)
                       WHERE all (x in $ids WHERE e[x] is not null) // check if properties exist
                       RETURN e
                   ',
                       'MERGE (n:Entity $labels {sysId: apoc.text.join([x in $ids| e[x]],"-")})
                        WITH n, e, apoc.map.submap(properties(e), $keys, null, false) as properties
                        CALL apoc.do.when(size(keys(properties)) > 1 AND any(x in keys(properties) WHERE properties[
                        x] is not null),
                            "CREATE (state:State)
                            SET state += properties
                            CREATE (n) - [:HAS_STATE] -> (state)
                            RETURN n",
                            "MATCH (n)
                            RETURN n",
                            {n:n, properties:properties}
                            ) YIELD value
                        WITH e, value.n as n
                        MERGE (e) - [:CORR] -> (n)
                        ',
                       {batchSize: 10000, params: {ids: $ids, keys:$keys}})            
                   '''

        return Query(query_str=query_str,
                     template_string_parameters={
                         "labels": ":" + ":".join(labels) if labels is not None else ""
                     },
                     parameters={
                         "ids": column_ids,
                         "keys": keys if keys is not None else []
                     })
