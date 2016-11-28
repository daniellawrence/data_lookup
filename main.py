#!/usr/bin/env python
import copy
import os
import re
import pprint
import json
import hashlib

# import pymongo
import yaml

# Used to rank the datasources that can overwrite the others
# <schema>://<database>/<path>
ladder = [
    'yaml://artisan_yaml/{node_name}.yaml',
    'yaml://artisan_yaml/{role_name}.yaml',
    'yaml://artisan_yaml/{fabric}.yaml',
    'yaml://artisan_yaml/{environment}.yaml',
    'yaml://artisan_yaml/{osfamily}.yaml',
    'yaml://artisan_yaml/{os}.yaml',
    'yaml://artisan_yaml/{env}.yaml',
    'yaml://artisan_yaml/base.yaml',
    #
    'mongo://fact_db/{node_name}',
    # 'https://remote-webservice/api/v0/{node_name}',
    #
    'dict://node/{node_name}',
]

# used to mock out "node" facts
node = {
    'xps13z': {
        'node_name': 'xps13z',
        'role_name': 'laptop',
        'os': 'linux',
    }
}


def dict2sha1(data):
    if isinstnace(data, dict):
        data = json.loads(data)

    m = hashlib.sha1()
    m.update(str(data))
    data_sha1 = m.hexdigest()
    return data_sha1


def ladder_parser(uri):
    """ Given an URI fetch the schema, database and lookup(key) """
    uri_re = re.compile(r"(?P<schema>.*)://(?P<database>.+)/(?P<lookup>.*)")
    uri_match = uri_re.match(uri)
    (schema, database, lookup) = uri_match.groups()

    if schema not in lookup_schemas:
        error = "schema '{0}' invalid, must be one of: {1}"
        allowed_schemas = ",".join(lookup_schemas)
        raise Exception(error.format(schema, allowed_schemas))

    return schema, database, lookup


def loop_merge(current, new, take_first_result=False, level=0):

    for new_key, new_data in list(new.items()):
        if new_key not in current:
            current[new_key] = copy.copy(new_data)
            continue

        current_data = current[new_key]

        if isinstance(new_data, list):
            if take_first_result and new_key in current:
                continue

            if not isinstance(current_data, list):
                current_data = list(current_data)

            current[new_key] += new_data
            continue

        if isinstance(new_data, dict):

            if isinstance(current_data, dict):
                current[new_key] = loop_merge(current_data, new_data, level+1)
                continue

        current[new_key] = new_data

    return current


def lookup_data_yaml(database, lookup_value):
    file_path = os.path.join(database, lookup_value)
    if not os.path.exists(file_path):
        return None
    return yaml.load(open(file_path).read())


def lookup_data_json(database, lookup_value):
    file_path = os.path.join(database, lookup_value)
    if not os.path.exists(file_path):
        return None
    return json.load(open(file_path).read())


def lookup_data_dict(database, lookup_value):
    return globals().get(database).get(lookup_value)


def lookup_data_mongo(database, lookup_value):
    # client = pymongo.MongoClient()
    # db = client[database]
    # return db[lookup_value]
    pass

lookup_schemas = {
    'yaml': lookup_data_yaml,
    'json': lookup_data_json,
    'dict': lookup_data_dict,
    'mongo': lookup_data_mongo,
}


def resolver(node_name):
    catalog_first = {}
    catalog_merge = {}

    for ladder_item in ladder:
        schema, database, lookup_path = ladder_parser(ladder_item)

        try:
            lookup_value = lookup_path.format(**node.get(node_name, {}))
        except KeyError:
            continue

        fetched_value = None

        if schema not in lookup_schemas:
            continue

        lookup_function = lookup_schemas.get(schema)
        fetched_value = lookup_function(database, lookup_value)

        if not fetched_value:
            continue

        loop_merge(catalog_first, fetched_value, take_first_result=True)
        loop_merge(catalog_merge, fetched_value, take_first_result=False)

    return catalog_first, catalog_merge


if __name__ == '__main__':
    f, m = resolver('xps13z')
    print("-" * 80)
    pprint.pprint(f)
    pprint.pprint(m)
