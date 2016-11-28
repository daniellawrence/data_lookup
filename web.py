#!/usr/bin/env python
from main import resolver, node, ladder, lookup_schemas
import json

from flask import Flask, jsonify, request

app = Flask(__name__)
app.url_map.strict_slashes = False


@app.route('/api/')
def api_GET():
    rules = []
    for rule in app.url_map.iter_rules():
        rules.append(str(rule))
    return jsonify({'api': rules})


@app.route('/api/ladder')
def api_GET_fetch_ladder():
    return jsonify({'ladder': ladder})


@app.route('/api/direct/schema/<schema>/database/<database_name>/path/<path:lookup_value>/')
def api_GET_fetch_database(schema, database_name, lookup_value):
    lookup_function = lookup_schemas.get(schema)
    fetched_value = lookup_function(database_name, lookup_value)
    return jsonify(fetched_value)


@app.route('/api/node/')
def api_GET_nodes():
    return jsonify({'nodes': list(node.keys())})


@app.route('/api/node/<node_name>/')
def api_GET_node(node_name):
    return jsonify(node.get(node_name))


@app.route('/api/node/<node_name>/', methods=['POST'])
def api_POST_node(node_name):
    new_node_data_raw = request.form.copy()
    new_node_data = json.load(new_node_data_raw)
    node[node_name].update(new_node_data)
    return node.get(node_name, {})


@app.route('/api/facts/<node_name>/merged/')
def api_GET_merged_facts_node(node_name):
    first, merged = resolver(node_name)
    return jsonify(merged)


@app.route('/api/facts/<node_name>/first/')
def api_GET_first_facts_node(node_name):
    first, merged = resolver(node_name)
    return jsonify(first)


@app.route('/api/facts/<node_name>/FACTS/')
def api_GET_keys_fact_node(node_name):
    first, merged = resolver(node_name)
    return jsonify({'facts': list(first.keys())})


@app.route('/api/facts/<node_name>/first/<fact_name>/')
def api_GET_first_facts_node_single_fact(node_name, fact_name):
    first, merged = resolver(node_name)
    return jsonify({fact_name: first.get(fact_name, {})})


@app.route('/api/facts/<node_name>/merged/<fact_name>/')
def api_GET_merged_facts_node_single_fact(node_name, fact_name):
    first, merged = resolver(node_name)
    return jsonify({fact_name: merged.get(fact_name, {})})


if __name__ == '__main__':
    app.debug = True
    app.run()
