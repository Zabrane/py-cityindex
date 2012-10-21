#!/usr/bin/env python
#
# Copyright 2012, the py-cityindex authors
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Read the CityIndex schema descriptions and output a Python module implementing
convertors and validators for its types.
"""

from __future__ import absolute_import

import json
import pprint


TYPE_MAP = {
    'boolean': 'bool',
    'number': 'int',
    'integer': 'int',
    'string': 'basestring'
}

types = {}
rpcs = {}


def load_json(path):
    with file(path) as fp:
        return json.load(fp, strict=False)


def make_type_from_props(name, props):
    fields = {}
    for name_, param in props.items():
        descr = {}
        fields[name_] = descr

        if param.get('items'):
            if 'null' in param['items']:
                param['items'].remove('null')
            descr['collection'] = True
            js_type = param['items'][0]
        else:
            js_type = param['type']

        if isinstance(js_type, dict):
            assert js_type.get('$ref')
            descr['type'] = js_type['$ref'][2:]
        elif isinstance(js_type, list):
            if 'null' in js_type:
                js_type.remove('null')
            descr['type'] = TYPE_MAP[js_type[0]]
        else:
            descr['type'] = TYPE_MAP[js_type]

    types[name] = fields


def make_type_from_params(name, params):
    name += 'RequestDTO'
    fields = {}
    for param in params:
        fields[param['name']] = {
            'type': TYPE_MAP[param['type']]
        }
    types[name] = fields
    return name


def write_types(fp):
    fp.write('TYPES = %s\n\n' % pprint.pformat(types))


def write_rpcs(fp):
    fp.write('RPCS = %s\n\n' % pprint.pformat(rpcs))


def main():
    schema = load_json('schema/schema.json.js')
    for name, typ in schema['properties'].items():
        if 'extends' in typ:
            new_props = schema['properties'][typ['extends'][2:]]['properties'].copy()
            new_props.update(typ['properties'])
            make_type_from_props(name, new_props)
        else:
            if 'properties' in typ:
                make_type_from_props(name, typ['properties'])

    smd = load_json('schema/smd.json.js')
    for name, svc in smd['services']['rpc']['services'].items():
        params = svc['parameters']
        if len(params) == 1 and params[0].get('$ref'):
            req_type = params[0]['$ref'][2:]
        else:
            req_type = make_type_from_params(name, params)

        assert svc['returns'].get('$ref')
        resp_type = svc['returns']['$ref'][2:]

        rpc = {
            'post': svc['envelope'] == 'JSON',
            'request': req_type,
            'response': resp_type,
            'url': svc.get('target', '') + svc.get('uriTemplate', '')
        }
        rpcs[name] = rpc

    with file('cityindex/schema.py', 'w') as fp:
        write_types(fp)
        write_rpcs(fp)

if __name__ == '__main__':
    main()
