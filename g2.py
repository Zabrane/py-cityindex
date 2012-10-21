
import json
import sys
import textwrap



def load_json(path):
    with file(path) as fp:
        return json.load(fp, strict=False)


schema = load_json('schema/schema.json.js')
smd = load_json('schema/smd.json.js')

TYPE_MAP = {
    'boolean': 'bool',
    'number': 'int',
    'integer': 'int',
    'string': 'basestring'
}


types = {}
rpcs = {}


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



for name, typ in schema['properties'].items():
    if 'extends' in typ:
        new_props = schema['properties'][typ['extends'][2:]]['properties'].copy()
        new_props.update(typ['properties'])
        make_type_from_props(name, new_props)
    else:
        if 'properties' in typ:
            make_type_from_props(name, typ['properties'])


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

from pprint import pprint
pprint(types)
print
pprint(rpcs)
