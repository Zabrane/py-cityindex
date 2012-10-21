
import json
from pprint import pprint


SILLY_VALUES = [2147483647, -2147483648, -7.9228162514264338E+28,
                7.9228162514264338E+28, None]


def get_js(path, key):
    f = file('meta/schema.js')
    f.readline()
    js = json.load(f)
    return js[key]


class SchemaWalker:
    def __init__(self, schema):
        self.schema = schema
        self.types = {}

    def _extends(self, typeinfo):
        if not typeinfo.get('extends'):
            return typeinfo

        new_ = self.schema[typeinfo['extends'][2:]].copy()
        assert 'extends' not in new_
        new_['properties'].update(typeinfo.pop('properties'))
        new_.update(typeinfo)
        return new_

    def walk_prop(self, typeinfo, name, info):
        dct = {
            'name': name
        }

        if info['type'] == 'array':
            dct['collection'] = True

        if isinstance(info['type'], list) and len(info['type']) == 1:
            info['type'] = info['type'][0]
            info['nullable'] = True

        if info.get('format') == 'decimal':
            info['type'] = 'decimal'
        elif info.get('format') == 'wcf-date':
            info['type'] = 'wcf-date'
        elif isinstance(info['type'], basestring):
            dct['type'] = info['type']
        elif isinstance(info['type'], dict):
            typename = info['type']['$ref'][2:]
            assert typename in self.schema
            dct['type'] = typename
        elif isinstance(info['type'], list):
            assert len(info['type']) == 2 and 'null' in info['type']
            info['type'].remove('null')
            dct['type'] = info['type']

        if info.get('minValue') not in SILLY_VALUES:
            dct['min'] = info['minValue']
        elif info.get('maxValue') not in SILLY_VALUES:
            dct['max'] = info['maxValue']

        return dct

    def walk_type(self, typeinfo):
        assert typeinfo['type'] == 'object', typeinfo
        dct = {}
        typeinfo = self._extends(typeinfo)
        for name, info in typeinfo['properties'].iteritems():
            self.walk_prop(typeinfo, name, info)

    def go(self):
        for name, typeinfo in self.schema.iteritems():
            self.walk_type(typeinfo)



schema = get_js('meta/schema.js', 'properties')




'''
walker = SchemaWalker(schema)
walker.go()
pprint(walker.types)
'''
