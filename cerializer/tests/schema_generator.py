import random
import string
import os


def random_strings(string_length):
    if string_length < 5:
        raise RuntimeError('string length has to be > 5.')
    used = set()
    while True:
        k = random.randint(5, string_length)
        random_string = ''.join(random.choices(string.ascii_letters, k = k))
        if random_string in used:
            continue
        used.add(random_string)
        yield random_string


def random_ints(int_max):
    used = set()
    while True:
        random_int = random.randint(0, int_max)
        if random_int in used:
            continue
        used.add(random_int)
        yield random_int

def null_gen():
    while True:
        yield None

def random_bool():
    while True:
        yield random.choice([True, False])










class RandomSchemaDataGenerator():
    def __init__(self, int_max, string_length):
        self.base_dir = '/Users/matejmicek/Library/Mobile Documents/com~apple~CloudDocs/PycharmProjects/CerializerDemo/BP_schemata'
        self.random_ints = random_ints(int_max)
        self.random_strings = random_strings(string_length)
        self.random_null = null_gen()
        self.random_bool = random_bool()
        self.primitive_generators = {
            'string': self.random_strings,
            'int': self.random_ints,
            'boolean': self.random_bool
        }
        self.supported_primitive_types = list(self.primitive_generators.keys())
        self.schema_name_generator = self.random_schema_name()
        self.supported_complex_types = ['array', 'map', 'union']



    def random_schema_name(self):
        while True:
            yield f'generated.{next(self.primitive_generators["string"])}'

    def get_schema_data(self, n_primitive, n_complex, schema_name = None):
        schema_name = next(self.schema_name_generator) if not schema_name else schema_name
        schema_header = f'''
name: {schema_name}
namespace: generated
type: record
fields:
            '''

        schema_dir = f'{self.base_dir}/{schema_name}'
        os.mkdir(schema_dir)
        schema_primitive, data_primitive = self.primitive_element(n_primitive)
        schema_complex, data_complex = self.complex_element(n_complex)
        schema_string = schema_header + schema_primitive + schema_complex
        data_string = data_primitive + data_complex
        with open(f'{schema_dir}/schema.yaml', 'w') as s:
            with open(f'{schema_dir}/example.yaml', 'w') as e:
                s.write(schema_string)
                e.write(data_string)

    def primitive_element(self, num_fields):
        schema = []
        data = []
        for _ in range(num_fields):
            types = [random.choice(
                self.supported_primitive_types
                )]
            schema_part, data_part = self.primitive_field(
                types
                )
            schema.append(schema_part)
            data.append(data_part)

        schema_string =  '\n'.join(schema)
        data_string = '\n'.join(data)
        return schema_string, data_string


    def complex_element(self, num_fields):
        schema = []
        data = []
        for _ in range(num_fields):
            types = [random.choice(self.supported_complex_types)]
            schema_part, data_part = self.complex_field(types)
            schema.append(schema_part)
            data.append(data_part)
        return '\n'.join(schema), '\n'.join(data)


    def primitive_field(self, types):
        name = next(self.primitive_generators['string'])
        if len(types) == 1:
            return (f'''
- name: {name}
  type: "{types[0]}"
        ''',
    f'''
{name}: {next(self.primitive_generators[types[0]])}
        ''')
        else:
            position = random.randint(0, len(types) - 1)
            return (f'''
- name: {name}
  type: {types}
            ''',
    f'''
{name}: {next(self.primitive_generators[types[position]])}
            ''')



    def complex_field(self, types):
        t = types[0]
        name = next(self.primitive_generators['string'])
        # needs to be done due to redunadancies
        items = random.sample(self.supported_primitive_types, k = 2)
        length = 100
        items_values = [
            next(self.primitive_generators[random.choice(items)])
            for _ in range(length)
            ]
        if t == 'array':
            schema = f'''
- name: {name}
  type:
    type: array
    items: {items}
            '''
            data = f'''
{name}: {items_values}
            '''
            return schema, data
        
        elif t == 'map':
            schema = f'''
- name: {name}
  type:
    type: map
    values: {items}
'''         
            data = [f'''
{name}:
''']

            for item_value in items_values:
                data_row = f'''
   '{next(self.primitive_generators['string'])}': {item_value}
            '''
                data.append(data_row)
            data = '\n'.join(data)

            return schema, data

        elif t == 'union':
            union_types = random.sample(self.supported_primitive_types, k = 2)
            return self.primitive_field(union_types)



    

if __name__ == '__main__':
    min_types = 0
    max_types = 30
    for n in range(min_types, max_types + 1, 1):
        x = RandomSchemaDataGenerator(200000, 40)
        name = f'generated.Schema_{n}_0'
        x.get_schema_data(n, 0, name)
        if n == 0:
            continue
        name = f'generated.Schema_0_{n}'
        x.get_schema_data(0, n, name)

