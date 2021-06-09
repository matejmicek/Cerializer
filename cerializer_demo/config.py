LOCALHOST = 'http://localhost'
SERVER_PORT = 8080


SCHEMA_URL = f'{LOCALHOST}:8081'
NAMESPACE = 'school'
SCHEMA_NAME = 'student_schema:1'
VERBOSE = False


schema = {
    'doc': 'schema to describe a student',
    'fields': [
        {
            'doc': 'name of the student',
            'name': 'name',
            'type': 'string'
        },
        {
            'doc': 'age of the student',
            'name': 'age',
            'type': 'int'
        },
        {
            'doc': 'study average',
            'name': 'average',
            'type': 'float'
        },
        {
            'name': 'id',
            'type': 'int'
        }
    ],
    'name': 'student_schema',
    'namespace': 'school',
    'type': 'record'
}


data = {
    'age': 23,
    'name': 'Matej Micek',
    'average': 1.0,
    'id': 1
}