import os


os.system('python setup.py build_ext --inplace')
print()
print()
print()

import projekt



FILENAME = b'ahoj'


first_data = {
    's': 'ahojky',
    'b': True,
    'i': 12
}

'''
projekt.serialize_two_chars(FILENAME, 12, 12)
result = projekt.deserialize_two_chars(FILENAME)
print(result)


projekt.serialize_short_int(FILENAME, 125)
res = projekt.deserialize_short_int(FILENAME)
print(res)
'''

projekt.do_full_circle(FILENAME, first_data)