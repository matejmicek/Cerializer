import os


os.system('python setup.py build_ext --inplace')
print()
print()
print()

import projekt



projekt.serialize_two_chars(12, 12)
projekt.deserialize_two_chars()