import os

from setuptools import setup
from Cython.Build import cythonize

import cerializer.quantlane_utils
import constants.constants


constants.constants.PROJECT_ROOT = os.path.dirname(__file__)

setup(
	ext_modules = cythonize(['cerializer/*.pyx']),
	name = 'cerializer',
	author = 'matejmicek.com',
	author_email = 'matej.micek@quantlane.com',
	version = '0.0.0',
	install_requires = [
		'Cython>=0.28.4,<1.0.0',
		'toolz>=0.6.0,<1.0.0',
		'logwood>=3.4.0,<4.0.0',
		'fastavro>=0.22.6',
		'Cython>=0.29.16',
		'PyYAML>=5.3.1',
		'setuptools>=46.0.0',
		'Jinja2>=2.11.2',
		'pytz>=2020.1',
	],
)

schema_roots = [os.path.join(constants.constants.PROJECT_ROOT, 'cerializer', 'tests', 'schemata')]

cerializer.quantlane_utils.add_compiled_cerializer_code(schema_roots)
