import os

from setuptools import Extension, setup



COMPILED_MODULES = {
	'prepare.pyx',
}
MODULES_TO_BUILD = []
EXTENSIONS = []

for file_name in COMPILED_MODULES:
	c_file = file_name.replace('.pyx', '.c').replace('.py', '.c')
	if os.path.isfile(c_file):
		EXTENSIONS.append(Extension(os.path.splitext(file_name)[0].replace('/', '.'), sources = [c_file]))
	else:
		MODULES_TO_BUILD.append(file_name)

try:
	from Cython.Build import cythonize

	EXTENSIONS += cythonize(MODULES_TO_BUILD)
except ImportError:
	if len(EXTENSIONS) != len(COMPILED_MODULES):
		raise RuntimeError('Cannot cythonize required modules')


setup(
	name = 'cerializer',
	author = 'matejmicek.com',
	author_email = 'matej.micek@quantlane.com',
	version = '0.0.0',
	install_requires = [
		'Cython>=0.28.4,<1.0.0',
		'toolz>=0.6.0,<1.0.0',
		'logwood>=3.4.0,<4.0.0',
		'ql-fastavro>=0.22.6,<1.0.0',
		'PyYAML>=5.3.1,<6.0.0',
		'setuptools>=46.0.0,<47.0.0',
		'Jinja2>=2.11.2,<3.0.0',
		'pytz>=2020.1,<2021.0',
		'ql-schemachinery>=1.6.0,<2.0.0',
	],
	setup_requires = [
		'Cython>=0.28.4,<1.0.0',
	],
	ext_modules = EXTENSIONS,
)
