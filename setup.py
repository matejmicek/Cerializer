import os

from setuptools import Extension, find_packages, setup


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
		raise RuntimeError('Cannot cythonize required modules.')

ROOT = os.path.dirname(__file__)

setup(
	name = 'cerializer',
	author = 'matejmicek.com',
	author_email = 'matej.micek@quantlane.com',
	url = 'https://github.com/matejmicek/Cerializer',
	version = open(os.path.join(ROOT, 'version.txt')).read().strip(),
	packages = find_packages(exclude = ['test*']),
	package_data = {
		'cerializer': [
			'templates/*.jinja2',
			'../write.pxd',
			'../read.pxd',
			'../prepare.pxd',
			'../prepare.pyx',
		],
	},
	install_requires = [
		'Cython>=0.28.4,<1.0.0',
		'PyYAML>=5.3.1,<6.0.0',
		'setuptools>=46.0.0,<47.0.0',
		'Jinja2>=2.11.2,<3.0.0',
		'pytz>=2020.1,<2021.0',
	],
	extras_require = {
		'quantlane': [
			'ql-qutils>=9.5.2,<10.0.0'
		],
	},
	setup_requires = [
		'Cython>=0.28.4,<1.0.0',
	],
	ext_modules = EXTENSIONS,
)
