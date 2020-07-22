# pylint: disable=protected-access

import distutils.core
import hashlib
import importlib.machinery
import os.path
import re
import sys

import Cython.Build.Dependencies
import Cython.Build.Inline
import Cython.Compiler.Main
import Cython.Utils
import cython


def _create_context(cython_include_dirs):
	return Cython.Compiler.Main.Context(list(cython_include_dirs), Cython.Compiler.Main.default_options)


_cython_inline_default_context = _create_context(('.',))
_cython_inline_cache = {}


def compile_code(code, *args, **kwds):
	return cython_inline(code, force = True, language_level = 3, *args, **kwds)


def load_dynamic(name, module_path):
	return importlib.machinery.ExtensionFileLoader(name, module_path).load_module()


def safe_type(arg, context = None):
	py_type = type(arg)
	if py_type is int:
		return 'long'
	elif py_type in (list, tuple, dict, str):
		return py_type.__name__
	elif py_type is complex:
		return 'double complex'
	elif py_type is float:
		return 'double'
	elif py_type is bool:
		return 'bint'
	else:
		for base_type in py_type.__mro__:
			if base_type.__module__ in ('__builtin__', 'builtins'):
				return 'object'
			module = context.find_module(base_type.__module__, need_pxd = False)
			if module:
				entry = module.lookup(base_type.__name__)
				if entry is not None and entry.is_type:
					return '%s.%s' % (base_type.__module__, base_type.__name__)
		return 'object'


def cython_inline(
	complete_code,
	get_type = safe_type,
	lib_dir = os.path.join(Cython.Utils.get_cython_cache_dir(), 'inline'),
	force = False,
	quiet = False,
	language_level = None,
	**kwds
):

	ctx = _cython_inline_default_context
	imports = []

	'''#method to compensate for a bug in timeit. Necesssary for benchmarking
	#TODO delete
	def remove_indent(code):
		retarded_indent = [line.startswith('    ') for line in code.split('\n')]
		if all(retarded_indent) or all(retarded_indent[1:]):
			code = '\n'.join([line[4:] for line in code.split('\n')])
		return code

	complete_code = remove_indent(complete_code)'''

	code_buffer = []
	for line in complete_code.split('\n'):
		if line.startswith('cpdef serialize'):
			# the main function has to be def not cpdef
			code_buffer.append(line.replace('cpdef', 'def'))
			continue
		elif not line.startswith('import') and not line.startswith('cimport'):
			code_buffer.append(line)
		else:
			imports.append(line)
	code = '\n'.join(code_buffer)

	_unbound_symbols = _cython_inline_cache.get(code)
	if _unbound_symbols is not None:
		bound_by_imports = set([i.split()[1] for i in imports])
		# removing import names from unbound symbols, since we know they are going to be imported
		_real_unbound_symbols = set(_unbound_symbols).difference(bound_by_imports)
		_unbound_symbols = tuple(_real_unbound_symbols)
		Cython.Build.Inline._populate_unbound(kwds, _unbound_symbols, None, None)
		args = sorted(kwds.items())
		arg_sigs = tuple([(get_type(value, ctx), arg) for arg, value in args])
		invoke = _cython_inline_cache.get((code, arg_sigs))
		if invoke is not None:
			arg_list = [arg[1] for arg in args]
			return invoke(*arg_list)

	orig_code = code
	code, literals = Cython.Build.Dependencies.strip_string_literals(code)
	code = Cython.Build.Inline.strip_common_indent(code)

	_cython_inline_cache[orig_code] = _unbound_symbols = Cython.Build.Inline.unbound_symbols(code)
	Cython.Build.Inline._populate_unbound(kwds, _unbound_symbols, None, None)

	cython_compiler_directives = dict({})
	if language_level is not None:
		cython_compiler_directives['language_level'] = language_level

	for name, arg in list(kwds.items()):
		if arg is cython:
			imports.append('\ncimport cython as %s' % name)
			del kwds[name]
	arg_names = sorted(kwds)
	arg_sigs = tuple([(get_type(kwds[arg], ctx), arg) for arg in arg_names])
	key = orig_code, arg_sigs, sys.version_info, sys.executable, language_level, Cython.__version__
	module_name = '_cython_inline_' + hashlib.md5(str(key).encode('utf-8')).hexdigest()

	if module_name in sys.modules:
		module = sys.modules[module_name]
	else:
		build_extension = None
		cython_inline.so_ext = None
		if cython_inline.so_ext is None:
			# Figure out and cache current extension suffix
			build_extension = Cython.Build.Inline._get_build_extension()
			cython_inline.so_ext = build_extension.get_ext_filename('')
		module_path = os.path.join(lib_dir, module_name + cython_inline.so_ext)

		if not os.path.exists(lib_dir):
			os.makedirs(lib_dir)
		if force or not os.path.isfile(module_path):
			cflags = []
			c_include_dirs = []
			qualified = re.compile(r'([.\w]+)[.]')
			for type_, _ in arg_sigs:
				m = qualified.match(type_)
				if m:
					imports.append('\ncimport %s' % m.groups()[0])
			module_body, func_body = Cython.Build.Inline.extract_func_code(code)
			params = ', '.join(['%s %s' % a for a in arg_sigs])
			module_code = '''
#cython: language_level=3
%(module_body)s
%(imports)s
import cython

@cython.boundscheck(False)
@cython.wraparound(False)
@cython.initializedcheck(False)
def __invoke(%(params)s):
%(func_body)s
    return locals()
			''' % {
				'imports': '\n'.join(imports),
				'module_body': module_body,
				'params': params,
				'func_body': func_body,
			}
			for key, value in literals.items():
				module_code = module_code.replace(key, value)
			pyx_file = os.path.join(lib_dir, module_name + '.pyx')
			fh = open(pyx_file, 'w')
			try:
				fh.write(module_code)
			finally:
				fh.close()
			extension = distutils.core.Extension(
				name = module_name,
				sources = [pyx_file],
				include_dirs = c_include_dirs,
				extra_compile_args = cflags,
			)
			if build_extension is None:
				build_extension = Cython.Build.Inline._get_build_extension()
			build_extension.extensions = Cython.Build.Dependencies.cythonize(
				[extension],
				include_path = ['.'],
				compiler_directives = cython_compiler_directives,
				quiet = quiet,
			)
			build_extension.build_temp = os.path.dirname(pyx_file)
			build_extension.build_lib = lib_dir
			build_extension.run()

		module = load_dynamic(module_name, module_path)

	_cython_inline_cache[orig_code, arg_sigs] = module.__invoke
	arg_list = [kwds[arg] for arg in arg_names]
	return module.__invoke(*arg_list)
