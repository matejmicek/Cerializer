from pathlib import Path

from setuptools import Extension, Distribution

from Cython.Build import cythonize
from Cython.Distutils.build_ext import new_build_ext as cython_build_ext

SOURCE_DIR = Path(".")


ext_modules = cythonize(
	module_list=[
		Extension(
			name="prepare",
			sources=["prepare.c"]
		)
	]
)

distribution = Distribution({
	"ext_modules": ext_modules,
	"cmdclass": {
		"build_ext": cython_build_ext,
	}
})

build_ext_cmd = distribution.get_command_obj("build_ext")
build_ext_cmd.inplace = True
build_ext_cmd.ensure_finalized()
build_ext_cmd.run()
