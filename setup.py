from setuptools import setup
from Cython.Build import cythonize
import os




os.chdir('cerializer')

setup(
    ext_modules = cythonize(
            '*.pyx', annotate=False
    )
)


os.chdir('cerializer_base')


setup(
    ext_modules = cythonize(
            '*.pyx', annotate=False
    )
)