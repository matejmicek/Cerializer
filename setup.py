from setuptools import setup
from Cython.Build import cythonize
import os

setup(
    ext_modules = cythonize(
        '*.pyx', annotate=False
    )
)

os.chdir('cerializer/cerializer_base')


setup(
    ext_modules = cythonize(
            '*.pyx', annotate=False
    )
)