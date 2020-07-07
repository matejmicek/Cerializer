from setuptools import setup
from Cython.Build import cythonize
import os



setup(
    ext_modules = cythonize(
            ['cerializer_base/*.pyx', '*.pyx'], annotate=False
    )
)
