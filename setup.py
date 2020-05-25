from setuptools import setup
from Cython.Build import cythonize
import os



setup(
    ext_modules = cythonize(
            ['cerializer/cerializer_base/*.pyx', 'cerializer/*.pyx'], annotate=False
    )
)
