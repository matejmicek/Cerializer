from setuptools import setup
from Cython.Build import cythonize



setup(
    ext_modules = cythonize(
            ['cerializer/cerializer_base/*.pyx', 'cerializer/*.pyx'], annotate=False
    )
)
