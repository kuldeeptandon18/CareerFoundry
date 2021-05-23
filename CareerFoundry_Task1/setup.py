from setuptools import setup
from setuptools import find_packages, setup
setup(
    name='pyspark-pytest',
    version='1.0',
    packages=find_packages(),
    author='Kuldeep Singh',
    description='',
    setup_requires=["pytest-runner"],
    py_modules=['src']
)