from setuptools import setup, find_packages

setup(
    name='sparkflight',
    version='0.1.0',
    packages=find_packages(include=['exampleproject', 'exampleproject.*']),
    install_requires=[
        'pyspark==3.1.1'
    ]
)