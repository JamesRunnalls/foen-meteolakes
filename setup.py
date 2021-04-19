# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='foen-meteolakes',
    version='0.1.0',
    description='A python package for downloading FOEN data for the Meteolakes project.',
    long_description=readme,
    author='James Runnalls',
    author_email='James.Runnalls@eawag.ch',
    url='https://github.com/JamesRunnalls/foen-meteolakes',
    license=license,
    packages=find_packages(exclude=('docs'))
)