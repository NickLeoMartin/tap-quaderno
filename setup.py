#!/usr/bin/env python

from setuptools import setup

setup(name='tap-quaderno',
      version='0.1.0',
      description='Singer.io tap for extracting data from the Quaderno API',
      author='nickleomartin@gmail.com',
      classifiers=[
          'Programming Language :: Python :: 3 :: Only'
      ],
      py_modules=['tap_quaderno'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.22.0',
          'singer-python==5.8.0'
      ],
      entry_points='''
          [console_scripts]
          tap-quaderno=tap_quaderno:main
      ''',
      packages=['tap_quaderno'],
      package_data={
          'tap_quaderno': ['schemas/*.json'],
      }
      )
