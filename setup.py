#!/usr/bin/env python

from setuptools import setup

setup(name='tap-oracle',
      version='1.3.0',
      description='Singer.io tap for extracting data from Oracle',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      install_requires=[
          'singer-python==6.1.0',
          'cx_Oracle==6.1',
          'strict-rfc3339==0.7'
      ],
      extras_require={
        'dev': [
            'pylint',
        ]
      },
      entry_points='''
          [console_scripts]
          tap-oracle=tap_oracle:main
      ''',
      packages=['tap_oracle', 'tap_oracle.sync_strategies']

)
