#!/usr/bin/env python

from setuptools import setup

setup(name='tap-s3-csv',
      version='1.3.5',
      description='Singer.io tap for extracting CSV files from S3',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_s3_csv'],
      install_requires=[
          'backoff==1.8.0',
          'boto3==1.9.57',
          'singer-encodings==0.1.2',
          'singer-python==5.12.1',
          'voluptuous==0.10.5'
      ],
      extras_require={
          'dev': [
              'ipdb==0.11'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-s3-csv=tap_s3_csv:main
      ''',
      packages=['tap_s3_csv'])
