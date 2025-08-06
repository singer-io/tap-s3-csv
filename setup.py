#!/usr/bin/env python

from setuptools import setup

setup(name='tap-s3-csv',
      version='1.5.3',
      description='Singer.io tap for extracting CSV files from S3',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_s3_csv'],
      install_requires=[
          'backoff==1.10.0',
          'boto3==1.39.8',
          'urllib3==2.5.0',
          'singer-encodings==0.1.2',
          'singer-python==5.13.2',
          'voluptuous==0.15.2'
      ],
      extras_require={
          'dev': [
              'ipdb'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-s3-csv=tap_s3_csv:main
      ''',
      packages=['tap_s3_csv'])
