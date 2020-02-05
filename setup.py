# -*- coding: utf-8 -*-

# Copyright © 2018 by IBPort. All rights reserved.
# @Author: Neal Wong
# @Email: ibprnd@gmail.com

from setuptools import setup

setup(
    name='scrapy_autoproxy',
    version='1.0.0',
    description='Machine learning proxy picker',
    long_description=open('README.rst').read(),
    keywords='scrapy proxy web-scraping',
    license='MIT License',
    author="Dan Chrostowski",
    author_email='dan@streetscrape.com',
    url='https://streetscrape.com',
    packages=[
        'scrapy_autoproxy',
    ],
    package_dir={'scrapy_autoproxy': 'scrapy_autoproxy'},
    package_data={'scrapy_autoproxy/data': ['docker-compose.yml','.env','config/autoproxy.cfg','database/','init_sql/1-schema.sql','init_sql/2-seeds.sql']},
    install_requires=[
        'redis',
        'psycopg2-binary',
        'docker'
    ],
)
