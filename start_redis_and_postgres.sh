#!/bin/sh

python3 scrapy_autoproxy/config.py
cd scrapy_autoproxy/data
ls -al
. ./.env && docker-compose up -d && docker-compose logs