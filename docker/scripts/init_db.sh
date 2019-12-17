#!/bin/bash
git clone https://github.com/tesseract-olap/tesseract-example-app.git
cat tesseract-example-app/sql/schema.sql | clickhouse-client -mn
tail -n +2 tesseract-example-app/data/time.csv | clickhouse-client --query="INSERT INTO tesseract_webshop_time FORMAT CSV";
tail -n +2 tesseract-example-app/data/categories.csv | clickhouse-client --query="INSERT INTO tesseract_webshop_categories FORMAT CSV";
tail -n +2 tesseract-example-app/data/geographies.csv | clickhouse-client --query="INSERT INTO tesseract_webshop_geographies FORMAT CSV";
tail -n +2 tesseract-example-app/data/sales.csv | clickhouse-client --query="INSERT INTO tesseract_webshop_sales FORMAT CSV";

