# Craigslist Apartments ETL Pipeline

Craiglist Apartments ETL Pipeline extracts all Minneapolis Craigslist apartment listings, transforms the data into a usable format, and loads the transformed data into a PostgreSQL database. The script can be executed multiple times to append new listings to the database without fear of duplicate rows or repeated index values.
