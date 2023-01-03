# Craigslist Apartments ETL Pipeline  

The Craiglist Apartments ETL Pipeline:  
•	Extracts all Minneapolis Craigslist apartment listings using the requests and Beautiful Soup libraries.  
•	Transforms data using string functions, regular expressions, and type casting, in addition to using Pandas and NumPy to handle null values.  
•	Loads transformed data into a PostgreSQL relational database using Pandas and SQLAlchemy.  

The script can be executed multiple times to append new listings to the database without fear of duplicate rows or repeated index values.
