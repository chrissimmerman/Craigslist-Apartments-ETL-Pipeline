import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlalchemy

def scrape():
    # Creating empty list to hold apartment data
    apartments = []
    
    # Capturing HTML data
    for i in range(0, 3000, 120):
        link = 'https://minneapolis.craigslist.org/search/apa?s=' + str(i)
        page = requests.get(link)
        soup = BeautifulSoup(page.content, 'html.parser')
        listings = soup.find_all('li', class_='result-row')

        # iterate through listings list, extracting relevant data
        for listing in listings:
            date = listing.find('time', class_='result-date')
            if date:
                date_text = date["datetime"]
            link = listing.find('a', class_='result-title hdrlnk')['href']
            header = listing.find('a', class_='result-title hdrlnk').text
            price = listing.find('span', class_='result-price').text
            address = listing.find('span', class_='result-hood').text
            
            # if no bedrooms listed, replace entry with emptry string
            try:
                bedrooms = listing.find('span', class_='housing').text.split()[0]
            except:
                bedrooms = ""
            
            # if no sq_foot listed, replace entry with emptry string
            try:
                sq_foot = listing.find('span', class_='housing').text
            except:
                sq_foot = ""

            # Append data to housing list
            apartments.append({
                'date': date_text,
                'link': link,
                'header' : header.lstrip(),
                'price' : price, 
                'address' : address.lstrip(), 
                'bedrooms': bedrooms.lstrip(),
                'sq_foot' : sq_foot.lstrip()})
    
    return apartments

def clean_price(df):
    # Create list to hold clean price values
    price_rows = []

    # Remove '$' and ',' characters from entries, append results as integers to price_rows list
    for entry in df['price']:
        if "$" in entry:
            price_substring = entry.split('$')[1]
            if "," in price_substring:
                price_substring = price_substring.replace(',', '')
        price_rows.append(int(price_substring))

    # Replace price column with cleaned values
    df = df.assign(price=price_rows)

    return df

def clean_sqft(df):
    # Create list to hold clean sq_ft values
    sq_ft_rows = []

    # Remove alpha characters, whitespace, and newline characters
    for entry in df['sq_foot']:
        if "ft2" in entry:
            sq_substring = entry.replace(' ', '')
            sq_substring = sq_substring.split('ft2')[0]
            if "\n" in sq_substring:
                sq_substring = sq_substring.split('\n')[1]
        # Replace empty value with null
        elif "" in entry:
            sq_substring = np.nan
        sq_ft_rows.append(sq_substring)

    # Replace sq_foot column with cleaned values
    df = df.assign(sq_foot=sq_ft_rows)

    # Calculate median number of bedrooms
    sqft_median = int(df['sq_foot'].median(skipna=True))

    # Replace null values with median number of bedrooms
    df['sq_foot'].fillna(sqft_median, inplace=True)

    # Convert remaining strings in bedroom column to ints
    df['sq_foot'] = [int(s) for s in df['sq_foot']]

    return df

def clean_bedrooms(df):
    # Iterate over the bedrooms column, isolating bedroom number
    for i, value in df['bedrooms'].items():
        # Replace empty values with Null abbrevation
        if "" in value:
            df.at[i, 'bedrooms'] = np.nan
        if "br" in value:
            df.at[i, 'bedrooms'] = value.split('br')[0]

    # Replace entries without bedroom number with string representation of null value
    df['bedrooms'] = df['bedrooms'].str.replace(r'(.*ft2)', "NaN", regex=True)

    # Replace string representation of null values with true null values
    df.replace('NaN', np.nan, inplace=True)

    # Calculate median number of bedrooms
    median = int(df['bedrooms'].median(skipna=True))

    # Replace null values with median number of bedrooms
    df['bedrooms'].fillna(median, inplace=True)

    # Convert remaining strings in bedroom column to ints
    df['bedrooms'] = [int(s) for s in df['bedrooms']]

    return df

def clean_address(df):
    # Create list to hold clean address values
    address_rows = []

    # Iterate through column, removing parentheses in entries
    for entry in df['address']:
        if "(" in entry:
            address_substring = entry.split('(')[1]
            if ")" in address_substring:
                address_substring = address_substring.split(')')[0]
        address_rows.append(address_substring)

    # Replace address column with cleaned values
    df = df.assign(address=address_rows)

    return df

def create_or_append(df):
    # Connect to the database
    engine = sqlalchemy.create_engine('postgresql://user:password@localhost/mydatabase')

    # Create a metadata object
    metadata = sqlalchemy.MetaData()

    # Create an Inspector object
    inspector = sqlalchemy.inspect(engine)

    if inspector.has_table("mytable"):
        # Reflect the table
        table = sqlalchemy.Table("mytable", metadata, autoload=True, autoload_with=engine)
        
        # Build the SELECT statement to get max index
        query = (
            sqlalchemy.select([sqlalchemy.func.max(table.c.id)])
            .select_from(table)
        )

        # Execute the SELECT statement
        result = engine.execute(query).fetchone()

        # Get the maximum value from the result
        max_value = result[0]

        # Increment the index column
        new_index = max_value + 1 if max_value is not None else 1

        # Create list of new indexes
        new_indexes = [i for i in range (new_index, new_index + 3000)]

        # Replace dataframe ids with incremented index column
        df = df.assign(id=new_indexes)

        # Append the new DataFrame to the existing table
        df.to_sql("mytable", engine, if_exists='append', index=False, index_label='id')

        # Reflect the table
        table = sqlalchemy.Table("mytable", metadata, autoload=True, autoload_with=engine)

        # Create a new alias for the table
        table_2 = table.alias()

        # create a subquery to identify duplicate rows to be deleted
        subquery = (
            table
            .select()
            .with_hint(table, "NO_INDEX_FFS", 'oracle')
            .where(
                sqlalchemy.and_(
                    table.c.link == table_2.c.link,
                    table.c.header == table_2.c.header,
                    table.c.id > table_2.c.id
                )
            )
            .select_from(
                table.join(table_2,
                        sqlalchemy.and_(
                            table.c.link == table_2.c.link,
                            table.c.header == table_2.c.header
                        )
                        )
            )
        ).alias()

        # create the DELETE statement
        query = table.delete().where(
            sqlalchemy.exists([1]).where(
                sqlalchemy.and_(
                    table.c.link == subquery.c.link,
                    table.c.header == subquery.c.header,
                    table.c.id == subquery.c.id
                )
            )
        )

        # Execute the query
        result = engine.execute(query)

    else:
        table = sqlalchemy.Table('mytable', metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column('date', sqlalchemy.String),
            sqlalchemy.Column('link', sqlalchemy.String),
            sqlalchemy.Column('header', sqlalchemy.String),
            sqlalchemy.Column('price', sqlalchemy.Integer),
            sqlalchemy.Column('address', sqlalchemy.String),
            sqlalchemy.Column('bedrooms', sqlalchemy.Integer),
            sqlalchemy.Column('sq_foot', sqlalchemy.Integer)
        )
        
        # Create the table
        metadata.create_all(engine)

        # Write the DataFrame to a table in the database
        df.to_sql('mytable', con=engine, if_exists='append', index=False)

    # Close connection
    metadata.clear()
    engine.dispose()

def main():
    print("Extracting data...")

    # Scrape housing listings, Put result in dataframe
    df = pd.DataFrame(scrape())

    print("Transforming data...")

    # Clean price column
    df = clean_price(df)

    # Clean sq_foot column
    df = clean_sqft(df)

    # Clean bedrooms column
    df = clean_bedrooms(df)

    # Clean address column
    df = clean_address(df)
    
    print("Loading data...")
    
    # Create or append databse
    create_or_append(df)

    print("Success")

if __name__ == "__main__":
    main()