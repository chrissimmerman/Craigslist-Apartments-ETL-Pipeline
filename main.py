import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sqlalchemy

def scrape(link):
    # Capturing HTML data
    page = requests.get(link)
    soup = BeautifulSoup(page.content, 'html.parser')
    listings = soup.find_all('li', class_='result-row')

    return listings

def get_data(listings):
    # Create list to hold housing data
    houses = []

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
        houses.append({
            'date': date_text,
            'link': link,
            'header' : header.lstrip(),
            'price' : price, 
            'address' : address.lstrip(), 
            'bedrooms': bedrooms.lstrip(),
            'sq_foot' : sq_foot.lstrip()})
    
    return houses

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

def main():
    # Scrape housing listings
    listings = scrape("https://minneapolis.craigslist.org/search/apa")

    # Get housing data and clear listings list
    housing = get_data(listings)
    listings.clear()

    # Put list in dataframe and clear housing list
    df = pd.DataFrame(housing)
    housing.clear()

    # Clean price column
    df = clean_price(df)

    # Clean sq_foot column
    df = clean_sqft(df)

    # Clean bedrooms column
    df = clean_bedrooms(df)

    # Clean address column
    df = clean_address(df)

    # Connect to the database
    engine = sqlalchemy.create_engine('postgresql://user:password@localhost/mydatabase')

    # Create a metadata object
    metadata = sqlalchemy.MetaData()

    # Create an Inspector object
    inspector = sqlalchemy.inspect(engine)

    if inspector.has_table("mytable"):
        # Find the maximum value of the index column
        max_index = df['index'].max()

        # Increment the index column
        new_index = max_index + 1 if max_index is not None else 1

        # Create a new DataFrame with an incremented index column
        data = {'index': [new_index], 'date': df['date'], 'link': df['link'], 'header': df['header'], 'price': df['price'], 'address': df['address'], 'bedrooms': df['bedrooms'], 'sq_foot': df['sq_foot']}
        df2 = pd.DataFrame(data)

        # Append the new DataFrame to the existing table or create a new table
        df2.to_sql("mytable", engine, if_exists='append', index=False)
    else:
        # Write the DataFrame to a table in the database
        df.to_sql('mytable', con=engine, if_exists='append')

        # Reflect the table
        table = sqlalchemy.Table("mytable", metadata, autoload=True, autoload_with=engine)

        # Build a query to delete duplicate rows
        query = table.delete().where(
            sqlalchemy.exists([1]).where(
                sqlalchemy.and_(
                    table.c.link == table.c.link,
                    table.c.header == table.c.header,
                    table.c.index > table.c.index
                )
            )
        )

    # Execute the query
    result = engine.execute(query)

    # Close connection
    engine.dispose()

if __name__ == "__main__":
    main()