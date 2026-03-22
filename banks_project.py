# Code for ETL operations on World's Largest Banks data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime


# ─────────────────────────────────────────────────────────────
# Known entities / Configuration
# ─────────────────────────────────────────────────────────────
url             = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
exchange_rate_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
table_attribs   = ['Name', 'MC_USD_Billion']
db_name         = 'Banks.db'
table_name      = 'Largest_banks'
csv_path        = './Largest_banks_data.csv'
output_path     = './Largest_banks_data.csv'
log_file        = 'code_log.txt'


# ─────────────────────────────────────────────────────────────
# TASK 1: Logging function
# ─────────────────────────────────────────────────────────────
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S'   # Year-Monthname-Day-Hour:Minute:Second
    now       = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ' : ' + message + '\n')


# ─────────────────────────────────────────────────────────────
# TASK 2: Extract
# ─────────────────────────────────────────────────────────────
def extract(url, table_attribs):
    ''' This function aims to extract the required information from
    the website and save it to a data frame. The function returns
    the data frame for further processing. '''

    page  = requests.get(url).text
    data  = BeautifulSoup(page, 'html.parser')
    df    = pd.DataFrame(columns=table_attribs)

    # The target table is under the heading "By market capitalization"
    tables = data.find_all('tbody')
    rows   = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            # col[1] = Bank Name (contains anchor tag)
            # col[2] = Market Cap in USD Billion
            if col[0].find('a') is not None:
                name       = col[1].find_all('a')[1].contents[0]    # Bank name
                market_cap = col[2].contents[0]                      # MC value string
                # Strip trailing newline and cast to float
                market_cap = float(market_cap.strip())
                data_dict  = {
                    'Name':          name,
                    'MC_USD_Billion': market_cap
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df  = pd.concat([df, df1], ignore_index=True)

    return df


# ─────────────────────────────────────────────────────────────
# TASK 3: Transform
# ─────────────────────────────────────────────────────────────
def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies (GBP, EUR, INR), rounded to 2 decimal places.'''

    # Read exchange rate CSV and convert to dictionary
    # Format: Currency -> Exchange Rate (relative to USD)
    exchange_rate_df = pd.read_csv(csv_path)
    exchange_rate    = exchange_rate_df.set_index('Currency').to_dict()['Rate']

    # Add transformed columns (USD -> GBP, EUR, INR)
    df['MC_GBP_Billion'] = [np.round(x * float(exchange_rate['GBP']), 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * float(exchange_rate['EUR']), 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * float(exchange_rate['INR']), 2) for x in df['MC_USD_Billion']]

    return df


# ─────────────────────────────────────────────────────────────
# TASK 4: Load to CSV
# ─────────────────────────────────────────────────────────────
def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path, index=False)


# ─────────────────────────────────────────────────────────────
# TASK 5: Load to Database
# ─────────────────────────────────────────────────────────────
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


# ─────────────────────────────────────────────────────────────
# TASK 6: Run Queries on Database
# ─────────────────────────────────────────────────────────────
def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


# ─────────────────────────────────────────────────────────────
# MAIN: Function calls in correct order with log entries
# ─────────────────────────────────────────────────────────────

# Log: Declaring known values
log_progress('Preliminaries complete. Initiating ETL process')

# Task 2: Extract
df = extract(url, table_attribs)
print(df)
log_progress('Data extraction complete. Initiating Transformation process')

# Task 3: Transform
# Download exchange rate CSV first (or reference local copy)
exchange_rate_path = './exchange_rate.csv'
df = transform(df, exchange_rate_path)
print(df)
# Quiz answer: Print MC_EUR_Billion for 5th largest bank (index 4)
print('\nMC_EUR_Billion[4] =', df['MC_EUR_Billion'][4])
log_progress('Data transformation complete. Initiating Loading process')

# Task 4: Load to CSV
load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

# Task 5: Load to Database
sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

# Task 6: Run Queries
# Query 1 — Full table (all offices)
print('\n--- Query 1: Full table ---')
run_query('SELECT * FROM Largest_banks', sql_connection)

# Query 2 — London office: Name and MC_GBP_Billion
print('\n--- Query 2: London Office (Name, MC_GBP_Billion) ---')
run_query('SELECT Name, MC_GBP_Billion FROM Largest_banks', sql_connection)

# Query 3 — Berlin office: Name and MC_EUR_Billion
print('\n--- Query 3: Berlin Office (Name, MC_EUR_Billion) ---')
run_query('SELECT Name, MC_EUR_Billion FROM Largest_banks', sql_connection)

# Query 4 — New Delhi office: Name and MC_INR_Billion
print('\n--- Query 4: New Delhi Office (Name, MC_INR_Billion) ---')
run_query('SELECT Name, MC_INR_Billion FROM Largest_banks', sql_connection)

# Query 5 — Average market cap in GBP
print('\n--- Query 5: Average MC in GBP Billion ---')
run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks', sql_connection)

# Query 6 — Top 5 banks by name
print('\n--- Query 6: Top 5 Bank Names ---')
run_query('SELECT Name FROM Largest_banks LIMIT 5', sql_connection)

log_progress('Process Complete')

# Close the database connection
sql_connection.close()
log_progress('Server Connection closed')