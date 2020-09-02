# SQL Data Aggregation Tool
import pandas as pd
import logging
import pyodbc
import os


# ------------ CONFIGURATION SETTINGS ------------ #
logging_file = 'log.txt' # The location and name of log file
sql_query_folder = 'SqlQueries' # The folder that contains all your sql queries
results_file = 'results.csv' # The results file to save results to
server_name = "XXXXX" # Insert server name
database_name =  "AdventureWorks2017" # Insert database name
# ----------------------------------------------- #

# Set up configurations for logging, we will write it to a text file and the screen
logging.basicConfig(format ='%(asctime)s: %(levelname)s [%(filename)s:%(lineno)d] %(message)s',
                    datefmt ='%Y-%m-%d %I:%M:%S %p',
                    handlers = [logging.FileHandler(logging_file, mode = 'w'), logging.StreamHandler()] ,             
                    level=logging.INFO)


conn =  pyodbc.connect(Driver = "{SQL Server Native Client 11.0}", 
                                Server = server_name,
                                Database = database_name,
                                Trusted_Connection = "yes")

def run_queries():
    # Set up an empty results data frame to get all the queries into a single view
    results_column_names = [
                            'Script Name',
                            'Row Number',
                            'Column Name',
                            'Value'
                            ]
    results_df = pd.DataFrame(columns=results_column_names)

    # Loop through all SQL queries, if the query runs, unpivot results and append to results data frame.
    # If script fails, log the error
    for file_name in os.listdir(sql_query_folder):
        try:
            full_path = os.path.join(sql_query_folder, file_name)
            query = open(full_path, mode='r', encoding="utf-8").read()
            full_query = "SET NOCOUNT ON ; SET ANSI_WARNINGS OFF;" + query # Prevent any warnings
            query_results = pd.read_sql_query(full_query, conn)
            query_results.reset_index(inplace=True)
            query_results.rename(columns={'index': 'Row Number'}, inplace=True)
            query_results = pd.melt(query_results,
                                    id_vars='Row Number',
                                    var_name='Column Name',
                                    value_name='Value')
            query_results['Script Name'] = file_name
            results_df = results_df.append(query_results, ignore_index=True)
            logging.info("{} successfully run.".format(file_name))
        except Exception as error:
            logging.error("{} failed.".format(file_name))

    return results_df


def main():  
    logging.info("Welcome to the SQL Data Aggregation Tool.")
    df = run_queries()
    df.to_csv(results_file, index=False)
    logging.info("CSV Output generated ")
    logging.info("Consider your data aggregated :)")


if __name__ == "__main__":
    main()