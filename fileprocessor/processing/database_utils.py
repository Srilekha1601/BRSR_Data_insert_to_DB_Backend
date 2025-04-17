"""
Utility functions for database operations and data insertion.
"""

import pandas as pd
import pymysql
from .sql_utils import generate_unique_multi_insert_query
from .config import table_name_sheet_dict
from .logger_utils import setup_logger

# Dictionary mapping table names to their corresponding sheet names
# table_name_sheet_dict is now imported from config.py

def insert_into_database(updated_sheets, db_connection, logger):
    """
    Inserts data from multiple updated sheets into a database while handling references 
    and dynamically structured JSON data.

    This function iterates over each sheet in the `updated_sheets` dictionary, processes 
    the data, and generates SQL `INSERT` statements. It handles:
    
    - Extracting column names and values from the DataFrame.
    - Handling reference tables by mapping data accordingly.
    - Parsing JSON structures from a reference mapper and dynamically inserting values.
    - Constructing batch `INSERT` queries for efficiency.

    Args:
        updated_sheets (dict): A dictionary where keys are sheet names, and values are 
                               Pandas DataFrames containing the extracted data.
        db_connection: A database connection object (e.g., pymysql connection).

    Returns:
        None: Commits the changes directly to the database.

    Raises:
        pymysql.Error: If an error occurs while executing SQL queries.
    """
    cursor = db_connection.cursor()

    company_id = None
    company_code = ""
    # Iterate over each sheet in the updated_sheets
    for sheet_name, updated_df in updated_sheets.items():
        if "mapper" in sheet_name.lower():
            continue
        if "master" in sheet_name.lower() and sheet_name.lower() != "company_master":
            continue

        all_columns = []  
        all_values = []   

        table_name = sheet_name.replace(" ", "_")

        # Iterate over each row in the DataFrame
        for _, row in updated_df.iterrows():
            datafields = row["Datafields"]
            extracted_value = row["Extracted Value"]
            many_row_flag = row["many_row"]
            if (table_name == "company_master" and datafields == "company_code"):
                company_code = extracted_value
                #print(company_code)
            extracted_value = str(extracted_value).strip() if pd.notna(extracted_value) else None
            
            datafields_list = [field.strip() for field in datafields.split(',')]

            reference_table_name = row["Reference Table Name"]
            
            if pd.isna(reference_table_name):  
                all_columns.extend(datafields_list)  
                all_values.extend(
                    [extracted_value if extracted_value is not None else "NULL"] * len(datafields_list)
                ) 
            
            reference_table_name = row["Reference Table Name"]
            col_name = row["Datafields"]
            # print("ref table name111",row["Reference Mapper Name"])

            if pd.notna(reference_table_name):  
                reference_table_name = str(reference_table_name).strip().replace(" ", "_")  
                if reference_table_name == "company_master":
                    all_columns.append(col_name)
                    extracted_value = company_code
                    all_values.append(str(extracted_value))
                else:
                    reference_mapper_name = row.get("Reference Mapper Name")
                    print("ref table name", reference_mapper_name)
                    if pd.notna(reference_mapper_name):
                        reference_mapper_name = str(reference_mapper_name).strip().replace(" ", "_")
                        
                        if reference_mapper_name in updated_sheets:
                            mapper_df = updated_sheets[reference_mapper_name]
                            insert_rows = []
                           
                            if "XML_tag_cont_ref_JSON" in mapper_df.columns:
                                columns_before_xml = list(mapper_df.columns[:mapper_df.columns.get_loc("XML_tag_cont_ref_JSON")])

                            if "Extracted JSON" in mapper_df.columns:
                                for _, mapper_row in mapper_df.iterrows():
                                    extracted_json = mapper_row["Extracted JSON"]
                                    
                                    try:
                                        json_data = eval(extracted_json) if isinstance(extracted_json, str) else extracted_json
                                        
                                        if isinstance(json_data, list):
                                            for item in json_data:
                                                column_name = item.get("column_name")
                                                column_value = item.get("value")
                                                #print(column_name)
                                            
                                                if column_name: 
                                                    insert_row = {column: mapper_row[column] for column in columns_before_xml}  # Include all columns before XML_tag_cont_ref_JSON
                                                    insert_row[column_name] = column_value  # Add extracted column
                                                    insert_rows.append(insert_row)
                                    except Exception as e:
                                        print(f"Error parsing Extracted JSON: {e}")
                                insert_df = pd.DataFrame(insert_rows)
                                group_by_columns = columns_before_xml
                                
                                insert_df_1 = insert_df.groupby(group_by_columns, as_index=False).sum()
                                insert_values_list = []
                                dynamic_columns = list(insert_df_1.columns)
                                all_columns_with_extra = []
                                all_columns_with_extra = all_columns.copy()
                                all_columns_with_extra.extend(dynamic_columns)
                                for _, row in insert_df_1.iterrows():
                                    insert_values = all_values.copy() 
                                    
                                    for col in dynamic_columns:
                                        insert_values.append(row[col] if pd.notna(row[col]) else "NULL")
                                    
                                    insert_values_list.append(tuple(insert_values))
                              
                                all_columns = all_columns_with_extra
                                all_values = insert_values_list

        if isinstance(all_values[0], tuple): 
            columns_str = ', '.join([f"`{col}`" for col in all_columns])
            
            values_str_list = []
            for row in all_values:
                values_str = ', '.join([f"'{val}'" if val != "NULL" else "NULL" for val in row])
                values_str_list.append(f"({values_str})")

            values_str = ', '.join(values_str_list)

            query = f"INSERT INTO `{table_name_sheet_dict[table_name]}` ({columns_str}) VALUES {values_str}"
            
        else:  
            columns_str = ', '.join([f"`{col}`" for col in all_columns])
            values_str = ', '.join([f"'{val}'" if val != "NULL" else "NULL" for val in all_values])

            query = f"INSERT INTO `{table_name_sheet_dict[table_name]}` ({columns_str}) VALUES ({values_str})"
            query = generate_unique_multi_insert_query(query)

        print(f"Executing query: {query}")
        
        # try:
        #     #print(f"Executing query: {query}")
        #     cursor.execute(query)
        # except pymysql.Error as e:
        #     print(f"Error inserting data into table '{table_name}': {e}")
        #     logger.error(f"Error inserting into table '{table_name}': {e}")
        #     logger.info(f"Failed query: {query}")
        #     continue  

        try:
            cursor.execute(query)
        except pymysql.Error as e:
            print(f"Error inserting data into table '{table_name}': {e}")
            # Initialize the logger here only if it's not already provided
            if logger is None:
                logger = setup_logger("db_error_log.log")
            logger.error(f"Error inserting into table '{table_name}': {e}")
            logger.info(f"Failed query: {query}")
            continue  
        
    db_connection.commit() 