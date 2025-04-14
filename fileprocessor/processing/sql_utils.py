"""
Utility functions for SQL query processing and manipulation.
"""

import re
import csv

def generate_unique_multi_insert_query(query):
    """
    Converts a single-row SQL INSERT statement into a multi-row INSERT statement 
    while handling duplicate column names.

    This function extracts the table name, columns, and values from the given SQL 
    `INSERT INTO` query. If any columns appear multiple times, it restructures the 
    query into multiple rows, ensuring each duplicate column gets its own row while 
    maintaining unique column structure.

    Args:
        query (str): A valid SQL `INSERT INTO` statement.

    Returns:
        str: A modified SQL query with a multi-row `INSERT INTO` format.

    Raises:
        ValueError: If the input query is not in a valid `INSERT INTO` format.

    Example:
        Input:
        ```
        INSERT INTO `users` (`id`, `name`, `name`, `email`) 
        VALUES (1, 'Alice', 'Bob', 'alice@example.com');
        ```

        Output:
        ```
        INSERT INTO `users` (`id`, `name`, `email`) VALUES 
        (1, 'Alice', 'alice@example.com'), 
        (1, 'Bob', 'alice@example.com');
        ```
    """
    
    match = re.search(r'INSERT INTO `?(\w+)`? \((.*?)\) VALUES \((.*)\);?$', query)

    if not match:
        raise ValueError("Invalid SQL INSERT query format")

    table_name, columns, values = match.groups()

    
    columns_list = [col.strip("` ") for col in columns.split(",")]
    
    values_list = next(csv.reader([values], skipinitialspace=True, quotechar="'"))

    
    column_index_map = {}
    for i, col in enumerate(columns_list):
        column_index_map.setdefault(col, []).append(i)

    
    duplicate_columns = {col: idxs for col, idxs in column_index_map.items() if len(idxs) > 1}

    
    if not duplicate_columns:
        return query  

    
    num_entries = max(len(indices) for indices in duplicate_columns.values())

    unique_columns = list(dict.fromkeys(columns_list))  

    common_values = {col: values_list[column_index_map[col][0]] for col in unique_columns if col not in duplicate_columns}

    multi_row_values = []
    for i in range(num_entries):
        new_values = []
        
        for col in unique_columns:
            if col in duplicate_columns:
                original_indexes = column_index_map[col]
                new_values.append(values_list[original_indexes[i]] if i < len(original_indexes) else "NULL")
            else:
                new_values.append(common_values[col])

        
        formatted_row = ", ".join(f"'{val}'" if val != "NULL" else "NULL" for val in new_values)
        multi_row_values.append(f"({formatted_row})")

    multi_insert_query = f"INSERT INTO `{table_name}` ({', '.join(f'`{col}`' for col in unique_columns)}) VALUES {', '.join(multi_row_values)};"

    return multi_insert_query 