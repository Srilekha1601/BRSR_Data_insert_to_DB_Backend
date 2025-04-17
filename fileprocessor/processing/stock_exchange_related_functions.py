import pymysql

from .db import db_connection
def extract_stock_exchange(stock_exchange):

    """
    Extracts the stock exchange code from the database.

    This function queries the `stock_exchange_master` table to check if the provided 
    stock exchange code exists. If found, it returns the corresponding code; 
    otherwise, it defaults to "OTHER".

    Args:
        stock_exchange (str): The stock exchange code to look up.

    Returns:
        str: The corresponding stock exchange code if found; otherwise, "OTHER".
    
    Exceptions:
        Handles MySQL errors gracefully and returns "OTHER" in case of a database error.
    """
    
    # try:
    with db_connection.cursor() as cursor:
        query = """
        SELECT stock_exchange_code
        FROM stock_exchange_master 
        WHERE stock_exchange_code = %s
        """
        cursor.execute(query, (stock_exchange,))
        result = cursor.fetchone()

        # print("result", result)

    return result["stock_exchange_code"] if result else "OTHER"

    # return result[0] if result else "OTHER"  
    # except pymysql.MySQLError as e:
    #     print("Error:", e)
    #     return "OTHER"

def get_year_code_from_context(context):
    """
    Determines the year code based on the context string.

    This function checks if the context contains 'PY' (Previous Year) and returns
    the appropriate year code.

    Args:
        context (str): The context string to analyze.

    Returns:
        str: 'PY' if context contains 'PY', otherwise 'CY' (Current Year).
    """
    if "PY" in context:
        return "PY"
    else:
        return "CY"

def get_product_code_from_context(context):
    """
    Returns the product code for waste.

    Args:
        context (str): The context string (not used in current implementation).

    Returns:
        str: Always returns 'o_waste' as the product code.
    """
    return "o_waste"

def extract_unit(unit_ref):
    """
    Extracts and returns the unit reference.

    Args:
        unit_ref (str): The unit reference to extract.

    Returns:
        str: The unit reference as is.
    """
    return unit_ref