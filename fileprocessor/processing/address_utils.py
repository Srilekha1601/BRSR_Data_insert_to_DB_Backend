# processing/address_utils.py

import re
from .db import db_connection
from .data_loader import load_city_data, load_pincode_data
import pymysql

city_df = load_city_data()
pincode_df = load_pincode_data()

def get_state_code(state_name):

    """
        Function: get_state_code

        This function retrieves the state or union territory code from the `state_master` database table 
        based on the given state name. It executes a SQL query using a database connection and returns 
        the corresponding state code if found; otherwise, it returns "OTH". 

        In case of a database error, it catches the exception, prints the error message, and returns "OTH" as a fallback.

        Parameters:
            state_name (str): The name of the state or union territory.

        Returns:
            str: The corresponding state code if found; otherwise, "OTH".
    """

    try:
        with db_connection.cursor() as cursor:
            query = """
            SELECT state_or_union_territory_code
            FROM state_master 
            WHERE state_or_union_territory_name = %s
            """
            cursor.execute(query, (state_name,))
            result = cursor.fetchone()

            print("result state", result)

        return result["state_or_union_territory_code"] if result else "OTH"
       

        # return result[0] if result else "OTH" 

    except pymysql.MySQLError as e:
        print("Error:", e)
        
        return "OTH"
    
    
def get_city_code(city_name):

    """
    Function: get_city_code

    The get_city_code function retrieves the city code from the `city_master` database table 
    based on the provided city name. It executes a SQL query using a database connection and 
    returns the corresponding city code if found; otherwise, it returns "OTH". 

    If a database error occurs, the function catches the exception, prints the error message, 
    and returns "OTH" as a fallback.

    Parameters:
        city_name (str): The name of the city.

    Returns:
        str: The corresponding city code if found; otherwise, "OTH".
    """
    try:
        with db_connection.cursor() as cursor:
            query = """
            SELECT city_code
            FROM city_master 
            WHERE city_name = %s
            """
            cursor.execute(query, (city_name,))
            result = cursor.fetchone()

            print("result city", result)
       
        return result["city_code"] if result else "OTH"

        # return result[0] if result else "OTH"  

    except pymysql.MySQLError as e:
        print("Error:", e)
        
        return "OTH"


    

def extract_zip(address):

    """
    Function: extract_zip

    The extract_zip function extracts a 6-digit zip code from a given address string. 
    It removes spaces from the address and uses a regular expression to find an exact 
    6-digit sequence that is not part of a longer number.

    If a valid 6-digit zip code is found, it returns the zip code as a string. 
    Otherwise, it returns "000000" as a default value.

    Parameters:
        address (str): The address string from which the zip code is to be extracted.

    Returns:
        str: The extracted 6-digit zip code if found; otherwise, "000000".
    """

    address = address.replace(" ", "")
    
    pattern = r"(?<!\d)\d{6}(?!\d)"
    match = re.search(pattern, address)
    if match:
        return match.group()
    return "000000"

def extract_state(address):

    """
    Function: extract_state

    The extract_state function determines the state code based on the pin code extracted 
    from the provided address. It first extracts the 6-digit pin code using the extract_zip 
    function and then retrieves the first three digits of the pin code. The function iterates 
    through the pincode_df DataFrame to find the corresponding state name based on the 
    pin code range. It then returns the state's code using the get_state_code function. 

    If no match is found, the function returns "OTH" as a default value.

    Parameters:
        address (str): The address string from which the state code is to be determined.

    Returns:
        str: The state code if a valid match is found; otherwise, "OTH".
    """
    pin_code=extract_zip(address)
    first_three = int(str(pin_code)[:3]) 
    for _, row in pincode_df.iterrows():
       
        start, end = map(int, row["range_of_first_3_digits_of_pincode"].split(" - "))
        if start <= first_three <= end:
            state_name = row["state_name"]
            return get_state_code(state_name)
            #return state_name
    return "OTH" 


def extract_city(address):

    """
    Function: extract_city

    The extract_city function identifies the city name from a given address by comparing 
    it against a list of cities stored in the city_df DataFrame. It first attempts a direct 
    word match, and if unsuccessful, it searches for a substring match within the address. 
    If multiple matches are found, the function selects the one appearing last in the text. 
    Finally, it returns the corresponding city code using the get_city_code function.

    Parameters:
        address (str): The address string from which the city is to be extracted.

    Returns:
        str: The city code if a valid match is found; otherwise, "OTH".
    """

    matches = []
    direct_match_flag = 0
    for city in city_df["city"]:
        address_words = address.split(" ")
        address_words_lower = []
        for words in address_words:
            address_words_lower.append(words.lower())
        if city.lower() in address_words_lower:
            matched_city = city
            direct_match_flag = 1
            break 
        
        else:
            match_pos = address.lower().find(city.lower())
            if match_pos != -1:
                matches.append((city, match_pos))
       

    if direct_match_flag == 1:
        
        final_city = matched_city
    else:
        matches.sort(key=lambda x: x[1])
        final_city = matches[-1][0] if matches else "OTH"
        
    
    return get_city_code(final_city)

    
def extract_country_code(address):
    """
    Function: extract_country_code

    This function extracts the country code from the given address. 
    Since the default assumption is that all addresses belong to India, 
    it always returns "IND".

    Parameters:
        address (str): The address string (not used in the function).

    Returns:
        str: The country code "IND".
    """
    return "IND"
def extract_street(address):
    """
    Function: extract_street

    This function extracts the street address by removing the pin code, city name, 
    and state name from the given address.

    Parameters:
        address (str): The full address string.

    Returns:
        str: The extracted street address after removing pin code, city, and state names.
    """
     
    pin_code = extract_zip(address)
    city_name = extract_city(address)
    state_name = extract_state(extract_zip(address))

    rest_address = address
    if pin_code:
        rest_address = rest_address.replace(pin_code, "")
    if city_name:
        
        rest_address = re.sub(city_name, "", rest_address, flags=re.IGNORECASE)
    if state_name:
        
        rest_address = re.sub(state_name, "", rest_address, flags=re.IGNORECASE)
    
    
    return rest_address.strip(", ").strip()
