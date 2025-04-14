import pymysql


# db_connection = pymysql.connect(
#     host='localhost',
#     user='root',
#     password='Indranil@001',
#     database='brsr_v1',
# )

from .db import db_connection



def length_nic_code(nic_code):

    """
    This function takes an NIC (National Industrial Classification) code as input 
    and returns a dictionary containing:
    
    - 'length': The total number of characters in the NIC code.
    - 'first_digit': The first digit of the NIC code.

    Parameters:
    nic_code (str): The NIC code as a string.

    Returns:
    dict: A dictionary with the length of the NIC code and its first digit.
    """


    length = len(nic_code)
    first_digit = nic_code[0]

    return {"length":length,"first_digit":first_digit}


def get_NIC_code_from_group(nic_code):

    """
    Retrieves the NIC group code from the `nic_group_master` database table 
    based on the provided NIC code.

    This function executes a SQL query to find the corresponding NIC group code.
    If a match is found, it returns the group code; otherwise, it returns "oth".
    In case of a database error, the function catches the exception, prints the error, 
    and returns "oth" as a fallback.

    Parameters:
    nic_code (str): The NIC code to search for.

    Returns:
    str: The corresponding NIC group code if found, otherwise "oth".
    """
    
    try:
        with db_connection.cursor() as cursor:
            query = """
            SELECT NIC_group_code
            FROM nic_group_master 
            WHERE NIC_group_code = %s
            """
            cursor.execute(query, (nic_code,))
            result = cursor.fetchone()

        return result[0] if result else "oth"  
    except pymysql.MySQLError as e:
        print("Error:", e)
        return "oth"
    
    
def get_NIC_code_from_class(nic_code):
    """
    Retrieves the NIC class code from the `nic_class_master` database table 
    based on the provided NIC code.

    This function executes a SQL query to find the corresponding NIC class code.
    If a match is found, it returns the class code; otherwise, it returns "oth".
    In case of a database error, the function catches the exception, prints the error, 
    and returns "oth" as a fallback.

    Parameters:
    nic_code (str): The NIC code to search for.

    Returns:
    str: The corresponding NIC class code if found, otherwise "oth".
    """
    
    try:
        with db_connection.cursor() as cursor:
            query = """
            SELECT NIC_class_code
            FROM nic_class_master 
            WHERE NIC_class_code = %s
            """
            cursor.execute(query, (nic_code,))
            result = cursor.fetchone()

        return result[0] if result else "oth"  
    except pymysql.MySQLError as e:
        print("Error:", e)
        return "oth"
    
def get_NIC_code_from_subclass(nic_code):

    """
    Retrieves the NIC subclass code from the `nic_subclass_master` database table 
    based on the provided NIC code.

    This function executes a SQL query to find the corresponding NIC subclass code.
    If a match is found, it returns the subclass code; otherwise, it returns "oth".
    In case of a database error, the function catches the exception, prints the error, 
    and returns "oth" as a fallback.

    Parameters:
    nic_code (str): The NIC code to search for.

    Returns:
    str: The corresponding NIC subclass code if found, otherwise "oth".
    """
    
    try:
        with db_connection.cursor() as cursor:
            query = """
            SELECT NIC_subclass_code
            FROM nic_subclass_master 
            WHERE NIC_subclass_code = %s
            """
            cursor.execute(query, (nic_code,))
            result = cursor.fetchone()

        return result[0] if result else "oth"  
    except pymysql.MySQLError as e:
        print("Error:", e)
        return "oth"



def find_group(nic_code,digit_to_cut):

    """
    Extracts a specific number of leading digits from the given NIC code 
    and retrieves the corresponding NIC group code.

    This function takes an NIC code and a specified number of digits to extract 
    from the beginning of the code. It then queries the NIC group master table 
    to find the corresponding NIC group code.

    Parameters:
    nic_code (str): The full NIC code.
    digit_to_cut (int): The number of leading digits to extract.

    Returns:
    str: The corresponding NIC group code if found, otherwise "oth".
    """

    group_code_digits = nic_code[:digit_to_cut]
    
    group_code = get_NIC_code_from_group(group_code_digits)
    return group_code



def find_class(nic_code,digit_to_cut):

    """
    Extracts a specific number of leading digits from the given NIC code 
    and retrieves the corresponding NIC class code.

    This function takes an NIC code and a specified number of digits to extract 
    from the beginning of the code. It then queries the NIC class master table 
    to find the corresponding NIC class code.

    Parameters:
    nic_code (str): The full NIC code.
    digit_to_cut (int): The number of leading digits to extract.

    Returns:
    str: The corresponding NIC class code if found, otherwise "oth".
    """

    class_code_digits = nic_code[:digit_to_cut]
    class_code = get_NIC_code_from_class(class_code_digits) 
    return class_code   

def find_NIC_group_class_subclass(nic_code):

    
    """
    Determines the NIC group, class, and subclass codes based on the given NIC code.

    This function takes an NIC code, checks its length, and retrieves the corresponding 
    group, class, and subclass codes using database queries. It also ensures that if the 
    NIC code starts with "0", it is trimmed before processing.

    The function follows these rules:
    - If the NIC code has 5 digits, it retrieves the subclass, class, and group codes.
    - If the NIC code has 4 digits, it checks the subclass first. If not found, it retrieves 
      the class and group codes.
    - If the NIC code has 3 digits, it retrieves the class and group codes.
    - If the NIC code has 2 digits, it only retrieves the group code.
    - If none of the above conditions are met, it returns "oth" for all.

    Parameters:
    nic_code (str): The NIC code to be processed.

    Returns:
    dict: A dictionary containing:
        - group_code (str): The corresponding NIC group code or "oth".
        - class_code (str): The corresponding NIC class code or "oth".
        - subclass_code (str): The corresponding NIC subclass code or "oth".
    """
    if nic_code is None:
        #print("Error: NIC code is None")
        return {"group_code": "oth", "class_code": "oth", "subclass_code": "oth"}

    length_info = length_nic_code(nic_code)
    if not length_info or "length" not in length_info:
        #print(f"Error: Could not determine length for NIC code {nic_code}")
        return {"group_code": "oth", "class_code": "oth", "subclass_code": "oth"}

    length = length_info["length"]
    first_digit = length_info["first_digit"]

    if first_digit == "0":
        nic_code = nic_code[1:]

    length = length_nic_code(nic_code)["length"]

    if length == 5:
        subclass_code = get_NIC_code_from_subclass(nic_code)
        group_code = find_group(nic_code, 3)
        class_code = find_class(nic_code, 4)
    elif length == 4:
        subclass_code = get_NIC_code_from_subclass(nic_code)
        if subclass_code == "oth":
            class_code = get_NIC_code_from_class(nic_code)
            group_code = find_group(nic_code, 3)
        else:
            class_code = find_class(nic_code, 3)
            group_code = find_group(nic_code, 2)
    elif length == 3:
        subclass_code = "oth"
        class_code = get_NIC_code_from_class(nic_code)
        if class_code == "oth":
            group_code = get_NIC_code_from_group(nic_code)
        else:
            group_code = find_group(nic_code, 2)
    elif length == 2:
        class_code = "oth"
        subclass_code = "oth"
        group_code = get_NIC_code_from_group(nic_code)
    else:
        group_code = "oth"
        class_code = "oth"
        subclass_code = "oth"

    return {"group_code": group_code, "class_code": class_code, "subclass_code": subclass_code}




def extract_nic_group_code(nic_code):
    """
    Extracts the NIC group code from the given NIC code.

    This function calls `find_NIC_group_class_subclass(nic_code)` to retrieve the 
    NIC classification details and extracts the group code. If an error occurs, 
    it handles exceptions and returns "oth".

    Parameters:
    nic_code (str): The NIC code to be processed.

    Returns:
    str: The extracted NIC group code, or "oth" if not found or in case of an error.
    """
    try:
        return find_NIC_group_class_subclass(nic_code)["group_code"]
    except KeyError:
        
        print(f"Error: 'group_code' not found for NIC code {nic_code}")
        return "oth"
    except Exception as e:
        
        print(f"Unexpected error for NIC code {nic_code}: {e}")
        return "oth"
    


def extract_nic_class_code(nic_code):

    """
    Extracts the NIC class code from the given NIC code.

    This function calls `find_NIC_group_class_subclass(nic_code)` to retrieve the 
    NIC classification details and extracts the class code. If an error occurs, 
    it handles exceptions and returns "oth".

    Parameters:
    nic_code (str): The NIC code to be processed.

    Returns:
    str: The extracted NIC class code, or "oth" if not found or in case of an error.
    """

    try:
        return find_NIC_group_class_subclass(nic_code)["class_code"]
    except KeyError:
        print(f"Error: 'class_code' not found for NIC code {nic_code}")
        return "oth"
    except Exception as e:
        print(f"Unexpected error for NIC code {nic_code}: {e}")
        return "oth"

def extract_nic_subclass_code(nic_code):

    """
    Extracts the NIC subclass code from the given NIC code.

    This function calls `find_NIC_group_class_subclass(nic_code)` to retrieve the 
    NIC classification details and extracts the subclass code. If an error occurs, 
    it handles exceptions and returns "oth".

    Parameters:
    nic_code (str): The NIC code to be processed.

    Returns:
    str: The extracted NIC subclass code, or "oth" if not found or in case of an error.
    """


    try:
        return find_NIC_group_class_subclass(nic_code)["subclass_code"]
    except KeyError:
        print(f"Error: 'subclass_code' not found for NIC code {nic_code}")
        return "oth"
    except Exception as e:
        print(f"Unexpected error for NIC code {nic_code}: {e}")
        return "oth"

