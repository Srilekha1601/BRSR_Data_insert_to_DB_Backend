"""
Utility functions for handling JSON data processing and conversion.
"""

import json

def json_convertor(string):
    """
    Converts a malformed JSON-like string into a properly formatted JSON object.

    Args:
        string (str): The input string containing JSON data with extra curly braces.

    Returns:
        dict or list: The parsed JSON object if successful, otherwise None.

    Notes:
        - Fixes improperly formatted curly braces.
        - Handles JSON decoding errors gracefully.
        - Returns None if parsing fails.
    """
    corrected_string = string.replace('{{', '{').replace('}}', '}')
    
    try:
        parsed_list = json.loads(corrected_string)
        return parsed_list
    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)
        return None 