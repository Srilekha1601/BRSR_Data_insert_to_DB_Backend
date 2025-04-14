"""
Utility functions for handling context-related operations in the file processing system.
"""

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

def get_benefit_code_from_context(context):
    return "OTH"    