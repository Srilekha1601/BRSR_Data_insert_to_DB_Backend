def extract_reporting_boundary(reporting_boundary):

    """
    Determines the reporting boundary type based on the input string.

    Args:
        reporting_boundary (str): The input text describing the reporting boundary.

    Returns:
        str: "standalone" if the input contains "standalone",
             "consolidated" if it contains "consolidated",
             None if neither is found.
    
    Notes:
        - The function performs a case-insensitive check.
        - If the input does not match either category, it returns None.
    """
    if "standalone" in reporting_boundary.lower():
        return "standalone"
    elif "consolidated" in reporting_boundary.lower():
        return "consolidated"