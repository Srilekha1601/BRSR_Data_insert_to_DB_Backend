"""
Utility functions for processing Excel templates and mapping extracted data.
"""

import pandas as pd
import os
from datetime import datetime
from .json_utils import json_convertor
from .function_mapping import function_map

def section_bysection_template_to_database_template(template_excel, extracted_data_excel):
    """
    Processes a template Excel file and maps extracted data from another Excel file.

    Args:
        template_excel (pd.ExcelFile): The Excel template file containing predefined structures.
        extracted_data_excel (pd.ExcelFile): The Excel file containing extracted data.

    Returns:
        dict: A dictionary where keys are sheet names and values are updated DataFrames.
    """
    updated_sheets = {}

    for sheet_name in template_excel.sheet_names:
        template_df = template_excel.parse(sheet_name)
        
        if "Sheet Name" not in template_df.columns or "XML Tag" not in template_df.columns or "Context Reference" not in template_df.columns:
            print(f"Sheet '{sheet_name}' in the template does not have the required columns. Skipping this sheet.")
            updated_sheets[sheet_name] = template_df  
            continue

        template_df["Extracted Value"] = None
        
        for index, template_row in template_df.iterrows():
            target_sheet_name = template_row["Sheet Name"]
            xml_tag = template_row["XML Tag"]
            context_reference = template_row["Context Reference"]
            many_flag = str(template_row.get("Many", "")).strip().lower() == "y"
            many_rows_flag = str(template_row.get("many_row", "")).strip().lower() == "y"
            reference_mapper_table_name = template_row.get("Reference Mapper Name", None)
            #print(reference_mapper_table_name)
            if pd.notna(reference_mapper_table_name):
                mapper_sheet_df = template_excel.parse(reference_mapper_table_name)
                mapper_sheet_df["Extracted JSON"] = None
                for mapper_index, mapper_sheet_row in mapper_sheet_df.iterrows():
                    mapper_sheet_name = mapper_sheet_row["Sheet Name"]
                    mapper_sheet_XML_context_JSON = json_convertor(mapper_sheet_row["XML_tag_cont_ref_JSON"])
                    extracted_mapper_sheet_df = extracted_data_excel.parse(mapper_sheet_name)
                
                    mapper_json_list = []
                    for mapper_json in mapper_sheet_XML_context_JSON:
                        extracted_val = extracted_mapper_sheet_df[
                            (extracted_mapper_sheet_df["XML Tag"] == mapper_json["XML Tag"]) &
                            (extracted_mapper_sheet_df["Context Reference"] == mapper_json["Context Reference"])
                        ]
                        if not extracted_val.empty:
                            mapper_json["value"] = extracted_val["Extracted Value"].iloc[0]
                            extracted_value = extracted_val["Extracted Value"].iloc[0]
                            if "function" in mapper_json and mapper_json["function"]:
                                function_name = mapper_json["function"]

                                # Dynamically lookup the function in the current Jupyter Notebook
                                if function_name in globals():
                                    try:
                                        func = globals()[function_name]

                                        if function_name == "extract_unit":
                                            mapper_json["value"] = func(extracted_val["unit_ref_y"].iloc[0])
                                        else:
                                            mapper_json["value"] = func(extracted_value)

                                        # Apply the function to the extracted value
                                        #mapper_json["value"] = func(extracted_value)
                                    except Exception as e:
                                        print(f"Error applying function {function_name}: {e}")
                                        mapper_json["value"] = None
                                else:
                                    print(f"Function {function_name} is not defined in the current notebook.")
                                    mapper_json["value"] = None
                            else:
                                # No function specified, directly assign the extracted value
                                mapper_json["value"] = extracted_value
                        else:
                            mapper_json["value"] = None
                            print("No match found")
                        mapper_json_list.append(mapper_json)
                        #print(mapper_json_list)
                        mapper_sheet_df.at[mapper_index, "Extracted JSON"] = mapper_json_list

                #print(mapper_sheet_df)
                #template_df = mapper_sheet_df
                updated_sheets[reference_mapper_table_name] = mapper_sheet_df
            if target_sheet_name not in extracted_data_excel.sheet_names:
                print(f"Sheet '{target_sheet_name}' not found in the extracted data file. Skipping this row.")
                continue

            extracted_sheet_df = extracted_data_excel.parse(target_sheet_name)

            if "XML Tag" not in extracted_sheet_df.columns or "Context Reference" not in extracted_sheet_df.columns or "Extracted Value" not in extracted_sheet_df.columns:
                print(f"Sheet '{target_sheet_name}' in the extracted data does not have the required columns. Skipping this row.")
                continue

            matching_rows = extracted_sheet_df.loc[extracted_sheet_df["XML Tag"] == xml_tag]

            if not matching_rows.empty:
                if many_flag:
                    context_references = []
                    extracted_values = []
                    for _, match in matching_rows.iterrows():
                        if pd.isna(match["Extracted Value"]): 
                            print(f"Skipping match due to NaN Extracted Value for XML Tag: {xml_tag} and Context Reference: {match['Context Reference']}")
                            continue 
                    
                        context_references.append(match["Context Reference"])
                        extracted_values.append(match["Extracted Value"])
                
                    template_row["Context Reference"] = ', '.join(context_references)
                    template_row["Extracted Value"] = ', '.join(map(str, extracted_values))
                    template_df.loc[index] = template_row
                elif many_rows_flag:
                    new_rows = []
                    for _, match in matching_rows.iterrows():
                        if pd.isna(match["Extracted Value"]):
                            print(f"Skipping match due to NaN Extracted Value for XML Tag: {xml_tag} and Context Reference: {match['Context Reference']}")
                            continue

                        #print(matching_rows)
                        new_row = template_row.copy()
                        #new_row = pd.Series()
                        new_row["Context Reference"] = match["Context Reference"]
                        new_row["Extracted Value"] = match["Extracted Value"]
                        new_rows.append(new_row)
                        
                        print("new row", type(new_row))
                    
                    new_rows_df = pd.DataFrame(new_rows)
                    
                    if not new_rows_df.empty:  
                        template_df = pd.concat([template_df, new_rows_df], ignore_index=True)
                        template_df_1 = pd.concat([template_df, new_rows_df], ignore_index=True)
                        non_null_mask = template_df['XML Tag'].notna() & template_df['Context Reference'].notna()

                        template_df.loc[non_null_mask] = template_df.loc[non_null_mask].drop_duplicates(
                            subset=['XML Tag', 'Context Reference','function'], keep='last'
                        )
                        template_df = template_df.dropna(subset=['Datafields'])
                        template_df = template_df.reset_index(drop=True)

                else:
                    matching_row = matching_rows.loc[matching_rows["Context Reference"] == context_reference]
                    if not matching_row.empty:
                        template_row["Extracted Value"] = matching_row.iloc[0]["Extracted Value"]
                        template_df.loc[index] = template_row

        updated_sheets[sheet_name] = template_df

    return updated_sheets


def process_sheets(updated_sheets, function_map):
    """
    Processes each sheet's DataFrame row by row.
    If a row has a non-null function name in the 'function' column,
    the function is applied to the 'Extracted Value' column.
    Additionally, replaces apostrophes with double quotes in 'Extracted Value'.

    If the function name contains "context", the function is passed both 
    the extracted value and the context.

    Parameters:
        updated_sheets (dict): Dictionary where keys are sheet names and values are DataFrames.
        function_map (dict): Dictionary mapping function names to actual function references.

    Returns:
        dict: Updated dictionary with processed DataFrames.
    """
    #print("updated_sheets",updated_sheets)
    for sheet_name, updated_df in updated_sheets.items():
        for index, row in updated_df.iterrows():
            function_name = row.get("function")
            extracted_value = row.get("Extracted Value")
            context = row.get("Context Reference")

            if pd.notna(extracted_value) and isinstance(extracted_value, str):
                # Replace single quotes with double quotes
                extracted_value = extracted_value.replace("'", "")

            if pd.notna(function_name) and function_name in function_map:
                func = function_map[function_name]

                # Check if the function name contains "context" and pass the context
                if "context" in function_name.lower():
                    extracted_value = func(context)
                else:
                    extracted_value = func(extracted_value)

            updated_df.at[index, "Extracted Value"] = extracted_value  
            print("updated_sheets",updated_sheets)
             # # Save combined sheets to output
            output_dir = os.path.join(os.getcwd(), "processed_sheets")
            os.makedirs(output_dir, exist_ok=True)
            # timestamp = time.strftime("%Y%m%d_%H%M%S")
            output_filename = f"processed_sheets.xlsx"
            output_file_path = os.path.join(output_dir, output_filename)

            #Optional: Write processed data to file
            with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
                for sheet_name, df in updated_sheets.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)

    return updated_sheets

def save_processed_template(updated_sheets, output_path=None, sheet_name_prefix=None):
    """
    Save the processed template sheets to an Excel file in a dedicated folder.
    
    Args:
        updated_sheets (dict): Dictionary where keys are sheet names and values are DataFrames.
        output_path (str, optional): Path to save the output file. If None, generates a default name in the 'processed_sheets' directory.
        sheet_name_prefix (str, optional): Prefix to add to sheet names in the output file. If None, original sheet names are used.
        
    Returns:
        str: Path to the saved file.
    """
    # Create a dedicated folder for processed sheets if it doesn't exist
    output_dir = os.path.join(os.getcwd(), "processed_sheets")
    os.makedirs(output_dir, exist_ok=True)
    
    if output_path is None:
        # Generate a default filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"updated_template_{timestamp}.xlsx"
        output_path = os.path.join(output_dir, filename)
    else:
        # If output_path is provided, ensure it's in the processed_sheets directory
        filename = os.path.basename(output_path)
        output_path = os.path.join(output_dir, filename)
    
    # Save all sheets to the Excel file
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        for sheet_name, updated_df in updated_sheets.items():
            # Apply sheet name prefix if provided
            output_sheet_name = sheet_name
            if sheet_name_prefix:
                output_sheet_name = f"{sheet_name_prefix}_{sheet_name}"
            
            updated_df.to_excel(writer, sheet_name=output_sheet_name, index=False)
    
    print(f"Processed template saved to: {output_path}")
    return output_path
