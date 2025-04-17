
import os
import tempfile
import traceback
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework import status
from django.http import HttpResponse
from .processing.extractor import section_wise_data_extraction_from_xml_by_template
from .processing.template_processor import section_bysection_template_to_database_template, process_sheets
from .processing.database_utils import insert_into_database
from .processing.function_mapping import function_map
from .processing.db import db_connection
import pandas as pd
import pymysql
from django.conf import settings
import time
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from rest_framework.decorators import api_view
from .processing.logger_utils import setup_logger
import threading
import tempfile, time, os
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from rest_framework.response import Response
from rest_framework import status
from.processing.variable import SECTION_A_FILENAME_IDENTIFIER


@csrf_exempt
@api_view(["POST"])
def process_xml_template(request):
    try:
        template_file = request.FILES.get("template")
        xml_file = request.FILES.get("xml")
        xml_name = xml_file.name

        if not template_file or not xml_file:
            return Response({"error": "Both template and xml files are required."}, status=400)

        insert_data_directory = os.path.join(os.getcwd(), "insert_data_project")
        os.makedirs(insert_data_directory, exist_ok=True)

        template_file.seek(0)
        xml_file.seek(0)

        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx", dir=insert_data_directory) as temp_template, \
                tempfile.NamedTemporaryFile(delete=False, suffix=".xml", dir=insert_data_directory) as temp_xml:

            temp_template.write(template_file.read())
            temp_template.flush()

            temp_xml.write(xml_file.read())
            temp_xml.flush()

            print("Template saved to:", temp_template.name)
            print("XML saved to:", temp_xml.name)

            try:
                sheet_dfs = section_wise_data_extraction_from_xml_by_template(
                    template_file=temp_template.name,
                    xml_file=temp_xml.name,
                    output_dir=insert_data_directory
                )
            except Exception as inner_e:
                traceback.print_exc()
                return Response({"error": f"Extraction failed: {str(inner_e)}"}, status=500)

        output_files = [f for f in os.listdir(insert_data_directory) if f.endswith(".xlsx")]
        output_files.sort(key=lambda x: os.path.getmtime(os.path.join(insert_data_directory, x)), reverse=True)

        if output_files:
            output_path = os.path.join(insert_data_directory, output_files[0])
            with open(output_path, "rb") as f:
                response_data = f.read()

            response = HttpResponse(response_data, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = f'attachment; filename="{output_files[0]}"'

            for f in os.listdir(insert_data_directory):
                file_path = os.path.join(insert_data_directory, f)
                if file_path != output_path:
                    os.remove(file_path)

            return response

        return Response({"error": "No output file generated."}, status=500)

    except Exception as e:
        traceback.print_exc()
        return Response({"error": str(e)}, status=500)


    # API endpoint for processing uploaded XLSX files and inserting data into the database.
    
    # This view accepts two file uploads:
    # 1. extracted_data_file: The XLSX file containing extracted data
    # 2. template_file: The XLSX template file
    
    # It processes these files using the template processor, saves the processed sheets to a file,
    # and then inserts the data into the database.

            
# @csrf_exempt
# @api_view(["POST"])
    

# def data_insert(request, **kwargs):
#     temp_template_path = None
#     template_excel = None
#     output_file_path = None
#     extracted_data_excel = None

#     try:
#         if 'section_template_file' not in request.FILES:
#             return Response(
#                 {"error": "Template file is required"},
#                 status=status.HTTP_400_BAD_REQUEST
#             )

#         # Get the latest extracted_data_file from 'insert_data_project'
#         insert_data_directory = os.path.join(os.getcwd(), "insert_data_project")
#         xlsx_files = [f for f in os.listdir(insert_data_directory) if f.endswith(".xlsx")]
#         if not xlsx_files:
#             return Response({"error": "No extracted data file found"}, status=404)

#         xlsx_files.sort(key=lambda f: os.path.getmtime(os.path.join(insert_data_directory, f)), reverse=True)
#         latest_extracted_file_path = os.path.join(insert_data_directory, xlsx_files[0])

#         # Save template_file from frontend to temp path
#         template_file = request.FILES['section_template_file']
#         print("template_files",template_file)

#         if not template_file.name.endswith('.xlsx'):
#             return Response(
#                 {"error": "Template file must be an XLSX file"},
#                 status=status.HTTP_400_BAD_REQUEST
#             )

#         with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_template:
#             for chunk in template_file.chunks():
#                 temp_template.write(chunk)
#             temp_template_path = temp_template.name

#         extracted_data_excel = pd.ExcelFile(latest_extracted_file_path)
#         template_excel = pd.ExcelFile(temp_template_path)

#         try:
#             updated_sheets = section_bysection_template_to_database_template(template_excel, extracted_data_excel)
#         except Exception as e:
#             return Response({"error": f"Error processing templates: {str(e)}"}, status=500)

#         try:
#             updated_sheets = process_sheets(updated_sheets, function_map)
#         except Exception as e:
#             return Response({"error": f"Error processing sheets: {str(e)}"}, status=500)

#         try:
#             output_dir = os.path.join(os.getcwd(), "processed_sheets")
#             # os.makedirs(output_dir, exist_ok=True)

#             timestamp = time.strftime("%Y%m%d_%H%M%S")
#             output_filename = f"processed_sheets_{timestamp}.xlsx"
#             output_file_path = os.path.join(output_dir, output_filename)

#             # with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
#             #     for sheet_name, df in updated_sheets.items():
#             #         df.to_excel(writer, sheet_name=sheet_name, index=False)

#             # print(f"Processed sheets saved to: {output_file_path}")
#         except Exception as e:
#             return Response({"error": f"Error saving processed sheets: {str(e)}"}, status=500)

#         try:
#             logger = setup_logger(output_filename)
#             success = insert_into_database(updated_sheets, db_connection, logger)
#             #print("success",success)
            
#             return Response({
#                 "message": "Data processed and inserted successfully",
#                 "file_path": output_file_path
#             }, status=200)
            

#         except Exception as e:
#             return Response({
#                 "error": f"Database error: {str(e)}",
#                 "file_path": output_file_path
#             }, status=500)

#     except Exception as e:
#         return Response({"error": f"Unexpected error: {str(e)}"}, status=500)

#     finally:
#         if extracted_data_excel:
#             extracted_data_excel.close()
#         if template_excel:
#             template_excel.close()
#         time.sleep(1)
#         try:
#             if temp_template_path and os.path.exists(temp_template_path):
#                 os.unlink(temp_template_path)
#         except Exception as e:
#             print(f"Error deleting template file: {e}")




# @csrf_exempt
# @api_view(["POST"])
# def data_insert(request, **kwargs):
    
 
#     output_file_path = None
#     extracted_data_excel = None
#     section_a_found = False
 
#     try:
#         if 'section_template_file' not in request.FILES:
#             return Response(
#                 {"error": "Template files are required"},
#                 status=status.HTTP_400_BAD_REQUEST
#             )
 
#         # Get the latest extracted_data_file from 'insert_data_project'
#         insert_data_directory = os.path.join(os.getcwd(), "insert_data_project")
#         xlsx_files = [f for f in os.listdir(insert_data_directory) if f.endswith(".xlsx")]
#         if not xlsx_files:
#             return Response({"error": "No extracted data file found"}, status=404)
 
#         xlsx_files.sort(key=lambda f: os.path.getmtime(os.path.join(insert_data_directory, f)), reverse=True)
#         latest_extracted_file_path = os.path.join(insert_data_directory, xlsx_files[0])
 
#         # Handle template files
#         template_files = request.FILES.getlist('section_template_file')
#         print("template_files", template_files)
#         section_a_file = None
#         other_files = []
 
#         for file in template_files:
#             if 'dummy_template_sectionA' in file.name:
#                 section_a_file = file
#                 section_a_found = True
#             else:
#                 other_files.append(file)
 
#         # Check for Section A if multiple files
#         if len(template_files) > 1 and not section_a_found:
#             return Response({
#                 "error": "Section A template file (dummy_template_sectionA) is required when uploading multiple files."
#             }, status=400)
        
#         if len(template_files) == 1 and not section_a_found:
#             # Still allow processing for B or C
#             other_files.append(template_files[0])

#             print("other_files",other_files)
 
#         def process_template_file(file):
#             temp_template_path = None
#             try:
#                 with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_template:
#                     for chunk in file.chunks():
#                         temp_template.write(chunk)
#                     temp_template_path = temp_template.name
 
#                 extracted_data = pd.ExcelFile(latest_extracted_file_path)
#                 template_data = pd.ExcelFile(temp_template_path)
 
#                 updated_sheets = section_bysection_template_to_database_template(template_data, extracted_data)
#                 updated_sheets = process_sheets(updated_sheets, function_map)
#                 return updated_sheets
 
#             finally:
#                 if extracted_data:
#                     extracted_data.close()
#                 if template_data:
#                     template_data.close()
#                 if temp_template_path and os.path.exists(temp_template_path):
#                     os.unlink(temp_template_path)
 
#         # Process Section A first
#         section_a_data = process_template_file(section_a_file) if section_a_file else {}
 
#         # Process other files concurrently
#         other_files_data = []
#         if len(template_files) > 1:
#             with ThreadPoolExecutor() as executor:
#                 futures = [executor.submit(process_template_file, f) for f in other_files]
#                 other_files_data = [future.result() for future in futures]
 
#         # Combine sheets
#         # updated_sheets = section_a_data or {}
#         updated_sheets = {}

#         if section_a_data:
#             updated_sheets.update(section_a_data)

#         for data in other_files_data:
#             if isinstance(data, dict):
#                 updated_sheets.update(data)
#                 for data in other_files_data:
#                     if isinstance(data, dict):
#                         updated_sheets.update(data)
 
#         # Save combined sheets to output
#         output_dir = os.path.join(os.getcwd(), "processed_sheets")
#         # os.makedirs(output_dir, exist_ok=True)
#         timestamp = time.strftime("%Y%m%d_%H%M%S")
#         output_filename = f"processed_sheets_{timestamp}.xlsx"
#         output_file_path = os.path.join(output_dir, output_filename)
 
#         # with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
#         #     for sheet_name, df in updated_sheets.items():
#         #         df.to_excel(writer, sheet_name=sheet_name, index=False)
 
#         # print(f"Processed sheets saved to: {output_file_path}")
 
#         # Insert into DB
#         logger = setup_logger(output_filename)
#         success = insert_into_database(updated_sheets, db_connection, logger)
 
#         return Response({
#             "message": "Data processed and inserted successfully",
#             "file_path": output_file_path
#         }, status=200)
 
#     except Exception as e:
#         return Response({
#             "error": f"Unexpected error: {str(e)}",
#             "file_path": output_file_path
#         }, status=500)
 
@csrf_exempt
@api_view(["POST"])
def data_insert(request, **kwargs):
    
 
    output_file_path = None
    extracted_data_excel = None
    section_a_found = False
 
    try:
        if 'section_template_file' not in request.FILES:
            return Response(
                {"error": "Template files are required"},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Get the latest extracted_data_file from 'insert_data_project'
        insert_data_directory = os.path.join(os.getcwd(), "insert_data_project")
        xlsx_files = [f for f in os.listdir(insert_data_directory) if f.endswith(".xlsx")]
        if not xlsx_files:
            return Response({"error": "No extracted data file found"}, status=404)

        xlsx_files.sort(key=lambda f: os.path.getmtime(os.path.join(insert_data_directory, f)), reverse=True)
        latest_extracted_file_path = os.path.join(insert_data_directory, xlsx_files[0])

        # Handle template files
        template_files = request.FILES.getlist('section_template_file')
        print("template_files", template_files)

        section_a_file = None
        other_files = []
        section_a_found = False

        for file in template_files:
            if "Data_insert_BRSR_SectionA" in file.name:
                section_a_file = file
                section_a_found = True
            else:
                other_files.append(file)

        # Section A validation
        if len(template_files) > 1 and not section_a_found:
            return Response({
                "error": f"Section A template file (Data_insert_BRSR_SectionA) is required when uploading multiple files."
            }, status=400)
        
        if len(template_files) == 1 and not section_a_found:
            other_files.append(template_files[0])  # Single file, not Section A

        def process_template_file(file):
            temp_template_path = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_template:
                    for chunk in file.chunks():
                        temp_template.write(chunk)
                    temp_template_path = temp_template.name

                extracted_data = pd.ExcelFile(latest_extracted_file_path)
                template_data = pd.ExcelFile(temp_template_path)

                updated_sheets = section_bysection_template_to_database_template(template_data, extracted_data)
                updated_sheets = process_sheets(updated_sheets, function_map)
                return updated_sheets

            finally:
                if extracted_data:
                    extracted_data.close()
                if template_data:
                    template_data.close()
                if temp_template_path and os.path.exists(temp_template_path):
                    os.unlink(temp_template_path)

        updated_sheets = {}

        # If only one file is uploaded, no need for threading
        if len(template_files) == 1:
            file = template_files[0]
            processed_data = process_template_file(file)
            if isinstance(processed_data, dict):
                updated_sheets.update(processed_data)

        else:
            # Process Section A first
            section_a_data = process_template_file(section_a_file) if section_a_file else {}
            if isinstance(section_a_data, dict):
                updated_sheets.update(section_a_data)

            # Process other files concurrently
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(process_template_file, f) for f in other_files]
                for future in futures:
                    result = future.result()
                    if isinstance(result, dict):
                        updated_sheets.update(result)

        # Save combined sheets to output
        output_dir = os.path.join(os.getcwd(), "processed_sheets")
        os.makedirs(output_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_filename = f"processed_sheets_{timestamp}.xlsx"
        output_file_path = os.path.join(output_dir, output_filename)

        #Optional: Write processed data to file
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            for sheet_name, df in updated_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        # logger = setup_logger(output_filename)
        success = insert_into_database(updated_sheets, db_connection, logger=None)

        

        return Response({
            "message": "Data processed and inserted successfully",
            "file_path": output_file_path
        }, status=200)

    except Exception as e:
        return Response({
            "error": f"Unexpected error: {str(e)}",
            "file_path": output_file_path if 'output_file_path' in locals() else None
        }, status=500)


# def process_template_file(file,latest_extracted_file_path):
#         temp_template_path = None
#         # try:
#         with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_template:
#             for chunk in file.chunks():
#                 temp_template.write(chunk)
#             temp_template_path = temp_template.name

#         extracted_data = pd.ExcelFile(latest_extracted_file_path)
#         template_data = pd.ExcelFile(temp_template_path)

#         updated_sheets = section_bysection_template_to_database_template(template_data, extracted_data)
#         updated_sheets = process_sheets(updated_sheets, function_map)
#         return updated_sheets


# @csrf_exempt
# @api_view(["POST"])
# def data_insert(request, **kwargs):
    
 
#     output_file_path = None
#     extracted_data_excel = None
#     section_a_found = False
 
    
#     if 'section_template_file' not in request.FILES:
#         return Response(
#             {"error": "Template files are required"},
#             status=status.HTTP_400_BAD_REQUEST
#         )

#     # Get the latest extracted_data_file from 'insert_data_project'
#     insert_data_directory = os.path.join(os.getcwd(), "insert_data_project")
#     xlsx_files = [f for f in os.listdir(insert_data_directory) if f.endswith(".xlsx")]
#     if not xlsx_files:
#         return Response({"error": "No extracted data file found"}, status=404)

#     xlsx_files.sort(key=lambda f: os.path.getmtime(os.path.join(insert_data_directory, f)), reverse=True)
#     latest_extracted_file_path = os.path.join(insert_data_directory, xlsx_files[0])

#     # Handle template files
#     template_files = request.FILES.getlist('section_template_file')
#     print("template_files", template_files)

#     section_a_file = None
#     other_files = []
#     section_a_found = False

#     for file in template_files:
#         if "Data_insert_BRSR_SectionA" in file.name:
#             section_a_file = file
#             section_a_found = True
#         else:
#             other_files.append(file)

#     # Section A validation
#     if len(template_files) > 1 and not section_a_found:
#         return Response({
#             "error": f"Section A template file (Data_insert_BRSR_SectionA) is required when uploading multiple files."
#         }, status=400)
    
#     if len(template_files) == 1 and not section_a_found:
#         other_files.append(template_files[0])  # Single file, not Section A

    
#         # finally:
#         #     if extracted_data:
#         #         extracted_data.close()
#         #     if template_data:
#         #         template_data.close()
#         #     if temp_template_path and os.path.exists(temp_template_path):
#         #         os.unlink(temp_template_path)

#     updated_sheets = {}

#     # If only one file is uploaded, no need for threading
#     if len(template_files) == 1:
#         file = template_files[0]
#         print("file from 1",file)
#         processed_data = process_template_file(file,latest_extracted_file_path)
#         if isinstance(processed_data, dict):
#             updated_sheets.update(processed_data)

#     else:
#         print("file from 2",section_a_file)
#         # Process Section A first
#         section_a_data = process_template_file(section_a_file,latest_extracted_file_path) if section_a_file else {}
#         if isinstance(section_a_data, dict):
#             updated_sheets.update(section_a_data)

#         # Process other files concurrently
#         with ThreadPoolExecutor() as executor:
#             futures = [executor.submit(process_template_file, f, latest_extracted_file_path) for f in other_files]
#             for future in futures:
#                 result = future.result()
#                 if isinstance(result, dict):
#                     updated_sheets.update(result)

#     # # Save combined sheets to output
#     output_dir = os.path.join(os.getcwd(), "processed_sheets")
#     os.makedirs(output_dir, exist_ok=True)
#     timestamp = time.strftime("%Y%m%d_%H%M%S")
#     output_filename = f"processed_sheets_{timestamp}.xlsx"
#     output_file_path = os.path.join(output_dir, output_filename)

#     #Optional: Write processed data to file
#     with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
#         for sheet_name, df in updated_sheets.items():
#             df.to_excel(writer, sheet_name=sheet_name, index=False)

#     # logger = setup_logger(output_filename)
#     # success = insert_into_database(updated_sheets, db_connection, logger=None)

    

#     return Response({
#         "message": "Data processed and inserted successfully",
#         "file_path": output_file_path
#     }, status=200)

#     # except Exception as e:
#     #     return Response({
#     #         "error": f"Unexpected error: {str(e)}",
#     #         "file_path": output_file_path if 'output_file_path' in locals() else None
#     #     }, status=500)

 