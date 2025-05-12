
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
from .processing.db import get_db_connection
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
from django.http import JsonResponse
from .processing.delete_company_data import delete_company_data
from rest_framework.permissions import IsAuthenticated
from rest_framework.decorators import api_view, permission_classes

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

            # for f in os.listdir(insert_data_directory):
            #     file_path = os.path.join(insert_data_directory, f)
            #     if file_path != output_path:
            #         os.remove(file_path)
            os.remove(temp_template.name)
            os.remove(temp_xml.name)

            return response

        return Response({"error": "No output file generated."}, status=500)

    except Exception as e:
        traceback.print_exc()
        return Response({"error": str(e)}, status=500)


@api_view(["GET"])
def list_files_in_directory(request):
    try:
      
        insert_data_directory = os.path.join(os.getcwd(), "insert_data_project")
        
       
        if not os.path.exists(insert_data_directory):
            return Response({"error": "Directory does not exist."}, status=400)

       
        files = [f for f in os.listdir(insert_data_directory) if f.endswith(".xlsx")]

        if not files:
            return Response({"files": []}, status=200)

        return JsonResponse({"files": files}, status=200)

    except Exception as e:
        return Response({"error": str(e)}, status=500)



# API endpoint for processing uploaded XLSX files and inserting data into the database.

# This view accepts two file uploads:
# 1. extracted_data_file: The XLSX file containing extracted data
# 2. template_file: The XLSX template file

# It processes these files using the template processor, saves the processed sheets to a file,
# and then inserts the data into the database.
@api_view(["POST"])
def data_insert(request, **kwargs):
    output_file_path = None
    section_a_found = False

    try:
        # Validate template file input
        if 'section_template_file' not in request.FILES:
            return Response(
                {"error": "Template files are required"},
                status=status.HTTP_400_BAD_REQUEST
            )
            
        industry_category_id = request.POST.get("industry_category_id")
        industry_subcategory_id = request.POST.get("industry_subcategory_id")
        
        # Get extracted filename from POST data
        extracted_filename = request.POST.get("extracted_data_filename")
        if not extracted_filename:
            return Response({"error": "Missing extracted_data_filename in request."}, status=400)

        # Check if extracted file exists in insert_data_project directory
        insert_data_directory = os.path.join(os.getcwd(), "insert_data_project")
        extracted_file_path = os.path.join(insert_data_directory, extracted_filename)

        if not os.path.exists(extracted_file_path):
            return Response({"error": f"File '{extracted_filename}' not found in 'insert_data_project' directory."}, status=404)

        # Load extracted file once and reuse
        extracted_file = pd.ExcelFile(extracted_file_path)

        # Separate Section A and other files
        template_files = request.FILES.getlist('section_template_file')
        section_a_file = None
        other_files = []

        for file in template_files:
            if SECTION_A_FILENAME_IDENTIFIER in file.name:
                section_a_file = file
                section_a_found = True
            else:
                other_files.append(file)

        # Section A validation
        if len(template_files) > 1 and not section_a_found:
            return Response({
                "error": "Section A template file {SECTION_A_FILENAME_IDENTIFIER} is required when uploading multiple files."
            }, status=400)

        if len(template_files) == 1 and not section_a_found:
            other_files.append(template_files[0])  # Treat single non-A file as other

        # Template processing function
        def process_template_file(file):
            temp_template_path = None
            template_data = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_template:
                    for chunk in file.chunks():
                        temp_template.write(chunk)
                    temp_template_path = temp_template.name

                template_data = pd.ExcelFile(temp_template_path)

                updated_sheets = section_bysection_template_to_database_template(template_data, extracted_file)
                updated_sheets = process_sheets(updated_sheets, function_map)
                return updated_sheets

            finally:
                if template_data:
                    template_data.close()
                if temp_template_path and os.path.exists(temp_template_path):
                    os.unlink(temp_template_path)

        updated_sheets = {}

        # If only one file, process directly
        if len(template_files) == 1:
            processed_data = process_template_file(template_files[0])
            if isinstance(processed_data, dict):
                updated_sheets.update(processed_data)

        else:
            # Process Section A first
            if section_a_file:
                section_a_data = process_template_file(section_a_file)
                if isinstance(section_a_data, dict):
                    updated_sheets.update(section_a_data)

            # Process other files concurrently
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(process_template_file, f) for f in other_files]
                for future in futures:
                    result = future.result()
                    if isinstance(result, dict):
                        updated_sheets.update(result)

        # Save combined processed sheets to output
        output_dir = os.path.join(os.getcwd(), "processed_sheets")
        os.makedirs(output_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_filename = f"processed_sheets_{timestamp}.xlsx"
        output_file_path = os.path.join(output_dir, output_filename)

        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            for sheet_name, df in updated_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
                
        # logger = setup_logger(output_filename)
        success = insert_into_database(updated_sheets,
                                       db_connection,
                                       logger=None,
                                       industry_category_id=industry_category_id,
                                       industry_subcategory_id=industry_subcategory_id
                                       )
        
        if extracted_file:
            extracted_file.close()


        return Response({
            "message": "Data processed and inserted successfully",
            "file_path": output_file_path
        }, status=200)

    except Exception as e:
        return Response({
            "error": f"Unexpected error: {str(e)}",
            "file_path": output_file_path if output_file_path else None
        }, status=500)


@api_view(["POST"])
def delete_inserted_file(request):
    try:
        # Correct key to match the payload
        filename = request.data.get("extractedfile")  # change from "filename" to "extractedfile"
        if not filename:
            return Response({"error": "Missing 'extractedfile' in request."}, status=400)

        # Set the folder path where files are stored
        insert_data_directory = os.path.join(os.getcwd(), "insert_data_project")
        file_path = os.path.join(insert_data_directory, filename)

        if not os.path.exists(file_path):
            return Response({"error": f"File '{filename}' not found in 'insert_data_project' folder."}, status=404)

        os.remove(file_path)

        return Response({"message": f"File '{filename}' deleted successfully."}, status=200)

    except Exception as e:
        return Response({"error": f"Error deleting file: {str(e)}"}, status=500)




@api_view(['POST'])
def check_company_name_year(request):
    company_name = request.data.get('company_name')
    year = str(request.data.get('year'))

    if not company_name or not year:
        return Response({'status': 'error', 'message': 'Company name and year are required'}, status=400)

    try:
        with db_connection.cursor() as cursor:
            cursor.execute("SELECT company_code FROM company_master WHERE company_name = %s", (company_name,))
            result = cursor.fetchone()
            if not result:
                return Response({'status': 'error', 'message': 'Company not found in the database. You may proceed to the next step (Step 2: Upload XML).'}, status=404)

            company_code = result['company_code']

         
            cursor.execute("SELECT year FROM company_contacts_details WHERE company_code = %s", (company_code,))
            rows = cursor.fetchall()
          
            unique_years = sorted(set(str(row['year']) for row in rows))

           
            years_string = ', '.join(unique_years)

            if year in unique_years:
                return Response({'status': 'success', 'message': f'Data for {company_name} is already present in the database for {year}, so you cannot proceed to the next step (Step 2: Upload XML).', 'code': 'y'})
            else:
                return Response({
                    'status': 'warning',
                    'message': f'Data for {company_name} is not available in the database for {year}. Available years in the database are: {years_string}. You can proceed to the next step (Step 2: Upload XML).',
                    'code': 'n'
                })

    except Exception as e:
        return Response({'status': 'error', 'message': f'Internal error: {str(e)}'}, status=500)





@csrf_exempt
@permission_classes([IsAuthenticated])
@api_view(["POST"])
def delete_company_by_name(request):
    company_name = request.data.get("company_name")

    # Check if company_name is provided in the request body
    if not company_name:
        return Response({"error": "Company name is required."}, status=status.HTTP_400_BAD_REQUEST)

    try:
        with db_connection.cursor() as cursor:
            # Retrieve company_code using company_name
            cursor.execute("SELECT company_code FROM company_master WHERE company_name = %s", (company_name,))
            result = cursor.fetchone()

            # If no company found with the given name, return an error response
            if not result:
                return Response({"error": "Company not found."}, status=status.HTTP_404_NOT_FOUND)

            company_code = result[0]  # Assuming company_code is the first column

            # Call the delete_company_data function to delete records related to the company
            deletion_result = delete_company_data(company_code)

            # Check if deletion was successful
            if deletion_result.get("success"):
                return Response({
                    "message": f"Company '{company_name}' deleted successfully.",
                    "company_code": company_code,
                    "rows_deleted_brsr": deletion_result.get("deleted_brsr", 0),
                    "rows_deleted_master": deletion_result.get("deleted_master", 0)
                }, status=status.HTTP_200_OK)
            else:
                return Response({
                    "error": "Deletion failed.",
                    "details": deletion_result.get("error")
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    except Exception as e:
        # General exception handling
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)




@api_view(['GET'])
def get_industry_categories(request):
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            query = "SELECT industry_category_id, industry_category FROM industry_category_master"
            cursor.execute(query)
            result = cursor.fetchall()
            
            # Returning the result as JSON
            return JsonResponse(result, safe=False)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)



@api_view(['GET'])
def get_sub_industry_categories(request, category_id):
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            query = """
                SELECT industry_subcategory_id, industry_category_id, industry_subcategory
                FROM industry_subcategory_master
                WHERE industry_category_id = %s
            """
            cursor.execute(query, (category_id,))
            result = cursor.fetchall()
            

            if not result:
                return JsonResponse({"message": "No subcategories found for the given category_id."}, status=404)
            
            return JsonResponse(result, safe=False)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)






### THIS CODE IS FOR DEBUGGING PURPOSES ONLY  FOR DATA INSERT ### 
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
#         # insert_data_directory = os.path.join(os.getcwd(), "insert_data_project")
#         # xlsx_files = [f for f in os.listdir(insert_data_directory) if f.endswith(".xlsx")]
#         # if not xlsx_files:
#         #     return Response({"error": "No extracted data file found"}, status=404)

#         # xlsx_files.sort(key=lambda f: os.path.getmtime(os.path.join(insert_data_directory, f)), reverse=True)
#         # latest_extracted_file_path = os.path.join(insert_data_directory, xlsx_files[0])

#         # Handle template files
#         template_files = request.FILES.getlist('section_template_file')
#         extracted_filename = request.POST.get("extracted_data_filename")
#         if not extracted_filename:
#             return Response({"error": "Missing extracted_data_filename in request."}, status=400)


#         section_a_file = None
#         other_files = []
#         section_a_found = False

#         for file in template_files:
#             if "Data_insert_BRSR_SectionA" in file.name:
#                 section_a_file = file
#                 section_a_found = True
#             else:
#                 other_files.append(file)

#         # Section A validation
#         if len(template_files) > 1 and not section_a_found:
#             return Response({
#                 "error": f"Section A template file (Data_insert_BRSR_SectionA) is required when uploading multiple files."
#             }, status=400)
        
#         if len(template_files) == 1 and not section_a_found:
#             other_files.append(template_files[0])  # Single file, not Section A

#         def process_template_file(file):
#             temp_template_path = None
#             try:
#                 with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as temp_template:
#                     for chunk in file.chunks():
#                         temp_template.write(chunk)
#                     temp_template_path = temp_template.name

#                 extracted_data = pd.ExcelFile(extracted_file)
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

#         updated_sheets = {}

#         # If only one file is uploaded, no need for threading
#         if len(template_files) == 1:
#             file = template_files[0]
#             processed_data = process_template_file(file)
#             if isinstance(processed_data, dict):
#                 updated_sheets.update(processed_data)

#         else:
#             # Process Section A first
#             section_a_data = process_template_file(section_a_file) if section_a_file else {}
#             if isinstance(section_a_data, dict):
#                 updated_sheets.update(section_a_data)

#             # Process other files concurrently
#             with ThreadPoolExecutor() as executor:
#                 futures = [executor.submit(process_template_file, f) for f in other_files]
#                 for future in futures:
#                     result = future.result()
#                     if isinstance(result, dict):
#                         updated_sheets.update(result)

#         # Save combined sheets to output
#         output_dir = os.path.join(os.getcwd(), "processed_sheets")
#         os.makedirs(output_dir, exist_ok=True)
#         timestamp = time.strftime("%Y%m%d_%H%M%S")
#         output_filename = f"processed_sheets_{timestamp}.xlsx"
#         output_file_path = os.path.join(output_dir, output_filename)

#         #Optional: Write processed data to file
#         with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
#             for sheet_name, df in updated_sheets.items():
#                 df.to_excel(writer, sheet_name=sheet_name, index=False)

#         # logger = setup_logger(output_filename)
#         # success = insert_into_database(updated_sheets, db_connection, logger=None)

        

#         return Response({
#             "message": "Data processed and inserted successfully",
#             "file_path": output_file_path
#         }, status=200)

#     except Exception as e:
#         return Response({
#             "error": f"Unexpected error: {str(e)}",
#             "file_path": output_file_path if 'output_file_path' in locals() else None
#         }, status=500)


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

 
 
