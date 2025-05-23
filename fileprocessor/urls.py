# fileprocessor/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('process/', views.process_xml_template, name='process-xml-template'),
    path('process-and-insert/', views.data_insert, name='process-and-insert'),
    path('list_files_in_directory/', views.list_files_in_directory, name='list-files-in-directory'),
    path('delete_inserted_files/', views.delete_inserted_file, name='delete-inserted-files'),
    path('check_company_name_year/', views.check_company_name_year, name='check-company-name-year'),
    path('delete_company_data/', views.delete_company_by_name, name='delete-company-by-name'),
    path('industry-categories/', views.get_industry_categories, name='get-industry-categories'),
    path('industry-subcategories/<int:category_id>/', views.get_sub_industry_categories, name='get-sub-industry-categories'),
]
