# fileprocessor/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('process/', views.process_xml_template, name='process-xml-template'),
    path('process-and-insert/', views.data_insert, name='process-and-insert'),
    path('list_files_in_directory/', views.list_files_in_directory, name='list-files-in-directory'),
    path('delete_inserted_files/', views.delete_inserted_file, name='delete-inserted-files'),
]
