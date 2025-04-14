# fileprocessor/urls.py
from django.urls import path
from . import views

urlpatterns = [
    path('process/', views.process_xml_template, name='process-xml-template'),
    path('process-and-insert/', views.data_insert, name='process-and-insert'),
]
