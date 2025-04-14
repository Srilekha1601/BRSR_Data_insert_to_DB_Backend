#!/usr/bin/env python
"""
Script to process a template with extracted data.
"""

import os
import sys
from fileprocessor.processing.template_processor import process_template_and_save

def main():
    """
    Main function to process a template with extracted data.
    """
    if len(sys.argv) < 3:
        print("Usage: python process_template.py <extracted_data_file> <template_file> [output_dir]")
        sys.exit(1)
    
    extracted_data_file = sys.argv[1]
    template_file = sys.argv[2]
    output_dir = sys.argv[3] if len(sys.argv) > 3 else None
    
    if not os.path.exists(extracted_data_file):
        print(f"Error: Extracted data file '{extracted_data_file}' does not exist.")
        sys.exit(1)
    
    if not os.path.exists(template_file):
        print(f"Error: Template file '{template_file}' does not exist.")
        sys.exit(1)
    
    try:
        output_file = process_template_and_save(
            extracted_data_file=extracted_data_file,
            template_file=template_file,
            output_dir=output_dir
        )
        print(f"Successfully processed template. Output file: {output_file}")
    except Exception as e:
        print(f"Error processing template: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 