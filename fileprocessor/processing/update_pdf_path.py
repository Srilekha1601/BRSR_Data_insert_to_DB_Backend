
import os

def pdf_path_update(db_connection):

    print("****")
    cursor = db_connection.cursor()

    # Get all companies
    cursor.execute("SELECT company_code, company_name FROM company_master")
    company_list = cursor.fetchall()

    # print("company_list", company_list)

    for company in company_list:
        company_name_clean = company['company_name'].strip()

        # print("company_name_clean", company_name_clean)

        company_code = company['company_code']

        # Get pdf_paths rows that are not yet linked to any company
        cursor.execute("SELECT relative_path FROM pdf_paths WHERE company_code IS NULL")
        pdf_entries = cursor.fetchall()

        # print("pdf_entries", pdf_entries)

        # print("pdf_entries", pdf_entries)

        for pdf_entry in pdf_entries:
            relative_path = pdf_entry['relative_path']

            # Step 1: Get only the filename (ignoring the directory and year like 2023\)
            filename = os.path.basename(relative_path)  # This gives something like 'Automotive_Stampings_and_Assemblies_Limited.pdf'

            # Step 2: Remove the file extension (.pdf)
            name_without_ext = os.path.splitext(filename)[0]  # Now we have 'Automotive_Stampings_and_Assemblies_Limited'

            # Step 3: Replace underscores with spaces
            processed_path = name_without_ext.replace('_', ' ').strip()

            # print("processed_path", processed_path)

            # Match with company_name
            if processed_path == company_name_clean:
                print("matched!!")
                # Set the company_code in pdf_paths
                cursor.execute(f"""
                    UPDATE pdf_paths
                    SET company_code = '{company_code}'
                    WHERE relative_path = %s
                """, (relative_path,))

                # Now find the RO location
                cursor.execute("""
                    SELECT city_code, state_or_union_territory_code
                    FROM company_location_details
                    WHERE company_code = %s AND office_type_code = 'RO'
                """, (company_code,))
                location = cursor.fetchone()

                print("location", location)

                if location:
                    city_code = location['city_code'] 
                    state_code = location['state_or_union_territory_code']
                    cursor.execute("""
                        UPDATE pdf_paths
                        SET city_code = %s, state_or_union_territory_code = %s
                        WHERE company_code = %s
                    """, (city_code, state_code, company_code))

    
