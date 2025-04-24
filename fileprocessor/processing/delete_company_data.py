# processing/delete_company_data.py
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import SQLAlchemyError
import os

def delete_company_data(company_code):
    print("company_code",company_code)
    engine = create_engine(
        f"mysql+pymysql://{os.getenv('DB_USER', 'root')}:{os.getenv('DB_PASSWORD', 'Indranil@001')}@{os.getenv('DB_HOST', 'localhost')}:3306/{os.getenv('DB_NAME', 'brsr_v1')}"
    )
    print("Database connection successful!")


    try:
        metadata = MetaData()
        metadata.reflect(bind=engine)
        print(f"Total number of tables found: {len(metadata.tables)}")

        with engine.begin() as conn:
            total_deleted = 0

            # Step 1: Delete from all tables except 'company_master'
            for table_name in metadata.tables:
                if table_name == 'company_master':
                    continue

                table = metadata.tables[table_name]

                if 'company_code' not in table.columns:
                    print(f"Skipping '{table_name}' (no 'company_code' column)")
                    continue

                try:
                    delete_stmt = table.delete().where(table.c.company_code == company_code)
                    result = conn.execute(delete_stmt)
                    deleted = result.rowcount
                    total_deleted += deleted
                    print(f"Deleted {deleted} row(s) from '{table_name}'")
                except SQLAlchemyError as e:
                    print(f"Error deleting from '{table_name}': {e}")

            # Step 2: Delete from 'company_master'
            if 'company_master' in metadata.tables:
                master_table = metadata.tables['company_master']
                try:
                    delete_master_stmt = master_table.delete().where(master_table.c.company_code == company_code)
                    result = conn.execute(delete_master_stmt)
                    deleted = result.rowcount
                    total_deleted += deleted
                    print(f"Deleted {deleted} row(s) from 'company_master'")
                except SQLAlchemyError as e:
                    print(f"Error deleting from 'company_master': {e}")
            else:
                print("'company_master' table not found in the database.")

            print("\n=== DELETION SUMMARY ===")
            print(f"Total rows deleted: {total_deleted}")

        return {"success": True, "deleted": total_deleted}
    except SQLAlchemyError as conn_err:
        return {"success": False, "error": str(conn_err)}
