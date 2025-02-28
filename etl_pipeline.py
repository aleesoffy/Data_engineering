import pandas as pd
import sqlite3

# Step 1: Extract
def extract_data(file_path):
    # Read the CSV file
    data = pd.read_csv(file_path)
    print("Data extracted successfully!")
    return data

# Step 2: Transform
def transform_data(data):
    # Filter rows where quantity > 10
    transformed_data = data[data["quantity"] > 10]
    print("Data transformed successfully!")
    return transformed_data

# Step 3: Load
def load_data(data, db_path, table_name):
    # Connect to SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(db_path)
    # Save the DataFrame to the database
    data.to_sql(table_name, conn, if_exists="replace", index=False)
    print("Data loaded successfully!")
    conn.close()

# Main function
def main():
    # File paths
    input_file = "sales_data.csv"
    db_path = "sales_database.db"
    table_name = "sales"

    # ETL process
    data = extract_data(input_file)
    transformed_data = transform_data(data)
    load_data(transformed_data, db_path, table_name)

if __name__ == "__main__":
    main()