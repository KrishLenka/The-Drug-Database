"""
Data Loader (for MySQL)
Config:
update DB_CONFIG with MySQL details

Run: 
python3 load_data.py
"""

import mysql.connector
import pandas as pd
import os
from datetime import datetime

# Database config
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'password',
    'database': 'drug_database'
}

# CSV file paths
PRODUCTS_CSV = 'data/products.csv'
EXCLUSIVITY_CSV = 'data/exclusivity.csv'
PATENT_CSV = 'data/patent.csv'
SALES_CSV = 'data/sales.csv'
NDC_CSV = 'data/ndc.csv'

# Convert dates into MySQL format
def parse_date(date_str):
    if pd.isna(date_str) or str(date_str).strip() == '':
        return None
    try:
        return datetime.strptime(str(date_str).strip(), '%Y-%m-%d').date()
    except:
        return None

# Loads products.csv into MySQL
def load_products(cursor, conn):
    print("Loading products...")
    df = pd.read_csv(PRODUCTS_CSV, low_memory=False)
    
    insert_sql = """
        INSERT INTO products (appl_no, appl_type, ingredient, dosage, form, route, 
            trade_name, applicant, strength, te_code, approval_date, 
            rld, rs, type, applicant_full_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    batch = []
    batch_size = 1000
    count = 0
    
    for _, row in df.iterrows():
        batch.append((
            str(row.get('Appl_No', '')) or None,
            str(row.get('Appl_Type', '')) or None,
            str(row.get('Ingredient', '')) or None,
            str(row.get('Dosage', '')) or None,
            str(row.get('Form', '')) or None,
            str(row.get('Route', '')) or None,
            str(row.get('Trade_Name', '')) or None,
            str(row.get('Applicant', '')) or None,
            str(row.get('Strength', '')) or None,
            str(row.get('TE_Code', '')) or None,
            parse_date(row.get('Approval_Date')),
            str(row.get('RLD', '')) or None,
            str(row.get('RS', '')) or None,
            str(row.get('Type', '')) or None,
            str(row.get('Applicant_Full_Name', '')) or None
        ))
        
        if len(batch) >= batch_size:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            count += len(batch)
            print(f"  Loaded {count} products...")
            batch = []
    
    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        count += len(batch)
    
    print(f"Loaded {count} products total")

# Loads exclusivity.csv into MySQL
def load_exclusivity(cursor, conn):
    print("Loading exclusivity...")
    df = pd.read_csv(EXCLUSIVITY_CSV, low_memory=False)
    
    insert_sql = """
        INSERT INTO exclusivity (appl_no, appl_type, ingredient, dosage, form, route,
            trade_name, strength, exclusivity_code, exclusivity_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    batch = []
    count = 0
    
    for _, row in df.iterrows():
        batch.append((
            str(row.get('Appl_No', '')) or None,
            str(row.get('Appl_Type', '')) or None,
            str(row.get('Ingredient', '')) or None,
            str(row.get('Dosage', '')) or None,
            str(row.get('Form', '')) or None,
            str(row.get('Route', '')) or None,
            str(row.get('Trade_Name', '')) or None,
            str(row.get('Strength', '')) or None,
            str(row.get('Exclusivity_Code', '')) or None,
            parse_date(row.get('Exclusivity_Date'))
        ))
    
    cursor.executemany(insert_sql, batch)
    conn.commit()
    print(f"Loaded {len(batch)} exclusivity records")

# Loads patents.csv into MySQL
def load_patents(cursor, conn):
    print("Loading patents...")
    df = pd.read_csv(PATENT_CSV, low_memory=False)
    
    insert_sql = """
        INSERT INTO patent (appl_no, appl_type, ingredient, dosage, form, route,
            trade_name, applicant, strength, patent_no, 
            patent_expire_date_text, drug_substance_flag, drug_product_flag,
            patent_use_code, submission_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    batch = []
    batch_size = 1000
    count = 0
    
    for _, row in df.iterrows():
        batch.append((
            str(row.get('Appl_No', '')) or None,
            str(row.get('Appl_Type', '')) or None,
            str(row.get('Ingredient', '')) or None,
            str(row.get('Dosage', '')) or None,
            str(row.get('Form', '')) or None,
            str(row.get('Route', '')) or None,
            str(row.get('Trade_Name', '')) or None,
            str(row.get('Applicant', '')) or None,
            str(row.get('Strength', '')) or None,
            str(row.get('Patent_No', '')) or None,
            str(row.get('Patent_Expire_Date_Text', '')) or None,
            str(row.get('Drug_Substance_Flag', '')) or None,
            str(row.get('Drug_Product_Flag', '')) or None,
            str(row.get('Patent_Use_Code', '')) or None,
            parse_date(row.get('Submission_Date'))
        ))
        
        if len(batch) >= batch_size:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            count += len(batch)
            print(f"  Loaded {count} patents...")
            batch = []
    
    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        count += len(batch)
    
    print(f"Loaded {count} patents total")

# Build a dictionary from ndc.csv mapping (labeler_code, product_code) -> appl_no
# Will be used to add appl_no to sales data
def build_ndc_lookup():
    """Build a lookup dictionary from ndc.csv mapping (labeler_code, product_code) -> appl_no"""
    print("Building NDC lookup dictionary...")
    # different because of a unicode decode error
    df = pd.read_csv(NDC_CSV, encoding="latin1", low_memory=False)
    lookup = {}
    for _, row in df.iterrows():
        labeler_code = zero_pad(row.get('Labeler Code', ''), 5)
        product_code = zero_pad(row.get('Product Code', ''), 4)
        appl_no = str(row.get('Application Number', '')).strip()
        if labeler_code and product_code and appl_no and appl_no != '':
            key = (labeler_code, product_code)
            # Special case if multiple appl_nos exist for same key [TO BE LOOKED AT LATER]
            if key not in lookup or not lookup[key]:
                lookup[key] = appl_no
    print(f"  Built lookup dictionary with {len(lookup)} entries")
    return lookup

# data cleaning for decimal/monetary and int fields
def parse_decimal(value):
    if pd.isna(value) or str(value).strip() == '':
        return None
    try:
        cleaned = str(value).replace('$', '').replace(',', '').replace(' ', '').strip()
        return float(cleaned) if cleaned else None
    except:
        return None

def parse_int(value):
    """Parse integer value, removing commas and whitespace"""
    if pd.isna(value) or str(value).strip() == '':
        return None
    try:
        cleaned = str(value).replace(',', '').strip()
        return int(cleaned) if cleaned else None
    except:
        return None

# normalize_code and zero_pad were used to fix NDC code matching issues
def normalize_code(code, length):
    code_str = str(code).strip()
    return code_str.zfill(length) if code_str else None

def zero_pad(code, length):
    if code is None:
        return None
    code_str = str(code).strip()
    if not code_str:
        return None
    return code_str.zfill(length)

# Loads sales.csv into MySQL (use ndc_lookup to get appl_no)
def load_sales(cursor, conn, ndc_lookup):
    """Load sales.csv into MySQL, using ndc_lookup to populate appl_no"""
    print("Loading sales...")
    df = pd.read_csv(SALES_CSV, encoding="latin1", low_memory=False)
    
    insert_sql = """
        INSERT INTO sales (appl_no, ingredient, route, route_extended, dosage,
            manufacturer, strength, pack_quantity, ndc_number, labeler_code, product_code, sales, packs, quantity, wac, price, number_of_sellers)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    batch = []
    batch_size = 1000
    count = 0
    matched_count = 0
    
    for _, row in df.iterrows():
        # Get labeler_code and product_code from sales.csv
        labeler_code = zero_pad(row.get('Labeler Code', ''), 5)
        product_code = zero_pad(row.get('Product Code', ''), 4)
        
        # Look up appl_no using the NDC lookup dictionary
        appl_no = None
        if labeler_code and product_code:
            appl_no = ndc_lookup.get((labeler_code, product_code))
            if appl_no:
                matched_count += 1
        
        batch.append((
            appl_no,  # Will be None if not found in lookup
            str(row.get('Ingredient', '')) or None,
            str(row.get('Route', '')) or None,
            str(row.get('Route Ext', '')) or None,
            str(row.get('Dosage', '')) or None,
            str(row.get('Manufacturer', '')) or None,
            str(row.get('Strength', '')) or None,
            parse_int(row.get('Pack_Quantity')),
            str(row.get('NDC Number', '')).strip() or None,
            labeler_code,
            product_code,
            parse_decimal(row.get('Sales')),
            parse_int(row.get('Packs')),
            parse_int(row.get('Quantity')),
            parse_decimal(row.get('WAC')),
            parse_decimal(row.get('Price')),
            None  # number_of_sellers - will be populated later
        ))
        
        if len(batch) >= batch_size:
            cursor.executemany(insert_sql, batch)
            conn.commit()
            count += len(batch)
            print(f"  Loaded {count} sales records... ({matched_count} with appl_no matched)")
            batch = []
    
    if batch:
        cursor.executemany(insert_sql, batch)
        conn.commit()
        count += len(batch)
    # Note: matched_count will be less than total count and that's okay
    print(f"Loaded {count} sales records total ({matched_count} with appl_no matched)")

# counts appl_no in product table based on ingredient and dosage
def populate_number_of_product_approvals(cursor, conn):
    """Populate number_of_approvals column based on ingredient and dosage combinations"""
    print("\nPopulating number_of_product_approvals...")
    
    update_sql = """
        UPDATE products p
        JOIN (
            SELECT
                ingredient,
                dosage,
                COUNT(DISTINCT appl_no) AS number_of_approvals
            FROM products
            GROUP BY ingredient, dosage
        ) s
        ON p.ingredient = s.ingredient
        AND p.dosage = s.dosage
        SET p.number_of_approvals = s.number_of_approvals
    """
    
    cursor.execute(update_sql)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM products WHERE number_of_approvals IS NOT NULL")
    updated_count = cursor.fetchone()[0]
    print(f"Updated {updated_count} products with number_of_approvals")

# counts distinct appl_no in sales table based on ingredient and dosage
def populate_number_of_sellers(cursor, conn):
    """Populate number_of_sellers column based on ingredient and dosage combinations"""
    print("\nPopulating number_of_sellers...")
    
    update_sql = """
        UPDATE sales s
        JOIN (
            SELECT
                ingredient,
                dosage,
                COUNT(DISTINCT appl_no) AS number_of_sellers
            FROM sales
            WHERE ingredient IS NOT NULL AND dosage IS NOT NULL
            GROUP BY ingredient, dosage
        ) x
        ON s.ingredient = x.ingredient
        AND s.dosage = x.dosage
        SET s.number_of_sellers = x.number_of_sellers
    """
    
    cursor.execute(update_sql)
    conn.commit()
    
    cursor.execute("SELECT COUNT(*) FROM sales WHERE number_of_sellers IS NOT NULL")
    updated_count = cursor.fetchone()[0]
    print(f"Updated {updated_count} sales with number_of_sellers")

# duplicates caused by product code field in products, exclusivity, and patent tables
# schema does not include the field, thus there may be some duplicates
def remove_duplicates(cursor, conn):
    """Remove duplicate rows from products, exclusivity, and patent tables"""
    print("\nRemoving duplicate rows...")
    
    # Remove duplicates from products table
    print("  Removing duplicates from products...")
    delete_products = """
        DELETE p1 FROM products p1
        INNER JOIN products p2 
        WHERE p1.id > p2.id
        AND p1.appl_no = p2.appl_no
        AND (p1.appl_type = p2.appl_type OR (p1.appl_type IS NULL AND p2.appl_type IS NULL))
        AND p1.ingredient = p2.ingredient
        AND (p1.dosage = p2.dosage OR (p1.dosage IS NULL AND p2.dosage IS NULL))
        AND (p1.form = p2.form OR (p1.form IS NULL AND p2.form IS NULL))
        AND (p1.route = p2.route OR (p1.route IS NULL AND p2.route IS NULL))
        AND (p1.trade_name = p2.trade_name OR (p1.trade_name IS NULL AND p2.trade_name IS NULL))
        AND (p1.applicant = p2.applicant OR (p1.applicant IS NULL AND p2.applicant IS NULL))
        AND (p1.strength = p2.strength OR (p1.strength IS NULL AND p2.strength IS NULL))
        AND (p1.te_code = p2.te_code OR (p1.te_code IS NULL AND p2.te_code IS NULL))
        AND (p1.approval_date = p2.approval_date OR (p1.approval_date IS NULL AND p2.approval_date IS NULL))
        AND (p1.rld = p2.rld OR (p1.rld IS NULL AND p2.rld IS NULL))
        AND (p1.rs = p2.rs OR (p1.rs IS NULL AND p2.rs IS NULL))
        AND (p1.type = p2.type OR (p1.type IS NULL AND p2.type IS NULL))
        AND (p1.applicant_full_name = p2.applicant_full_name OR (p1.applicant_full_name IS NULL AND p2.applicant_full_name IS NULL))
        AND (p1.number_of_approvals = p2.number_of_approvals OR (p1.number_of_approvals IS NULL AND p2.number_of_approvals IS NULL))
    """
    cursor.execute(delete_products)
    products_deleted = cursor.rowcount
    conn.commit()
    print(f"    Deleted {products_deleted} duplicate product rows")
    
    # Remove duplicates from exclusivity table
    print("  Removing duplicates from exclusivity...")
    delete_exclusivity = """
        DELETE e1 FROM exclusivity e1
        INNER JOIN exclusivity e2 
        WHERE e1.id > e2.id
        AND e1.appl_no = e2.appl_no
        AND (e1.appl_type = e2.appl_type OR (e1.appl_type IS NULL AND e2.appl_type IS NULL))
        AND e1.ingredient = e2.ingredient
        AND (e1.dosage = e2.dosage OR (e1.dosage IS NULL AND e2.dosage IS NULL))
        AND (e1.form = e2.form OR (e1.form IS NULL AND e2.form IS NULL))
        AND (e1.route = e2.route OR (e1.route IS NULL AND e2.route IS NULL))
        AND (e1.trade_name = e2.trade_name OR (e1.trade_name IS NULL AND e2.trade_name IS NULL))
        AND (e1.strength = e2.strength OR (e1.strength IS NULL AND e2.strength IS NULL))
        AND (e1.exclusivity_code = e2.exclusivity_code OR (e1.exclusivity_code IS NULL AND e2.exclusivity_code IS NULL))
        AND (e1.exclusivity_date = e2.exclusivity_date OR (e1.exclusivity_date IS NULL AND e2.exclusivity_date IS NULL))
    """
    cursor.execute(delete_exclusivity)
    exclusivity_deleted = cursor.rowcount
    conn.commit()
    print(f"    Deleted {exclusivity_deleted} duplicate exclusivity rows")
    
    # Remove duplicates from patent table
    print("  Removing duplicates from patent...")
    delete_patent = """
        DELETE pt1 FROM patent pt1
        INNER JOIN patent pt2 
        WHERE pt1.id > pt2.id
        AND pt1.appl_no = pt2.appl_no
        AND (pt1.appl_type = pt2.appl_type OR (pt1.appl_type IS NULL AND pt2.appl_type IS NULL))
        AND pt1.ingredient = pt2.ingredient
        AND (pt1.dosage = pt2.dosage OR (pt1.dosage IS NULL AND pt2.dosage IS NULL))
        AND (pt1.form = pt2.form OR (pt1.form IS NULL AND pt2.form IS NULL))
        AND (pt1.route = pt2.route OR (pt1.route IS NULL AND pt2.route IS NULL))
        AND (pt1.trade_name = pt2.trade_name OR (pt1.trade_name IS NULL AND pt2.trade_name IS NULL))
        AND (pt1.applicant = pt2.applicant OR (pt1.applicant IS NULL AND pt2.applicant IS NULL))
        AND (pt1.strength = pt2.strength OR (pt1.strength IS NULL AND pt2.strength IS NULL))
        AND (pt1.patent_no = pt2.patent_no OR (pt1.patent_no IS NULL AND pt2.patent_no IS NULL))
        AND (pt1.patent_expire_date_text = pt2.patent_expire_date_text OR (pt1.patent_expire_date_text IS NULL AND pt2.patent_expire_date_text IS NULL))
        AND (pt1.drug_substance_flag = pt2.drug_substance_flag OR (pt1.drug_substance_flag IS NULL AND pt2.drug_substance_flag IS NULL))
        AND (pt1.drug_product_flag = pt2.drug_product_flag OR (pt1.drug_product_flag IS NULL AND pt2.drug_product_flag IS NULL))
        AND (pt1.patent_use_code = pt2.patent_use_code OR (pt1.patent_use_code IS NULL AND pt2.patent_use_code IS NULL))
        AND (pt1.submission_date = pt2.submission_date OR (pt1.submission_date IS NULL AND pt2.submission_date IS NULL))
    """
    cursor.execute(delete_patent)
    patent_deleted = cursor.rowcount
    conn.commit()
    print(f"    Deleted {patent_deleted} duplicate patent rows")
    
    print(f"Duplicate removal complete. Total deleted: {products_deleted + exclusivity_deleted + patent_deleted} rows")

def main():
    print("="*50)
    print("MySQL Data Loader for Drug Discovery Database")
    print("="*50)
    
    # Connect to MySQL
    print(f"\nConnecting to MySQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}...")
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    try:
        # Build NDC lookup dictionary (needed for sales data)
        ndc_lookup = build_ndc_lookup()
        
        # Load data
        load_products(cursor, conn)
        load_exclusivity(cursor, conn)
        load_patents(cursor, conn)
        load_sales(cursor, conn, ndc_lookup)
        
        # Populate calculated fields
        populate_number_of_product_approvals(cursor, conn)
        populate_number_of_sellers(cursor, conn)
        
        # Remove duplicate rows
        remove_duplicates(cursor, conn)
        
        # Verify counts
        print("\n" + "="*50)
        print("Final record counts:")
        cursor.execute("SELECT COUNT(*) FROM products")
        print(f"  Products: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM exclusivity")
        print(f"  Exclusivity: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM patent")
        print(f"  Patents: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM sales")
        print(f"  Sales: {cursor.fetchone()[0]}")
        cursor.execute("SELECT COUNT(*) FROM sales WHERE appl_no IS NOT NULL")
        print(f"  Sales with appl_no: {cursor.fetchone()[0]}")
        print("="*50)
        print("Data loading complete!")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
