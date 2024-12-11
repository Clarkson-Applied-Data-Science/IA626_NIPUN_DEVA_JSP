# %%
import csv
import pymysql
from datetime import datetime
import yaml
from pathlib import Path


create_table_queries = [
    '''
    CREATE TABLE Products (
        product_key INT AUTO_INCREMENT PRIMARY KEY,
        product_id VARCHAR(100) NOT NULL UNIQUE,
        product_category VARCHAR(100),
        product_name_length INT,
        product_description_length INT,
        product_photos_qty INT,
        product_weight_g FLOAT,
        product_length_cm FLOAT,
        product_height_cm FLOAT,
        product_width_cm FLOAT,
        price DECIMAL(10, 2),
        freight_value DECIMAL(10, 2)
    );
    ''',

    '''
    CREATE TABLE Locations (
        location_key INT AUTO_INCREMENT PRIMARY KEY,
        zip_code CHAR(5),
        city VARCHAR(50),
        state CHAR(2)
    );
    ''',
    '''
    CREATE TABLE GeoLocations (
        geolocation_key INT AUTO_INCREMENT PRIMARY KEY,
        latitude DECIMAL(10, 6),
        longitude DECIMAL(10, 6),
        location_key INT,
        FOREIGN KEY (location_key) REFERENCES Locations(location_key)
    );
    ''',
    '''
    CREATE TABLE Customers (
        customer_key INT AUTO_INCREMENT PRIMARY KEY,
        customer_unique_id VARCHAR(100) NOT NULL UNIQUE,
        location_key INT,
        FOREIGN KEY (location_key) REFERENCES Locations(location_key)
    );
    ''',
    '''
    CREATE TABLE Sellers (
        seller_key INT AUTO_INCREMENT PRIMARY KEY,
        seller_id VARCHAR(100) NOT NULL UNIQUE,
        location_key INT,
        FOREIGN KEY (location_key) REFERENCES Locations(location_key)
    );
    ''',
    '''
    CREATE TABLE Orders (
        order_key INT AUTO_INCREMENT PRIMARY KEY,
        order_id VARCHAR(100) NOT NULL UNIQUE,
        customer_key INT,
        order_status VARCHAR(25),
        order_purchase_date DATETIME,
        order_approved_date DATETIME,
        order_delivered_carrier_date DATETIME,
        order_delivered_customer_date DATETIME,
        order_estimated_delivery_date DATETIME,
        FOREIGN KEY (customer_key) REFERENCES Customers(customer_key)
    );
    ''',
    '''
    CREATE TABLE Payments (
        payments_key INT AUTO_INCREMENT PRIMARY KEY,
        order_key INT,
        payment_type VARCHAR(25),
        payment_installments INT,
        payment_value DECIMAL(10, 2),
        FOREIGN KEY (order_key) REFERENCES Orders(order_key)
    );
    ''',
    '''
    CREATE TABLE OrderItems (
        order_items_key INT AUTO_INCREMENT PRIMARY KEY,
        order_key INT,
        product_key INT,
        seller_key INT,
        qty INT,
        unit_price DECIMAL(10, 2),
        total_price DECIMAL(10, 2),
        FOREIGN KEY (order_key) REFERENCES Orders(order_key),
        FOREIGN KEY (product_key) REFERENCES Products(product_key),
        FOREIGN KEY (seller_key) REFERENCES Sellers(seller_key)
    );
    '''
]

# For Database connection
def dbconn():
    # conn = pymysql.connect(
    #     host='mysql.clarksonmsda.org', 
    #     user='vempatd', 
    #     password='welcome123', 
    #     db='vempatd_bigdata_final_project', 
    #     port=3306
    # )
    config = yaml.safe_load(Path("config.yml").read_text())

    conn = pymysql.connect(host=config['db']['host'], port=config['db']['port'], user=config['db']['user'],
                       passwd=config['db']['passwd'], db=config['db']['db'], autocommit=True)
    
    cur = conn.cursor()
    
    return conn, cur

# For dropping and creating tables
def create_tables():
    conn, cur = dbconn()
    try:
        # Dropping tables if exist
        tables = [
            'OrderItems', 'Payments', 'Orders', 'Sellers', 
            'Customers', 'GeoLocations', 'Locations', 
            'Products'
        ]
        for table in tables:
            cur.execute(f'DROP TABLE IF EXISTS {table};')
        
        # Creating tables
        for query in create_table_queries:
            cur.execute(query)
        conn.commit()
        print("Tables created successfully.")
    except pymysql.Error as e:
        print(f"Error creating tables: {e}")
    finally:
        cur.close()
        conn.close()

# %%
def insert_locations_and_geolocation(geo_file):
    conn, cur = dbconn()
    try:
        with open(geo_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # To track unique zip codes for Locations
            unique_zip = {}
            geolocation_data = []

            for row in reader:
                zip_code = row['geolocation_zip_code_prefix']
                city = row['geolocation_city']
                state = row['geolocation_state']
                latitude = float(row['geolocation_lat'])
                longitude = float(row['geolocation_lng'])

                if zip_code not in unique_zip:
                    unique_zip[zip_code] = (city, state)

                geolocation_data.append((zip_code, latitude, longitude))

            # Inserting unique locations into Locations
            insert_location_data_sql = '''
                    INSERT INTO Locations (zip_code, city, state)
                    VALUES (%s, %s, %s)
                '''
            location_batch = []
            for zip_code, (city, state) in unique_zip.items():
                location_batch.append((zip_code, city, state))
                if len(location_batch) == 5000:
                    cur.executemany(insert_location_data_sql, location_batch)
                    conn.commit()
                    location_batch = []
            
            if location_batch:
                cur.executemany(insert_location_data_sql, location_batch)
                conn.commit()

            print(f"Inserted data into Locations.")

            # Retrieving location keys for GeoLocations
            location_map = {}
            cur.execute('SELECT zip_code, location_key FROM Locations')
            for zip_code, location_key in cur.fetchall():
                location_map[zip_code] = location_key

            # Inserting data into GeoLocations using location_key
            insert_geo_loc_data_sql = '''
                    INSERT INTO GeoLocations (latitude, longitude, location_key)
                    VALUES (%s, %s, %s)
                '''
            geolocation_batch = []
            for zip_code, latitude, longitude in geolocation_data:
                location_key = location_map.get(zip_code)
                geolocation_batch.append((latitude, longitude, location_key))
                if len(geolocation_batch) == 25000:
                    cur.executemany(insert_geo_loc_data_sql, geolocation_batch)
                    conn.commit()
                    geolocation_batch = []

            if geolocation_batch:
                cur.executemany(insert_geo_loc_data_sql, geolocation_batch)
                conn.commit()
            print(f"Inserted data into GeoLocations.")

    except pymysql.Error as e:
        print(f"Error processing geolocation data: {e}")
    finally:
        cur.close()
        conn.close()

# %%
from collections import defaultdict
def insert_products(product_file, order_items_file):
    conn, cur = dbconn()
    try:
        product_price_mapping = defaultdict(lambda: {'price': 0.0, 'freight_value': 0.0})
        
        with open(order_items_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                product_id = row['product_id']
                price = float(row['price']) if row['price'] else 0.0
                freight_value = float(row['freight_value']) if row['freight_value'] else 0.0

                product_price_mapping[product_id]['price'] += price
                product_price_mapping[product_id]['freight_value'] += freight_value
        with open(product_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            product_data = []
            for row in reader:
                product_id = row['product_id']
                product_category = row['product category'] if row['product category'] else None
                product_name_length = row['product_name_length'] if row['product_name_length'] else None
                product_description_length = row['product_description_length'] if row['product_description_length'] else None
                product_photos_qty = row['product_photos_qty'] if row['product_photos_qty'] else None
                product_weight_g = row['product_weight_g'] if row['product_weight_g'] else None
                product_length_cm = row['product_length_cm'] if row['product_length_cm'] else None
                product_height_cm = row['product_height_cm'] if row['product_height_cm'] else None
                product_width_cm = row['product_width_cm'] if row['product_width_cm'] else None

                price = product_price_mapping[product_id]['price']
                freight_value = product_price_mapping[product_id]['freight_value']
                

                product_data.append((product_id, product_category, product_name_length, product_description_length, product_photos_qty, product_weight_g,
                                     product_length_cm, product_height_cm, product_width_cm, price, freight_value))

                if len(product_data) == 5000:
                    insert_product_sql = '''
                        INSERT INTO Products (product_id, product_category, product_name_length, product_description_length, product_photos_qty, product_weight_g,
                                     product_length_cm, product_height_cm, product_width_cm, price, freight_value)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    cur.executemany(insert_product_sql, product_data)
                    conn.commit()
                    product_data = []

            if product_data:
                cur.executemany(insert_product_sql, product_data)
                conn.commit()

        print("Inserted Products data into Products table.")

    except pymysql.Error as e:
        print(f"Error inserting product data: {e}")


# %%
def extract_mapping(table_name, id, key):
    conn, cur = dbconn()
    try:
        query = f"SELECT {id}, {key} FROM {table_name}"
        cur.execute(query)
        column_names = [desc[0] for desc in cur.description]
        rows = [dict(zip(column_names, row)) for row in cur.fetchall()]
        return {row[id]: row[key] for row in rows}
    finally:
        cur.close()
        conn.close()


# %%
def insert_and_map_customers(customer_file, zip_and_locationKey_mapping):
    conn, cur = dbconn()
    # zip_and_locationKey_mapping = zip_locationKey_mapping()
    customer_id_to_customer_key = {}

    try:
        with open(customer_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            customer_data = []
            unique_customers = {}
            customer_id_and_unique_customer_id_mapping = {}

            for row in reader:
                customer_id = row['customer_id']
                customer_unique_id = row['customer_unique_id']
                customer_zip_code_prefix = row['customer_zip_code_prefix']
                location_key = zip_and_locationKey_mapping.get(customer_zip_code_prefix)

                customer_id_and_unique_customer_id_mapping[customer_id] = customer_unique_id

                if customer_unique_id not in unique_customers:
                    unique_customers[customer_unique_id] = location_key
                    customer_data.append((customer_unique_id, location_key))

                if len(customer_data) == 25000:
                    insert_customer_sql = '''
                        INSERT INTO Customers (customer_unique_id, location_key)
                        VALUES (%s, %s)
                    '''
                    cur.executemany(insert_customer_sql, customer_data)
                    conn.commit()
                    customer_data = []

            if customer_data:
                insert_customer_sql = '''
                    INSERT INTO Customers (customer_unique_id, location_key)
                    VALUES (%s, %s)
                '''
                cur.executemany(insert_customer_sql, customer_data)
                conn.commit()

        print("Inserted customer data into Customers table.")

        cur.execute('SELECT customer_unique_id, customer_key FROM Customers')
        unique_customer_key_mapping = dict(cur.fetchall())

        for customer_id, customer_unique_id in customer_id_and_unique_customer_id_mapping.items():
            customer_key = unique_customer_key_mapping.get(customer_unique_id)
            if customer_key:
                customer_id_to_customer_key[customer_id] = customer_key
            else:
                print(f"No key found for customer_unique_id: {customer_unique_id}")

        print("customer_id to customer_key mapping successful.")
        return customer_id_to_customer_key

    except pymysql.Error as e:
        print(f"Error processing customer data: {e}")
        return {}  # Return empty mapping on error

    finally:
        cur.close()
        conn.close()

def insert_orders(orders_file, customer_id_to_customer_key):
    conn, cur = dbconn()
    try:
        with open(orders_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            order_data = []
            for row in reader:
                order_id = row['order_id']
                customer_id = row['customer_id']
                order_status = row['order_status']
                def validate_date(date_str):
                    if not date_str or date_str.strip() == '':
                        return None
                    try:
                        parsed_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                        return parsed_date
                    except ValueError:
                        return None

                order_purchase_date = validate_date(row['order_purchase_timestamp'])
                order_approved_date = validate_date(row['order_approved_at'])
                order_delivered_carrier_date = validate_date(row['order_delivered_carrier_date'])
                order_delivered_customer_date = validate_date(row['order_delivered_customer_date'])
                order_estimated_delivery_date = validate_date(row['order_estimated_delivery_date'])
                customer_key = customer_id_to_customer_key.get(customer_id)

                if customer_key:
                    order_data.append((customer_key, order_id, order_status, order_purchase_date, order_approved_date, order_delivered_carrier_date,
                                       order_delivered_customer_date, order_estimated_delivery_date))

                if len(order_data) == 25000:
                    insert_order_sql = '''
                        INSERT INTO Orders (customer_key, order_id, order_status, order_purchase_date, order_approved_date, order_delivered_carrier_date,
                                       order_delivered_customer_date, order_estimated_delivery_date)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    '''
                    cur.executemany(insert_order_sql, order_data)
                    conn.commit()
                    order_data = []

            if order_data:
                cur.executemany(insert_order_sql, order_data)
                conn.commit()

        print("Inserted orders into Orders table.")

    except pymysql.Error as e:
        print(f"Error inserting orders: {e}")
    finally:
        cur.close()
        conn.close()



# %%
def insert_sellers(seller_file, zip_and_locationKey_mapping):
    conn, cur = dbconn()
    # zip_and_locationKey_mapping = zip_locationKey_mapping()
    try:
        with open(seller_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            seller_data = []
            missing_location_records = []
            unique_missing_zip = {}
            pending_seller_records = []
            for row in reader:
                seller_id = row['seller_id']
                seller_zip_code_prefix = row['seller_zip_code_prefix']
                seller_city = row['seller_city']
                seller_state = row['seller_state']
                location_key = zip_and_locationKey_mapping.get(seller_zip_code_prefix, None)

                if location_key:
                    seller_data.append((seller_id, location_key))

                else:
                    if seller_zip_code_prefix not in unique_missing_zip:
                        unique_missing_zip[seller_zip_code_prefix] = (seller_city, seller_state)
                        for seller_zip_code_prefix, (seller_city, seller_state) in unique_missing_zip.items():
                            missing_location_records.append((seller_zip_code_prefix, seller_state, seller_state))
                    pending_seller_records.append((seller_id, seller_zip_code_prefix))

                if len(seller_data) == 1000:
                    insert_seller_sql = '''
                        INSERT INTO Sellers (seller_id, location_key)
                        VALUES (%s, %s)
                    '''
                    cur.executemany(insert_seller_sql, seller_data)
                    conn.commit()
                    seller_data = []

            if seller_data:
                cur.executemany(insert_seller_sql, seller_data)
                conn.commit()

        # Handling missing locations
        if missing_location_records:

            insert_missing_location_sql = '''
                INSERT INTO Locations (zip_code, city, state)
                VALUES (%s, %s, %s)
            '''
            cur.executemany(insert_missing_location_sql, missing_location_records)
            conn.commit()

            # Fetching new location keys for the inserted zip codes
            cur.execute('SELECT zip_code, location_key FROM Locations WHERE zip_code IN %s', (tuple(unique_missing_zip),))
            new_zip_locationKey_mapping = dict(cur.fetchall())

            # Updating the original mapping with the new entries
            zip_and_locationKey_mapping.update(new_zip_locationKey_mapping)

            # Inserting sellers with newly fetched location keys
            new_seller_data = [
                (seller_id, zip_and_locationKey_mapping[seller_zip_code_prefix])
                for seller_id, seller_zip_code_prefix in pending_seller_records
            ]
            cur.executemany(insert_seller_sql, new_seller_data)
            conn.commit()

        print("Inserted Seller data into Sellers table.")

    except pymysql.Error as e:
        print(f"Error inserting seller data: {e}")


# %%
def insert_payments(payments_file, order_id_to_order_key):
    conn, cur = dbconn()
    try:
        with open(payments_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            payments_data = []
            processed_data = {}

            for row in reader:
                order_id = row['order_id']
                payment_type = row['payment_type']
                payment_sequential = row['payment_sequential']
                installments = int(row['payment_installments']) if row['payment_installments'] else 0
                payment_value = float(row['payment_value']) if row['payment_value'] else 0.0

                # Using a tuple, combination of (order_id, payment_sequential) as the key
                key = (order_id, payment_sequential)

                if key not in processed_data:
                    processed_data[key] = {
                        'payment_type': payment_type,
                        'installments': installments,
                        'payment_value': payment_value,
                    }
                else:
                    processed_data[key]['installments'] += installments
                    processed_data[key]['payment_value'] += payment_value

            for (order_id, payment_sequential), values in processed_data.items():
                order_key = order_id_to_order_key.get(order_id)
                if order_key:
                    payments_data.append((
                        order_key,
                        values['payment_type'],
                        values['installments'],
                        values['payment_value']
                    ))

            insert_payments_query = """
                INSERT INTO Payments (order_key, payment_type, payment_installments, payment_value)
                VALUES (%s, %s, %s, %s)
            """
            cur.executemany(insert_payments_query, payments_data)
            conn.commit()
            print(f"Successfully inserted payments data into Payments.")

    except Exception as e:
        conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        cur.close()
        conn.close()


# %%
from collections import defaultdict
def insert_order_items(order_items_file, order_id_to_order_key, product_id_to_product_key, seller_id_to_seller_key):
    conn, cur = dbconn()
    try:
        with open(order_items_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            order_items_data = []
            processed_data = defaultdict(lambda: {'qty': 0, 'unit_price': 0, 'total_price': 0})
            
            for row in reader:               
                order_id = row['order_id']
                product_id = row['product_id']
                seller_id = row['seller_id']
                price = float(row['price']) if row['price'] else 0.0
                freight_value = float(row['freight_value']) if row['freight_value'] else 0.0
                # Generating a unique key with combination of (order_id, product_id)
                key = (order_id, product_id)
                
                processed_data[key]['qty'] += 1
                unit_price = price + freight_value
                processed_data[key]['price'] = price
                processed_data[key]['freight_value'] = freight_value
                processed_data[key]['unit_price'] = unit_price
                processed_data[key]['total_price'] = processed_data[key]['qty'] * unit_price
                processed_data[key]['seller_id'] = seller_id

            order_items_data = []
            # product_info = {}
            for (order_id, product_id), values in processed_data.items():
                seller_key = seller_id_to_seller_key.get(values['seller_id'], None)
                product_key = product_id_to_product_key.get(product_id, None)
                order_key = order_id_to_order_key.get(order_id, None)
                if order_key and product_key and seller_key:
                    order_items_data.append((
                        order_key, product_key, seller_key,
                        values['unit_price'], values['qty'], values['total_price']
                    ))
                # if product_key:
                #     product_info.append((values['price'], values['freight_value'], product_id))

            insert_order_items_query = """
                INSERT INTO OrderItems (order_key, product_key, seller_key, unit_price, qty, total_price)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            cur.executemany(insert_order_items_query, order_items_data)
            conn.commit()

            # update_product_details_in_Products_query = """
            #     UPDATE Products
            #     SET
            #     price = %s,
            #     freight_value = %s
            #     WHERE product_key = %s
            # """
            # cur.executemany(update_product_details_in_Products_query, product_info)
            # conn.commit()

    except Exception as e:
        conn.rollback()
        print(f"Error: {e}")
    finally:
        conn.close()



# %%
# Main function
def main():
    geo_file = 'geolocation.csv'
    products_file = 'products.csv'
    customers_file = 'customers.csv'
    sellers_file = 'sellers.csv'
    orders_file = 'orders.csv'
    order_items_file = 'order_items.csv'
    payments_file = 'payments.csv'

    create_tables()
    insert_locations_and_geolocation(geo_file)
    insert_products(products_file, order_items_file)
    zipcode_to_location_key = extract_mapping("Locations", "zip_code", "location_key")
    customer_id_to_customer_key = insert_and_map_customers(customers_file, zipcode_to_location_key)
    insert_orders(orders_file, customer_id_to_customer_key)
    insert_sellers(sellers_file, zipcode_to_location_key)
    order_id_to_order_key = extract_mapping("Orders", "order_id", "order_key")
    insert_payments(payments_file, order_id_to_order_key)
    product_id_to_product_key = extract_mapping("Products", "product_id", "product_key")
    seller_id_to_seller_key = extract_mapping("Sellers", "seller_id", "seller_key")
    insert_order_items(order_items_file, order_id_to_order_key, product_id_to_product_key, seller_id_to_seller_key)
    

# %%
if __name__ == "__main__":
    main()


