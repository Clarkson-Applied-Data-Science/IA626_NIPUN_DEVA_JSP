import json
import pymysql
import time
from flask import Flask, request, jsonify
from datetime import datetime

app = Flask(__name__)

queries = {
    "getNCustomers" : '''SELECT c.customer_unique_id, l.zip_code, l.city, l.state FROM `Customers` c JOIN `Locations` l
                            ON l.location_key = c.location_key LIMIT %s;''',

    "getNOrders" : '''SELECT o.order_id, c.customer_unique_id, o.order_status, DATE(o.order_purchase_date) AS order_purchase_date, DATE(o.order_delivered_customer_date) AS order_delivered_customer_date
                        FROM `Orders` o JOIN Customers c
                        ON o.customer_key = c.customer_key LIMIT %s;''',

    "getNSellers" : '''SELECT s.seller_id, l.zip_code, l.city, l.state FROM `Sellers` s JOIN `Locations` l
                        ON l.location_key = s.location_key LIMIT %s;''',

    "getNProducts" : '''SELECT * FROM `Products` LIMIT %s;''',

    "getOrders": "SELECT * FROM `Orders` WHERE `order_purchase_date` BETWEEN %s AND %s ORDER BY `order_purchase_date` LIMIT 0,50;",

    "highestAvg_Ordervalue_By_Location" : '''SELECT l.city, l.state, 
                                        AVG(oi.total_price) AS avg_order_value
                                        FROM Locations l
                                        JOIN Customers c ON l.location_key = c.location_key
                                        JOIN Orders o ON c.customer_key = o.customer_key
                                        JOIN OrderItems oi ON o.order_key = oi.order_key
                                        GROUP BY l.city, l.state
                                        ORDER BY avg_order_value DESC
                                        LIMIT %s;
                                        ''',

    "getMostFrequentProductCategories" : '''SELECT p.product_category, COUNT(oi.order_items_key) AS total_purchases
                                        FROM Products p
                                        JOIN OrderItems oi ON p.product_key = oi.product_key
                                        GROUP BY p.product_category
                                        ORDER BY total_purchases DESC
                                        LIMIT %s;  -- Parameterized placeholder for limit
                                        ''',
    "getMostFrequentPurchaseHours" : '''
                                    SELECT HOUR(o.order_purchase_date) AS purchase_hour, COUNT(o.order_key) AS total_orders
                                        FROM Orders o
                                        GROUP BY HOUR(o.order_purchase_date)
                                        ORDER BY total_orders DESC
                                        LIMIT %s;
                                        ''',
    "getMostProfitableLocations" : '''
                                    SELECT l.city, l.state, SUM(oi.qty * oi.unit_price) AS total_revenue
                                    FROM Locations l
                                    JOIN Customers c ON l.location_key = c.location_key
                                    JOIN Orders o ON c.customer_key = o.customer_key
                                    JOIN OrderItems oi ON o.order_key = oi.order_key
                                    GROUP BY l.city, l.state
                                    ORDER BY total_revenue DESC
                                    LIMIT %s;
                                    ''',
    "getTop5CustomersOnSpendings" :    '''
                            SELECT c.customer_unique_id, SUM(oi.qty * oi.unit_price) AS total_spent
                            FROM Customers c
                            JOIN Orders o ON c.customer_key = o.customer_key
                            JOIN OrderItems oi ON o.order_key = oi.order_key
                            GROUP BY c.customer_unique_id
                            ORDER BY total_spent DESC
                            LIMIT 5;
                            '''                                    
}
def dbconn():

    try:
        conn = pymysql.connect(
            host="mysql.clarksonmsda.org",
            user="vempatd",
            password="welcome123",
            db="vempatd_bigdata_final_project",
            port=3306,
        )
        cur = conn.cursor(pymysql.cursors.DictCursor)
        return conn, cur
    except pymysql.MySQLError as e:
        print(f"Error connecting to MySQL: {e}")
        return None, None

def create_response(query, tokens=None):
    response = {"code": 1, "msg": "Request successful", "req": None, "sqltime": None, "result": []}
    try:
        start_time = time.time()
        conn, cur = dbconn()
        cur.execute(query, tokens)
        response["result"] = cur.fetchall()
        response["sqltime"] = time.time() - start_time
        if not response["result"]:
            response["code"] = 1
            response["msg"] = "No data found"
    except Exception as e:
        response["code"] = 0
        response["msg"] = f"Error: {e}"
    finally:
        cur.close()
        conn.close()
    return response

# API Endpoints
@app.route("/", methods=["GET", "POST"])
def root():
    return jsonify({"code": 0, "msg": "No endpoint specified", "req": "/", "sqltime": 0})

@app.route("/getNOrders", methods=["GET", "POST"])
def get_N_orders():
    limit = request.args.get("limit", 10, type=int)
    if not limit:
        return jsonify({"code": 0, "msg": "Missing limit", "req": "getNOrders", "sqltime": 0})
    # print(limit)
    response = create_response(queries["getNOrders"], (limit,))
    # print(response)
    response["req"] = "getNOrders"
    return jsonify(response)

@app.route("/getNCustomers", methods=["GET", "POST"])
def get_N_customers():
    limit = request.args.get("limit", 10, type=int)
    if not limit:
        return jsonify({"code": 0, "msg": "Missing limit", "req": "getNCustomers", "sqltime": 0})
    response = create_response(queries["getNCustomers"], (limit,))
    response["req"] = "getNCustomers"
    return jsonify(response)

@app.route("/getNSellers", methods=["GET", "POST"])
def get_N_sellers():
    limit = request.args.get("limit", 10, type=int)
    if not limit:
        return jsonify({"code": 0, "msg": "Missing limit", "req": "getNSellers", "sqltime": 0})
    response = create_response(queries["getNSellers"], (limit,))
    response["req"] = "getNSellers"
    return jsonify(response)

@app.route("/getNProducts", methods=["GET", "POST"])
def get_N_products():
    limit = request.args.get("limit", 10, type=int)
    if not limit:
        return jsonify({"code": 0, "msg": "Missing limit", "req": "getNProducts", "sqltime": 0})
    response = create_response(queries["getNProducts"], (limit,))
    response["req"] = "getNProducts"
    return jsonify(response)

@app.route("/getOrders", methods=["GET", "POST"])
def getOrders():
    start_date = request.args.get("start")
    end_date = request.args.get("end")
    if not start_date or not end_date:
        return jsonify({"code": 0, "msg": "Missing 'start' or 'end' date parameters", "req": "getOrders", "sqltime": 0})
    def validate_date(date_string, format="%Y-%m-%d"):
        try:
            datetime.strptime(date_string, format)
            return True
        except ValueError:
            return False
    if not validate_date(start_date) or not validate_date(end_date):
        return jsonify({"code": 0, "msg": "Invalid date format. Use 'YYYY-MM-DD'", "req": "getOrders", "sqltime": 0})
    response = create_response(queries["getOrders"], (start_date, end_date))
    response["req"] = "getOrders"
    return jsonify(response)

@app.route("/getLocationsWithHighestAvgOrderValue", methods=["GET", "POST"])
def get_locations_with_highest_avg_order_value():
    limit = request.args.get("limit", 10, type=int)
    if not limit:
        return jsonify({"code": 0, "msg": "Missing limit", "req": "getLocationsWithHighestAvgOrderValue", "sqltime": 0})
    response = create_response(queries["highestAvg_Ordervalue_By_Location"], (limit,))
    response["req"] = "getLocationsWithHighestAvgOrderValue"
    return jsonify(response)

@app.route("/getMostFrequentProductCategories", methods=["GET", "POST"])
def get_most_frequent_product_categories():
    limit = request.args.get("limit", 5, type=int)
    if not limit:
        return jsonify({"code": 0, "msg": "Missing limit", "req": "getMostFrequentProductCategories", "sqltime": 0})

    response = create_response(queries['getMostFrequentProductCategories'], (limit,))
    response["req"] = "getMostFrequentProductCategories"
    return jsonify(response)

@app.route("/getMostFrequentPurchaseHours", methods=["GET"])
def get_most_frequent_purchase_hours():
    limit = request.args.get("limit", 5, type=int)
    if not limit:
        return jsonify({"code": 0, "msg": "Missing limit", "req": "getMostFrequentPurchaseHours", "sqltime": 0})

    response = create_response(queries['getMostFrequentPurchaseHours'], (limit,))
    response["req"] = "getMostFrequentPurchaseHours"
    return jsonify(response)

@app.route("/getMostProfitableLocations", methods=["GET"])
def get_most_profitable_locations():
    limit = request.args.get("limit", 10, type=int)
    if not limit:
        return jsonify({"code": 0, "msg": "Missing limit", "req": "getMostProfitableLocations", "sqltime": 0})
    response = create_response(queries['getMostProfitableLocations'], (limit,))
    response["req"] = "getMostProfitableLocations"
    return jsonify(response)

@app.route("/getTop5CustomersOnSpendings", methods=["GET"])
def get_top_5_customers():
    response = create_response(queries['getTop5CustomersOnSpendings'], ())
    response["req"] = "getTop5CustomersOnSpendings"
    return jsonify(response)

if __name__ == "__main__":
    app.run(debug=True)
