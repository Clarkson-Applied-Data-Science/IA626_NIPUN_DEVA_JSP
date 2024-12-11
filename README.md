# Target E-Commerce Sales Data

## Project Decription

This project leverages the e-commerce dataset from Kaggle to extract valuable insights and identify trends that can inform and optimize business strategies. The dataset includes various attributes such as customer demographics, product details, and transaction data, which are essential for understanding behavior and sales patterns.

The main objectives of this project include:

1. Data Preparation: Cleaning and transforming the data into a structured format.

2. Normalization: Ensuring data consistency and standardization across all the tables.

3. API Development: Creating efficient Flask-based APIs for seamless database querying and JSON output.

Our project aims to drive actionable insights through improvements in marketing strategies, demand forecasting, and customer engagement. This will enhance the decision-making process and overall business performance.

## Relational Schema

![bd](https://github.com/user-attachments/assets/ac1bfda8-4b83-4a2e-a8ad-6d461acf3009)

## Data Set Description

This dataset pertains to Target's operations, encompassing 100,000 orders placed between 2016 and 2018. It includes detailed information about each order. The dataset, which is a subset of Target's operational data, consists of seven CSV files. We sourced it from Kaggle. It can be accessed via this [Kaggle Link](https://www.kaggle.com/datasets/devarajv88/target-dataset/data)

1. Customers file: This dataset contains 99,441 records of customer information. Each record includes Customer_ID along with associated details such as zip code, city, and state.

2. Geolocation file: This dataset consists of 1,000,163 records, providing geographical data, including state, city, zip code. It contains 19,015 distinct zip codes data.

3. Order Items file: This dataset includes 112,650 records, each representing an order item. It links order_id to relevant product_id, seller_id, and order_item_id for each transaction.

4. Orders file: Comprising 99,441 records, this dataset contains order-specific details, capturing the full context of each customer order.

5. Payments file: This dataset includes 103,886 records related to payment transactions, offering details about the payment method and transaction information.

6. Product file: The dataset consists of 32,951 records, each representing individual product details, including product attributes and specifications.

7. Seller file: This dataset contains 3,095 records with detailed information about each seller, including seller location specific details.

## Relational Database

We created a relational database to efficiently store this data using normalization, a common and efficient way for analytical use cases. This structure enables optimized querying and easier reporting. Our python script is idempotent in creating the tables and storing the data into the tables.

Below are some of the normalization steps we took in order to create this relational database.

- Made sure no grouped data is redundant and any such data is stored as an individual table and assigned a key such as zip, city and state information is part of 3 different files.
- All the zip code information from different files is gathered and created a Locations table with location_key as a unique id.
- Made sure no data is redundant.
- For example, the same product in an order is stored in multiple rows, combined these records as one record by introducing a new column as quantity.
- All the infomation is grouped with the respective entities, for example, product price and freight value is assigned to the products table.

## Flask based API

We created a flask based API to provide efficient access to the database and a test client for testing the endpoints.

API Response Schema:

Each response contains:

- code: 1 for success, 0 for failure.
- msg: A message describing the result.
- req: Endpoint method requested (hardcoded).
- sqltime: The time taken by the SQL query to complete.
- result: The query result in JSON format(or empty list on failure).

## API Appendix

### Overview:

The API provides endpoints for retrieving data from multiple tables in a MySQL database, including information on customers, sellers, products, and orders. The API accepts query parameters for specifying limits on the number of records returned, as well as date range filters.

### Authentication

It does not require any API keys at the moment. However, this could be added for security purposes.

### API Endpoints

Base URL: http://127.0.0.1:5000

Some of the Endpoints are:

- getNOrders : Retrieves order details based on the specified limit.

  - Endpoint: /getNOrders/{limit}
  - Description: Fetches details for up to {limit} orders.
  - limit (integer): Maximum number of orders to retrieve. Must be a positive integer.
  - Example : http://127.0.0.1:5000/getNOrders/50.

- getNCustomers : Retrieves customer details based on the specified limit.

  - Endpoint: /getNCustomers/{limit}
  - limit (integer): Maximum number of customers to retrieve. Must be a positive integer.
  - Example : http://127.0.0.1:5000/getNCustomers/100

- getNSellers : Retrieves seller details based on the specified limit.

  - Endpoint: /getNSellers/{limit}
  - Description: Fetches details for up to {limit} sellers.
  - limit (integer): Maximum number of sellers to retrieve. Must be a positive integer.
  - Example: http://127.0.0.1:5000/getNSellers/20.

- getOrders : Retrieves order details based on the specified date range.

  - Endpoint: /getOrders
  - Query Parameters:
    - start_date (string): Start of the date range in YYYY-MM-DD format.
    - end_date (string): End of the date range in YYYY-MM-DD format.
    - Year Ranges from year 2016 to 2017
  - Example: http://127.0.0.1:5000/getOrders?start_date=2024-01-01&end_date=2024-01-31

- getNProducts : Retrieves product details based on the specified limit.

  - Endpoint: /getNProducts/{limit}
  - limit (integer): Maximum number of products to retrieve. Must be a positive integer.
  - Example: http://127.0.0.1:5000/getNProducts/30

- getLocationsWithHighestAvgOrderValue : Retrieves cities and states with the highest average order value.

  - Endpoint: /getLocationsWithHighestAvgOrderValue
  - limit (positive integer, optional): The number of locations to retrieve.
  - Example : http://127.0.0.1:5000/getLocationsWithHighestAvgOrderValue?limit=5

- getMostFrequentProductCategories : Fetches product categories with the highest number of purchases.

  - Endpoint: /getMostFrequentProductCategories
  - limit (positive integer, optional): The number of categories to retrieve (default: 5).
  - Example: http://127.0.0.1:5000/getMostFrequentProductCategories?limit=3

- getMostFrequentPurchaseHours : Retrieves the hours with the highest number of purchases.

  - Endpoint: /getMostFrequentPurchaseHours
  - limit (positive integer, optional): The number of hours to retrieve (default: 5).
  - Example : http://127.0.0.1:5000/getMostFrequentPurchaseHours?limit=3

- getMostProfitableLocations : Fetches cities and states generating the highest total revenue.

  - Endpoint: /getMostProfitableLocations
  - limit (positive integer, optional): The number of locations to retrieve (default: 10).
  - Example : http://127.0.0.1:5000/getMostProfitableLocations?limit=5

- getTop5CustomersOnSpendings : Retrieves the top 5 customers with the highest total spending.

  - Endpoint: /getTop5CustomersOnSpendings
  - Example : http://127.0.0.1:5000/getTop5CustomersOnSpendings

### Error Codes and Messages

Code 0: Indicates an error or missing required parameters.

Example error messages:

- "Missing limit" (for missing limit parameter).
- "Missing 'start' or 'end' date parameters" (for missing date range).
- "Invalid date format. Use 'YYYY-MM-DD'" (for incorrect date format).
