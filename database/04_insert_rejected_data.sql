-- Connect to store_db database
\c store_db;



-- Customer with NULL company_name (required field)
INSERT INTO customers VALUES ('REJ01', NULL, 'John Doe', 'Manager', '123 Main St', 'New York', 'NY', '10001', 'USA', '555-0123', '555-0124');

-- Customer with duplicated customer_id
INSERT INTO customers VALUES ('ALFKI', 'Duplicate Company', 'Jane Smith', 'CEO', '456 Oak Ave', 'Los Angeles', 'CA', '90210', 'USA', '555-0456', '555-0457');

-- Customer with invalid phone format
INSERT INTO customers VALUES ('REJ02', 'Invalid Phone Corp', 'Bob Johnson', 'Director', '789 Pine St', 'Chicago', 'IL', '60601', 'USA', 'INVALID_PHONE', '555-0789');

-- Customer with NULL required fields
INSERT INTO customers VALUES (NULL, 'Null ID Company', 'Alice Brown', 'Manager', '321 Elm St', 'Houston', 'TX', '77001', 'USA', '555-0321', '555-0322');

-- =============================================
-- REJECTED PRODUCTS DATA
-- =============================================

-- Product with negative unit price
INSERT INTO products VALUES (9991, 'Negative Price Product', 1, 1, '10 boxes', -15.50, 100, 0, 10, 0);

-- Product with zero quantity in stock (business rule violation)
INSERT INTO products VALUES (9992, 'Zero Stock Product', 1, 1, '12 bottles', 25.00, 0, 0, 5, 0);

-- Product with negative units on order
INSERT INTO products VALUES (9993, 'Negative Order Product', 1, 1, '24 cans', 18.75, 50, -10, 15, 0);

-- Product with NULL product_name (required field)
INSERT INTO products VALUES (9994, NULL, 1, 1, '6 packs', 12.00, 30, 0, 5, 0);

-- Product with invalid discontinued value (should be 0 or 1)
INSERT INTO products VALUES (9995, 'Invalid Discontinued Product', 1, 1, '8 units', 35.00, 20, 0, 8, 2);

-- Product with duplicated product_id
INSERT INTO products VALUES (1, 'Duplicate ID Product', 1, 1, '10 boxes', 20.00, 25, 0, 5, 0);

-- =============================================
-- REJECTED ORDERS DATA
-- =============================================

-- Order with NULL order_date (required field)
INSERT INTO orders VALUES (99901, 'ALFKI', 1, NULL, '2024-01-15', '2024-01-10', 1, 25.50, 'Test Customer', '123 Test St', 'Test City', 'TS', '12345', 'Test Country');

-- Order with invalid date (shipped before order date)
INSERT INTO orders VALUES (99902, 'ANATR', 2, '2024-01-15', '2024-01-20', '2024-01-10', 2, 45.75, 'Invalid Date Customer', '456 Test Ave', 'Test City', 'TS', '12345', 'Test Country');

-- Order with negative freight
INSERT INTO orders VALUES (99903, 'ANTON', 3, '2024-01-10', '2024-01-17', '2024-01-12', 3, -10.25, 'Negative Freight Customer', '789 Test Blvd', 'Test City', 'TS', '12345', 'Test Country');

-- Order with NULL customer_id (foreign key violation)
INSERT INTO orders VALUES (99904, NULL, 4, '2024-01-12', '2024-01-19', '2024-01-14', 1, 30.00, 'No Customer Order', '321 Test Rd', 'Test City', 'TS', '12345', 'Test Country');

-- Order with invalid employee_id (non-existent foreign key)
INSERT INTO orders VALUES (99905, 'AROUT', 999, '2024-01-14', '2024-01-21', '2024-01-16', 2, 55.00, 'Invalid Employee Order', '654 Test Ln', 'Test City', 'TS', '12345', 'Test Country');

-- Order with duplicated order_id
INSERT INTO orders VALUES (10248, 'BERGS', 5, '2024-01-16', '2024-01-23', '2024-01-18', 3, 40.25, 'Duplicate Order ID', '987 Test Way', 'Test City', 'TS', '12345', 'Test Country');

-- =============================================
-- REJECTED ORDER_DETAILS DATA
-- =============================================

-- Order detail with zero quantity (business rule violation)
INSERT INTO order_details VALUES (99901, 1, 15.50, 0, 0.10);

-- Order detail with negative quantity
INSERT INTO order_details VALUES (99902, 2, 22.75, -5, 0.05);

-- Order detail with negative unit price
INSERT INTO order_details VALUES (99903, 3, -18.25, 10, 0.00);

-- Order detail with invalid discount (> 1.0)
INSERT INTO order_details VALUES (99904, 4, 35.00, 8, 1.50);

-- Order detail with NULL order_id (required field)
INSERT INTO order_details VALUES (NULL, 5, 28.50, 12, 0.15);

-- Order detail with NULL product_id (required field)
INSERT INTO order_details VALUES (99905, NULL, 42.00, 6, 0.20);

-- Order detail with duplicated primary key (order_id, product_id)
INSERT INTO order_details VALUES (10248, 11, 25.00, 15, 0.25);

-- Order detail with invalid product_id (non-existent foreign key)
INSERT INTO order_details VALUES (99906, 9999, 50.00, 3, 0.00);

-- =============================================
-- REJECTED SUPPLIERS DATA
-- =============================================

-- Supplier with NULL company_name (required field)
INSERT INTO suppliers VALUES (9991, NULL, 'John Supplier', 'Manager', '123 Supply St', 'Supply City', 'SC', '12345', 'Supply Country', '555-0001', '555-0002', 'http://example.com');

-- Supplier with duplicated supplier_id
INSERT INTO suppliers VALUES (1, 'Duplicate Supplier', 'Jane Supplier', 'CEO', '456 Supply Ave', 'Supply City', 'SC', '12345', 'Supply Country', '555-0003', '555-0004', 'http://duplicate.com');

-- Supplier with invalid phone format
INSERT INTO suppliers VALUES (9992, 'Invalid Phone Supplier', 'Bob Supplier', 'Director', '789 Supply Blvd', 'Supply City', 'SC', '12345', 'Supply Country', 'INVALID_PHONE', '555-0005', 'http://invalid.com');

-- =============================================
-- REJECTED CATEGORIES DATA
-- =============================================

-- Category with NULL category_name (required field)
INSERT INTO categories VALUES (9991, NULL, 'Invalid category description', '\x');

-- Category with duplicated category_id
INSERT INTO categories VALUES (1, 'Duplicate Category', 'This is a duplicate category', '\x');

-- =============================================
-- REJECTED EMPLOYEES DATA
-- =============================================

-- Employee with NULL last_name (required field)
INSERT INTO employees VALUES (9991, NULL, 'John', 'Manager', 'Mr.', '1990-01-01', '2020-01-01', '123 Employee St', 'Employee City', 'EC', '12345', 'Employee Country', '555-0100', '1234', '\x', 'Test notes', NULL, '/path/to/photo.jpg');

-- Employee with NULL first_name (required field)
INSERT INTO employees VALUES (9992, 'Smith', NULL, 'Developer', 'Ms.', '1992-05-15', '2021-03-01', '456 Employee Ave', 'Employee City', 'EC', '12345', 'Employee Country', '555-0101', '1235', '\x', 'Test notes', 1, '/path/to/photo2.jpg');

-- Employee with invalid birth_date (future date)
INSERT INTO employees VALUES (9993, 'Johnson', 'Jane', 'Analyst', 'Dr.', '2030-12-31', '2022-06-01', '789 Employee Blvd', 'Employee City', 'EC', '12345', 'Employee Country', '555-0102', '1236', '\x', 'Test notes', 1, '/path/to/photo3.jpg');

-- Employee with hire_date before birth_date
INSERT INTO employees VALUES (9994, 'Brown', 'Bob', 'Consultant', 'Mr.', '1995-08-20', '1990-01-01', '321 Employee Rd', 'Employee City', 'EC', '12345', 'Employee Country', '555-0103', '1237', '\x', 'Test notes', 1, '/path/to/photo4.jpg');

-- Employee with duplicated employee_id
INSERT INTO employees VALUES (1, 'Duplicate', 'Employee', 'Manager', 'Mr.', '1988-03-10', '2019-01-01', '654 Employee Ln', 'Employee City', 'EC', '12345', 'Employee Country', '555-0104', '1238', '\x', 'Test notes', NULL, '/path/to/photo5.jpg');

-- =============================================
-- REJECTED SHIPPERS DATA
-- =============================================

-- Shipper with NULL company_name (required field)
INSERT INTO shippers VALUES (9991, NULL, '555-0200');

-- Shipper with duplicated shipper_id
INSERT INTO shippers VALUES (1, 'Duplicate Shipper', '555-0201');

-- =============================================
-- REJECTED REGION DATA
-- =============================================

-- Region with NULL region_description (required field)
INSERT INTO region VALUES (9991, NULL);

-- Region with duplicated region_id
INSERT INTO region VALUES (1, 'Duplicate Region');

-- =============================================
-- REJECTED TERRITORIES DATA
-- =============================================

-- Territory with NULL territory_description (required field)
INSERT INTO territories VALUES ('99999', NULL, 1);

-- Territory with invalid region_id (non-existent foreign key)
INSERT INTO territories VALUES ('99998', 'Invalid Region Territory', 999);

-- =============================================
-- REJECTED US_STATES DATA
-- =============================================

-- State with NULL state_name
INSERT INTO us_states VALUES (9991, NULL, 'XX', 'invalid');

-- State with duplicated state_id
INSERT INTO us_states VALUES (1, 'Duplicate State', 'DS', 'duplicate');

-- State with invalid state_abbr (too long)
INSERT INTO us_states VALUES (9992, 'Invalid Abbr State', 'INVALID', 'invalid');

-- =============================================
-- REJECTED CUSTOMER_DEMOGRAPHICS DATA
-- =============================================

-- Customer demographic with NULL customer_type_id (required field)
INSERT INTO customer_demographics VALUES (NULL, 'Invalid customer type description');

-- =============================================
-- REJECTED CUSTOMER_CUSTOMER_DEMO DATA
-- =============================================

-- Customer demo with NULL customer_id (required field)
INSERT INTO customer_customer_demo VALUES (NULL, 'DEMO1');

-- Customer demo with NULL customer_type_id (required field)
INSERT INTO customer_customer_demo VALUES ('CUST1', NULL);

-- =============================================
-- REJECTED EMPLOYEE_TERRITORIES DATA
-- =============================================

-- Employee territory with NULL employee_id (required field)
INSERT INTO employee_territories VALUES (NULL, '99999');

-- Employee territory with NULL territory_id (required field)
INSERT INTO employee_territories VALUES (9991, NULL);


