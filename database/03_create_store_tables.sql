-- Connect to store_db and create tables
\c store_db;

-- Drop existing tables if they exist
DROP TABLE IF EXISTS "Brand";
DROP TABLE IF EXISTS "Categorys";
DROP TABLE IF EXISTS "Customers_phone";
DROP TABLE IF EXISTS "Customers";
DROP TABLE IF EXISTS "order_details";
DROP TABLE IF EXISTS "orders";
DROP TABLE IF EXISTS "products";
DROP TABLE IF EXISTS "product_brand";
DROP TABLE IF EXISTS "ratings";
DROP TABLE IF EXISTS "Store";
DROP TABLE IF EXISTS "Subcategorys";
DROP TABLE IF EXISTS "Suppliers";
DROP TABLE IF EXISTS "Supplier_product";
DROP TABLE IF EXISTS "product_store";

-- Create tables
CREATE TABLE categories (
    category_id smallint NOT NULL,
    category_name character varying(15) NOT NULL,
    description text,
    picture bytea
);

CREATE TABLE customer_customer_demo (
    customer_id character varying(5) NOT NULL,
    customer_type_id character varying(5) NOT NULL
);

CREATE TABLE customer_demographics (
    customer_type_id character varying(5) NOT NULL,
    customer_desc text
);

CREATE TABLE customers (
    customer_id character varying(5) NOT NULL,
    company_name character varying(40) NOT NULL,
    contact_name character varying(30),
    contact_title character varying(30),
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postal_code character varying(10),
    country character varying(15),
    phone character varying(24),
    fax character varying(24)
);

CREATE TABLE employees (
    employee_id smallint NOT NULL,
    last_name character varying(20) NOT NULL,
    first_name character varying(10) NOT NULL,
    title character varying(30),
    title_of_courtesy character varying(25),
    birth_date date,
    hire_date date,
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postal_code character varying(10),
    country character varying(15),
    home_phone character varying(24),
    extension character varying(4),
    photo bytea,
    notes text,
    reports_to smallint,
    photo_path character varying(255)
);

CREATE TABLE employee_territories (
    employee_id smallint NOT NULL,
    territory_id character varying(20) NOT NULL
);

CREATE TABLE order_details (
    order_id smallint NOT NULL,
    product_id smallint NOT NULL,
    unit_price real NOT NULL,
    quantity smallint NOT NULL,
    discount real NOT NULL
);

CREATE TABLE orders (
    order_id smallint NOT NULL,
    customer_id character varying(5),
    employee_id smallint,
    order_date date,
    required_date date,
    shipped_date date,
    ship_via smallint,
    freight real,
    ship_name character varying(40),
    ship_address character varying(60),
    ship_city character varying(15),
    ship_region character varying(15),
    ship_postal_code character varying(10),
    ship_country character varying(15)
);

CREATE TABLE products (
    product_id smallint NOT NULL,
    product_name character varying(40) NOT NULL,
    supplier_id smallint,
    category_id smallint,
    quantity_per_unit character varying(20),
    unit_price real,
    units_in_stock smallint,
    units_on_order smallint,
    reorder_level smallint,
    discontinued integer NOT NULL
);

CREATE TABLE region (
    region_id smallint NOT NULL,
    region_description character varying(50) NOT NULL
);

CREATE TABLE shippers (
    shipper_id smallint NOT NULL,
    company_name character varying(40) NOT NULL,
    phone character varying(24)
);

CREATE TABLE suppliers (
    supplier_id smallint NOT NULL,
    company_name character varying(40) NOT NULL,
    contact_name character varying(30),
    contact_title character varying(30),
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postal_code character varying(10),
    country character varying(15),
    phone character varying(24),
    fax character varying(24),
    homepage text
);

CREATE TABLE territories (
    territory_id character varying(20) NOT NULL,
    territory_description character varying(50) NOT NULL,
    region_id smallint NOT NULL
);

CREATE TABLE us_states (
    state_id smallint NOT NULL,
    state_name character varying(100),
    state_abbr character varying(2),
    state_region character varying(50)
);


CREATE TABLE processed_files (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add primary keys and constraints
ALTER TABLE ONLY categories ADD CONSTRAINT pk_categories PRIMARY KEY (category_id);
ALTER TABLE ONLY customer_customer_demo ADD CONSTRAINT pk_customer_customer_demo PRIMARY KEY (customer_id, customer_type_id);
ALTER TABLE ONLY customer_demographics ADD CONSTRAINT pk_customer_demographics PRIMARY KEY (customer_type_id);
ALTER TABLE ONLY customers ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);
ALTER TABLE ONLY employees ADD CONSTRAINT pk_employees PRIMARY KEY (employee_id);
ALTER TABLE ONLY employee_territories ADD CONSTRAINT pk_employee_territories PRIMARY KEY (employee_id, territory_id);
ALTER TABLE ONLY order_details ADD CONSTRAINT pk_order_details PRIMARY KEY (order_id, product_id);
ALTER TABLE ONLY orders ADD CONSTRAINT pk_orders PRIMARY KEY (order_id);
ALTER TABLE ONLY products ADD CONSTRAINT pk_products PRIMARY KEY (product_id);
ALTER TABLE ONLY region ADD CONSTRAINT pk_region PRIMARY KEY (region_id);
ALTER TABLE ONLY shippers ADD CONSTRAINT pk_shippers PRIMARY KEY (shipper_id);
ALTER TABLE ONLY suppliers ADD CONSTRAINT pk_suppliers PRIMARY KEY (supplier_id);
ALTER TABLE ONLY territories ADD CONSTRAINT pk_territories PRIMARY KEY (territory_id);
ALTER TABLE ONLY us_states ADD CONSTRAINT pk_us_states PRIMARY KEY (state_id);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
