-- Snowflake user creation
-- Step 1: Use an admin role
USE ROLE accountadmin;

-- Step 2: Create the `transform` role and assign it to accountadmin
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE transform TO ROLE accountadmin;

-- Step 3: Create a default warehouse
CREATE WAREHOUSE IF NOT EXISTS compute_wh;
GRANT OPERATE ON WAREHOUSE compute_wh TO ROLE transform;

-- Step 4: Create a database and schema for the projectdb project
CREATE DATABASE IF NOT EXISTS projectdb;
CREATE SCHEMA IF NOT EXISTS projectdb.raw;

-- Step 5: Create the `dbt` user and assign to the transform role
CREATE USER IF NOT EXISTS dbt
  PASSWORD='dbtPassword123'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='compute_wh'
  DEFAULT_ROLE=transform
  DEFAULT_NAMESPACE='projectdb.raw'
  COMMENT='DBT user used for data transformation';
ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE transform TO USER dbt;

-- Step 6: Grant permissions to the `transform` role
GRANT ALL ON WAREHOUSE compute_wh TO ROLE transform;
GRANT ALL ON DATABASE projectdb TO ROLE transform;
GRANT ALL ON ALL SCHEMAS IN DATABASE projectdb TO ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE projectdb TO ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA projectdb.raw TO ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA projectdb.raw TO ROLE transform;

CREATE STAGE projectstage
  URL='s3://source'
  CREDENTIALS=(AWS_KEY_ID=' ' AWS_SECRET_KEY=' ');

-- Set defaults
USE WAREHOUSE compute_wh;
USE DATABASE projectdb;
USE SCHEMA raw;

-- Load raw_cust_info
CREATE OR REPLACE TABLE raw_cust_info (
  cst_id              INT,
  cst_key             VARCHAR(50),
  cst_firstname       VARCHAR(50),
  cst_lastname        VARCHAR(50),
  cst_marital_status  VARCHAR(50),
  cst_gndr            VARCHAR(50),
  cst_create_date     DATE
);

COPY INTO raw_cust_info
FROM '@projectstage/crm/raw_cust_info.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load raw_prd_info
CREATE OR REPLACE TABLE raw_prd_info (
  prd_id       INT,
  prd_key      VARCHAR(50),
  prd_nm       VARCHAR(50),
  prd_cost     INT,
  prd_line     VARCHAR(50),
  prd_start_dt DATETIME,
  prd_end_dt   DATETIME
);

COPY INTO raw_prd_info
FROM '@projectstage/crm/raw_prd_info.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load raw_sales_details
CREATE OR REPLACE TABLE raw_sales_details (
  sls_ord_num  VARCHAR(50),
  sls_prd_key  VARCHAR(50),
  sls_cust_id  INT,
  sls_order_dt INT,
  sls_ship_dt  INT,
  sls_due_dt   INT,
  sls_sales    INT,
  sls_quantity INT,
  sls_price    INT
);

COPY INTO raw_sales_details
FROM '@projectstage/crm/raw_sales_details.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- Load raw_CUST_AZ12
CREATE OR REPLACE TABLE raw_CUST_AZ12 (
  CID    VARCHAR(50),
  BDATE  DATE,
  GEN    VARCHAR(50)
);

COPY INTO raw_CUST_AZ12
FROM '@projectstage/erp/raw_CUST_AZ12.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load raw_LOC_A101
CREATE OR REPLACE TABLE raw_LOC_A101 (
  CID    VARCHAR(50),
  CNTRY  VARCHAR(50)
);

COPY INTO raw_LOC_A101
FROM '@projectstage/erp/raw_LOC_A101.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load raw_PX_CAT_G1V2
CREATE OR REPLACE TABLE raw_PX_CAT_G1V2 (
  ID           VARCHAR(50),
  CAT          VARCHAR(50),
  SUBCAT       VARCHAR(50),
  MAINTENANCE  VARCHAR(50)
);

COPY INTO raw_PX_CAT_G1V2
FROM '@projectstage/erp/raw_PX_CAT_G1V2.csv'
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');


/*
-- Windows
py -3.11 -m venv .venv
.venv\Scripts\activate

-- install dbt snowflake
pip install dbt-snowflake==1.9.0

-- create dbt profile
-- windows 
mkdir %userprofile%\.dbt

#initiate dbt project 
dbt init <projectname>

*/



