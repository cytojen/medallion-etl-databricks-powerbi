import dlt
from pyspark.sql.functions import (
    col, to_date, concat_ws, round,
    sum, countDistinct, avg,
    year, month, date_format
)

# -----------------------------------------------------------------------------
# Config
# S3 landing zone - each table has its own subfolder so Auto Loader
# can watch per table and detect new files independently
# -----------------------------------------------------------------------------
S3_LANDING = dbutils.secrets.get("scope", "s3-landing-path")

# BRONZE LAYER - Auto Loader (cloudFiles)
# Auto Loader watches each S3 subfolder for new CSV files.
# It keeps a checkpoint of processed files - only new files are ingested.
# New rows in new files are inserted, existing files are never reprocessed.

@dlt.table(
    name    = "bronze_categories",
    comment = "Incremental ingestion of categories CSV files from S3 via Auto Loader"
)
def bronze_categories():
    return (
        spark.readStream
        .format("cloudFiles") # Auto Loader format
        .option("cloudFiles.format", "csv") # source file format
        .option("cloudFiles.schemaLocation", # schema checkpoint — Auto Loader
                f"{S3_LANDING}/categories/_schema") # infers and stores schema here
        .option("header", True)
        .load(f"{S3_LANDING}/categories/") # watches this S3 folder for new files
    )

@dlt.table(
    name    = "bronze_customers",
    comment = "Incremental ingestion of customers CSV files from S3 via Auto Loader"
)
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",
                f"{S3_LANDING}/customers/_schema")
        .option("header", True)
        .load(f"{S3_LANDING}/customers/")
    )

@dlt.table(
    name    = "bronze_employees",
    comment = "Incremental ingestion of employees CSV files from S3 via Auto Loader"
)
def bronze_employees():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",
                f"{S3_LANDING}/employees/_schema")
        .option("header", True)
        .load(f"{S3_LANDING}/employees/")
    )

@dlt.table(
    name    = "bronze_orders",
    comment = "Incremental ingestion of orders CSV files from S3 via Auto Loader"
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",
                f"{S3_LANDING}/orders/_schema")
        .option("header", True)
        .load(f"{S3_LANDING}/orders/")
    )

@dlt.table(
    name    = "bronze_ordersdetails",
    comment = "Incremental ingestion of order details CSV files from S3 via Auto Loader"
)
def bronze_ordersdetails():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",
                f"{S3_LANDING}/ordersdetails/_schema")
        .option("header", True)
        .load(f"{S3_LANDING}/ordersdetails/")
    )

@dlt.table(
    name    = "bronze_products",
    comment = "Incremental ingestion of products CSV files from S3 via Auto Loader"
)
def bronze_products():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",
                f"{S3_LANDING}/products/_schema")
        .option("header", True)
        .load(f"{S3_LANDING}/products/")
    )

@dlt.table(
    name    = "bronze_shippers",
    comment = "Incremental ingestion of shippers CSV files from S3 via Auto Loader"
)
def bronze_shippers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",
                f"{S3_LANDING}/shippers/_schema")
        .option("header", True)
        .load(f"{S3_LANDING}/shippers/")
    )

@dlt.table(
    name    = "bronze_suppliers",
    comment = "Incremental ingestion of suppliers CSV files from S3 via Auto Loader"
)
def bronze_suppliers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation",
                f"{S3_LANDING}/suppliers/_schema")
        .option("header", True)
        .load(f"{S3_LANDING}/suppliers/")
    )


# SILVER LAYER - Cleaned, joined, and enriched tables
# Fact tables use dlt.read_stream() — processes only new Bronze rows each run
# Dimension tables use dlt.read() — fully joined each run for enrichment

@dlt.table(
    name    = "silver_orders_enriched",
    comment = "Orders joined with customers, employees, and shippers. One row per order."
)
@dlt.expect("valid_order_id",    "OrderID IS NOT NULL") # reject rows with no order ID
@dlt.expect("valid_order_date",  "OrderDate IS NOT NULL") # reject rows with no order date
@dlt.expect("valid_customer",    "CustomerName IS NOT NULL") # reject rows with no customer
def silver_orders_enriched():
    # Fact table - read_stream processes only new Bronze rows since last run
    orders = (
        dlt.read_stream("bronze_orders")
           .withColumn("OrderDate", to_date(col("OrderDate")))
    )

    # Dimension tables - read() fully joined each run for enrichment
    customers = (
        dlt.read("bronze_customers")
           .withColumnRenamed("ContractName", "ContactName")
    )

    employees = (
        dlt.read("bronze_employees")
           .drop("Photo", "Notes")
           .withColumn("BirthDate", to_date(col("BirthDate")))
           .withColumn("FullName", concat_ws(" ", col("FirstName"), col("LastName")))
    )

    shippers = dlt.read("bronze_shippers")

    return (
        orders
        .join(
            customers.select("CustomerID", "CustomerName", "ContactName", "City", "Country"),
            on="CustomerID", how="left"
        )
        .withColumnRenamed("City",    "CustomerCity")
        .withColumnRenamed("Country", "CustomerCountry")
        .join(
            employees.select("EmployeeID", "FullName"),
            on="EmployeeID", how="left"
        )
        .withColumnRenamed("FullName", "EmployeeName")
        .join(
            shippers.select("ShipperID", "ShipperName"),
            on="ShipperID", how="left"
        )
        .drop("CustomerID", "EmployeeID", "ShipperID")
    )


@dlt.table(
    name    = "silver_order_items_enriched",
    comment = "Order details joined with products, categories, and suppliers. One row per line item."
)
@dlt.expect("valid_order_id",    "OrderID IS NOT NULL") # reject orphaned line items
@dlt.expect("valid_product",     "ProductName IS NOT NULL") # reject items with no product
@dlt.expect("positive_price",    "Price > 0") # reject items with invalid price
@dlt.expect("positive_quantity", "Quantity > 0") # reject items with zero quantity
def silver_order_items_enriched():
    # Fact table - read_stream processes only new Bronze rows since last run
    ordersdetails = dlt.read_stream("bronze_ordersdetails")

    # Dimension tables - read() fully joined each run for enrichment
    products = (
        dlt.read("bronze_products")
           .withColumnRenamed("SuppliersID", "SupplierID")
    )

    categories = (
        dlt.read("bronze_categories")
           .withColumnRenamed("DescriptionText", "Description")
    )

    suppliers = (
        dlt.read("bronze_suppliers")
           .withColumnRenamed("SuppliersName", "SupplierName")
    )

    return (
        ordersdetails
        .join(
            products.select("ProductID", "ProductName", "SupplierID", "CategoryID", "Unit", "Price"),
            on="ProductID", how="left"
        )
        .join(
            categories.select("CategoryID", "CategoryName", "Description"),
            on="CategoryID", how="left"
        )
        .join(
            suppliers.select("SupplierID", "SupplierName", "City", "Country"),
            on="SupplierID", how="left"
        )
        .withColumnRenamed("City",    "SupplierCity")
        .withColumnRenamed("Country", "SupplierCountry")
        .withColumn("LineRevenue", round(col("Price") * col("Quantity"), 2))
        .drop("SupplierID", "CategoryID")
    )


# GOLD LAYER - Materialized Views
# Gold tables recompute fully on each run since they are aggregations

@dlt.table(
    name    = "gold_sales_overview",
    comment = "Monthly revenue, orders, and items sold. Powers Sales Overview page in Power BI."
)
def gold_sales_overview():
    master = (
        dlt.read("silver_order_items_enriched")
           .join(dlt.read("silver_orders_enriched"), on="OrderID", how="left")
    )
    return (
        master
        .withColumn("Year",      year(col("OrderDate")))
        .withColumn("Month",     month(col("OrderDate")))
        .withColumn("YearMonth", date_format(col("OrderDate"), "yyyy-MM"))
        .groupBy("Year", "Month", "YearMonth")
        .agg(
            round(sum("LineRevenue"), 2).alias("TotalRevenue"),
            countDistinct("OrderID").alias("TotalOrders"),
            sum("Quantity").alias("TotalItemsSold"),
            round(avg("LineRevenue"), 2).alias("AvgOrderValue")
        )
        .orderBy("YearMonth")
    )

@dlt.table(
    name    = "gold_product_performance",
    comment = "Revenue and quantity by product and category. Powers Product & Category page in Power BI."
)
def gold_product_performance():
    return (
        dlt.read("silver_order_items_enriched")
        .groupBy("ProductID", "ProductName", "CategoryName", "Unit", "Price")
        .agg(
            sum("Quantity").alias("TotalQuantitySold"),
            round(sum("LineRevenue"), 2).alias("TotalRevenue"),
            countDistinct("OrderID").alias("TotalOrders")
        )
        .orderBy(col("TotalRevenue").desc())
    )

@dlt.table(
    name    = "gold_customer_geography",
    comment = "Revenue and orders by customer and country. Powers Customer Geography page in Power BI."
)
def gold_customer_geography():
    return (
        dlt.read("silver_order_items_enriched")
        .join(dlt.read("silver_orders_enriched"), on="OrderID", how="left")
        .groupBy("CustomerName", "CustomerCity", "CustomerCountry")
        .agg(
            round(sum("LineRevenue"), 2).alias("TotalRevenue"),
            countDistinct("OrderID").alias("TotalOrders"),
            sum("Quantity").alias("TotalItemsBought")
        )
        .orderBy(col("TotalRevenue").desc())
    )

@dlt.table(
    name    = "gold_employee_performance",
    comment = "Sales performance per employee. Powers Employee Performance page in Power BI."
)
def gold_employee_performance():
    return (
        dlt.read("silver_order_items_enriched")
        .join(dlt.read("silver_orders_enriched"), on="OrderID", how="left")
        .groupBy("EmployeeName")
        .agg(
            round(sum("LineRevenue"), 2).alias("TotalRevenue"),
            countDistinct("OrderID").alias("TotalOrders"),
            sum("Quantity").alias("TotalItemsSold"),
            round(avg("LineRevenue"), 2).alias("AvgRevenuePerOrder")
        )
        .orderBy(col("TotalRevenue").desc())
    )

@dlt.table(
    name    = "gold_shipper_performance",
    comment = "Revenue and orders per shipper. Powers Revenue by Shipper visual in Power BI."
)
def gold_shipper_performance():
    return (
        dlt.read("silver_order_items_enriched")
        .join(dlt.read("silver_orders_enriched"), on="OrderID", how="left")
        .groupBy("ShipperName")
        .agg(
            round(sum("LineRevenue"), 2).alias("TotalRevenue"),
            countDistinct("OrderID").alias("TotalOrders"),
            sum("Quantity").alias("TotalItemsShipped")
        )
        .orderBy(col("TotalRevenue").desc())
    )