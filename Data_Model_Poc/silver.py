# Databricks notebook source
# MAGIC %sql
# MAGIC use gen_test

# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gen_test.OrderLineItem WHERE Quantity IS NOT NULL;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC data deduplication

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT * FROM gen_test.OrderLineItem;
# MAGIC

# COMMAND ----------



# COMMAND ----------

# Read from Bronze layer
df_order = spark.sql("select * from gen_test.order")
df_order_line_items = spark.sql("select * from gen_test.OrderLineItem ")

# COMMAND ----------

# Assume we have new data to upsert
new_orders = [
    (308, 103, '2024-05-06', 'Completed'), # Status updated
    (313, 102, '2024-05-30', 'Pending') # New order
]

new_order_line_items = [
    (410, 306, 203, 1, 'Shipped'), # Quantity updated
    (422, 313, 206, 1, 'Delivered') # New line item
]


# COMMAND ----------

columns_orders = ["OrderID", "CustomerID", "OrderDate", "Status"]
columns_order_line_items = ["OrderLineItemID", "OrderID", "ProductID", "Quantity", "Status"]

# COMMAND ----------

# Creating DataFrames for new data
df_new_orders = spark.createDataFrame(data=new_orders, schema=columns_orders)
df_new_order_line_items = spark.createDataFrame(data=new_order_line_items, schema=columns_order_line_items)

# COMMAND ----------


delta_orders = spark.read.table("gen_test.order")

# COMMAND ----------

df=spark.sql("select * from gen_test.order where OrderID = 308")
df.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gen_test.order

# COMMAND ----------

# Create or replace temporary views for the dataframes
delta_orders.createOrReplaceTempView("delta_orders")
df_new_orders.createOrReplaceTempView("df_new_orders")

# Perform the merge operation using SQL syntax
merged_data = spark.sql("""
    MERGE INTO delta_orders AS target
    USING df_new_orders AS source
    ON target.OrderID = source.OrderID
    WHEN MATCHED THEN
        UPDATE SET
            target.CustomerID = source.CustomerID,
            target.OrderDate = source.OrderDate,
            target.Status = source.Status
    WHEN NOT MATCHED THEN
        INSERT (OrderID, CustomerID, OrderDate, Status)
        VALUES (source.OrderID, source.CustomerID, source.OrderDate, source.Status)
""")

# Execute the merge operation
merged_data.show()

# COMMAND ----------

delta_order_line_items = spark.read.table("gen_test.orderlineitem")

# COMMAND ----------

delta_order_line_items.createOrReplaceTempView("delta_order_line_items")
df_new_order_line_items.createOrReplaceTempView("df_new_order_line_items")

# COMMAND ----------

spark.sql("""
    MERGE INTO delta_order_line_items AS target
    USING df_new_order_line_items AS source
    ON target.OrderLineItemID = source.OrderLineItemID
    WHEN MATCHED THEN
        UPDATE SET
            target.OrderID = source.OrderID,
            target.ProductID = source.ProductID,
            target.Quantity = source.Quantity,
            target.Status = source.Status
    WHEN NOT MATCHED THEN
        INSERT (OrderLineItemID, OrderID, ProductID, Quantity, Status)
        VALUES (source.OrderLineItemID, source.OrderID, source.ProductID, source.Quantity, source.Status)
""")

# COMMAND ----------

merged_data = spark.sql("SELECT * FROM delta_order_line_items")
merged_data.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gen_test.order

# COMMAND ----------


