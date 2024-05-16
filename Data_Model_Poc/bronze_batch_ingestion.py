# Databricks notebook source
# MAGIC %md
# MAGIC ##Task 1: 
# MAGIC ###Create a model for the above system and define entity-relationship between the objects

# COMMAND ----------

# %sql
# CREATE DATABASE IF NOT EXISTS kusha_solutions.gen_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE kusha_solutions.gen_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE kusha_solutions.gen_test;
# MAGIC
# MAGIC -- Drop Customer table
# MAGIC -- DROP TABLE IF EXISTS Customer;
# MAGIC
# MAGIC -- Drop Product table
# MAGIC -- DROP TABLE IF EXISTS Product;
# MAGIC
# MAGIC -- Drop Order table
# MAGIC -- DROP TABLE IF EXISTS Order;
# MAGIC
# MAGIC -- Drop OrderItem table
# MAGIC DROP TABLE IF EXISTS OrderItem;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE gen_test.order;
# MAGIC -- TRUNCATE TABLE gen_test.customer;
# MAGIC -- TRUNCATE TABLE gen_test.product;
# MAGIC -- TRUNCATE TABLE gen_test.orderitem;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE kusha_solutions.gen_test;
# MAGIC CREATE TABLE Customer (
# MAGIC     CustomerID INT PRIMARY KEY,
# MAGIC     Name VARCHAR(100),
# MAGIC     Email VARCHAR(100),
# MAGIC     Address VARCHAR(255),
# MAGIC     Phone VARCHAR(20)
# MAGIC );
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE kusha_solutions.gen_test;
# MAGIC CREATE TABLE Product (
# MAGIC     ProductID INT PRIMARY KEY,
# MAGIC     ProductName VARCHAR(100),
# MAGIC     Price DECIMAL(10, 2)
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE kusha_solutions.gen_test;
# MAGIC CREATE TABLE `Order` (
# MAGIC     OrderID INT PRIMARY KEY,
# MAGIC     CustomerID INT,
# MAGIC     OrderDate DATE,
# MAGIC     Status VARCHAR(20),
# MAGIC     FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
# MAGIC );
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC USE kusha_solutions.gen_test;
# MAGIC CREATE TABLE OrderLineItem (
# MAGIC     OrderLineItemID INT PRIMARY KEY,
# MAGIC     OrderID INT,
# MAGIC     ProductID INT,
# MAGIC     Quantity INT,
# MAGIC     Status VARCHAR(20),
# MAGIC     FOREIGN KEY (OrderID) REFERENCES `Order`(OrderID),
# MAGIC     FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Customer (CustomerID, Name, Email, Address, Phone)
# MAGIC VALUES
# MAGIC (101, 'Alice Johnson', 'alice@example.com', '123 Maple St', '555-1234'),
# MAGIC (102, 'Bob Smith', 'bob@example.com', '456 Oak St', '555-5678'),
# MAGIC (103, 'Charlie Brown', 'charlie@example.com', '789 Pine St', '555-9012');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gen_test.customer

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Product (ProductID, ProductName, Price)
# MAGIC VALUES
# MAGIC (201, 'Laptop', 999.99),
# MAGIC (202, 'Smartphone', 499.99),
# MAGIC (203, 'Tablet', 299.99),
# MAGIC (204, 'Headphones', 89.99),
# MAGIC (205, 'Smartwatch', 199.99);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO `Order` (OrderID, CustomerID, OrderDate, Status)
# MAGIC VALUES
# MAGIC (301, 101, '2024-05-01', 'Completed'),
# MAGIC (302, 102, '2024-05-02', 'Pending'),
# MAGIC (303, 101, '2024-05-03', 'Completed'),
# MAGIC (304, 103, '2024-05-03', 'Pending');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO OrderLineItem (OrderLineItemID, OrderID, ProductID, Quantity, Status)
# MAGIC VALUES
# MAGIC (401, 301, 201, 1, 'Delivered'),  -- Laptop for Order 301
# MAGIC (402, 301, 204, 2, 'Delivered'),  -- 2 Headphones for Order 301
# MAGIC (403, 302, 202, 1, 'Pending'),    -- Smartphone for Order 302
# MAGIC (404, 302, 205, 1, 'Pending'),    -- Smartwatch for Order 302
# MAGIC (405, 303, 203, 1, 'Delivered'),  -- Tablet for Order 303
# MAGIC (406, 303, 204, 1, 'Delivered'),  -- Headphones for Order 303
# MAGIC (407, 304, 201, 1, 'Pending');    -- Laptop for Order 304
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Customer (CustomerID, Name, Email, Address, Phone)
# MAGIC VALUES
# MAGIC (104, 'Diana Prince', 'diana@example.com', '101 Elm St', '555-2345'),
# MAGIC (105, 'Edward Norton', 'edward@example.com', '202 Birch St', '555-6789');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Product (ProductID, ProductName, Price)
# MAGIC VALUES
# MAGIC (206, 'Camera', 399.99),
# MAGIC (207, 'Printer', 149.99);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO `Order` (OrderID, CustomerID, OrderDate, Status)
# MAGIC VALUES
# MAGIC (305, 102, '2024-05-04', 'Completed'),
# MAGIC (306, 104, '2024-05-05', 'Pending'),
# MAGIC (307, 105, '2024-05-06', 'Completed'),
# MAGIC (308, 103, '2024-05-06', 'Pending');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO OrderLineItem (OrderLineItemID, OrderID, ProductID, Quantity, Status)
# MAGIC VALUES
# MAGIC (408, 305, 201, 1, 'Delivered'),  -- Laptop for Order 305
# MAGIC (409, 305, 202, 2, 'Delivered'),  -- 2 Smartphones for Order 305
# MAGIC (410, 306, 203, 1, 'Pending'),    -- Tablet for Order 306
# MAGIC (411, 306, 204, 3, 'Pending'),    -- 3 Headphones for Order 306
# MAGIC (412, 307, 206, 1, 'Delivered'),  -- Camera for Order 307
# MAGIC (413, 307, 202, 1, 'Delivered'),  -- Smartphone for Order 307
# MAGIC (414, 308, 205, 2, 'Pending'),    -- 2 Smartwatches for Order 308
# MAGIC (415, 308, 207, 1, 'Pending');    -- Printer for Order 308
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO `Order` (OrderID, CustomerID, OrderDate, Status)
# MAGIC VALUES
# MAGIC (309, 101, '2024-04-15', 'Completed'),
# MAGIC (310, 104, '2024-04-20', 'Completed'),
# MAGIC (311, 105, '2024-04-25', 'Completed'),
# MAGIC (312, 102, '2024-04-30', 'Completed');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO OrderLineItem (OrderLineItemID, OrderID, ProductID, Quantity, Status)
# MAGIC VALUES
# MAGIC (416, 309, 203, 1, 'Delivered'),  -- Tablet for Order 309
# MAGIC (417, 309, 204, 2, 'Delivered'),  -- 2 Headphones for Order 309
# MAGIC (418, 310, 201, 1, 'Delivered'),  -- Laptop for Order 310
# MAGIC (419, 311, 205, 1, 'Delivered'),  -- Smartwatch for Order 311
# MAGIC (420, 312, 202, 1, 'Delivered'),  -- Smartphone for Order 312
# MAGIC (421, 312, 206, 1, 'Delivered');  -- Camera for Order 312
# MAGIC

# COMMAND ----------


