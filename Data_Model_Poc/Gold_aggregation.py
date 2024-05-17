# Databricks notebook source
# MAGIC %md
# MAGIC ##Task 2: 
# MAGIC ####Calculate orders by date,
# MAGIC
# MAGIC
# MAGIC ####Order total by date
# MAGIC
# MAGIC
# MAGIC ####Total orders by product per week
# MAGIC
# MAGIC
# MAGIC ####Most sold product in last month

# COMMAND ----------

# MAGIC %md
# MAGIC ##Calculate orders by date:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT OrderDate, COUNT(OrderID) AS TotalOrders
# MAGIC FROM gen_test.`order`
# MAGIC GROUP BY OrderDate
# MAGIC ORDER BY OrderDate;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Order total by date:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT o.OrderDate, SUM(p.Price * li.Quantity) AS TotalOrderValue
# MAGIC FROM gen_test.`Order` o
# MAGIC JOIN gen_test.OrderLineItem li ON o.OrderID = li.OrderID
# MAGIC JOIN gen_test.Product p ON li.ProductID = p.ProductID
# MAGIC GROUP BY o.OrderDate
# MAGIC ORDER BY o.OrderDate;
# MAGIC

# COMMAND ----------

# %sql
# SELECT o.order_id, o.order_date, oi.product_id, p.product_name, oi.quantity, p.unit_price, oi.quantity * p.unit_price AS total_price
# FROM gen_test.`Order` o
# JOIN gen_test.OrderItem oi ON o.order_id = oi.order_id
# JOIN gen_test.Product p ON oi.product_id = p.product_id
# WHERE o.customer_id = 3 AND o.order_date = '2024-04-05';


# COMMAND ----------

# MAGIC %md
# MAGIC ##Total orders by product per week:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     CONCAT(YEAR(o.OrderDate), '-', WEEKOFYEAR(o.OrderDate)) AS Week, 
# MAGIC     li.ProductID, 
# MAGIC     p.ProductName,
# MAGIC     COUNT(DISTINCT li.OrderID) AS TotalOrders
# MAGIC FROM gen_test.`Order` o
# MAGIC JOIN gen_test.OrderLineItem li ON o.OrderID = li.OrderID
# MAGIC JOIN gen_test.Product p ON li.ProductID = p.ProductID
# MAGIC GROUP BY CONCAT(YEAR(o.OrderDate), '-', WEEKOFYEAR(o.OrderDate)), li.ProductID, p.ProductName
# MAGIC ORDER BY Week, li.ProductID;

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Most sold product in last month

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     li.ProductID, 
# MAGIC     p.ProductName, 
# MAGIC     SUM(li.Quantity) AS TotalQuantitySold
# MAGIC FROM gen_test.OrderLineItem li
# MAGIC JOIN gen_test.Product p ON li.ProductID = p.ProductID
# MAGIC JOIN gen_test.`Order` o ON li.OrderID = o.OrderID
# MAGIC WHERE o.OrderDate >= ADD_MONTHS(CURRENT_DATE(), -1)
# MAGIC GROUP BY li.ProductID, p.ProductName
# MAGIC ORDER BY TotalQuantitySold DESC
# MAGIC LIMIT 1;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ####visualization

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     c.CustomerID,
# MAGIC     c.Name AS CustomerName,
# MAGIC     c.Email,
# MAGIC     o.OrderID,
# MAGIC     o.OrderDate,
# MAGIC     p.ProductID,
# MAGIC     p.ProductName,
# MAGIC     li.Quantity,
# MAGIC     p.Price,
# MAGIC     li.Status AS LineItemStatus,
# MAGIC     o.Status AS OrderStatus
# MAGIC FROM gen_test.Customer c
# MAGIC JOIN gen_test.`Order` o ON c.CustomerID = o.CustomerID
# MAGIC JOIN gen_test.OrderLineItem li ON o.OrderID = li.OrderID
# MAGIC JOIN gen_test.Product p ON li.ProductID = p.ProductID
# MAGIC ORDER BY c.CustomerID, o.OrderDate;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####Calculate total sales per month:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT YEAR(OrderDate) AS Year,
# MAGIC        MONTH(OrderDate) AS Month,
# MAGIC        SUM(Quantity * p.Price) AS TotalSales
# MAGIC FROM gen_test.order o
# MAGIC JOIN gen_test.OrderLineItem ol ON o.OrderID = ol.OrderID
# MAGIC JOIN gen_test.Product p ON ol.ProductID = p.ProductID
# MAGIC GROUP BY YEAR(OrderDate), MONTH(OrderDate)
# MAGIC ORDER BY Year, Month;
# MAGIC

# COMMAND ----------


