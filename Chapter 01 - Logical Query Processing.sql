---------------------------------------------------------------------
-- Chapter 01 - Logical Query Processing
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Sample Query based on Customers/Orders Scenario
---------------------------------------------------------------------

-- Listing 1-2: Data Definition Language (DDL) and sample data for Customers and Orders
SET NOCOUNT ON;
USE tempdb;
GO
IF OBJECT_ID('dbo.Orders') IS NOT NULL
  DROP TABLE dbo.Orders;
GO
IF OBJECT_ID('dbo.Customers') IS NOT NULL
  DROP TABLE dbo.Customers;
GO
CREATE TABLE dbo.Customers
(
  customerid  CHAR(5)     NOT NULL PRIMARY KEY,
  city        VARCHAR(10) NOT NULL
);

INSERT INTO dbo.Customers(customerid, city) VALUES('FISSA', 'Madrid');
INSERT INTO dbo.Customers(customerid, city) VALUES('FRNDO', 'Madrid');
INSERT INTO dbo.Customers(customerid, city) VALUES('KRLOS', 'Madrid');
INSERT INTO dbo.Customers(customerid, city) VALUES('MRPHS', 'Zion');

CREATE TABLE dbo.Orders
(
  orderid    INT     NOT NULL PRIMARY KEY,
  customerid CHAR(5)     NULL REFERENCES Customers(customerid)
);

INSERT INTO dbo.Orders(orderid, customerid) VALUES(1, 'FRNDO');
INSERT INTO dbo.Orders(orderid, customerid) VALUES(2, 'FRNDO');
INSERT INTO dbo.Orders(orderid, customerid) VALUES(3, 'KRLOS');
INSERT INTO dbo.Orders(orderid, customerid) VALUES(4, 'KRLOS');
INSERT INTO dbo.Orders(orderid, customerid) VALUES(5, 'KRLOS');
INSERT INTO dbo.Orders(orderid, customerid) VALUES(6, 'MRPHS');
INSERT INTO dbo.Orders(orderid, customerid) VALUES(7, NULL);
GO

-- Listing 1-3: Query: Madrid customers with Fewer than three orders

-- The query returns customers from Madrid that made fewer than
-- three orders (including zero), and their order count.
-- The result is sorted by the order count.
SELECT C.customerid, COUNT(O.orderid) AS numorders
FROM dbo.Customers AS C
  LEFT OUTER JOIN dbo.Orders AS O
    ON C.customerid = O.customerid
WHERE C.city = 'Madrid'
GROUP BY C.customerid
HAVING COUNT(O.orderid) < 3
ORDER BY numorders;
GO

---------------------------------------------------------------------
-- Query Logical Processing Phase Details
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Step 10 – Applying ORDER BY Clause
---------------------------------------------------------------------

-- Sorting by Ordinal Positions
SELECT orderid, customerid FROM dbo.Orders ORDER BY 2, 1;
GO

-- ORDER BY in Derived Table is not Allowed
SELECT *
FROM (SELECT orderid, customerid
      FROM dbo.Orders
      ORDER BY orderid) AS D;
GO

-- ORDER BY in View is not Allowed
IF OBJECT_ID('dbo.VSortedOrders') IS NOT NULL
  DROP VIEW dbo.VSortedOrders;
GO
CREATE VIEW dbo.VSortedOrders
AS

SELECT orderid, customerid
FROM dbo.Orders
ORDER BY orderid
GO

---------------------------------------------------------------------
-- Step 11 – Applying TOP Option
---------------------------------------------------------------------

-- Circumventing ORDER BY Limitation with TOP
SELECT *
FROM (SELECT TOP 100 PERCENT orderid, customerid
      FROM dbo.Orders
      ORDER BY orderid) AS D;
GO

IF OBJECT_ID('dbo.VSortedOrders') IS NOT NULL
  DROP VIEW dbo.VSortedOrders;
GO

-- Note: This does not create a “sorted view”!
CREATE VIEW dbo.VSortedOrders
AS

SELECT TOP 100 PERCENT orderid, customerid
FROM dbo.Orders
ORDER BY orderid
GO

---------------------------------------------------------------------
-- New Logical Processing Phases in SQL Server 2005
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Table Operators
---------------------------------------------------------------------

---------------------------------------------------------------------
-- APPLY
---------------------------------------------------------------------

-- Two most recent orders for each customer
SELECT C.customerid, city, orderid
FROM dbo.Customers AS C
  CROSS APPLY
    (SELECT TOP(2) orderid, customerid
     FROM dbo.Orders AS O
     WHERE O.customerid = C.customerid
     ORDER BY orderid DESC) AS CA;

-- Two most recent orders for each customer,
-- including customers that made no orders
SELECT C.customerid, city, orderid
FROM dbo.Customers AS C
  OUTER APPLY
    (SELECT TOP(2) orderid, customerid
     FROM dbo.Orders AS O
     WHERE O.customerid = C.customerid
     ORDER BY orderid DESC) AS OA;
GO

---------------------------------------------------------------------
-- PIVOT
---------------------------------------------------------------------

-- Customer Categories based on Count of Orders
SELECT C.customerid, city,
  CASE
    WHEN COUNT(orderid)  = 0 THEN 'no_orders'
    WHEN COUNT(orderid) <= 2 THEN 'upto_two_orders' 
    WHEN COUNT(orderid)  > 2 THEN 'more_than_two_orders' 
  END AS category
FROM dbo.Customers AS C
  LEFT OUTER JOIN dbo.Orders AS O
    ON C.customerid = O.customerid
GROUP BY C.customerid, city;

-- Number of Customers that Fall in each Category per City
SELECT city, no_orders, upto_two_orders, more_than_two_orders
FROM (SELECT C.customerid, city,
        CASE
          WHEN COUNT(orderid) = 0 THEN 'no_orders'
          WHEN COUNT(orderid) <= 2 THEN 'upto_two_orders' 
          WHEN COUNT(orderid) > 2 THEN 'more_than_two_orders' 
        END AS category
      FROM dbo.Customers AS C
        LEFT OUTER JOIN dbo.Orders AS O
          ON C.customerid = O.customerid
      GROUP BY C.customerid, city) AS D
  PIVOT(COUNT(customerid) FOR 
    category IN([no_orders],
                [upto_two_orders],
                [more_than_two_orders])) AS P;

-- Logical Equivalent to the PIVOT Query
SELECT city, 
  COUNT(CASE WHEN category = 'no_orders'
               THEN customerid END) AS [no_orders],
  COUNT(CASE WHEN category = 'upto_two_orders'
               THEN customerid END) AS [upto_two_orders],
  COUNT(CASE WHEN category = 'more_than_two_orders'
               THEN customerid END) AS [more_than_two_orders]
FROM (SELECT C.customerid, city,
        CASE
          WHEN COUNT(orderid) = 0 THEN 'no_orders'
          WHEN COUNT(orderid) <= 2 THEN 'upto_two_orders' 
          WHEN COUNT(orderid) > 2 THEN 'more_than_two_orders' 
        END AS category
      FROM dbo.Customers AS C
        LEFT OUTER JOIN dbo.Orders AS O
          ON C.customerid = O.customerid
      GROUP BY C.customerid, city) AS D
GROUP BY city;
GO

---------------------------------------------------------------------
-- UNPIVOT
---------------------------------------------------------------------

IF OBJECT_ID('dbo.PivotedCategories') IS NOT NULL
  DROP TABLE dbo.PivotedCategories;
GO

-- Listing 1-4: Creating and Populating the PivotedCategories Table
SELECT city, no_orders, upto_two_orders, more_than_two_orders
INTO dbo.PivotedCategories
FROM (SELECT C.customerid, city,
        CASE
          WHEN COUNT(orderid)  = 0 THEN 'no_orders'
          WHEN COUNT(orderid) <= 2 THEN 'upto_two_orders' 
          WHEN COUNT(orderid)  > 2 THEN 'more_than_two_orders' 
        END AS category
      FROM dbo.Customers AS C
        LEFT OUTER JOIN dbo.Orders AS O
          ON C.customerid = O.customerid
      GROUP BY C.customerid, city) AS D
  PIVOT(COUNT(customerid) FOR 
    category IN([no_orders],
                [upto_two_orders],
                [more_than_two_orders])) AS P;

UPDATE dbo.PivotedCategories
  SET no_orders = NULL, upto_two_orders = 3
WHERE city = 'Madrid';
GO

-- Unpivoted Customer Categories
SELECT city, category, num_custs
FROM dbo.PivotedCategories
  UNPIVOT(num_custs FOR
    category IN([no_orders],
                [upto_two_orders],
                [more_than_two_orders])) AS U;
GO

-- Cleanup
IF OBJECT_ID('dbo.PivotedCategories') IS NOT NULL
  DROP TABLE dbo.PivotedCategories;
GO

---------------------------------------------------------------------
-- OVER Clause
---------------------------------------------------------------------

-- OVER Clause applied in SELECT Phase
SELECT orderid, customerid,
  COUNT(*) OVER(PARTITION BY customerid) AS num_orders
FROM dbo.Orders
WHERE customerid IS NOT NULL
  AND orderid % 2 = 1;

-- OVER Clause applied in ORDER BY Phase
SELECT orderid, customerid
FROM dbo.Orders
WHERE customerid IS NOT NULL
  AND orderid % 2 = 1
ORDER BY COUNT(*) OVER(PARTITION BY customerid) DESC;
GO

---------------------------------------------------------------------
-- Set Operations
---------------------------------------------------------------------

-- UNION ALL Set Operation
SELECT 'O' AS letter, customerid, orderid FROM dbo.Orders 
WHERE customerid LIKE '%O%'

UNION ALL

SELECT 'S' AS letter, customerid, orderid FROM dbo.Orders 
WHERE customerid LIKE '%S%'

ORDER BY letter, customerid, orderid;

-- Customers that placed no Orders
SELECT customerid FROM dbo.Customers
EXCEPT
SELECT customerid FROM dbo.Orders;
GO