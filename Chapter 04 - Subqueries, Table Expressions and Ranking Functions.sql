---------------------------------------------------------------------
-- Chapter 04 - Subqueries, Table Expressions and Ranking Functions
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Subqueries
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Self-Contained Subqueries
---------------------------------------------------------------------

-- Scalar subquery example
SET NOCOUNT ON;
USE Northwind;

SELECT OrderID FROM dbo.Orders
WHERE EmployeeID = 
  (SELECT EmployeeID FROM dbo.Employees
   -- also try with N'Kollar' and N'D%'
   WHERE LastName LIKE N'Davolio');

-- Customers with orders handled by all employees from the USA
-- using literals
SELECT CustomerID
FROM dbo.Orders
WHERE EmployeeID IN(1, 2, 3, 4, 8)
GROUP BY CustomerID
HAVING COUNT(DISTINCT EmployeeID) = 5;

-- Customers with orders handled by all employees from the USA
-- using subqueries
SELECT CustomerID
FROM dbo.Orders
WHERE EmployeeID IN
  (SELECT EmployeeID FROM dbo.Employees
   WHERE Country = N'USA')
GROUP BY CustomerID
HAVING COUNT(DISTINCT EmployeeID) =
  (SELECT COUNT(*) FROM dbo.Employees
   WHERE Country = N'USA');

-- Orders placed on last actual order date of the month
SELECT OrderID, CustomerID, EmployeeID, OrderDate
FROM dbo.Orders
WHERE OrderDate IN
  (SELECT MAX(OrderDate)
   FROM dbo.Orders
   GROUP BY CONVERT(CHAR(6), OrderDate, 112));
GO

---------------------------------------------------------------------
-- Correlated Subqueries
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Tiebreaker
---------------------------------------------------------------------

-- Index for tiebreaker problems
CREATE UNIQUE INDEX idx_eid_od_oid 
  ON dbo.Orders(EmployeeID, OrderDate, OrderID);
CREATE UNIQUE INDEX idx_eid_od_rd_oid 
  ON dbo.Orders(EmployeeID, OrderDate, RequiredDate, OrderID);
GO

-- Orders with the maximum OrderDate for each employee
-- Incorrect solution
SELECT OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate 
FROM dbo.Orders
WHERE OrderDate IN
  (SELECT MAX(OrderDate) FROM dbo.Orders
   GROUP BY EmployeeID);

-- Orders with maximum OrderDate for each employee
SELECT OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate 
FROM dbo.Orders AS O1
WHERE OrderDate =
  (SELECT MAX(OrderDate)
   FROM dbo.Orders AS O2
   WHERE O2.EmployeeID = O1.EmployeeID);

-- Most recent order for each employee
-- Tiebreaker: max order id
SELECT OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate 
FROM dbo.Orders AS O1
WHERE OrderDate =
  (SELECT MAX(OrderDate)
   FROM dbo.Orders AS O2
   WHERE O2.EmployeeID = O1.EmployeeID)
  AND OrderID =
  (SELECT MAX(OrderID)
   FROM dbo.Orders AS O2
   WHERE O2.EmployeeID = O1.EmployeeID
     AND O2.OrderDate = O1.OrderDate);

-- Most recent order for each employee, nesting subqueries
-- Tiebreaker: max order id
SELECT OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate 
FROM dbo.Orders AS O1
WHERE OrderID = 
  (SELECT MAX(OrderID)
   FROM dbo.Orders AS O2
   WHERE O2.EmployeeID = O1.EmployeeID
     AND O2.OrderDate = 
       (SELECT MAX(OrderDate)
        FROM dbo.Orders AS O3
        WHERE O3.EmployeeID = O1.EmployeeID));

-- Most recent order for each employee
-- Tiebreaker: max RequiredDate, max OrderID
SELECT OrderID, CustomerID, EmployeeID, OrderDate, RequiredDate 
FROM dbo.Orders AS O1
WHERE OrderDate =
  (SELECT MAX(OrderDate)
   FROM dbo.Orders AS O2
   WHERE O2.EmployeeID = O1.EmployeeID)
  AND RequiredDate =
  (SELECT MAX(RequiredDate)
   FROM dbo.Orders AS O2
   WHERE O2.EmployeeID = O1.EmployeeID
     AND O2.OrderDate = O1.OrderDate)
  AND OrderID =
  (SELECT MAX(OrderID)
   FROM dbo.Orders AS O2
   WHERE O2.EmployeeID = O1.EmployeeID
     AND O2.OrderDate = O1.OrderDate
     AND O2.RequiredDate = O1.RequiredDate);

-- Cleanup
DROP INDEX dbo.Orders.idx_eid_od_oid;
DROP INDEX dbo.Orders.idx_eid_od_rd_oid;
GO

---------------------------------------------------------------------
-- EXISTS
---------------------------------------------------------------------

-- Customers from Spain that made orders
-- Using EXISTS
SELECT CustomerID, CompanyName
FROM dbo.Customers AS C
WHERE Country = N'Spain'
  AND EXISTS
    (SELECT * FROM dbo.Orders AS O
     WHERE O.CustomerID = C.CustomerID);

---------------------------------------------------------------------
-- EXISTS vs. IN
---------------------------------------------------------------------

-- Customers from Spain that made orders
-- Using IN
SELECT CustomerID, CompanyName
FROM dbo.Customers AS C
WHERE Country = N'Spain'
  AND CustomerID IN(SELECT CustomerID FROM dbo.Orders);

---------------------------------------------------------------------
-- NOT EXISTS vs. NOT IN
---------------------------------------------------------------------

-- Customers from Spain who made no Orders
-- Using EXISTS
SELECT CustomerID, CompanyName
FROM dbo.Customers AS C
WHERE Country = N'Spain'
  AND NOT EXISTS
    (SELECT * FROM dbo.Orders AS O
     WHERE O.CustomerID = C.CustomerID);

-- Customers from Spain who made no Orders
-- Using IN, try 1
SELECT CustomerID, CompanyName
FROM dbo.Customers AS C
WHERE Country = N'Spain'
  AND CustomerID NOT IN(SELECT CustomerID FROM dbo.Orders);

-- Add a row to Orders with a NULL customer id
INSERT INTO dbo.Orders DEFAULT VALUES;

-- Customers from Spain that made no Orders
-- Using IN, try 2
SELECT CustomerID, CompanyName
FROM dbo.Customers AS C
WHERE Country = N'Spain'
  AND CustomerID NOT IN(SELECT CustomerID FROM dbo.Orders
                        WHERE CustomerID IS NOT NULL);

-- Remove the row from Orders with the NULL customer id
DELETE FROM dbo.Orders WHERE CustomerID IS NULL;
GO

---------------------------------------------------------------------
-- Min missing value
---------------------------------------------------------------------

-- Listing 4-1: Creating and Populating the Table T1
USE tempdb;
GO
IF OBJECT_ID('dbo.T1') IS NOT NULL
  DROP TABLE dbo.T1;
GO

CREATE TABLE dbo.T1
(
  keycol  INT         NOT NULL PRIMARY KEY CHECK(keycol > 0),
  datacol VARCHAR(10) NOT NULL
);
INSERT INTO dbo.T1(keycol, datacol) VALUES(3, 'a');
INSERT INTO dbo.T1(keycol, datacol) VALUES(4, 'b');
INSERT INTO dbo.T1(keycol, datacol) VALUES(6, 'c');
INSERT INTO dbo.T1(keycol, datacol) VALUES(7, 'd');

-- Incomplete CASE expression for minimum missing value query
/*
SELECT
  CASE
    WHEN NOT EXISTS(SELECT * FROM dbo.T1 WHERE keycol = 1) THEN 1
    ELSE (...subquery returning minimum missing value...)
  END;
*/

-- Minimum missing value query
SELECT MIN(A.keycol + 1) as missing
FROM dbo.T1 AS A
WHERE NOT EXISTS
  (SELECT * FROM dbo.T1 AS B
   WHERE B.keycol = A.keycol + 1);

-- Complete CASE expression for minimum missing value query
SELECT
  CASE
    WHEN NOT EXISTS(SELECT * FROM dbo.T1 WHERE keycol = 1) THEN 1
    ELSE (SELECT MIN(A.keycol + 1)
          FROM dbo.T1 AS A
          WHERE NOT EXISTS
            (SELECT * FROM dbo.T1 AS B
             WHERE B.keycol = A.keycol + 1))
  END;

-- Populating T1 with more rows
INSERT INTO dbo.T1(keycol, datacol) VALUES(1, 'e');
INSERT INTO dbo.T1(keycol, datacol) VALUES(2, 'f');

-- Embedding the CASE expression in an INSERT SELECT statement
INSERT INTO dbo.T1(keycol, datacol)
  SELECT 
    CASE
      WHEN NOT EXISTS(SELECT * FROM dbo.T1 WHERE keycol = 1) THEN 1
      ELSE (SELECT MIN(A.keycol + 1)
            FROM dbo.T1 AS A
            WHERE NOT EXISTS
              (SELECT * FROM dbo.T1 AS B
               WHERE B.keycol = A.keycol + 1))
    END,
    'f';

-- Examining the content of T1 after the INSERT
SELECT * FROM dbo.T1;

-- Merging the two cases into one query
SELECT COALESCE(MIN(A.keycol + 1), 1)
FROM dbo.T1 AS A
WHERE NOT EXISTS
  (SELECT * FROM dbo.T1 AS B
    WHERE B.keycol= A.keycol + 1)
  AND EXISTS(SELECT * FROM dbo.T1 WHERE keycol = 1);
GO

---------------------------------------------------------------------
-- Reverse Logic applied to Relational Division Problems
---------------------------------------------------------------------

-- Return all customers with orders handled by all employees from the USA
USE Northwind;

SELECT * FROM dbo.Customers AS C
WHERE NOT EXISTS
  (SELECT * FROM dbo.Employees AS E
   WHERE Country = N'USA'
     AND NOT EXISTS
       (SELECT * FROM dbo.Orders AS O
        WHERE O.CustomerID = C.CustomerID
          AND O.EmployeeID = E.EmployeeID));
GO

---------------------------------------------------------------------
-- Misbehaving Subqueries
---------------------------------------------------------------------

-- Shippers that did not ship orders to customer LAZYK
-- Bug
SELECT ShipperID, CompanyName
FROM dbo.Shippers
WHERE ShipperID NOT IN
  (SELECT ShipperID FROM dbo.Orders
   WHERE CustomerID = N'LAZYK');

-- Bug apparent when explictly specifying aliases
SELECT ShipperID, CompanyName
FROM dbo.Shippers AS S
WHERE ShipperID NOT IN
  (SELECT S.ShipperID FROM dbo.Orders AS O
   WHERE O.CustomerID = N'LAZYK');

-- Logically equivalent non-existence query
SELECT ShipperID, CompanyName
FROM dbo.Shippers
WHERE NOT EXISTS
  (SELECT * FROM dbo.Orders
   WHERE CustomerID = N'LAZYK');

-- Bug corrected
SELECT ShipperID, CompanyName
FROM dbo.Shippers AS S
WHERE ShipperID NOT IN
  (SELECT ShipVia FROM dbo.Orders AS O
   WHERE CustomerID = N'LAZYK');
GO

-- The safe way using aliases, bug identified
SELECT ShipperID, CompanyName
FROM dbo.Shippers AS S
WHERE ShipperID NOT IN
  (SELECT O.ShipperID FROM dbo.Orders AS O
   WHERE O.CustomerID = N'LAZYK');
GO

-- The safe way using aliases, bug corrected
SELECT ShipperID, CompanyName
FROM dbo.Shippers AS S
WHERE ShipperID NOT IN
  (SELECT O.ShipVia FROM dbo.Orders AS O
   WHERE O.CustomerID = N'LAZYK');
GO

---------------------------------------------------------------------
-- Uncommon Predicates
---------------------------------------------------------------------

-- Order with minimum order id for each employee
-- ANY
SELECT OrderID, CustomerID, EmployeeID, OrderDate
FROM dbo.Orders AS O1
WHERE NOT OrderID >
  ANY(SELECT OrderID
      FROM dbo.Orders AS O2
      WHERE O2.EmployeeID = O1.EmployeeID);

-- ALL
SELECT OrderID, CustomerID, EmployeeID, OrderDate
FROM dbo.Orders AS O1
WHERE OrderID <=
  ALL(SELECT OrderID
      FROM dbo.Orders AS O2
      WHERE O2.EmployeeID = O1.EmployeeID);

-- The Natural Way
SELECT OrderID, CustomerID, EmployeeID, OrderDate
FROM dbo.Orders AS O1
WHERE OrderID =
  (SELECT MIN(OrderID)
   FROM dbo.Orders AS O2
   WHERE O2.EmployeeID = O1.EmployeeID);
GO

---------------------------------------------------------------------
-- Table Expressions
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Derived Tables
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Column Aliases
---------------------------------------------------------------------

-- Following fails
SELECT
  YEAR(OrderDate) AS OrderYear,
  COUNT(DISTINCT CustomerID) AS NumCusts
FROM dbo.Orders
GROUP BY OrderYear;
GO

-- Inline column aliasing
SELECT OrderYear, COUNT(DISTINCT CustomerID) AS NumCusts
FROM (SELECT YEAR(OrderDate) AS OrderYear, CustomerID
      FROM dbo.Orders) AS D
GROUP BY OrderYear;

-- External column aliasing
SELECT OrderYear, COUNT(DISTINCT CustomerID) AS NumCusts
FROM (SELECT YEAR(OrderDate), CustomerID
      FROM dbo.Orders) AS D(OrderYear, CustomerID)
GROUP BY OrderYear;
GO

---------------------------------------------------------------------
-- Using Arguments
---------------------------------------------------------------------

-- Yearly Count of Customers handled by Employee 3
DECLARE @EmpID AS INT;
SET @EmpID = 3;

SELECT OrderYear, COUNT(DISTINCT CustomerID) AS NumCusts
FROM (SELECT YEAR(OrderDate) AS OrderYear, CustomerID
      FROM dbo.Orders
      WHERE EmployeeID = @EmpID) AS D
GROUP BY OrderYear;
GO

---------------------------------------------------------------------
-- Nesting
---------------------------------------------------------------------

-- Order Years and Number of Customers for Years with more than
-- 70 Active Customers
SELECT OrderYear, NumCusts
FROM (SELECT OrderYear, COUNT(DISTINCT CustomerID) AS NumCusts
      FROM (SELECT YEAR(OrderDate) AS OrderYear, CustomerID
            FROM dbo.Orders) AS D1
      GROUP BY OrderYear) AS D2
WHERE NumCusts > 70;

---------------------------------------------------------------------
-- Multiple References
---------------------------------------------------------------------

-- Comparing Current to Previous Year’s Number of Customers
SELECT Cur.OrderYear, 
  Cur.NumCusts AS CurNumCusts, Prv.NumCusts AS PrvNumCusts,
  Cur.NumCusts - Prv.NumCusts AS Growth
FROM (SELECT YEAR(OrderDate) AS OrderYear,
        COUNT(DISTINCT CustomerID) AS NumCusts
      FROM dbo.Orders
      GROUP BY YEAR(OrderDate)) AS Cur
  LEFT OUTER JOIN
     (SELECT YEAR(OrderDate) AS OrderYear,
        COUNT(DISTINCT CustomerID) AS NumCusts
      FROM dbo.Orders
      GROUP BY YEAR(OrderDate)) AS Prv
    ON Cur.OrderYear = Prv.OrderYear + 1;
GO

---------------------------------------------------------------------
-- Common Table Expressions (CTE)
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Reusing Aliases
---------------------------------------------------------------------

-- Inline column aliasing
WITH C AS
(
  SELECT YEAR(OrderDate) AS OrderYear, CustomerID
  FROM dbo.Orders
)
SELECT OrderYear, COUNT(DISTINCT CustomerID) AS NumCusts
FROM C
GROUP BY OrderYear;

-- External column aliasing
WITH C(OrderYear, CustomerID) AS
(
  SELECT YEAR(OrderDate) AS OrderYear, CustomerID
  FROM dbo.Orders
)
SELECT OrderYear, COUNT(DISTINCT CustomerID) AS NumCusts
FROM C
GROUP BY OrderYear;
GO

---------------------------------------------------------------------
-- Using Arguments
---------------------------------------------------------------------

-- Using arguments
DECLARE @EmpID AS INT;
SET @EmpID = 3;

WITH C AS
(
  SELECT YEAR(OrderDate) AS OrderYear, CustomerID
  FROM dbo.Orders
  WHERE EmployeeID = @EmpID
)
SELECT OrderYear, COUNT(DISTINCT CustomerID) AS NumCusts
FROM C
GROUP BY OrderYear;
GO

---------------------------------------------------------------------
-- Multiple CTEs
---------------------------------------------------------------------

-- Defining multiple CTEs
WITH C1 AS
(
  SELECT YEAR(OrderDate) AS OrderYear, CustomerID
  FROM dbo.Orders
),
C2 AS
(
  SELECT OrderYear, COUNT(DISTINCT CustomerID) AS NumCusts
  FROM C1
  GROUP BY OrderYear
)
SELECT OrderYear, NumCusts
FROM C2
WHERE NumCusts > 70;

---------------------------------------------------------------------
-- Multiple References
---------------------------------------------------------------------

-- Multiple references
WITH YearlyCount AS
(
  SELECT YEAR(OrderDate) AS OrderYear,
    COUNT(DISTINCT CustomerID) AS NumCusts
  FROM dbo.Orders
  GROUP BY YEAR(OrderDate)
)
SELECT Cur.OrderYear, 
  Cur.NumCusts AS CurNumCusts, Prv.NumCusts AS PrvNumCusts,
  Cur.NumCusts - Prv.NumCusts AS Growth
FROM YearlyCount AS Cur
  LEFT OUTER JOIN YearlyCount AS Prv
    ON Cur.OrderYear = Prv.OrderYear + 1;
GO

---------------------------------------------------------------------
-- Modifying Data
---------------------------------------------------------------------

-- Listing 4-2: Creating and Populating the CustomersDups Table
IF OBJECT_ID('dbo.CustomersDups') IS NOT NULL
  DROP TABLE dbo.CustomersDups;
GO

WITH CrossCustomers AS
(
  SELECT 1 AS c, C1.*
  FROM dbo.Customers AS C1, dbo.Customers AS C2
)
SELECT ROW_NUMBER() OVER(ORDER BY c) AS KeyCol,
  CustomerID, CompanyName, ContactName, ContactTitle, Address,
  City, Region, PostalCode, Country, Phone, Fax
INTO dbo.CustomersDups
FROM CrossCustomers;

CREATE UNIQUE INDEX idx_CustomerID_KeyCol
  ON dbo.CustomersDups(CustomerID, KeyCol);
GO

-- Modifying data through CTEs
WITH JustDups AS
(
  SELECT * FROM dbo.CustomersDups AS C1
  WHERE KeyCol < 
    (SELECT MAX(KeyCol) FROM dbo.CustomersDups AS C2
     WHERE C2.CustomerID = C1.CustomerID)
)
DELETE FROM JustDups;
GO

---------------------------------------------------------------------
-- Container Objects
---------------------------------------------------------------------

-- View with CTE
IF OBJECT_ID('dbo.VYearCnt') IS NOT NULL
  DROP VIEW dbo.VYearCnt;
GO
CREATE VIEW dbo.VYearCnt
AS
WITH YearCnt AS
(
  SELECT YEAR(OrderDate) AS OrderYear,
    COUNT(DISTINCT CustomerID) AS NumCusts
  FROM dbo.Orders
  GROUP BY YEAR(OrderDate)
)
SELECT * FROM YearCnt;
GO

-- Querying view with CTE
SELECT * FROM dbo.VYearCnt;
GO

-- UDF with CTE
IF OBJECT_ID('dbo.fn_EmpYearCnt') IS NOT NULL
  DROP FUNCTION dbo.fn_EmpYearCnt;
GO
CREATE FUNCTION dbo.fn_EmpYearCnt(@EmpID AS INT) RETURNS TABLE
AS
RETURN
  WITH EmpYearCnt AS
  (
    SELECT YEAR(OrderDate) AS OrderYear,
      COUNT(DISTINCT CustomerID) AS NumCusts
    FROM dbo.Orders
    WHERE EmployeeID = @EmpID
    GROUP BY YEAR(OrderDate)
  )
  SELECT * FROM EmpYearCnt;
GO

-- Querying UDF with CTE
SELECT * FROM dbo.fn_EmpYearCnt(3);
GO

---------------------------------------------------------------------
-- Recursive CTEs
---------------------------------------------------------------------

-- Create index for recursive CTE
CREATE UNIQUE INDEX idx_mgr_emp_ifname_ilname
  ON dbo.Employees(ReportsTo, EmployeeID)
  INCLUDE(FirstName, LastName);

-- Recursive CTE returning subordinates of employee 5 in all levels
WITH EmpsCTE AS
(
  SELECT EmployeeID, ReportsTo, FirstName, LastName
  FROM dbo.Employees
  WHERE EmployeeID = 2

  UNION ALL

  SELECT EMP.EmployeeID, EMP.ReportsTo, EMP.FirstName, EMP.LastName
  FROM EmpsCTE AS MGR
    JOIN dbo.Employees AS EMP
      ON EMP.ReportsTo = MGR.EmployeeID
)
SELECT * FROM EmpsCTE;

-- Cleanup
DROP INDEX dbo.Employees.idx_mgr_emp_ifname_ilname;

---------------------------------------------------------------------
-- Analytical Ranking Functions
---------------------------------------------------------------------

-- Listing 4-3: Creating and Populating the Sales Table
SET NOCOUNT ON;
USE tempdb;
GO
IF OBJECT_ID('dbo.Sales') IS NOT NULL
  DROP TABLE dbo.Sales;
GO

CREATE TABLE dbo.Sales
(
  empid VARCHAR(10) NOT NULL PRIMARY KEY,
  mgrid VARCHAR(10) NOT NULL,
  qty   INT         NOT NULL
);

INSERT INTO dbo.Sales(empid, mgrid, qty) VALUES('A', 'Z', 300);
INSERT INTO dbo.Sales(empid, mgrid, qty) VALUES('B', 'X', 100);
INSERT INTO dbo.Sales(empid, mgrid, qty) VALUES('C', 'X', 200);
INSERT INTO dbo.Sales(empid, mgrid, qty) VALUES('D', 'Y', 200);
INSERT INTO dbo.Sales(empid, mgrid, qty) VALUES('E', 'Z', 250);
INSERT INTO dbo.Sales(empid, mgrid, qty) VALUES('F', 'Z', 300);
INSERT INTO dbo.Sales(empid, mgrid, qty) VALUES('G', 'X', 100);
INSERT INTO dbo.Sales(empid, mgrid, qty) VALUES('H', 'Y', 150);
INSERT INTO dbo.Sales(empid, mgrid, qty) VALUES('I', 'X', 250);
INSERT INTO dbo.Sales(empid, mgrid, qty) VALUES('J', 'Z', 100);
INSERT INTO dbo.Sales(empid, mgrid, qty) VALUES('K', 'Y', 200);

CREATE INDEX idx_qty_empid ON dbo.Sales(qty, empid);
CREATE INDEX idx_mgrid_qty_empid ON dbo.Sales(mgrid, qty, empid);
GO

-- Querying the Sales table
SELECT * FROM dbo.Sales;

---------------------------------------------------------------------
-- Row Number
---------------------------------------------------------------------

---------------------------------------------------------------------
-- ROW_NUMBER Function, SQL Server 2005
---------------------------------------------------------------------

-- Row number
SELECT empid, qty,
  ROW_NUMBER() OVER(ORDER BY qty) AS rownum
FROM dbo.Sales
ORDER BY qty;

---------------------------------------------------------------------
-- Determinism
---------------------------------------------------------------------

-- Row number, determinism
SELECT empid, qty,
  ROW_NUMBER() OVER(ORDER BY qty)        AS nd_rownum,
  ROW_NUMBER() OVER(ORDER BY qty, empid) AS d_rownum
FROM dbo.Sales
ORDER BY qty, empid;

---------------------------------------------------------------------
-- Partitioning
---------------------------------------------------------------------

-- Row number, partitioned
SELECT mgrid, empid, qty,
  ROW_NUMBER() OVER(PARTITION BY mgrid ORDER BY qty, empid) AS rownum
FROM dbo.Sales
ORDER BY mgrid, qty, empid;

---------------------------------------------------------------------
-- Set-Based, Pre-SQL Server 2005
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Unique Sort Column
---------------------------------------------------------------------

-- Row number, unique sort column
SELECT empid,
  (SELECT COUNT(*)
   FROM dbo.Sales AS S2
   WHERE S2.empid <= S1.empid) AS rownum
FROM dbo.Sales AS S1
ORDER BY empid;

---------------------------------------------------------------------
-- Non-Unique Sort Column and Tiebreaker
---------------------------------------------------------------------

-- Row number, non-unique sort column and tiebreaker
SELECT empid, qty,
  (SELECT COUNT(*)
   FROM dbo.Sales AS S2
   WHERE S2.qty < S1.qty
      OR (S2.qty = S1.qty AND S2.empid <= S1.empid)) AS rownum
FROM dbo.Sales AS S1
ORDER BY qty, empid;
GO

---------------------------------------------------------------------
-- Non-Unique Sort Column without a Tiebreaker
---------------------------------------------------------------------

-- Listing 4-4: Creating and Populating the T1 Table
IF OBJECT_ID('dbo.T1') IS NOT NULL
  DROP TABLE dbo.T1;
GO
CREATE TABLE dbo.T1(col1 VARCHAR(5));
INSERT INTO dbo.T1(col1) VALUES('A');
INSERT INTO dbo.T1(col1) VALUES('A');
INSERT INTO dbo.T1(col1) VALUES('A');
INSERT INTO dbo.T1(col1) VALUES('B');
INSERT INTO dbo.T1(col1) VALUES('B');
INSERT INTO dbo.T1(col1) VALUES('C');
INSERT INTO dbo.T1(col1) VALUES('C');
INSERT INTO dbo.T1(col1) VALUES('C');
INSERT INTO dbo.T1(col1) VALUES('C');
INSERT INTO dbo.T1(col1) VALUES('C');
GO

-- Row number, non-unique sort column, no tiebreaker, step 1
SELECT col1, COUNT(*) AS dups,
  (SELECT COUNT(*) FROM dbo.T1 AS B
   WHERE B.col1 < A.col1) AS smaller
FROM dbo.T1 AS A
GROUP BY col1;

-- Row number, non-unique sort column, no tiebreaker, step 2
SELECT col1, dups, smaller, n
FROM (SELECT col1, COUNT(*) AS dups,
        (SELECT COUNT(*) FROM dbo.T1 AS B
         WHERE B.col1 < A.col1) AS smaller
      FROM dbo.T1 AS A
      GROUP BY col1) AS D, Nums
WHERE n <= dups;

-- Row number, non-unique sort column, no tiebreaker, final
SELECT n + smaller AS rownum, col1
FROM (SELECT col1, COUNT(*) AS dups,
        (SELECT COUNT(*) FROM dbo.T1 AS B
         WHERE B.col1 < A.col1) AS smaller
      FROM dbo.T1 AS A
      GROUP BY col1) AS D, Nums
WHERE n <= dups;

---------------------------------------------------------------------
-- Partitioning 
---------------------------------------------------------------------

-- Row number, partitioned
SELECT mgrid, empid, qty,
  (SELECT COUNT(*)
   FROM dbo.Sales AS S2
   WHERE S2.mgrid = S1.mgrid
     AND (S2.qty < S1.qty
          OR (S2.qty = S1.qty AND S2.empid <= S1.empid))) AS rownum
FROM dbo.Sales AS S1
ORDER BY mgrid, qty, empid;

---------------------------------------------------------------------
-- Cursor-Based
---------------------------------------------------------------------

-- Listing 4-5: Calculating Row Numbers with a Cursor
DECLARE @SalesRN TABLE(empid VARCHAR(5), qty INT, rn INT);
DECLARE @empid AS VARCHAR(5), @qty AS INT, @rn AS INT;

DECLARE rncursor CURSOR FAST_FORWARD FOR
  SELECT empid, qty FROM dbo.Sales ORDER BY qty, empid;
OPEN rncursor;

SET @rn = 0;

FETCH NEXT FROM rncursor INTO @empid, @qty;
WHILE @@fetch_status = 0
BEGIN
  SET @rn = @rn + 1;
  INSERT INTO @SalesRN(empid, qty, rn) VALUES(@empid, @qty, @rn);
  FETCH NEXT FROM rncursor INTO @empid, @qty;
END

CLOSE rncursor;
DEALLOCATE rncursor;

SELECT empid, qty, rn FROM @SalesRN;
GO

---------------------------------------------------------------------
-- IDENTITY-Based
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Non-Partitioned
---------------------------------------------------------------------

-- Calculating row number with IDENTITY, non-guaranteed order
SELECT empid, qty, IDENTITY(int, 1, 1) AS rn
INTO #SalesRN FROM dbo.Sales
ORDER BY qty, empid;

SELECT * FROM #SalesRN;

DROP TABLE #SalesRN;
GO

-- Calculating row number with IDENTITY, guaranteed order
CREATE TABLE #SalesRN(empid VARCHAR(5), qty INT, rn INT IDENTITY);

INSERT INTO #SalesRN(empid, qty)
  SELECT empid, qty FROM dbo.Sales ORDER BY qty, empid;

SELECT * FROM #SalesRN;

DROP TABLE #SalesRN;
GO

---------------------------------------------------------------------
-- Partitioned
---------------------------------------------------------------------

-- Listing 4-6: Calculating Partitioned Row Numbers with a IDENTITY
CREATE TABLE #SalesRN
  (mgrid VARCHAR(5), empid VARCHAR(5), qty INT, rn INT IDENTITY);
CREATE UNIQUE CLUSTERED INDEX idx_mgrid_rn ON #SalesRN(mgrid, rn);

INSERT INTO #SalesRN(mgrid, empid, qty)
  SELECT mgrid, empid, qty FROM dbo.Sales ORDER BY mgrid, qty, empid;

-- Option 1 – using a subquery
SELECT mgrid, empid, qty,
  rn - (SELECT MIN(rn) FROM #SalesRN AS S2
        WHERE S2.mgrid = S1.mgrid) + 1 AS rn
FROM #SalesRN AS S1;

-- Option 2 – using a join
SELECT S.mgrid, empid, qty, rn - minrn + 1 AS rn
FROM #SalesRN AS S
  JOIN (SELECT mgrid, MIN(rn) AS minrn
        FROM #SalesRN
        GROUP BY mgrid) AS M
    ON S.mgrid = M.mgrid;

DROP TABLE #SalesRN;
GO

---------------------------------------------------------------------
-- Performance Comparisons
---------------------------------------------------------------------

-- Listing 4-7: Benchmark Comparing Techniques to Calculate Row Numbers

-- Change Tool's Options to Discard Query Results
SET NOCOUNT ON;
USE tempdb;
GO
IF OBJECT_ID('dbo.RNBenchmark') IS NOT NULL
  DROP TABLE dbo.RNBenchmark;
GO
IF OBJECT_ID('dbo.RNTechniques') IS NOT NULL
  DROP TABLE dbo.RNTechniques;
GO
IF OBJECT_ID('dbo.SalesBM') IS NOT NULL
  DROP TABLE dbo.SalesBM;
GO
IF OBJECT_ID('dbo.SalesBMIdentity') IS NOT NULL
  DROP TABLE dbo.SalesBMIdentity;
GO
IF OBJECT_ID('dbo.SalesBMCursor') IS NOT NULL
  DROP TABLE dbo.SalesBMCursor;
GO

CREATE TABLE dbo.RNTechniques
(
  tid INT NOT NULL PRIMARY KEY,
  technique VARCHAR(25) NOT NULL
);
INSERT INTO RNTechniques(tid, technique) VALUES(1, 'Set-Based 2000');
INSERT INTO RNTechniques(tid, technique) VALUES(2, 'IDENTITY');
INSERT INTO RNTechniques(tid, technique) VALUES(3, 'Cursor');
INSERT INTO RNTechniques(tid, technique) VALUES(4, 'ROW_NUMBER 2005');
GO

CREATE TABLE dbo.RNBenchmark
(
  tid       INT    NOT NULL REFERENCES dbo.RNTechniques(tid),
  numrows   INT    NOT NULL,
  runtimems BIGINT NOT NULL,
  PRIMARY KEY(tid, numrows)
);
GO

CREATE TABLE dbo.SalesBM
(
  empid INT NOT NULL IDENTITY PRIMARY KEY,
  qty   INT NOT NULL
);
CREATE INDEX idx_qty_empid ON dbo.SalesBM(qty, empid);
GO
CREATE TABLE dbo.SalesBMIdentity(empid INT, qty INT, rn INT IDENTITY);
GO
CREATE TABLE dbo.SalesBMCursor(empid INT, qty INT, rn INT);
GO

DECLARE
  @maxnumrows    AS INT,
  @steprows      AS INT,
  @curnumrows    AS INT,
  @dt            AS DATETIME;

SET @maxnumrows    = 100000;
SET @steprows      = 10000;
SET @curnumrows    = 10000;

WHILE @curnumrows <= @maxnumrows
BEGIN

  TRUNCATE TABLE dbo.SalesBM;
  INSERT INTO dbo.SalesBM(qty)
    SELECT CAST(1+999.9999999999*RAND(CHECKSUM(NEWID())) AS INT)
    FROM dbo.Nums
    WHERE n <= @curnumrows;

  -- 'Set-Based 2000'
  
  DBCC FREEPROCCACHE WITH NO_INFOMSGS;
  DBCC DROPCLEANBUFFERS WITH NO_INFOMSGS;

  SET @dt = GETDATE();

  SELECT empid, qty,
    (SELECT COUNT(*)
     FROM dbo.SalesBM AS S2
     WHERE S2.qty < S1.qty
         OR (S2.qty = S1.qty AND S2.empid <= S1.empid)) AS rn
  FROM dbo.SalesBM AS S1
  ORDER BY qty, empid;

  INSERT INTO dbo.RNBenchmark(tid, numrows, runtimems)
    VALUES(1, @curnumrows, DATEDIFF(ms, @dt, GETDATE()));

  -- 'IDENTITY'
  
  TRUNCATE TABLE dbo.SalesBMIdentity;

  DBCC FREEPROCCACHE WITH NO_INFOMSGS;
  DBCC DROPCLEANBUFFERS WITH NO_INFOMSGS;

  SET @dt = GETDATE();

  INSERT INTO dbo.SalesBMIdentity(empid, qty)
    SELECT empid, qty FROM dbo.SalesBM ORDER BY qty, empid;

  SELECT empid, qty, rn FROM dbo.SalesBMIdentity;

  INSERT INTO dbo.RNBenchmark(tid, numrows, runtimems)
    VALUES(2, @curnumrows, DATEDIFF(ms, @dt, GETDATE()));

  -- 'Cursor'

  TRUNCATE TABLE dbo.SalesBMCursor;

  DBCC FREEPROCCACHE WITH NO_INFOMSGS;
  DBCC DROPCLEANBUFFERS WITH NO_INFOMSGS;

  SET @dt = GETDATE();

  DECLARE @empid AS INT, @qty AS INT, @rn AS INT;

  BEGIN TRAN

  DECLARE rncursor CURSOR FAST_FORWARD FOR
    SELECT empid, qty FROM dbo.SalesBM ORDER BY qty, empid;
  OPEN rncursor;

  SET @rn = 0;

  FETCH NEXT FROM rncursor INTO @empid, @qty;
  WHILE @@fetch_status = 0
  BEGIN
    SET @rn = @rn + 1;
    INSERT INTO dbo.SalesBMCursor(empid, qty, rn)
      VALUES(@empid, @qty, @rn);
    FETCH NEXT FROM rncursor INTO @empid, @qty;
  END

  CLOSE rncursor;
  DEALLOCATE rncursor;

  COMMIT TRAN

  SELECT empid, qty, rn FROM dbo.SalesBMCursor;

  INSERT INTO dbo.RNBenchmark(tid, numrows, runtimems)
    VALUES(3, @curnumrows, DATEDIFF(ms, @dt, GETDATE()));

  -- 'ROW_NUMBER 2005'

  DBCC FREEPROCCACHE WITH NO_INFOMSGS;
  DBCC DROPCLEANBUFFERS WITH NO_INFOMSGS;

  SET @dt = GETDATE();

  SELECT empid, qty, ROW_NUMBER() OVER(ORDER BY qty, empid) AS rn
  FROM dbo.SalesBM;

  INSERT INTO dbo.RNBenchmark(tid, numrows, runtimems)
    VALUES(4, @curnumrows, DATEDIFF(ms, @dt, GETDATE()));

  SET @curnumrows = @curnumrows + @steprows;

END
GO

-- Query Benchmark Results
SELECT numrows,
  [Set-Based 2000], [IDENTITY], [Cursor], [ROW_NUMBER 2005]
FROM (SELECT technique, numrows, runtimems
      FROM dbo.RNBenchmark AS B
        JOIN dbo.RNTechniques AS T
          ON B.tid = T.tid) AS D
PIVOT(MAX(runtimems) FOR technique IN(
  [Set-Based 2000], [IDENTITY], [Cursor], [ROW_NUMBER 2005])) AS P
ORDER BY numrows;
GO

---------------------------------------------------------------------
-- Paging
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Ad-hoc
---------------------------------------------------------------------

-- Second Page of Sales based on qty, empid Order
-- with a page size of 5 rows
DECLARE @pagesize AS INT, @pagenum AS INT;
SET @pagesize = 5;
SET @pagenum = 2;

WITH SalesCTE AS
(
  SELECT ROW_NUMBER() OVER(ORDER BY qty, empid) AS rownum,
    empid, mgrid, qty
  FROM dbo.Sales
)
SELECT rownum, empid, mgrid, qty
FROM SalesCTE
WHERE rownum > @pagesize * (@pagenum-1)
  AND rownum <= @pagesize * @pagenum
ORDER BY rownum;
GO

---------------------------------------------------------------------
-- Multi Page Access
---------------------------------------------------------------------

-- Creating a table with row numbers
IF OBJECT_ID('tempdb..#SalesRN') IS NOT NULL
  DROP TABLE #SalesRN;
GO
SELECT ROW_NUMBER() OVER(ORDER BY qty, empid) AS rownum,
  empid, mgrid, qty
INTO #SalesRN
FROM dbo.Sales;

CREATE UNIQUE CLUSTERED INDEX idx_rn ON #SalesRN(rownum);
GO

-- Run for each page request
DECLARE @pagesize AS INT, @pagenum AS INT;
SET @pagesize = 5;
SET @pagenum = 2;

SELECT rownum, empid, mgrid, qty
FROM #SalesRN
WHERE rownum BETWEEN @pagesize * (@pagenum-1) + 1
                 AND @pagesize * @pagenum
ORDER BY rownum;
GO

-- Cleanup
DROP TABLE #SalesRN;
GO

---------------------------------------------------------------------
-- Rank and Dense Rank
---------------------------------------------------------------------

---------------------------------------------------------------------
-- RANK and DENSE_RANK Functions, SQL Server 2005
---------------------------------------------------------------------

-- Rank and dense rank
SELECT empid, qty,
  RANK() OVER(ORDER BY qty) AS rnk,
  DENSE_RANK() OVER(ORDER BY qty) AS drnk
FROM dbo.Sales
ORDER BY qty;

---------------------------------------------------------------------
-- Set-Based, Pre-2005
---------------------------------------------------------------------

-- Rank and dense rank
SELECT empid, qty,
  (SELECT COUNT(*) FROM dbo.Sales AS S2
   WHERE S2.qty < S1.qty) + 1 AS rnk,
  (SELECT COUNT(DISTINCT qty) FROM dbo.Sales AS S2
   WHERE S2.qty < S1.qty) + 1 AS drnk
FROM dbo.Sales AS S1
ORDER BY qty;

---------------------------------------------------------------------
-- NTILE
---------------------------------------------------------------------

---------------------------------------------------------------------
-- NTILE Function, SQL Server 2005
---------------------------------------------------------------------

-- NTILE
SELECT empid, qty,
  NTILE(3) OVER(ORDER BY qty, empid) AS tile
FROM dbo.Sales
ORDER BY qty, empid;

-- Descriptive Tiles
SELECT empid, qty,
  CASE NTILE(3) OVER(ORDER BY qty, empid)
    WHEN 1 THEN 'low'
    WHEN 2 THEN 'meduim'
    WHEN 3 THEN 'high'
  END AS lvl
FROM dbo.Sales
ORDER BY qty, empid;

-- Ranges of Quantities Corresponding to each Category
WITH Tiles AS
(
  SELECT empid, qty,
    NTILE(3) OVER(ORDER BY qty, empid) AS tile
  FROM dbo.Sales
)
SELECT tile, MIN(qty) AS lb, MAX(qty) AS hb
FROM Tiles
GROUP BY tile
ORDER BY tile;

---------------------------------------------------------------------
-- Other Solutions to NTILE
---------------------------------------------------------------------

-- NTILE, even Distribution of Remainder
DECLARE @numtiles AS INT;
SET @numtiles = 3;

SELECT empid, qty, 
  CAST((rn - 1) / tilesize + 1 AS INT) AS tile
FROM (SELECT empid, qty, rn,
        1.*numrows/@numtiles AS tilesize
      FROM (SELECT empid, qty,
              (SELECT COUNT(*) FROM dbo.Sales AS S2
               WHERE S2.qty < S1.qty
                  OR S2.qty = S1.qty
                     AND S2.empid <= S1.empid) AS rn,
              (SELECT COUNT(*) FROM dbo.Sales) AS numrows
            FROM dbo.Sales AS S1) AS D1) AS D2
ORDER BY qty, empid;
GO

-- NTILE, pre-2005, remainder added to first groups
DECLARE @numtiles AS INT;
SET @numtiles = 9;

SELECT empid, qty, 
  CASE 
    WHEN rn <= (tilesize+1) * remainder
      THEN (rn-1) / (tilesize+1) + 1
    ELSE (rn - remainder - 1) / tilesize + 1
  END AS tile
FROM (SELECT empid, qty, rn,
        numrows/@numtiles AS tilesize,
        numrows%@numtiles AS remainder
      FROM (SELECT empid, qty,
              (SELECT COUNT(*) FROM dbo.Sales AS S2
                WHERE S2.qty < S1.qty
                  OR S2.qty = S1.qty
                      AND S2.empid <= S1.empid) AS rn,
              (SELECT COUNT(*) FROM dbo.Sales) AS numrows
            FROM dbo.Sales AS S1) AS D1) AS D2
ORDER BY qty, empid;
GO

---------------------------------------------------------------------
-- Auxiliry Table of Numbers
---------------------------------------------------------------------

-- Listing 4-8: Creating and Populating Auxiliary Table of Numbers
SET NOCOUNT ON;
USE AdventureWorks;
GO
IF OBJECT_ID('dbo.Nums') IS NOT NULL
  DROP TABLE dbo.Nums;
GO
CREATE TABLE dbo.Nums(n INT NOT NULL PRIMARY KEY);
DECLARE @max AS INT, @rc AS INT;
SET @max = 1000000;
SET @rc = 1;

INSERT INTO Nums VALUES(1);
WHILE @rc * 2 <= @max
BEGIN
  INSERT INTO dbo.Nums SELECT n + @rc FROM dbo.Nums;
  SET @rc = @rc * 2;
END

INSERT INTO dbo.Nums 
  SELECT n + @rc FROM dbo.Nums WHERE n + @rc <= @max;
GO

-- Naive Solution Returning an Auxiliary Table of Numbers
DECLARE @n AS BIGINT;
SET @n = 1000000;

WITH Nums AS
(
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM Nums WHERE n < @n
)
SELECT n FROM Nums
OPTION(MAXRECURSION 0);
GO

-- Optimized Solution 1
DECLARE @n AS BIGINT;
SET @n = 1000000;

WITH Base AS
(
  SELECT 1 AS n
  UNION ALL
  SELECT n + 1 FROM Base WHERE n < CEILING(SQRT(@n))
),
Expand AS
(
  SELECT 1 AS c
  FROM Base AS B1, Base AS B2
),
Nums AS
(
  SELECT ROW_NUMBER() OVER(ORDER BY c) AS n
  FROM Expand
)
SELECT n FROM Nums WHERE n <= @n
OPTION(MAXRECURSION 0);
GO

-- Optimized Solution 2
DECLARE @n AS BIGINT;
SET @n = 1000000;

WITH
L0   AS(SELECT 1 AS c UNION ALL SELECT 1),
L1   AS(SELECT 1 AS c FROM L0 AS A, L0 AS B),
L2   AS(SELECT 1 AS c FROM L1 AS A, L1 AS B),
L3   AS(SELECT 1 AS c FROM L2 AS A, L2 AS B),
L4   AS(SELECT 1 AS c FROM L3 AS A, L3 AS B),
L5   AS(SELECT 1 AS c FROM L4 AS A, L4 AS B),
Nums AS(SELECT ROW_NUMBER() OVER(ORDER BY c) AS n FROM L5)
SELECT n FROM Nums WHERE n <= @n;
GO

-- Listing 4-9: UDF Returning an Auxiliary Table of Numbers
IF OBJECT_ID('dbo.fn_nums') IS NOT NULL
  DROP FUNCTION dbo.Nums;
GO
CREATE FUNCTION dbo.fn_nums(@n AS BIGINT) RETURNS TABLE
AS
RETURN
  WITH
  L0   AS(SELECT 1 AS c UNION ALL SELECT 1),
  L1   AS(SELECT 1 AS c FROM L0 AS A, L0 AS B),
  L2   AS(SELECT 1 AS c FROM L1 AS A, L1 AS B),
  L3   AS(SELECT 1 AS c FROM L2 AS A, L2 AS B),
  L4   AS(SELECT 1 AS c FROM L3 AS A, L3 AS B),
  L5   AS(SELECT 1 AS c FROM L4 AS A, L4 AS B),
  Nums AS(SELECT ROW_NUMBER() OVER(ORDER BY c) AS n FROM L5)
  SELECT n FROM Nums WHERE n <= @n;
GO

-- Test function
SELECT * FROM dbo.fn_nums(10) AS F;
GO

---------------------------------------------------------------------
-- Missing and Existing Ranges
---------------------------------------------------------------------

-- Listing 4-10: Creating and Populating the T1 Table

USE tempdb;
GO
IF OBJECT_ID('dbo.T1') IS NOT NULL
  DROP TABLE dbo.T1
GO
CREATE TABLE dbo.T1(col1 INT NOT NULL PRIMARY KEY);
INSERT INTO dbo.T1(col1) VALUES(1);
INSERT INTO dbo.T1(col1) VALUES(2);
INSERT INTO dbo.T1(col1) VALUES(3);
INSERT INTO dbo.T1(col1) VALUES(100);
INSERT INTO dbo.T1(col1) VALUES(101);
INSERT INTO dbo.T1(col1) VALUES(103);
INSERT INTO dbo.T1(col1) VALUES(104);
INSERT INTO dbo.T1(col1) VALUES(105);
INSERT INTO dbo.T1(col1) VALUES(106);

---------------------------------------------------------------------
-- Missing Ranges
---------------------------------------------------------------------

-- Solution 1

-- Points Before Gaps
SELECT col1
FROM dbo.T1 AS A
WHERE NOT EXISTS
  (SELECT * FROM dbo.T1 AS B
   WHERE B.col1 = A.col1 + 1);

-- Starting Points of Gaps
SELECT col1 + 1 AS start_range
FROM dbo.T1 AS A
WHERE NOT EXISTS
  (SELECT * FROM dbo.T1 AS B
   WHERE B.col1 = A.col1 + 1)
  AND col1 < (SELECT MAX(col1) FROM dbo.T1);

-- Match Next Existing Value - 1 to each Starting Point
SELECT col1 + 1 AS start_range,
  (SELECT MIN(col1) FROM dbo.T1 AS B
   WHERE B.col1 > A.col1) - 1 AS end_range
FROM dbo.T1 AS A
WHERE NOT EXISTS
  (SELECT * FROM dbo.T1 AS B
   WHERE B.col1 = A.col1 + 1)
  AND col1 < (SELECT MAX(col1) FROM dbo.T1);

-- Solution 2

-- Match Next to Current
SELECT col1 AS cur,
  (SELECT MIN(col1) FROM dbo.T1 AS B
   WHERE B.col1 > A.col1) AS nxt
FROM dbo.T1 AS A;

-- Isolate Pairs that are more than 1 Apart
SELECT cur + 1 AS start_range, nxt - 1 AS end_range
FROM (SELECT col1 AS cur,
        (SELECT MIN(col1) FROM dbo.T1 AS B
        WHERE B.col1 > A.col1) AS nxt
      FROM dbo.T1 AS A) AS D
WHERE nxt - cur > 1;
GO

-- Missing Values

-- Return Missing Values
SELECT n FROM dbo.Nums
WHERE n BETWEEN (SELECT MIN(col1) FROM dbo.T1)
            AND (SELECT MAX(col1) FROM dbo.T1)
  AND NOT EXISTS(SELECT * FROM dbo.T1 WHERE col1 = n);

---------------------------------------------------------------------
-- Existing Ranges
---------------------------------------------------------------------

-- Solution 1

-- Calculating Grouping Factor
SELECT col1,
  (SELECT MIN(col1) FROM dbo.T1 AS B
   WHERE B.col1 >= A.col1
     AND NOT EXISTS
       (SELECT * FROM dbo.T1 AS C
        WHERE B.col1 = C.col1 - 1)) AS grp
FROM dbo.T1 AS A;

-- Returning Existing Ranges
SELECT MIN(col1) AS start_range, MAX(col1) AS end_range
FROM (SELECT col1,
        (SELECT MIN(col1) FROM dbo.T1 AS B
        WHERE B.col1 >= A.col1
          AND NOT EXISTS
            (SELECT * FROM dbo.T1 AS C
              WHERE B.col1 = C.col1 - 1)) AS grp
      FROM dbo.T1 AS A) AS D
GROUP BY grp;

-- Solution 2

-- Calculating Row Numbers
SELECT col1, ROW_NUMBER() OVER(ORDER BY col1) AS rn
FROM dbo.T1;

-- Calculating Diff between col1 and Row Number
SELECT col1, col1 - ROW_NUMBER() OVER(ORDER BY col1) AS diff
FROM dbo.T1;

-- Returning Existing Ranges
SELECT MIN(col1) AS start_range, MAX(col1) AS end_range
FROM (SELECT col1, col1 - ROW_NUMBER() OVER(ORDER BY col1) AS grp
      FROM dbo.T1) AS D
GROUP BY grp;
