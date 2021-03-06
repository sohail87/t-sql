---------------------------------------------------------------------
-- Chapter 06 - Aggregating and Pivoting Data
---------------------------------------------------------------------

---------------------------------------------------------------------
-- OVER Clause
---------------------------------------------------------------------
SET NOCOUNT ON;
USE pubs;
GO

-- Obtaining Aggregates with Cross Join
SELECT stor_id, ord_num, title_id, 
  CONVERT(VARCHAR(10), ord_date, 120) AS ord_date, qty,
  CAST(1.*qty / sumqty * 100 AS DECIMAL(5, 2)) AS per,
  CAST(qty - avgqty AS DECIMAL(9, 2)) as diff
FROM dbo.sales,
  (SELECT SUM(qty) AS sumqty, AVG(1.*qty) AS avgqty
   FROM dbo.sales) AS AGG;

-- Obtaining Aggregates with OVER Clause, without Partitioning
SELECT stor_id, ord_num, title_id, 
  CONVERT(VARCHAR(10), ord_date, 120) AS ord_date, qty,
  CAST(1.*qty / SUM(qty) OVER() * 100 AS DECIMAL(5, 2)) AS per,
  CAST(qty - AVG(1.*qty) OVER() AS DECIMAL(9, 2)) AS diff
FROM dbo.sales;

-- Comparing Single and Multiple Aggregates using OVER Clause
SELECT stor_id, ord_num, title_id,
  SUM(qty) OVER() AS sumqty
FROM dbo.sales;

SELECT stor_id, ord_num, title_id, 
  SUM(qty)   OVER() AS sumqty,
  COUNT(qty) OVER() AS cntqty,
  AVG(qty)   OVER() AS avgqty,
  MIN(qty)   OVER() AS minqty,
  MAX(qty)   OVER() AS maxqty
FROM dbo.sales;

-- Comparing Single and Multiple Aggregates using Subqueries
SELECT stor_id, ord_num, title_id,
  (SELECT SUM(qty) FROM dbo.sales) AS sumqty
FROM dbo.sales;

SELECT stor_id, ord_num, title_id, 
  (SELECT SUM(qty)   FROM dbo.sales) AS sumqty,
  (SELECT COUNT(qty) FROM dbo.sales) AS cntqty,
  (SELECT AVG(qty)   FROM dbo.sales) AS avgqty,
  (SELECT MIN(qty)   FROM dbo.sales) AS minqty,
  (SELECT MAX(qty)   FROM dbo.sales) AS maxqty
FROM dbo.sales;

-- Obtaining Aggregates with OVER Clause, with Partitioning
SELECT stor_id, ord_num, title_id, 
  CONVERT(VARCHAR(10), ord_date, 120) AS ord_date, qty,
  CAST(1.*qty / SUM(qty) OVER(PARTITION BY stor_id) * 100
    AS DECIMAL(5, 2)) AS per,
  CAST(qty - AVG(1.*qty) OVER(PARTITION BY stor_id)
    AS DECIMAL(9, 2)) AS diff
FROM dbo.sales
ORDER BY stor_id;
GO

---------------------------------------------------------------------
-- Tiebreakers
---------------------------------------------------------------------
USE Northwind;
GO

-- Orders with max order date for each Employee
-- Tiebreaker: max order id
SELECT EmployeeID,
  CAST(SUBSTRING(binstr, 1, 8)   AS DATETIME) AS OrderDate,
  CAST(SUBSTRING(binstr, 9, 4)   AS INT)      AS OrderID,
  CAST(SUBSTRING(binstr, 13, 10) AS NCHAR(5)) AS CustomerID,
  CAST(SUBSTRING(binstr, 23, 8)  AS DATETIME) AS RequiredDate
FROM (SELECT EmployeeID, 
        MAX(CAST(OrderDate        AS BINARY(8))
              + CAST(OrderID      AS BINARY(4))
              + CAST(CustomerID   AS BINARY(10))
              + CAST(RequiredDate AS BINARY(8))) AS binstr
      FROM dbo.Orders
      GROUP BY EmployeeID) AS D;

-- Tiebreaker: min order id
SELECT EmployeeID,
  CAST(SUBSTRING(binstr, 1, 8)   AS DATETIME) AS OrderDate,
  2147483647 - CAST(SUBSTRING(binstr, 9, 4) AS INT) AS OrderID,
  CAST(SUBSTRING(binstr, 13, 10) AS NCHAR(5)) AS CustomerID,
  CAST(SUBSTRING(binstr, 23, 8)  AS DATETIME) AS RequiredDate
FROM (SELECT EmployeeID, 
        MAX(CAST(OrderDate        AS BINARY(8))
              + CAST(2147483647 - OrderID AS BINARY(4))
              + CAST(CustomerID   AS BINARY(10))
              + CAST(RequiredDate AS BINARY(8))) AS binstr
      FROM dbo.Orders
      GROUP BY EmployeeID) AS D;

-- Tiebreaker: max required date, max orderid
SELECT EmployeeID,
  CAST(SUBSTRING(binstr, 1, 8)   AS DATETIME) AS OrderDate,
  CAST(SUBSTRING(binstr, 9, 8)   AS DATETIME) AS RequiredDate,
  CAST(SUBSTRING(binstr, 17, 4)  AS INT)      AS OrderID,
  CAST(SUBSTRING(binstr, 21, 10) AS NCHAR(5)) AS CustomerID  
FROM (SELECT EmployeeID, 
        MAX(CAST(OrderDate        AS BINARY(8))
              + CAST(RequiredDate AS BINARY(8))
              + CAST(OrderID      AS BINARY(4))
              + CAST(CustomerID   AS BINARY(10))
              ) AS binstr
      FROM dbo.Orders
      GROUP BY EmployeeID) AS D;
GO

---------------------------------------------------------------------
-- Running Aggregations
---------------------------------------------------------------------

-- Listing 6-1: Creating and Populating the EmpOrders Table
USE tempdb;
GO

IF OBJECT_ID('dbo.EmpOrders') IS NOT NULL
  DROP TABLE dbo.EmpOrders;
GO

CREATE TABLE dbo.EmpOrders
(
  empid    INT      NOT NULL,
  ordmonth DATETIME NOT NULL,
  qty      INT      NOT NULL,
  PRIMARY KEY(empid, ordmonth)
);

INSERT INTO dbo.EmpOrders(empid, ordmonth, qty)
  SELECT O.EmployeeID, 
    CAST(CONVERT(CHAR(6), O.OrderDate, 112) + '01'
      AS DATETIME) AS ordmonth,
    SUM(Quantity) AS qty
  FROM Northwind.dbo.Orders AS O
    JOIN Northwind.dbo.[Order Details] AS OD
      ON O.OrderID = OD.OrderID
  GROUP BY EmployeeID,
    CAST(CONVERT(CHAR(6), O.OrderDate, 112) + '01'
      AS DATETIME);

-- Content of EmpOrders Table
SELECT empid, CONVERT(VARCHAR(7), ordmonth, 121) AS ordmonth, qty
FROM dbo.EmpOrders
ORDER BY empid, ordmonth;
GO

---------------------------------------------------------------------
-- Cumulative Aggregations
---------------------------------------------------------------------

-- Cumulative Aggregates Per Employee, Month
SELECT O1.empid, CONVERT(VARCHAR(7), O1.ordmonth, 121) AS ordmonth,
  O1.qty AS qtythismonth, SUM(O2.qty) AS totalqty,
  CAST(AVG(1.*O2.qty) AS DECIMAL(12, 2)) AS avgqty
FROM dbo.EmpOrders AS O1
  JOIN dbo.EmpOrders AS O2
    ON O2.empid = O1.empid
    AND O2.ordmonth <= O1.ordmonth
GROUP BY O1.empid, O1.ordmonth, O1.qty
ORDER BY O1.empid, O1.ordmonth;

-- Cumulative Aggregates Per Employee, Month, Using Subqueries
SELECT O1.empid, CONVERT(VARCHAR(7), O1.ordmonth, 121) AS ordmonth,
  O1.qty AS qtythismonth,
  (SELECT SUM(O2.qty) 
   FROM dbo.EmpOrders AS O2
   WHERE O2.empid = O1.empid
     AND O2.ordmonth <= O1.ordmonth) AS totalqty
FROM dbo.EmpOrders AS O1
GROUP BY O1.empid, O1.ordmonth, O1.qty;

-- Cumulative Aggregates Per Employee, Month, where totalqty < 1000
SELECT O1.empid, CONVERT(VARCHAR(7), O1.ordmonth, 121) AS ordmonth,
  O1.qty AS qtythismonth, SUM(O2.qty) AS totalqty,
  CAST(AVG(1.*O2.qty) AS DECIMAL(12, 2)) AS avgqty
FROM dbo.EmpOrders AS O1
  JOIN dbo.EmpOrders AS O2
    ON O2.empid = O1.empid
    AND O2.ordmonth <= O1.ordmonth
GROUP BY O1.empid, O1.ordmonth, O1.qty
HAVING SUM(O2.qty) < 1000
ORDER BY O1.empid, O1.ordmonth;

-- Cumulative Aggregates Per Employee, Month,
-- until totalqty Reaches 1000
SELECT O1.empid, CONVERT(VARCHAR(7), O1.ordmonth, 121) AS ordmonth,
  O1.qty AS qtythismonth, SUM(O2.qty) AS totalqty,
  CAST(AVG(1.*O2.qty) AS DECIMAL(12, 2)) AS avgqty
FROM dbo.EmpOrders AS O1
  JOIN dbo.EmpOrders AS O2
    ON O2.empid = O1.empid
    AND O2.ordmonth <= O1.ordmonth
GROUP BY O1.empid, O1.ordmonth, O1.qty
HAVING SUM(O2.qty) - O1.qty < 1000
ORDER BY O1.empid, O1.ordmonth;

-- Point where totalqty Reaches 1000 Per Employee
SELECT O1.empid, CONVERT(VARCHAR(7), O1.ordmonth, 121) AS ordmonth,
  O1.qty AS qtythismonth, SUM(O2.qty) AS totalqty,
  CAST(AVG(1.*O2.qty) AS DECIMAL(12, 2)) AS avgqty
FROM dbo.EmpOrders AS O1
  JOIN dbo.EmpOrders AS O2
    ON O2.empid = O1.empid
    AND O2.ordmonth <= O1.ordmonth
GROUP BY O1.empid, O1.ordmonth, O1.qty
HAVING SUM(O2.qty) - O1.qty < 1000
  AND SUM(O2.qty) >= 1000
ORDER BY O1.empid, O1.ordmonth;

---------------------------------------------------------------------
-- Sliding Aggregations
---------------------------------------------------------------------

-- Sliding Aggregates Per Employee of Three Months Leading to Current
SELECT O1.empid, 
  CONVERT(VARCHAR(7), O1.ordmonth, 121) AS tomonth, O1.qty AS qtythismonth,
  SUM(O2.qty) AS totalqty,
  CAST(AVG(1.*O2.qty) AS DECIMAL(12, 2)) AS avgqty
FROM dbo.EmpOrders AS O1
  JOIN dbo.EmpOrders AS O2
    ON O2.empid = O1.empid
    AND (O2.ordmonth > DATEADD(month, -3, O1.ordmonth)
         AND O2.ordmonth <=  O1.ordmonth)
GROUP BY O1.empid, O1.ordmonth, O1.qty
ORDER BY O1.empid, O1.ordmonth;

---------------------------------------------------------------------
-- Year-To-Date (YTD)
---------------------------------------------------------------------

-- YTD Aggregates Per Employee, Month
SELECT O1.empid, 
  CONVERT(VARCHAR(7), O1.ordmonth, 121) AS ordmonth,
  O1.qty AS qtythismonth,
  SUM(O2.qty) AS totalqty,
  CAST(AVG(1.*O2.qty) AS DECIMAL(12, 2)) AS avgqty
FROM dbo.EmpOrders AS O1
  JOIN dbo.EmpOrders AS O2
    ON O2.empid = O1.empid
    AND (O2.ordmonth >= CAST(CAST(YEAR(O1.ordmonth) AS CHAR(4))
                               + '0101' AS DATETIME)
         AND O2.ordmonth <= O1.ordmonth)
GROUP BY O1.empid, O1.ordmonth, O1.qty
ORDER BY O1.empid, O1.ordmonth;
GO

---------------------------------------------------------------------
-- PIVOT
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Pivoting Attributes
---------------------------------------------------------------------

-- Listing 6-2: Creating and Populating the OpenSchema Table
SET NOCOUNT ON;
USE tempdb;
GO

IF OBJECT_ID('dbo.OpenSchema') IS NOT NULL
  DROP TABLE dbo.OpenSchema;
GO

CREATE TABLE dbo.OpenSchema
(
  objectid  INT          NOT NULL,
  attribute NVARCHAR(30) NOT NULL,
  value     SQL_VARIANT  NOT NULL, 
  PRIMARY KEY (objectid, attribute)
);

INSERT INTO dbo.OpenSchema(objectid, attribute, value)
  VALUES(1, N'attr1', CAST('ABC'      AS VARCHAR(10))  );
INSERT INTO dbo.OpenSchema(objectid, attribute, value)
  VALUES(1, N'attr2', CAST(10         AS INT)          );
INSERT INTO dbo.OpenSchema(objectid, attribute, value)
  VALUES(1, N'attr3', CAST('20040101' AS SMALLDATETIME));
INSERT INTO dbo.OpenSchema(objectid, attribute, value)
  VALUES(2, N'attr2', CAST(12         AS INT)          );
INSERT INTO dbo.OpenSchema(objectid, attribute, value)
  VALUES(2, N'attr3', CAST('20060101' AS SMALLDATETIME));
INSERT INTO dbo.OpenSchema(objectid, attribute, value)
  VALUES(2, N'attr4', CAST('Y'        AS CHAR(1))      );
INSERT INTO dbo.OpenSchema(objectid, attribute, value)
  VALUES(2, N'attr5', CAST(13.7       AS DECIMAL(9,3)) );
INSERT INTO dbo.OpenSchema(objectid, attribute, value)
  VALUES(3, N'attr1', CAST('XYZ'      AS VARCHAR(10))  );
INSERT INTO dbo.OpenSchema(objectid, attribute, value)
  VALUES(3, N'attr2', CAST(20         AS INT)          );
INSERT INTO dbo.OpenSchema(objectid, attribute, value)
  VALUES(3, N'attr3', CAST('20050101' AS SMALLDATETIME));
GO

-- Pivoting Attributes, Pre-2005 Solution
SELECT objectid,
  MAX(CASE WHEN attribute = 'attr1' THEN value END) AS attr1,
  MAX(CASE WHEN attribute = 'attr2' THEN value END) AS attr2,
  MAX(CASE WHEN attribute = 'attr3' THEN value END) AS attr3,
  MAX(CASE WHEN attribute = 'attr4' THEN value END) AS attr4,
  MAX(CASE WHEN attribute = 'attr5' THEN value END) AS attr5
FROM dbo.OpenSchema
GROUP BY objectid;

-- Pivoting Attributes, SQL Server 2005 Solution
SELECT objectid, attr1, attr2, attr3, attr4, attr5
FROM dbo.OpenSchema
  PIVOT(MAX(value) FOR attribute
    IN([attr1],[attr2],[attr3],[attr4],[attr5])) AS P;
GO

---------------------------------------------------------------------
-- Relational Division
---------------------------------------------------------------------

-- Listing 6-3: Creating and Populating the OrderDetails Table
USE tempdb;
GO

IF OBJECT_ID('dbo.OrderDetails') IS NOT NULL
  DROP TABLE dbo.OrderDetails;
GO

CREATE TABLE dbo.OrderDetails
(
  orderid   VARCHAR(10) NOT NULL,
  productid INT         NOT NULL,
  PRIMARY KEY(orderid, productid)
  /* other colums */
);

INSERT INTO dbo.OrderDetails(orderid, productid) VALUES('A', 1);
INSERT INTO dbo.OrderDetails(orderid, productid) VALUES('A', 2);
INSERT INTO dbo.OrderDetails(orderid, productid) VALUES('A', 3);
INSERT INTO dbo.OrderDetails(orderid, productid) VALUES('A', 4);
INSERT INTO dbo.OrderDetails(orderid, productid) VALUES('B', 2);
INSERT INTO dbo.OrderDetails(orderid, productid) VALUES('B', 3);
INSERT INTO dbo.OrderDetails(orderid, productid) VALUES('B', 4);
INSERT INTO dbo.OrderDetails(orderid, productid) VALUES('C', 3);
INSERT INTO dbo.OrderDetails(orderid, productid) VALUES('C', 4);
INSERT INTO dbo.OrderDetails(orderid, productid) VALUES('D', 4);
GO

-- Relational Division, Pre-2005 Solution
SELECT orderid
FROM (SELECT
        orderid,
        MAX(CASE WHEN productid = 2 THEN 1 END) AS P2,
        MAX(CASE WHEN productid = 3 THEN 1 END) AS P3,
        MAX(CASE WHEN productid = 4 THEN 1 END) AS P4
      FROM dbo.OrderDetails
      GROUP BY orderid) AS P
WHERE P2 = 1 AND P3 = 1 AND P4 = 1;

-- Relational Division, SQL Server 2005 Solution
SELECT orderid
FROM (SELECT * 
      FROM dbo.OrderDetails
        PIVOT(MAX(productid) FOR productid IN([2],[3],[4])) AS P) AS T
WHERE [2] = 2 AND [3] = 3 AND [4] = 4;
GO

-- Relational Division, Pre-2005 Solution, using COUNT
SELECT orderid
FROM (SELECT
        orderid,
        COUNT(CASE WHEN productid = 2 THEN 1 END) AS P2,
        COUNT(CASE WHEN productid = 3 THEN 1 END) AS P3,
        COUNT(CASE WHEN productid = 4 THEN 1 END) AS P4
      FROM dbo.OrderDetails
      GROUP BY orderid) AS P
WHERE P2 = 1 AND P3 = 1 AND P4 = 1;

-- Relational Division, SQL Server 2005 Solution, using COUNT
SELECT orderid
FROM (SELECT * 
      FROM dbo.OrderDetails
        PIVOT(COUNT(productid) FOR productid IN([2],[3],[4])) AS P) AS T
WHERE [2] = 1 AND [3] = 1 AND [4] = 1;
GO
---------------------------------------------------------------------
-- Aggregating Data
---------------------------------------------------------------------

-- Listing 6-4: Creating and Populating the Orders Table
USE tempdb;
GO

IF OBJECT_ID('dbo.Orders') IS NOT NULL
  DROP TABLE dbo.Orders;
GO

CREATE TABLE dbo.Orders
(
  orderid   int        NOT NULL PRIMARY KEY NONCLUSTERED,
  orderdate datetime   NOT NULL,
  empid     int        NOT NULL,
  custid    varchar(5) NOT NULL,
  qty       int        NOT NULL
);

CREATE UNIQUE CLUSTERED INDEX idx_orderdate_orderid
  ON dbo.Orders(orderdate, orderid);

INSERT INTO dbo.Orders(orderid, orderdate, empid, custid, qty)
  VALUES(30001, '20020802', 3, 'A', 10);
INSERT INTO dbo.Orders(orderid, orderdate, empid, custid, qty)
  VALUES(10001, '20021224', 1, 'A', 12);
INSERT INTO dbo.Orders(orderid, orderdate, empid, custid, qty)
  VALUES(10005, '20021224', 1, 'B', 20);
INSERT INTO dbo.Orders(orderid, orderdate, empid, custid, qty)
  VALUES(40001, '20030109', 4, 'A', 40);
INSERT INTO dbo.Orders(orderid, orderdate, empid, custid, qty)
  VALUES(10006, '20030118', 1, 'C', 14);
INSERT INTO dbo.Orders(orderid, orderdate, empid, custid, qty)
  VALUES(20001, '20030212', 2, 'B', 12);
INSERT INTO dbo.Orders(orderid, orderdate, empid, custid, qty)
  VALUES(40005, '20040212', 4, 'A', 10);
INSERT INTO dbo.Orders(orderid, orderdate, empid, custid, qty)
  VALUES(20002, '20040216', 2, 'C', 20);
INSERT INTO dbo.Orders(orderid, orderdate, empid, custid, qty)
  VALUES(30003, '20040418', 3, 'B', 15);
INSERT INTO dbo.Orders(orderid, orderdate, empid, custid, qty)
  VALUES(30004, '20020418', 3, 'C', 22);
INSERT INTO dbo.Orders(orderid, orderdate, empid, custid, qty)
  VALUES(30007, '20020907', 3, 'D', 30);
GO

-- Aggregating Data, Pre-2005 Solution, Total Qty
SELECT custid,
  SUM(CASE WHEN orderyear = 2002 THEN qty END) AS [2002],
  SUM(CASE WHEN orderyear = 2003 THEN qty END) AS [2003],
  SUM(CASE WHEN orderyear = 2004 THEN qty END) AS [2004]
FROM (SELECT custid, YEAR(orderdate) AS orderyear, qty
      FROM dbo.Orders) AS D
GROUP BY custid;
GO

-- Listing 6-5: Creating and Populating the Matrix Table
USE tempdb;
GO

IF OBJECTPROPERTY(OBJECT_ID('dbo.Matrix'), 'IsUserTable') = 1
  DROP TABLE dbo.Matrix;
GO

CREATE TABLE dbo.Matrix
(
  orderyear INT NOT NULL PRIMARY KEY,
  y2002 INT NULL,
  y2003 INT NULL,
  y2004 INT NULL
);

INSERT INTO dbo.Matrix(orderyear, y2002) VALUES(2002, 1);
INSERT INTO dbo.Matrix(orderyear, y2003) VALUES(2003, 1);
INSERT INTO dbo.Matrix(orderyear, y2004) VALUES(2004, 1);
GO

-- Aggregating Data using the Matrix Table
SELECT custid,
  SUM(qty*y2002) AS [2002],
  SUM(qty*y2003) AS [2003],
  SUM(qty*y2004) AS [2004]
FROM (SELECT custid, YEAR(orderdate) AS orderyear, qty
      FROM dbo.Orders) AS D
  JOIN dbo.Matrix AS M ON D.orderyear = M.orderyear
GROUP BY custid;

-- Counting Orders, Pre-2005 Solution
SELECT custid,
  COUNT(CASE WHEN orderyear = 2002 THEN 1 END) AS [2002],
  COUNT(CASE WHEN orderyear = 2003 THEN 1 END) AS [2003],
  COUNT(CASE WHEN orderyear = 2004 THEN 1 END) AS [2004]
FROM (SELECT custid, YEAR(orderdate) AS orderyear
      FROM dbo.Orders) AS D
GROUP BY custid;

-- Counting Orders using the Matrix Table
SELECT custid,
  COUNT(y2002) AS [2002],
  COUNT(y2003) AS [2003],
  COUNT(y2004) AS [2004]
FROM (SELECT custid, YEAR(orderdate) AS orderyear
      FROM dbo.Orders) AS D
  JOIN dbo.Matrix AS M ON D.orderyear = M.orderyear
GROUP BY custid;


-- Aggregating Data, SQL Server 2005 Solution
SELECT *
FROM (SELECT custid, YEAR(orderdate) AS orderyear, qty
      FROM dbo.Orders) AS D
  PIVOT(SUM(qty) FOR orderyear IN([2002],[2003],[2004])) AS P;

-- Counting Orders, SQL Server 2005 Solution
SELECT *
FROM (SELECT custid, YEAR(orderdate) AS orderyear
      FROM dbo.Orders) AS D
  PIVOT(COUNT(orderyear) FOR orderyear IN([2002],[2003],[2004])) AS P;
GO

---------------------------------------------------------------------
-- UNPIVOT
---------------------------------------------------------------------

-- Listing 6-6: Creating and Populating the PvtCustOrders Table
USE tempdb;
GO
IF OBJECT_ID('dbo.PvtCustOrders') IS NOT NULL
  DROP TABLE dbo.PvtCustOrders;
GO

SELECT custid, 
  ISNULL([2002], 0) AS [2002],
  ISNULL([2003], 0) AS [2003],
  ISNULL([2004], 0) AS [2004]
INTO dbo.PvtCustOrders
FROM (SELECT custid, YEAR(orderdate) AS orderyear, qty
      FROM dbo.Orders) AS D
  PIVOT(SUM(qty) FOR orderyear IN([2002],[2003],[2004])) AS P;
GO

-- UNPIVOT, Pre-2005 Solution
SELECT custid, orderyear, qty
FROM (SELECT custid, orderyear,
        CASE orderyear
          WHEN 2002 THEN [2002]
          WHEN 2003 THEN [2003]
          WHEN 2004 THEN [2004]
        END AS qty
      FROM dbo.PvtCustOrders,
        (SELECT 2002 AS orderyear
         UNION ALL SELECT 2003
         UNION ALL SELECT 2004) AS OrderYears) AS D
WHERE qty IS NOT NULL;

-- UNPIVOT, SQL Server 2005 Solution
SELECT custid, orderyear, qty
FROM dbo.PvtCustOrders
  UNPIVOT(qty FOR orderyear IN([2002],[2003],[2004])) AS U
GO

---------------------------------------------------------------------
-- Custom Aggregations
---------------------------------------------------------------------

-- Listing 6-7: Creating and Populating the Groups Table
USE tempdb;
GO
IF OBJECT_ID('dbo.Groups') IS NOT NULL
  DROP TABLE dbo.Groups;
GO

CREATE TABLE dbo.Groups
(
  groupid  VARCHAR(10) NOT NULL,
  memberid INT         NOT NULL,
  string   VARCHAR(10) NOT NULL,
  val      INT         NOT NULL,
  PRIMARY KEY (groupid, memberid)
);
    
INSERT INTO dbo.Groups(groupid, memberid, string, val)
  VALUES('a', 3, 'stra1', 6);
INSERT INTO dbo.Groups(groupid, memberid, string, val)
  VALUES('a', 9, 'stra2', 7);
INSERT INTO dbo.Groups(groupid, memberid, string, val)
  VALUES('b', 2, 'strb1', 3);
INSERT INTO dbo.Groups(groupid, memberid, string, val)
  VALUES('b', 4, 'strb2', 7);
INSERT INTO dbo.Groups(groupid, memberid, string, val)
  VALUES('b', 5, 'strb3', 3);
INSERT INTO dbo.Groups(groupid, memberid, string, val)
  VALUES('b', 9, 'strb4', 11);
INSERT INTO dbo.Groups(groupid, memberid, string, val)
  VALUES('c', 3, 'strc1', 8);
INSERT INTO dbo.Groups(groupid, memberid, string, val)
  VALUES('c', 7, 'strc2', 10);
INSERT INTO dbo.Groups(groupid, memberid, string, val)
  VALUES('c', 9, 'strc3', 12);
GO

---------------------------------------------------------------------
-- Custom Aggregations using Pivoting
---------------------------------------------------------------------

---------------------------------------------------------------------
-- String Concatenation Using Pivoting
---------------------------------------------------------------------
SELECT groupid,
    MAX(CASE WHEN rn = 1 THEN string ELSE '' END)
  + MAX(CASE WHEN rn = 2 THEN ',' + string ELSE '' END)
  + MAX(CASE WHEN rn = 3 THEN ',' + string ELSE '' END)
  + MAX(CASE WHEN rn = 4 THEN ',' + string ELSE '' END) AS string
FROM (SELECT groupid, string,
        (SELECT COUNT(*)
         FROM dbo.Groups AS B
         WHERE B.groupid = A.groupid
           AND B.memberid <= A.memberid) AS rn
      FROM dbo.Groups AS A) AS D
GROUP BY groupid;

---------------------------------------------------------------------
-- Aggregate Product Using Pivoting
---------------------------------------------------------------------
SELECT groupid,
    MAX(CASE WHEN rn = 1 THEN val ELSE 1 END)
  * MAX(CASE WHEN rn = 2 THEN val ELSE 1 END)
  * MAX(CASE WHEN rn = 3 THEN val ELSE 1 END)
  * MAX(CASE WHEN rn = 4 THEN val ELSE 1 END) AS product
FROM (SELECT groupid, val,
        (SELECT COUNT(*)
         FROM dbo.Groups AS B
         WHERE B.groupid = A.groupid
           AND B.memberid <= A.memberid) AS rn
      FROM dbo.Groups AS A) AS D
GROUP BY groupid;
GO

---------------------------------------------------------------------
-- User Defined Aggregates (UDA)
---------------------------------------------------------------------

-- Listing 6-8: C# UDAs Code
/*
using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Text;
using System.IO;
using System.Runtime.InteropServices;

[Serializable]
[SqlUserDefinedAggregate(
   Format.UserDefined,              // use user-defined serialization
   IsInvariantToDuplicates = false, // duplicates make difference
    // for the result
   IsInvariantToNulls = true,       // don't care about NULLs
   IsInvariantToOrder = false,      // whether order makes difference 
   IsNullIfEmpty = false,           // do not yield a NULL
    // for a set of zero strings
   MaxByteSize = 8000)]             // maximum size in bytes of persisted value 
public struct CSStrAgg : IBinarySerialize
{
    private StringBuilder sb;
    private bool firstConcat;

    public void Init()
    {
        this.sb = new StringBuilder();
        this.firstConcat = true;
    }

    public void Accumulate(SqlString s)
    {
        if (s.IsNull)
        {
            return;                 // simply skip Nulls approach
        }
        if (this.firstConcat)
        {
            this.sb.Append(s.Value);
            this.firstConcat = false;
        }
        else
        {
            this.sb.Append(",");
            this.sb.Append(s.Value);
        }
    }

    public void Merge(CSStrAgg Group)
    {
        this.sb.Append(Group.sb);
    }

    public SqlString Terminate()
    {
        return new SqlString(this.sb.ToString());
    }

    public void Read(BinaryReader r)
    {
        sb = new StringBuilder(r.ReadString());
    }

    public void Write(BinaryWriter w)
    {
        if (this.sb.Length > 4000)  // check we don't
                                    // go over 8000 bytes

                                    // simply return first 8000 bytes
            w.Write(this.sb.ToString().Substring(0, 4000));
        else
            w.Write(this.sb.ToString());
    }

}   // end CSStrAgg

[Serializable]
[StructLayout(LayoutKind.Sequential)]
[SqlUserDefinedAggregate(
   Format.Native,                   // use native serialization 
   IsInvariantToDuplicates = false, // duplicates make difference
    // for the result
   IsInvariantToNulls = true,       // don't care about NULLs
   IsInvariantToOrder = false)]     // whether order makes difference 
public class CSProdAgg
{
    private SqlInt64 si;

    public void Init()
    {
        si = 1;
    }


    public void Accumulate(SqlInt64 v)
    {
        if (v.IsNull || si.IsNull)  // Null input = Null output approach
        {
            si = SqlInt64.Null;
            return;
        }
        if (v == 0 || si == 0)      // to prevent an exception in next if
        {
            si = 0;
            return;
        }
                                    // stop before we reach max value
        if (Math.Abs(v.Value) <= SqlInt64.MaxValue / Math.Abs(si.Value))
        {
            si = si * v;
        }
        else
        {
            si = 0;                 // if we reach too big value, return 0
        }

    }

    public void Merge(CSProdAgg Group)
    {
        Accumulate(Group.Terminate());
    }

    public SqlInt64 Terminate()
    {
        return (si);
    }

}  // end CSProdAgg
*/

-- Listing 6-9: VB.NET UDAs Code
/*
Imports System
Imports System.Data
Imports System.Data.SqlTypes
Imports Microsoft.SqlServer.Server
Imports System.Text
Imports System.IO
Imports System.Runtime.InteropServices


<Serializable(), _
 SqlUserDefinedAggregate( _
               Format.UserDefined, _
               IsInvariantToDuplicates:=False, _
               IsInvariantToNulls:=True, _
               IsInvariantToOrder:=False, _
               IsNullIfEmpty:=False, _
               MaxByteSize:=8000)> _
Public Class VBStrAgg
    Implements IBinarySerialize

    Private sb As StringBuilder
    Private firstConcat As Boolean = True

    Public Sub Init()
        Me.sb = New StringBuilder()
        Me.firstConcat = True
    End Sub


    Public Sub Accumulate(ByVal s As SqlString)
        If s.IsNull Then
            Return
        End If
        If Me.firstConcat = True Then
            Me.sb.Append(s.Value)
            Me.firstConcat = False
        Else
            Me.sb.Append(",")
            Me.sb.Append(s.Value)
        End If
    End Sub

    Public Sub Merge(ByVal Group As VBStrAgg)
        Me.sb.Append(Group.sb)
    End Sub

    Public Function Terminate() As SqlString
        Return New SqlString(sb.ToString())
    End Function

    Public Sub Read(ByVal r As BinaryReader) _
      Implements IBinarySerialize.Read
        sb = New StringBuilder(r.ReadString())
    End Sub

    Public Sub Write(ByVal w As BinaryWriter) _
      Implements IBinarySerialize.Write
        If Me.sb.Length > 4000 Then
            w.Write(Me.sb.ToString().Substring(0, 4000))
        Else
            w.Write(Me.sb.ToString())
        End If
    End Sub

End Class


<Serializable(), _
 StructLayout(LayoutKind.Sequential), _
 SqlUserDefinedAggregate( _
               Format.Native, _
               IsInvariantToOrder:=False, _
               IsInvariantToNulls:=True, _
               IsInvariantToDuplicates:=False)> _
Public Class VBProdAgg

    Private si As SqlInt64

    Public Sub Init()
        si = 1
    End Sub

    Public Sub Accumulate(ByVal v As SqlInt64)
        If v.IsNull = True Or si.IsNull = True Then
            si = SqlInt64.Null
            Return
        End If
        If v = 0 Or si = 0 Then
            si = 0
            Return
        End If
        If (Math.Abs(v.Value) <= SqlInt64.MaxValue / Math.Abs(si.Value)) _
          Then
            si = si * v
        Else
            si = 0
        End If
    End Sub

    Public Sub Merge(ByVal Group As VBProdAgg)
        Accumulate(Group.Terminate())
    End Sub

    Public Function Terminate() As SqlInt64
        If si.IsNull = True Then
            Return SqlInt64.Null
        Else
            Return si
        End If
    End Function

End Class
*/

-- Listing 6-10: Enabling CLR and Queried Catalog Views

-- Creating UDAs Database for UDA Demos
-- Enable CLR
EXEC sp_configure 'clr enabled', 1;
RECONFIGURE WITH OVERRIDE;
GO
USE tempdb;
GO
SELECT * FROM sys.assemblies;
SELECT * FROM sys.assembly_modules;
GO

-- test
SELECT groupid, dbo.CSStrAgg(string) AS string
FROM tempdb.dbo.Groups
GROUP BY groupid;

SELECT groupid, dbo.VBStrAgg(string) AS string
FROM tempdb.dbo.Groups
GROUP BY groupid;

SELECT groupid, dbo.CSProdAgg(val) AS product
FROM tempdb.dbo.Groups
GROUP BY groupid;

SELECT groupid, dbo.VBProdAgg(val) AS product
FROM tempdb.dbo.Groups
GROUP BY groupid;
GO

-- clean-up
EXEC sp_configure 'clr enabled', 0;
RECONFIGURE WITH OVERRIDE;
GO

---------------------------------------------------------------------
-- Specialized Solutions
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Specialized Solution for Aggregate String Concatenation
---------------------------------------------------------------------
USE tempdb;
GO

SELECT groupid,
  STUFF((SELECT ',' + string AS [text()]
         FROM dbo.Groups AS G2
         WHERE G2.groupid = G1.groupid
         ORDER BY memberid
         FOR XML PATH('')), 1, 1, '') AS string
FROM dbo.Groups AS G1
GROUP BY groupid;
GO

---------------------------------------------------------------------
-- Specialized Solution for Aggregate Product
---------------------------------------------------------------------
SELECT groupid, POWER(10., SUM(LOG10(val))) AS product
FROM dbo.Groups
GROUP BY groupid;

-- Handling Zeros and Negatives with Pivoting
SELECT groupid,
  CASE
    WHEN MAX(CASE WHEN val = 0 THEN 1 END) = 1 THEN 0
    ELSE 
      CASE WHEN COUNT(CASE WHEN val < 0 THEN 1 END) % 2 = 0
        THEN 1 ELSE -1
      END * POWER(10., SUM(LOG10(NULLIF(ABS(val), 0))))
  END AS product
FROM dbo.Groups
GROUP BY groupid;
GO

-- Handling Zeros and Negatives Mathematically
SELECT groupid,
  CAST(ROUND(EXP(SUM(LOG(ABS(NULLIF(val,0)))))*
    (1-SUM(1-SIGN(val))%4)*(1-SUM(1-SQUARE(SIGN(val)))),0) AS INT)
 AS product
FROM dbo.Groups
GROUP BY groupid;

---------------------------------------------------------------------
-- Specialized Solutions for Aggregate Bitwise Operations
---------------------------------------------------------------------

-- Listing 6-11: Creation Script for the fn_dectobase Function
IF OBJECT_ID('dbo.fn_dectobase') IS NOT NULL
  DROP FUNCTION dbo.fn_dectobase;
GO
CREATE FUNCTION dbo.fn_dectobase(@val AS BIGINT, @base AS INT)
  RETURNS VARCHAR(63)
AS
BEGIN
  DECLARE @r AS VARCHAR(63), @alldigits AS VARCHAR(36);

  SET @alldigits = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ';

  SET @r = '';
  WHILE @val > 0
  BEGIN
    SET @r = SUBSTRING(@alldigits, @val % @base + 1, 1) + @r;
    SET @val = @val / @base;
  END

  RETURN @r;
END
GO

-- Binary Representation of Values
SELECT groupid, val, 
  RIGHT(REPLICATE('0', 32) + CAST(dbo.fn_dectobase(val, 2) AS VARCHAR(64)),
        32) AS binval
FROM dbo.Groups;

---------------------------------------------------------------------
-- Aggregate Bitwise OR
---------------------------------------------------------------------

-- Aggregate Bitwise OR, using a Series of MAX Expressions
SELECT groupid,
    MAX(val & 1)
  + MAX(val & 2)
  + MAX(val & 4)
  + MAX(val & 8)
-- ...
  + MAX(val & 1073741824) AS agg_or
FROM dbo.Groups
GROUP BY groupid;

-- Aggregate Bitwise OR, using a Series of SUM(DISTINCT) Expressions
SELECT groupid,
    SUM(DISTINCT val & 1)
  + SUM(DISTINCT val & 2)
  + SUM(DISTINCT val & 4)
  + SUM(DISTINCT val & 8)
-- ...
  + SUM(DISTINCT val & 1073741824) AS agg_or
FROM dbo.Groups
GROUP BY groupid;

-- Aggregate Bitwise OR, using Nums
SELECT groupid, SUM(DISTINCT bitval) AS agg_or
FROM dbo.Groups
  JOIN (SELECT POWER(2, n-1) AS bitval
        FROM dbo.Nums
        WHERE n <= 31) AS Bits
    ON val & bitval = bitval
GROUP BY groupid;

---------------------------------------------------------------------
-- Aggregate Bitwise AND
---------------------------------------------------------------------
SELECT groupid, SUM(bitval) AS agg_and
FROM (SELECT groupid, bitval
      FROM dbo.Groups,
        (SELECT POWER(2, n-1) AS bitval
         FROM dbo.Nums
         WHERE n <= 31) AS Bits
      GROUP BY groupid, bitval
      HAVING MIN(val & bitval) > 0) AS D
GROUP BY groupid;

---------------------------------------------------------------------
-- Aggregate Bitwise XOR
---------------------------------------------------------------------
SELECT groupid, SUM(bitval) AS agg_xor
FROM (SELECT groupid, bitval
      FROM dbo.Groups,
        (SELECT POWER(2, n-1) AS bitval
         FROM dbo.Nums
         WHERE n <= 31) AS Bits
      GROUP BY groupid, bitval
      HAVING SUM(SIGN(val & bitval)) % 2 = 1) AS D
GROUP BY groupid;

---------------------------------------------------------------------
-- Aggregate Median
---------------------------------------------------------------------
WITH Tiles AS
(
  SELECT groupid, val,
    NTILE(2) OVER(PARTITION BY groupid ORDER BY val) AS tile
  FROM dbo.Groups
),
GroupedTiles AS
(
  SELECT groupid, tile, COUNT(*) AS cnt,
    CASE WHEN tile = 1 THEN MAX(val) ELSE MIN(val) END AS val
  FROM Tiles
  GROUP BY groupid, tile
)
SELECT groupid,
  CASE WHEN MIN(cnt) = MAX(cnt) THEN AVG(1.*val)
       ELSE MIN(val) END AS median
FROM GroupedTiles
GROUP BY groupid;

-- Other Solutions for Median
WITH RN AS
(
  SELECT groupid, val,
    ROW_NUMBER()
      OVER(PARTITION BY groupid ORDER BY val, memberid) AS rna,
    ROW_NUMBER()
      OVER(PARTITION BY groupid ORDER BY val DESC, memberid DESC) AS rnd
  FROM dbo.Groups
)
SELECT groupid, AVG(1.*val) AS median
FROM RN
WHERE ABS(rna - rnd) <= 1
GROUP BY groupid;

WITH RN AS
(
  SELECT groupid, val,
    ROW_NUMBER() OVER(PARTITION BY groupid ORDER BY val) AS rn,
    COUNT(*) OVER(PARTITION BY groupid) AS cnt
  FROM dbo.Groups
)
SELECT groupid, AVG(1.*val) AS median
FROM RN
WHERE ABS(2*rn - cnt - 1) <= 1
GROUP BY groupid;

WITH RN AS
(
  SELECT groupid, val,
    ROW_NUMBER() OVER(PARTITION BY groupid ORDER BY val) AS rn,
    COUNT(*) OVER(PARTITION BY groupid) AS cnt
  FROM dbo.Groups
)
SELECT groupid, AVG(1.*val) AS median
FROM RN
WHERE rn IN((cnt+1)/2, (cnt+2)/2)
GROUP BY groupid;
GO

---------------------------------------------------------------------
-- Histograms
---------------------------------------------------------------------

-- Code Returning Histogram Steps Table
DECLARE @numsteps AS INT;
SET @numsteps = 3;

SELECT n AS step,
  mn + (n - 1) * stepsize AS lb,
  mn + n * stepsize AS hb
FROM dbo.Nums,
  (SELECT MIN(qty) AS mn,
     ((1E0*MAX(qty) + 0.0000000001) - MIN(qty))
     / @numsteps AS stepsize
   FROM dbo.Orders) AS D
WHERE n < = @numsteps;
GO

-- Listing 6-12: Creation Script for fn_histsteps Function
IF OBJECT_ID('dbo.fn_histsteps') IS NOT NULL
  DROP FUNCTION dbo.fn_histsteps;
GO
CREATE FUNCTION dbo.fn_histsteps(@numsteps AS INT) RETURNS TABLE
AS
RETURN
  SELECT n AS step,
    mn + (n - 1) * stepsize AS lb,
    mn + n * stepsize AS hb
  FROM dbo.Nums,
   (SELECT MIN(qty) AS mn,
      ((1E0*MAX(qty) + 0.0000000001) - MIN(qty))
      / @numsteps AS stepsize
    FROM dbo.Orders) AS D
  WHERE n < = @numsteps;
GO

-- Test Function
SELECT * FROM dbo.fn_histsteps(3) AS S;
GO

-- Returning Histogram with 3 Steps
SELECT step, COUNT(*) AS numorders
FROM dbo.fn_histsteps(3) AS S
  JOIN dbo.Orders AS O
    ON qty >= lb AND qty < hb
GROUP BY step;

-- Returning Histogram with 10 Steps
SELECT step, COUNT(*) AS numorders
FROM dbo.fn_histsteps(10) AS S
  JOIN dbo.Orders AS O
    ON qty >= lb AND qty < hb
GROUP BY step;

-- Returning Histogram, Including Empty Steps, Using an Outer Join
SELECT step, COUNT(qty) AS numorders
FROM dbo.fn_histsteps(10) AS S
  LEFT OUTER JOIN dbo.Orders AS O
    ON qty >= lb AND qty < hb
GROUP BY step;

-- Returning Histogram, Including Empty Steps, Using GROUP BY ALL
SELECT step, COUNT(qty) AS numorders
FROM dbo.fn_histsteps(10) AS S, dbo.Orders AS O
WHERE qty >= lb AND qty < hb
GROUP BY ALL step;
GO


-- Listing 6-13: Altering the Implementation of the fn_histsteps Function
ALTER FUNCTION dbo.fn_histsteps(@numsteps AS INT) RETURNS TABLE
AS
RETURN
  SELECT n AS step,
    mn + (n - 1) * stepsize AS lb,
    mn + n * stepsize + CASE WHEN n = @numsteps THEN 1 ELSE 0 END AS hb
  FROM dbo.Nums,
    (SELECT MIN(qty) AS mn,
       (1E0*MAX(qty) - MIN(qty)) / @numsteps AS stepsize
    FROM dbo.Orders) AS D
  WHERE n < = @numsteps;
GO

-- Test Function
SELECT * FROM dbo.fn_histsteps(10) AS S;

-- Getting the Histogram
SELECT step, COUNT(qty) AS numorders
FROM dbo.fn_histsteps(3) AS S
  LEFT OUTER JOIN dbo.Orders AS O
    ON qty >= lb AND qty < hb
GROUP BY step;
GO

---------------------------------------------------------------------
-- Grouping Factor
---------------------------------------------------------------------

-- Listing 6-14: Creating and Populating the Stocks Table
USE tempdb;
GO
IF OBJECT_ID('Stocks') IS NOT NULL
  DROP TABLE Stocks;
GO

CREATE TABLE dbo.Stocks
(
  dt    DATETIME NOT NULL PRIMARY KEY,
  price INT      NOT NULL
);

INSERT INTO dbo.Stocks(dt, price) VALUES('20060801', 13);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060802', 14);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060803', 17);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060804', 40);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060805', 40);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060806', 52);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060807', 56);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060808', 60);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060809', 70);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060810', 30);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060811', 29);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060812', 29);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060813', 40);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060814', 45);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060815', 60);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060816', 60);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060817', 55);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060818', 60);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060819', 60);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060820', 15);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060821', 20);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060822', 30);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060823', 40);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060824', 20);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060825', 60);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060826', 60);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060827', 70);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060828', 70);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060829', 40);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060830', 30);
INSERT INTO dbo.Stocks(dt, price) VALUES('20060831', 10);

CREATE UNIQUE INDEX idx_price_dt ON Stocks(price, dt);
GO

-- Ranges where Stock Price was >= 50
SELECT MIN(dt) AS startrange, MAX(dt) AS endrange,
  DATEDIFF(day, MIN(dt), MAX(dt)) + 1 AS numdays,
  MAX(price) AS maxprice
FROM (SELECT dt, price,
        (SELECT MIN(dt)
         FROM dbo.Stocks AS S2
         WHERE S2.dt > S1.dt
          AND price < 50) AS grp
      FROM dbo.Stocks AS S1
      WHERE price >= 50) AS D
GROUP BY grp;

-- Solution using ROW_NUMBER
SELECT MIN(dt) AS startrange, MAX(dt) AS endrange,
  DATEDIFF(day, MIN(dt), MAX(dt)) + 1 AS numdays,
  MAX(price) AS maxprice
FROM (SELECT dt, price,
        dt - ROW_NUMBER() OVER(ORDER BY dt) AS grp
      FROM dbo.Stocks AS S1
      WHERE price >= 50) AS D
GROUP BY grp;
GO

---------------------------------------------------------------------
-- CUBE and ROLLUP
---------------------------------------------------------------------

---------------------------------------------------------------------
-- CUBE
---------------------------------------------------------------------

-- Cube
SELECT empid, custid,
  YEAR(orderdate) AS orderyear, SUM(qty) AS totalqty
FROM dbo.Orders
GROUP BY empid, custid, YEAR(orderdate)
WITH CUBE;
GO

-- Listing 6-15: Populating a #Cube with Cube Query's Resultset
SELECT empid, custid,
  YEAR(orderdate) AS orderyear, SUM(qty) AS totalqty
INTO #Cube
FROM dbo.Orders
GROUP BY empid, custid, YEAR(orderdate)
WITH CUBE;

CREATE CLUSTERED INDEX idx_emp_cust_year
  ON #Cube(empid, custid, orderyear);
GO

-- Querying #Cube
SELECT totalqty
FROM #Cube
WHERE empid = 1
  AND custid IS NULL
  AND orderyear IS NULL;

-- Cleanup
DROP TABLE #Cube;
GO
 
-- Allow NULLs in the Orders.empid column
ALTER TABLE dbo.Orders ALTER COLUMN empid INT NULL;
UPDATE dbo.Orders SET empid = NULL WHERE orderid IN(10001, 20001);
GO

-- Cube, Dealing with Unknown Employees using COALESCE
SELECT COALESCE(empid, -1) AS empid, custid,
  YEAR(orderdate) AS orderyear, SUM(qty) AS totalqty
FROM dbo.Orders
GROUP BY COALESCE(empid, -1), custid, YEAR(orderdate)
WITH CUBE;

-- Cube, Dealing with Unknown Employees using Grouping Function
SELECT empid, GROUPING(empid) AS grp_empid, custid,
  YEAR(orderdate) AS orderyear, SUM(qty) AS totalqty
FROM dbo.Orders
GROUP BY empid, custid, YEAR(orderdate)
WITH CUBE;
GO

---------------------------------------------------------------------
-- ROLLUP
---------------------------------------------------------------------
SELECT 
  YEAR(orderdate)  AS orderyear,
  MONTH(orderdate) AS ordermonth,
  DAY(orderdate)   AS orderday,
  SUM(qty) AS totalqty
FROM dbo.Orders
GROUP BY YEAR(orderdate), MONTH(orderdate), DAY(orderdate)
WITH ROLLUP;
GO
