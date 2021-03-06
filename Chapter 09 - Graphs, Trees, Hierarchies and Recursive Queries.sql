---------------------------------------------------------------------
-- Chapter 09 - Graphs, Trees, Hierarchies and Recursive Queries
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Scenarios
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Employees Organizational Chart
---------------------------------------------------------------------

-- Listing 9-1: DDL & Sample Data for Employees
SET NOCOUNT ON;
USE tempdb;
GO
IF OBJECT_ID('dbo.Employees') IS NOT NULL
  DROP TABLE dbo.Employees;
GO
CREATE TABLE dbo.Employees
(
  empid   INT         NOT NULL PRIMARY KEY,
  mgrid   INT         NULL     REFERENCES dbo.Employees,
  empname VARCHAR(25) NOT NULL,
  salary  MONEY       NOT NULL,
  CHECK (empid <> mgrid)
);

INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(1, NULL, 'David', $10000.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(2, 1, 'Eitan', $7000.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(3, 1, 'Ina', $7500.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(4, 2, 'Seraph', $5000.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(5, 2, 'Jiru', $5500.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(6, 2, 'Steve', $4500.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(7, 3, 'Aaron', $5000.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(8, 5, 'Lilach', $3500.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(9, 7, 'Rita', $3000.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(10, 5, 'Sean', $3000.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(11, 7, 'Gabriel', $3000.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(12, 9, 'Emilia' , $2000.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(13, 9, 'Michael', $2000.00);
INSERT INTO dbo.Employees(empid, mgrid, empname, salary)
  VALUES(14, 9, 'Didi', $1500.00);

CREATE UNIQUE INDEX idx_unc_mgrid_empid ON dbo.Employees(mgrid, empid);
GO

---------------------------------------------------------------------
-- Bill Of Materials (BOM)
---------------------------------------------------------------------

-- Listing 9-2: DDL & Sample Data for Parts, BOM
SET NOCOUNT ON;
USE tempdb;
GO
IF OBJECT_ID('dbo.BOM') IS NOT NULL
  DROP TABLE dbo.BOM;
GO
IF OBJECT_ID('dbo.Parts') IS NOT NULL
  DROP TABLE dbo.Parts;
GO
CREATE TABLE dbo.Parts
(
  partid   INT         NOT NULL PRIMARY KEY,
  partname VARCHAR(25) NOT NULL
);

INSERT INTO dbo.Parts(partid, partname) VALUES( 1, 'Black Tea');
INSERT INTO dbo.Parts(partid, partname) VALUES( 2, 'White Tea');
INSERT INTO dbo.Parts(partid, partname) VALUES( 3, 'Latte');
INSERT INTO dbo.Parts(partid, partname) VALUES( 4, 'Espresso');
INSERT INTO dbo.Parts(partid, partname) VALUES( 5, 'Double Espresso');
INSERT INTO dbo.Parts(partid, partname) VALUES( 6, 'Cup Cover');
INSERT INTO dbo.Parts(partid, partname) VALUES( 7, 'Regular Cup');
INSERT INTO dbo.Parts(partid, partname) VALUES( 8, 'Stirrer');
INSERT INTO dbo.Parts(partid, partname) VALUES( 9, 'Espresso Cup');
INSERT INTO dbo.Parts(partid, partname) VALUES(10, 'Tea Shot');
INSERT INTO dbo.Parts(partid, partname) VALUES(11, 'Milk');
INSERT INTO dbo.Parts(partid, partname) VALUES(12, 'Coffee Shot');
INSERT INTO dbo.Parts(partid, partname) VALUES(13, 'Tea Leaves');
INSERT INTO dbo.Parts(partid, partname) VALUES(14, 'Water');
INSERT INTO dbo.Parts(partid, partname) VALUES(15, 'Sugar Bag');
INSERT INTO dbo.Parts(partid, partname) VALUES(16, 'Ground Coffee');
INSERT INTO dbo.Parts(partid, partname) VALUES(17, 'Coffee Beans');

CREATE TABLE dbo.BOM
(
  partid     INT           NOT NULL REFERENCES dbo.Parts,
  assemblyid INT           NULL     REFERENCES dbo.Parts,
  unit       VARCHAR(3)    NOT NULL,
  qty        DECIMAL(8, 2) NOT NULL,
  UNIQUE(partid, assemblyid),
  CHECK (partid <> assemblyid)
);

INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 1, NULL, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 2, NULL, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 3, NULL, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 4, NULL, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 5, NULL, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 6,    1, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 7,    1, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(10,    1, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(14,    1, 'mL', 230.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 6,    2, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 7,    2, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(10,    2, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(14,    2, 'mL', 205.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(11,    2, 'mL',  25.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 6,    3, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 7,    3, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(11,    3, 'mL', 225.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(12,    3, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 9,    4, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(12,    4, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES( 9,    5, 'EA',   1.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(12,    5, 'EA',   2.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(13,   10, 'g' ,   5.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(14,   10, 'mL',  20.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(14,   12, 'mL',  20.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(16,   12, 'g' ,  15.00);
INSERT INTO dbo.BOM(partid, assemblyid, unit, qty)
  VALUES(17,   16, 'g' ,  15.00);
GO

---------------------------------------------------------------------
-- Road System
---------------------------------------------------------------------

-- Listing 9-3: DDL & Sample Data for Cities, Roads
SET NOCOUNT ON;
USE tempdb;
GO
IF OBJECT_ID('dbo.Roads') IS NOT NULL
  DROP TABLE dbo.Roads;
GO
IF OBJECT_ID('dbo.Cities') IS NOT NULL
  DROP TABLE dbo.Cities;
GO

CREATE TABLE dbo.Cities
(
  cityid  CHAR(3)     NOT NULL PRIMARY KEY,
  city    VARCHAR(30) NOT NULL,
  region  VARCHAR(30) NULL,
  country VARCHAR(30) NOT NULL
);

INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('ATL', 'Atlanta', 'GA', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('ORD', 'Chicago', 'IL', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('DEN', 'Denver', 'CO', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('IAH', 'Houston', 'TX', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('MCI', 'Kansas City', 'KS', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('LAX', 'Los Angeles', 'CA', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('MIA', 'Miami', 'FL', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('MSP', 'Minneapolis', 'MN', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('JFK', 'New York', 'NY', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('SEA', 'Seattle', 'WA', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('SFO', 'San Francisco', 'CA', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('ANC', 'Anchorage', 'AK', 'USA');
INSERT INTO dbo.Cities(cityid, city, region, country)
  VALUES('FAI', 'Fairbanks', 'AK', 'USA');

CREATE TABLE dbo.Roads
(
  city1       CHAR(3) NOT NULL REFERENCES dbo.Cities,
  city2       CHAR(3) NOT NULL REFERENCES dbo.Cities,
  distance INT     NOT NULL,
  PRIMARY KEY(city1, city2),
  CHECK(city1 < city2),
  CHECK(distance > 0)
);

INSERT INTO dbo.Roads(city1, city2, distance) VALUES('ANC', 'FAI', 359);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('ATL', 'ORD', 715);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('ATL', 'IAH', 800);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('ATL', 'MCI', 805);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('ATL', 'MIA', 665);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('ATL', 'JFK', 865);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('DEN', 'IAH', 1120);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('DEN', 'MCI', 600);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('DEN', 'LAX', 1025);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('DEN', 'MSP', 915);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('DEN', 'SEA', 1335);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('DEN', 'SFO', 1270);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('IAH', 'MCI', 795);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('IAH', 'LAX', 1550);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('IAH', 'MIA', 1190);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('JFK', 'ORD', 795);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('LAX', 'SFO', 385);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('MCI', 'ORD', 525);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('MCI', 'MSP', 440);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('MSP', 'ORD', 410);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('MSP', 'SEA', 2015);
INSERT INTO dbo.Roads(city1, city2, distance) VALUES('SEA', 'SFO', 815);
GO

---------------------------------------------------------------------
-- Iterations/Recursion
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Subordinates
---------------------------------------------------------------------

-- Listing 9-4: Creation Script for Function fn_subordinates1

---------------------------------------------------------------------
-- Function: fn_subordinates1, Descendants
--
-- Input   : @root INT: Manager id
--
-- Output  : @Subs Table: id and level of subordinates of
--                        input manager (empid = @root) in all levels
--
-- Process : * Insert into @Subs row of input manager
--           * In a loop, while previous insert loaded more than 0 rows
--             insert into @Subs next level of subordinates
---------------------------------------------------------------------
USE tempdb;
GO
IF OBJECT_ID('dbo.fn_subordinates1') IS NOT NULL
  DROP FUNCTION dbo.fn_subordinates1;
GO
CREATE FUNCTION dbo.fn_subordinates1(@root AS INT) RETURNS @Subs Table
(
  empid INT NOT NULL PRIMARY KEY NONCLUSTERED,
  lvl   INT NOT NULL,
  UNIQUE CLUSTERED(lvl, empid)  -- Index will be used to filter level
)
AS
BEGIN
  DECLARE @lvl AS INT;
  SET @lvl = 0;                 -- Initialize level counter with 0

  -- Insert root node to @Subs
  INSERT INTO @Subs(empid, lvl)
    SELECT empid, @lvl FROM dbo.Employees WHERE empid = @root;

  WHILE @@rowcount > 0          -- while previous level had rows
  BEGIN
    SET @lvl = @lvl + 1;        -- Increment level counter

    -- Insert next level of subordinates to @Subs
    INSERT INTO @Subs(empid, lvl)
      SELECT C.empid, @lvl
      FROM @Subs AS P           -- P = Parent
        JOIN dbo.Employees AS C -- C = Child
          ON P.lvl = @lvl - 1   -- Filter parents from previous level
          AND C.mgrid = P.empid;
  END

  RETURN;
END
GO

-- Node ids of descendants of a given node
SELECT empid, lvl FROM dbo.fn_subordinates1(3) AS S;

-- Descendants of a given node
SELECT E.empid, E.empname, S.lvl
FROM dbo.fn_subordinates1(3) AS S
  JOIN dbo.Employees AS E
    ON E.empid = S.empid;

-- Leaf nodes underneath a given node
SELECT empid 
FROM dbo.fn_subordinates1(3) AS P
WHERE NOT EXISTS
  (SELECT * FROM dbo.Employees AS C
   WHERE C.mgrid = P.empid);

-- Listing 9-5: Subtree of a Given Root, CTE Solution
DECLARE @root AS INT;
SET @root = 3;

WITH SubsCTE
AS
(
  -- Anchor member returns root node
  SELECT empid, empname, 0 AS lvl 
  FROM dbo.Employees
  WHERE empid = @root

  UNION ALL

  -- Recursive member returns next level of children
  SELECT C.empid, C.empname, P.lvl + 1
  FROM SubsCTE AS P
    JOIN dbo.Employees AS C
      ON C.mgrid = P.empid
)
SELECT * FROM SubsCTE;

-- Listing 9-6: Creation Script for Function fn_partsexplosion

---------------------------------------------------------------------
-- Function: fn_partsexplosion, Parts Explosion
--
-- Input   : @root INT: Root part id
--
-- Output  : @PartsExplosion Table:
--              id and level of contained parts of input part
--              in all levels
--
-- Process : * Insert into @PartsExplosion row of input root part
--           * In a loop, while previous insert loaded more than 0 rows
--             insert into @PartsExplosion next level of parts
---------------------------------------------------------------------
USE tempdb;
GO
IF OBJECT_ID('dbo.fn_partsexplosion') IS NOT NULL
  DROP FUNCTION dbo.fn_partsexplosion;
GO
CREATE FUNCTION dbo.fn_partsexplosion(@root AS INT)
  RETURNS @PartsExplosion Table
(
  partid INT           NOT NULL,
  qty    DECIMAL(8, 2) NOT NULL,
  unit   VARCHAR(3)    NOT NULL,
  lvl    INT           NOT NULL,
  n      INT           NOT NULL IDENTITY, -- surrogate key
  UNIQUE CLUSTERED(lvl, n)  -- Index will be used to filter lvl
)
AS
BEGIN
  DECLARE @lvl AS INT;
  SET @lvl = 0;                  -- Initialize level counter with 0

  -- Insert root node to @PartsExplosion
  INSERT INTO @PartsExplosion(partid, qty, unit, lvl)
    SELECT partid, qty, unit, @lvl
    FROM dbo.BOM
    WHERE partid = @root;

  WHILE @@rowcount > 0           -- while previous level had rows
  BEGIN
    SET @lvl = @lvl + 1;         -- Increment level counter

    -- Insert next level of subordinates to @PartsExplosion
    INSERT INTO @PartsExplosion(partid, qty, unit, lvl)
      SELECT C.partid, P.qty * C.qty, C.unit, @lvl
      FROM @PartsExplosion AS P  -- P = Parent
        JOIN dbo.BOM AS C        -- C = Child
          ON P.lvl = @lvl - 1    -- Filter parents from previous level
          AND C.assemblyid = P.partid;
  END

  RETURN;
END
GO

-- Parts Explosion
SELECT P.partid, P.partname, PE.qty, PE.unit, PE.lvl
FROM dbo.fn_partsexplosion(2) AS PE
  JOIN dbo.Parts AS P
    ON P.partid = PE.partid;

-- Listing 9-7: CTE Solution for Parts Explosion
DECLARE @root AS INT;
SET @root = 2;

WITH PartsExplosionCTE
AS
(
  -- Anchor member returns root part
  SELECT partid, qty, unit, 0 AS lvl
  FROM dbo.BOM
  WHERE partid = @root

  UNION ALL

  -- Recursive member returns next level of parts
  SELECT C.partid, CAST(P.qty * C.qty AS DECIMAL(8, 2)),
    C.unit, P.lvl + 1
  FROM PartsExplosionCTE AS P
    JOIN dbo.BOM AS C
      ON C.assemblyid = P.partid
)
SELECT P.partid, P.partname, PE.qty, PE.unit, PE.lvl
FROM PartsExplosionCTE AS PE
  JOIN dbo.Parts AS P
    ON P.partid = PE.partid;

-- Parts Explosion, Aggregating Parts
SELECT P.partid, P.partname, PES.qty, PES.unit
FROM (SELECT partid, unit, SUM(qty) AS qty
      FROM dbo.fn_partsexplosion(2) AS PE
      GROUP BY partid, unit) AS PES
  JOIN dbo.Parts AS P
    ON P.partid = PES.partid;

SELECT P.partid, P.partname, PES.qty, PES.unit
FROM (SELECT partid, unit, SUM(qty) AS qty
      FROM dbo.fn_partsexplosion(5) AS PE
      GROUP BY partid, unit) AS PES
  JOIN dbo.Parts AS P
    ON P.partid = PES.partid;

-- Listing 9-8: Creation Script for Function fn_subordinates2

---------------------------------------------------------------------
-- Function: fn_subordinates2,
--           Descendants with optional level limit
--
-- Input   : @root      INT: Manager id
--           @maxlevels INT: Max number of levels to return 
--
-- Output  : @Subs TABLE: id and level of subordinates of
--                        input manager in all levels <= @maxlevels
--
-- Process : * Insert into @Subs row of input manager
--           * In a loop, while previous insert loaded more than 0 rows
--             and previous level is smaller than @maxlevels
--             insert into @Subs next level of subordinates
---------------------------------------------------------------------
USE tempdb;
GO
IF OBJECT_ID('dbo.fn_subordinates2') IS NOT NULL
  DROP FUNCTION dbo.fn_subordinates2;
GO
CREATE FUNCTION dbo.fn_subordinates2
  (@root AS INT, @maxlevels AS INT = NULL) RETURNS @Subs TABLE
(
  empid INT NOT NULL PRIMARY KEY NONCLUSTERED,
  lvl   INT NOT NULL,
  UNIQUE CLUSTERED(lvl, empid)  -- Index will be used to filter level
)
AS
BEGIN
  DECLARE @lvl AS INT;
  SET @lvl = 0;                 -- Initialize level counter with 0
  -- If input @maxlevels is NULL, set it to maximum integer
  -- to virtually have no limit on levels
  SET @maxlevels = COALESCE(@maxlevels, 2147483647);

  -- Insert root node to @Subs
  INSERT INTO @Subs(empid, lvl)
    SELECT empid, @lvl FROM dbo.Employees WHERE empid = @root;

  WHILE @@rowcount > 0          -- while previous level had rows
    AND @lvl < @maxlevels       -- and previous level < @maxlevels
  BEGIN
    SET @lvl = @lvl + 1;        -- Increment level counter

    -- Insert next level of subordinates to @Subs
    INSERT INTO @Subs(empid, lvl)
      SELECT C.empid, @lvl
      FROM @Subs AS P           -- P = Parent
        JOIN dbo.Employees AS C -- C = Child
          ON P.lvl = @lvl - 1   -- Filter parents from previous level
          AND C.mgrid = P.empid;
  END

  RETURN;
END
GO

-- Descendants of a given node, no limit on levels
SELECT empid, lvl
FROM dbo.fn_subordinates2(3, NULL) AS S;

-- Descendants of a given node, limit 2 levels
SELECT empid, lvl
FROM dbo.fn_subordinates2(3, 2) AS S;

-- Descendants that are 2 levels underneath a given node
SELECT empid
FROM dbo.fn_subordinates2(3, 2) AS S
WHERE lvl = 2;
GO

-- Listing 9-9: Subtree with level limit, CTE Solution, with MAXRECURSION

DECLARE @root AS INT;
SET @root = 3;

WITH SubsCTE
AS
(
  SELECT empid, empname, 0 AS lvl 
  FROM dbo.Employees
  WHERE empid = @root

  UNION ALL

  SELECT C.empid, C.empname, P.lvl + 1
  FROM SubsCTE AS P
    JOIN dbo.Employees AS C
      ON C.mgrid = P.empid
)
SELECT * FROM SubsCTE
OPTION (MAXRECURSION 2);
GO

-- Listing 9-10: Subtree with level limit, CTE Solution, with level column
DECLARE @root AS INT, @maxlevels AS INT;
SET @root = 3;
SET @maxlevels = 2;

WITH SubsCTE
AS
(
  SELECT empid, empname, 0 AS lvl 
  FROM dbo.Employees
  WHERE empid = @root

  UNION ALL

  SELECT C.empid, C.empname, P.lvl + 1
  FROM SubsCTE AS P
    JOIN dbo.Employees AS C
      ON C.mgrid = P.empid
      AND P.lvl < @maxlevels -- limit parent's level
)
SELECT * FROM SubsCTE;

---------------------------------------------------------------------
-- Sub-Path
---------------------------------------------------------------------

-- Listing 9-11: Creation Script for Function fn_managers

---------------------------------------------------------------------
-- Function: fn_managers, Ancestors with optional level limit
--
-- Input   : @empid INT : Employee id
--           @maxlevels : Max number of levels to return 
--
-- Output  : @Mgrs Table: id and level of managers of
--                        input employee in all levels <= @maxlevels
--
-- Process : * In a loop, while current manager is not null
--             and previous level is smaller than @maxlevels
--             insert into @Mgrs current manager,
--             and get next level manager
---------------------------------------------------------------------
USE tempdb;
GO
IF OBJECT_ID('dbo.fn_managers') IS NOT NULL
  DROP FUNCTION dbo.fn_managers;
GO
CREATE FUNCTION dbo.fn_managers
  (@empid AS INT, @maxlevels AS INT = NULL) RETURNS @Mgrs TABLE
(
  empid INT NOT NULL PRIMARY KEY,
  lvl   INT NOT NULL
)
AS
BEGIN
  IF NOT EXISTS(SELECT * FROM dbo.Employees WHERE empid = @empid)
    RETURN;  

  DECLARE @lvl AS INT;
  SET @lvl = 0;                 -- Initialize level counter with 0
  -- If input @maxlevels is NULL, set it to maximum integer
  -- to virtually have no limit on levels
  SET @maxlevels = COALESCE(@maxlevels, 2147483647);

  WHILE @empid IS NOT NULL      -- while current employee has a manager
    AND @lvl <= @maxlevels      -- and previous level < @maxlevels
  BEGIN
    -- Insert current manager to @Mgrs
    INSERT INTO @Mgrs(empid, lvl) VALUES(@empid, @lvl);
    SET @lvl = @lvl + 1;        -- Increment level counter
    -- Get next level manager
    SET @empid = (SELECT mgrid FROM dbo.Employees WHERE empid = @empid);
  END

  RETURN;
END
GO

-- Ancestors of a given node, no limit on levels
SELECT empid, lvl
FROM dbo.fn_managers(8, NULL) AS M;

-- Listing 9-12: Ancestors of a Given Node, CTE Solution

-- Ancestors of a given node, CTE Solution
DECLARE @empid AS INT;
SET @empid = 8;

WITH MgrsCTE
AS
(
  SELECT empid, mgrid, empname, 0 AS lvl 
  FROM dbo.Employees
  WHERE empid = @empid

  UNION ALL

  SELECT P.empid, P.mgrid, P.empname, C.lvl + 1
  FROM MgrsCTE AS C
    JOIN dbo.Employees AS P
      ON C.mgrid = P.empid
)
SELECT * FROM MgrsCTE;

-- Ancestors of a given node, limit 2 levels
SELECT empid, lvl
FROM dbo.fn_managers(8, 2) AS M;
GO

-- Listing 9-13: Ancestors with Level Limit, CTE Solution
DECLARE @empid AS INT, @maxlevels AS INT;
SET @empid = 8;
SET @maxlevels = 2;

WITH MgrsCTE
AS
(
  SELECT empid, mgrid, empname, 0 AS lvl 
  FROM dbo.Employees
  WHERE empid = @empid

  UNION ALL

  SELECT P.empid, P.mgrid, P.empname, C.lvl + 1
  FROM MgrsCTE AS C
    JOIN dbo.Employees AS P
      ON C.mgrid = P.empid
      AND C.lvl < @maxlevels -- limit child's level
)
SELECT * FROM MgrsCTE;

-- Ancestor that is 2 levels above a given node
SELECT empid
FROM dbo.fn_managers(8, 2) AS M
WHERE lvl = 2;
GO

---------------------------------------------------------------------
-- Subtree/Subgraph with Path Enumeration
---------------------------------------------------------------------

-- Listing 9-14: Creation Script for Function fn_subordinates3

---------------------------------------------------------------------
-- Function: fn_subordinates3,
--           Descendants with optional level limit,
--           and path enumeration
--
-- Input   : @root      INT: Manager id
--           @maxlevels INT: Max number of levels to return 
--
-- Output  : @Subs TABLE: id, level and materialized ancestors path
--                        of subordinates of input manager
--                        in all levels <= @maxlevels
--
-- Process : * Insert into @Subs row of input manager
--           * In a loop, while previous insert loaded more than 0 rows
--             and previous level is smaller than @maxlevels:
--             - insert into @Subs next level of subordinates
--             - calculate a materialized ancestors path for each
--               by concatenating current node id to parent's path
---------------------------------------------------------------------
USE tempdb;
GO
IF OBJECT_ID('dbo.fn_subordinates3') IS NOT NULL
  DROP FUNCTION dbo.fn_subordinates3;
GO
CREATE FUNCTION dbo.fn_subordinates3
  (@root AS INT, @maxlevels AS INT = NULL) RETURNS @Subs TABLE
(
  empid INT          NOT NULL PRIMARY KEY NONCLUSTERED,
  lvl   INT          NOT NULL,
  path  VARCHAR(900) NOT NULL
  UNIQUE CLUSTERED(lvl, empid)  -- Index will be used to filter level
)
AS
BEGIN
  DECLARE @lvl AS INT;
  SET @lvl = 0;                 -- Initialize level counter with 0
  -- If input @maxlevels is NULL, set it to maximum integer
  -- to virtually have no limit on levels
  SET @maxlevels = COALESCE(@maxlevels, 2147483647);

  -- Insert root node to @Subs
  INSERT INTO @Subs(empid, lvl, path)
    SELECT empid, @lvl, '.' + CAST(empid AS VARCHAR(10)) + '.'
    FROM dbo.Employees WHERE empid = @root;

  WHILE @@rowcount > 0          -- while previous level had rows
    AND @lvl < @maxlevels       -- and previous level < @maxlevels
  BEGIN
    SET @lvl = @lvl + 1;        -- Increment level counter

    -- Insert next level of subordinates to @Subs
    INSERT INTO @Subs(empid, lvl, path)
      SELECT C.empid, @lvl,
        P.path + CAST(C.empid AS VARCHAR(10)) + '.'
      FROM @Subs AS P           -- P = Parent
        JOIN dbo.Employees AS C -- C = Child
          ON P.lvl = @lvl - 1   -- Filter parents from previous level
          AND C.mgrid = P.empid;
  END

  RETURN;
END
GO

-- Return descendants of a given node, along with a materialized path
SELECT empid, lvl, path
FROM dbo.fn_subordinates3(1, NULL) AS S;

-- Return descendants of a given node, sorted and indented
SELECT E.empid, REPLICATE(' | ', lvl) + empname AS empname
FROM dbo.fn_subordinates3(1, NULL) AS S
  JOIN dbo.Employees AS E
    ON E.empid = S.empid
ORDER BY path;

-- Listing 9-15: Subtree with Path Enumeration, CTE Solution

-- Descendants of a given node, with Materialized Path, CTE Solution
DECLARE @root AS INT;
SET @root = 1;

WITH SubsCTE
AS
(
  SELECT empid, empname, 0 AS lvl,
    -- Path of root = '.' + empid + '.'
    CAST('.' + CAST(empid AS VARCHAR(10)) + '.'
         AS VARCHAR(MAX)) AS path
  FROM dbo.Employees
  WHERE empid = @root

  UNION ALL

  SELECT C.empid, C.empname, P.lvl + 1,
    -- Path of child = parent's path + child empid + '.'
    CAST(P.path + CAST(C.empid AS VARCHAR(10)) + '.'
         AS VARCHAR(MAX)) AS path
  FROM SubsCTE AS P
    JOIN dbo.Employees AS C
      ON C.mgrid = P.empid
)
SELECT empid, REPLICATE(' | ', lvl) + empname AS empname
FROM SubsCTE
ORDER BY path;

---------------------------------------------------------------------
-- Sorting
---------------------------------------------------------------------

-- Listing 9-16: Creation Script for Procedure usp_sortsubs

---------------------------------------------------------------------
-- Stored Procedure: usp_sortsubs,
--   Descendants with optional level limit and sort values
--
-- Input   : @root      INT: Manager id
--           @maxlevels INT: Max number of levels to return
--           @orderby   sysname: determines sort order 
--
-- Output  : Rowset: id, level and sort values
--                   of subordinates of input manager
--                   in all levels <= @maxlevels
--
-- Process : * Use a loop to load the desired subtree into #SubsPath
--           * For each node, construct a binary sort path
--           * The row number represents the node's position among 
--             siblings based on the input ORDER BY list
--           * Load the rows from #SubPath into #SubsSort sorted
--             by the binary sortpath
--           * IDENTITY values representing the global sort value
--             in the subtree will be generated in the target
--             #SubsSort table
--           * Return all rows from #SubsSort sorted by the
--             sort value
---------------------------------------------------------------------
USE tempdb;
GO
IF OBJECT_ID('dbo.usp_sortsubs') IS NOT NULL
  DROP PROC dbo.usp_sortsubs;
GO
CREATE PROC dbo.usp_sortsubs
  @root      AS INT     = NULL,
  @maxlevels AS INT     = NULL,
  @orderby   AS sysname = N'empid'
AS

SET NOCOUNT ON;

-- #SubsPath is a temp table that will hold binary sort paths
CREATE TABLE #SubsPath
(
  rownum   INT NOT NULL IDENTITY,
  nodeid   INT NOT NULL,
  lvl      INT NOT NULL,
  sortpath VARBINARY(900) NULL
);
CREATE UNIQUE CLUSTERED INDEX idx_uc_lvl_empid ON #SubsPath(lvl, nodeid);

-- #SubsPath is a temp table that will hold the final
-- integer sort values
CREATE TABLE #SubsSort
(
  nodeid   INT NOT NULL,
  lvl      INT NOT NULL,
  sortval  INT NOT NULL IDENTITY
);
CREATE UNIQUE CLUSTERED INDEX idx_uc_sortval ON #SubsSort(sortval);

-- If @root is not specified, set it to root of the tree
IF @root IS NULL
  SET @root = (SELECT empid FROM dbo.Employees WHERE mgrid IS NULL);
-- If @maxlevels is not specified, set it maximum integer
IF @maxlevels IS NULL
  SET @maxlevels = 2147483647;

DECLARE @lvl AS INT, @sql AS NVARCHAR(4000);
SET @lvl = 0;

-- Load row for input root to #SubsPath
-- The root's sort path is simply 1 converted to binary
INSERT INTO #SubsPath(nodeid, lvl, sortpath)
  SELECT empid, @lvl, CAST(1 AS BINARY(4))
  FROM dbo.Employees
  WHERE empid = @root;

-- Form a loop to load the next level of suboridnates
-- to #SubsPath in each iteration
WHILE @@rowcount > 0 AND @lvl < @maxlevels
BEGIN
  SET @lvl = @lvl + 1;

  -- Insert next level of subordinates
  -- Initially, just copy parent's path to child
  -- Note that IDENTITY values will be generated in #SubsPath
  -- based on input order by list
  --
  -- Then update the path of the employees in the current level
  -- to their parent's path + their rownum converted to binary
  INSERT INTO #SubsPath(nodeid, lvl, sortpath)
    SELECT C.empid, @lvl, P.sortpath
    FROM #SubsPath AS P
      JOIN dbo.Employees AS C
        ON P.lvl = @lvl - 1
        AND C.mgrid = P.nodeid
    ORDER BY -- determines order of siblings
      CASE WHEN @orderby = N'empid'   THEN empid   END,
      CASE WHEN @orderby = N'empname' THEN empname END,
      CASE WHEN @orderby = N'salary'  THEN salary  END;

  UPDATE #SubsPath
    SET sortpath = sortpath + CAST(rownum AS BINARY(4))
  WHERE lvl = @lvl;
END

-- Load the rows from #SubsPath to @SubsSort sorted by the binary
-- sort path
-- The target identity values in the sortval column will represent
-- the global sort value of the nodes within the result subtree
INSERT INTO #SubsSort(nodeid, lvl)
  SELECT nodeid, lvl FROM #SubsPath ORDER BY sortpath;

-- Return for each node the id, level and sort value
SELECT nodeid AS empid, lvl, sortval FROM #SubsSort
ORDER BY sortval;
GO

-- Get all employees with sort values by empname
-- (relying on proc's defaults)
EXEC dbo.usp_sortsubs @orderby = N'empname';

-- Get 3 levels of subordinates underneath employee 1
-- with sort values by empname
EXEC dbo.usp_sortsubs
  @root = 1,
  @maxlevels = 3,
  @orderby = N'empname';
GO

-- Listing 9-17: Script Returning All Employees, Sorted by empname
CREATE TABLE #Subs
(
  empid    INT NULL,
  lvl      INT NULL,
  sortval  INT NULL
);
CREATE UNIQUE CLUSTERED INDEX idx_uc_sortval ON #Subs(sortval);

-- By empname
INSERT INTO #Subs(empid, lvl, sortval)
  EXEC dbo.usp_sortsubs
    @orderby = N'empname';

SELECT E.empid, REPLICATE(' | ', lvl) + E.empname AS empname
FROM #Subs AS S
  JOIN dbo.Employees AS E
    ON S.empid = E.empid
ORDER BY sortval;

-- Listing 9-18: Script Returning All Employees, Sorted by salary
TRUNCATE TABLE #Subs;

INSERT INTO #Subs(empid, lvl, sortval)
  EXEC dbo.usp_sortsubs
    @orderby = N'salary';

SELECT E.empid, salary, REPLICATE(' | ', lvl) + E.empname AS empname
FROM #Subs AS S
  JOIN dbo.Employees AS E
    ON S.empid = E.empid
ORDER BY sortval;

-- Cleanup
DROP TABLE #Subs
GO

-- Listing 9-19: Sorting Hierarchy by empname, CTE Solution
DECLARE @root AS INT;
SET @root = 1;

WITH SubsCTE
AS
(
  SELECT empid, empname, 0 AS lvl,
    -- Path of root is 1 (binary)
    CAST(1 AS VARBINARY(MAX)) AS sortpath
  FROM dbo.Employees
  WHERE empid = @root

  UNION ALL

  SELECT C.empid, C.empname, P.lvl + 1,
    -- Path of child = parent's path + child row number (binary)
    P.sortpath + CAST(
      ROW_NUMBER() OVER(PARTITION BY C.mgrid
                        ORDER BY C.empname) -- sort col(s)
      AS BINARY(4))
  FROM SubsCTE AS P
    JOIN dbo.Employees AS C
      ON C.mgrid = P.empid
)
SELECT empid, ROW_NUMBER() OVER(ORDER BY sortpath) AS sortval,
  REPLICATE(' | ', lvl) + empname AS empname
FROM SubsCTE
ORDER BY sortval;
GO

-- Listing 9-20: Sorting Hierarchy by salary, CTE Solution
DECLARE @root AS INT;
SET @root = 1;

WITH SubsCTE
AS
(
  SELECT empid, empname, salary, 0 AS lvl,
    -- Path of root = 1 (binary)
    CAST(1 AS VARBINARY(MAX)) AS sortpath
  FROM dbo.Employees
  WHERE empid = @root

  UNION ALL

  SELECT C.empid, C.empname, C.salary, P.lvl + 1,
    -- Path of child = parent's path + child row number (binary)
    P.sortpath + CAST(
      ROW_NUMBER() OVER(PARTITION BY C.mgrid
                        ORDER BY C.salary) -- sort col(s)
      AS BINARY(4))
  FROM SubsCTE AS P
    JOIN dbo.Employees AS C
      ON C.mgrid = P.empid
)
SELECT empid, salary, ROW_NUMBER() OVER(ORDER BY sortpath) AS sortval,
  REPLICATE(' | ', lvl) + empname AS empname
FROM SubsCTE
ORDER BY sortval;

---------------------------------------------------------------------
-- Cycles
---------------------------------------------------------------------

-- Create a cyclic path
UPDATE dbo.Employees SET mgrid = 14 WHERE empid = 1;
GO

-- Listing 9-21: Detecting Cycles, CTE Solution
DECLARE @root AS INT;
SET @root = 1;

WITH SubsCTE
AS
(
  SELECT empid, empname, 0 AS lvl,
    CAST('.' + CAST(empid AS VARCHAR(10)) + '.'
         AS VARCHAR(MAX)) AS path,
    -- Obviously root has no cycle
    0 AS cycle
  FROM dbo.Employees
  WHERE empid = @root

  UNION ALL

  SELECT C.empid, C.empname, P.lvl + 1,
    CAST(P.path + CAST(C.empid AS VARCHAR(10)) + '.'
         AS VARCHAR(MAX)) AS path,
    -- Cycle detected if parent's path contains child's id
    CASE WHEN P.path LIKE '%.' + CAST(C.empid AS VARCHAR(10)) + '.%'
      THEN 1 ELSE 0 END
  FROM SubsCTE AS P
    JOIN dbo.Employees AS C
      ON C.mgrid = P.empid
)
SELECT empid, empname, cycle, path
FROM SubsCTE;
GO

-- Listing 9-22: Not Pursuing Cycles, CTE Solution
DECLARE @root AS INT;
SET @root = 1;

WITH SubsCTE
AS
(
  SELECT empid, empname, 0 AS lvl,
    CAST('.' + CAST(empid AS VARCHAR(10)) + '.'
         AS VARCHAR(MAX)) AS path,
    -- Obviously root has no cycle
    0 AS cycle
  FROM dbo.Employees
  WHERE empid = @root

  UNION ALL

  SELECT C.empid, C.empname, P.lvl + 1,
    CAST(P.path + CAST(C.empid AS VARCHAR(10)) + '.'
         AS VARCHAR(MAX)) AS path,
    -- Cycle detected if parent's path contains child's id
    CASE WHEN P.path LIKE '%.' + CAST(C.empid AS VARCHAR(10)) + '.%'
      THEN 1 ELSE 0 END
  FROM SubsCTE AS P
    JOIN dbo.Employees AS C
      ON C.mgrid = P.empid
      AND P.cycle = 0 -- do not pursue branch for parent with cycle
)
SELECT empid, empname, cycle, path
FROM SubsCTE;
GO

-- Listing 9-23: Isolating Cyclic Paths, CTE Solution
DECLARE @root AS INT;
SET @root = 1;

WITH SubsCTE
AS
(
  SELECT empid, empname, 0 AS lvl,
    CAST('.' + CAST(empid AS VARCHAR(10)) + '.'
         AS VARCHAR(MAX)) AS path,
    -- Obviously root has no cycle
    0 AS cycle
  FROM dbo.Employees
  WHERE empid = @root

  UNION ALL

  SELECT C.empid, C.empname, P.lvl + 1,
    CAST(P.path + CAST(C.empid AS VARCHAR(10)) + '.'
         AS VARCHAR(MAX)) AS path,
    -- Cycle detected if parent's path contains child's id
    CASE WHEN P.path LIKE '%.' + CAST(C.empid AS VARCHAR(10)) + '.%'
      THEN 1 ELSE 0 END
  FROM SubsCTE AS P
    JOIN dbo.Employees AS C
      ON C.mgrid = P.empid
      AND P.cycle = 0
)
SELECT path FROM SubsCTE WHERE cycle = 1;

-- Fix cyclic path
UPDATE dbo.Employees SET mgrid = NULL WHERE empid = 1;

---------------------------------------------------------------------
-- Materialized Path
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Maintaining Data
---------------------------------------------------------------------

-- Listing 9-24: DDL for Employees with Path
SET NOCOUNT ON;
USE tempdb;
GO
IF OBJECT_ID('dbo.Employees') IS NOT NULL
  DROP TABLE dbo.Employees;
GO
CREATE TABLE dbo.Employees
(
  empid   INT          NOT NULL PRIMARY KEY NONCLUSTERED,
  mgrid   INT          NULL     REFERENCES dbo.Employees,
  empname VARCHAR(25)  NOT NULL,
  salary  MONEY        NOT NULL,
  lvl     INT          NOT NULL,
  path    VARCHAR(900) NOT NULL UNIQUE CLUSTERED
);
CREATE UNIQUE INDEX idx_unc_mgrid_empid ON dbo.Employees(mgrid, empid);
GO

---------------------------------------------------------------------
-- Adding Employees who Manage No One (Leaves)
---------------------------------------------------------------------

-- Listing 9-25: Creation Script for Procedure usp_insertemp

---------------------------------------------------------------------
-- Stored Procedure: usp_insertemp,
--   Inserts new employee who manages no one into the table
---------------------------------------------------------------------
USE tempdb;
GO
IF OBJECT_ID('dbo.usp_insertemp') IS NOT NULL
  DROP PROC dbo.usp_insertemp;
GO
CREATE PROC dbo.usp_insertemp
  @empid   INT,
  @mgrid   INT,
  @empname VARCHAR(25),
  @salary  MONEY
AS

SET NOCOUNT ON;

-- Handle case where the new employee has no manager (root)
IF @mgrid IS NULL
  INSERT INTO dbo.Employees(empid, mgrid, empname, salary, lvl, path)
    VALUES(@empid, @mgrid, @empname, @salary,
      0, '.' + CAST(@empid AS VARCHAR(10)) + '.');
-- Handle subordinate case (non-root)
ELSE
  INSERT INTO dbo.Employees(empid, mgrid, empname, salary, lvl, path)
    SELECT @empid, @mgrid, @empname, @salary, 
      lvl + 1, path + CAST(@empid AS VARCHAR(10)) + '.'
    FROM dbo.Employees
    WHERE empid = @mgrid;
GO

-- Listing 9-26: Sample Data for Employees with Path
EXEC dbo.usp_insertemp
  @empid = 1, @mgrid = NULL, @empname = 'David', @salary = $10000.00;
EXEC dbo.usp_insertemp
  @empid = 2, @mgrid = 1, @empname = 'Eitan', @salary = $7000.00;
EXEC dbo.usp_insertemp
  @empid = 3, @mgrid = 1, @empname = 'Ina', @salary = $7500.00;
EXEC dbo.usp_insertemp
  @empid = 4, @mgrid = 2, @empname = 'Seraph', @salary = $5000.00;
EXEC dbo.usp_insertemp
  @empid = 5, @mgrid = 2, @empname = 'Jiru', @salary = $5500.00;
EXEC dbo.usp_insertemp
  @empid = 6, @mgrid = 2, @empname = 'Steve', @salary = $4500.00;
EXEC dbo.usp_insertemp
  @empid = 7, @mgrid = 3, @empname = 'Aaron', @salary = $5000.00;
EXEC dbo.usp_insertemp
  @empid = 8, @mgrid = 5, @empname = 'Lilach', @salary = $3500.00;
EXEC dbo.usp_insertemp
  @empid = 9, @mgrid = 7, @empname = 'Rita', @salary = $3000.00;
EXEC dbo.usp_insertemp
  @empid = 10, @mgrid = 5, @empname = 'Sean', @salary = $3000.00;
EXEC dbo.usp_insertemp
  @empid = 11, @mgrid = 7, @empname = 'Gabriel', @salary = $3000.00;
EXEC dbo.usp_insertemp
  @empid = 12, @mgrid = 9, @empname = 'Emilia', @salary = $2000.00;
EXEC dbo.usp_insertemp
  @empid = 13, @mgrid = 9, @empname = 'Michael', @salary = $2000.00;
EXEC dbo.usp_insertemp
  @empid = 14, @mgrid = 9, @empname = 'Didi', @salary = $1500.00;
GO

-- Examine data after load
SELECT empid, mgrid, empname, salary, lvl, path
FROM dbo.Employees
ORDER BY path;

---------------------------------------------------------------------
-- Moving a Subtree
---------------------------------------------------------------------

-- Listing 9-27: Creation Script for Procedure usp_movesubtree

---------------------------------------------------------------------
-- Stored Procedure: usp_movesubtree,
--   Moves a whole subtree of a given root to a new location
--   under a given manager
---------------------------------------------------------------------
USE tempdb;
GO
IF OBJECT_ID('dbo.usp_movesubtree') IS NOT NULL
  DROP PROC dbo.usp_movesubtree;
GO
CREATE PROC dbo.usp_movesubtree
  @root  INT,
  @mgrid INT
AS

SET NOCOUNT ON;

BEGIN TRAN;
  -- Update level and path of all employees in the subtree (E)
  -- Set level = 
  --   current level + new manager's level - old manager's level
  -- Set path = 
  --   in current path remove old manager's path 
  --   and substitute with new manager's path
  UPDATE E
    SET lvl  = E.lvl + NM.lvl - OM.lvl,
        path = STUFF(E.path, 1, LEN(OM.path), NM.path)
  FROM dbo.Employees AS E          -- E = Employees    (subtree)
    JOIN dbo.Employees AS R        -- R = Root         (one row)
      ON R.empid = @root
      AND E.path LIKE R.path + '%'
    JOIN dbo.Employees AS OM       -- OM = Old Manager (one row)
      ON OM.empid = R.mgrid
    JOIN dbo.Employees AS NM       -- NM = New Manager (one row)
      ON NM.empid = @mgrid;
  
  -- Update root's new manager
  UPDATE dbo.Employees SET mgrid = @mgrid WHERE empid = @root;
COMMIT TRAN;
GO

-- Before moving subtree
SELECT empid, REPLICATE(' | ', lvl) + empname AS empname, lvl, path
FROM dbo.Employees
ORDER BY path;

-- Move Subtree
BEGIN TRAN;

  EXEC dbo.usp_movesubtree
  @root  = 7,
  @mgrid = 10;

  -- After moving subtree
  SELECT empid, REPLICATE(' | ', lvl) + empname AS empname, lvl, path
  FROM dbo.Employees
  ORDER BY path;

ROLLBACK TRAN; -- rollback used in order not to apply the change

---------------------------------------------------------------------
-- Removing a Subtree
---------------------------------------------------------------------

-- Before deleteting subtree
SELECT empid, REPLICATE(' | ', lvl) + empname AS empname, lvl, path
FROM dbo.Employees
ORDER BY path;

-- Delete Subtree
BEGIN TRAN;

  DELETE FROM dbo.Employees
  WHERE path LIKE 
    (SELECT M.path + '%'
     FROM dbo.Employees as M
     WHERE M.empid = 7);

  -- After deleting subtree
  SELECT empid, REPLICATE(' | ', lvl) + empname AS empname, lvl, path
  FROM dbo.Employees
  ORDER BY path;

ROLLBACK TRAN; -- rollback used in order not to apply the change

---------------------------------------------------------------------
-- Querying Materialized Path
---------------------------------------------------------------------

-- Subtree of a given root
SELECT REPLICATE(' | ', E.lvl - M.lvl) + E.empname
FROM dbo.Employees AS E
  JOIN dbo.Employees AS M
    ON M.empid = 3 -- root
    AND E.path LIKE M.path + '%'
ORDER BY E.path;

-- Subtree of a given root, excluding root
SELECT REPLICATE(' | ', E.lvl - M.lvl - 1) + E.empname
FROM dbo.Employees AS E
  JOIN dbo.Employees AS M
    ON M.empid = 3
    AND E.path LIKE M.path + '_%'
ORDER BY E.path;

-- Leaf nodes under a given root
SELECT E.empid, E.empname
FROM dbo.Employees AS E
  JOIN dbo.Employees AS M
    ON M.empid = 3
    AND E.path LIKE M.path + '%'
WHERE NOT EXISTS
  (SELECT * 
   FROM dbo.Employees AS E2
   WHERE E2.mgrid = E.empid);

-- Subtree of a given root, limit number of levels
SELECT REPLICATE(' | ', E.lvl - M.lvl) + E.empname
FROM dbo.Employees AS E
  JOIN dbo.Employees AS M
    ON M.empid = 3
    AND E.path LIKE M.path + '%'
    AND E.lvl - M.lvl <= 2
ORDER BY E.path;

-- Nodes that are n levels under a given root
SELECT E.empid, E.empname
FROM dbo.Employees AS E
  JOIN dbo.Employees AS M
    ON M.empid = 3
    AND E.path LIKE M.path + '%'
    AND E.lvl - M.lvl = 2;

-- Ancestors of a given node (requires a table scan)
SELECT REPLICATE(' | ', M.lvl) + M.empname
FROM dbo.Employees AS E
  JOIN dbo.Employees AS M
    ON E.empid = 14
    AND E.path LIKE M.path + '%'
ORDER BY E.path;

-- Listing 9-28: Creating and Populating Auxiliary Table of Numbers
SET NOCOUNT ON;
USE tempdb;
GO
IF OBJECT_ID('dbo.Nums') IS NOT NULL
  DROP TABLE dbo.Nums;
GO
CREATE TABLE dbo.Nums(n INT NOT NULL PRIMARY KEY);
DECLARE @max AS INT, @rc AS INT;
SET @max = 8000;
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

-- Listing 9-29: Creation Script for Function fn_splitpath
USE tempdb;
GO
IF OBJECT_ID('dbo.fn_splitpath') IS NOT NULL
  DROP FUNCTION dbo.fn_splitpath;
GO
CREATE FUNCTION dbo.fn_splitpath(@empid AS INT) RETURNS TABLE
AS
RETURN
  SELECT
    n - LEN(REPLACE(LEFT(path, n), '.', '')) AS pos,
    CAST(SUBSTRING(path, n + 1,
           CHARINDEX('.', path, n+1) - n - 1) AS INT) AS empid
  FROM dbo.Employees
    JOIN dbo.Nums
      ON empid = @empid
      AND n < LEN(path)
      AND SUBSTRING(path, n, 1) = '.'
GO

-- Test fn_splitpath function
SELECT pos, empid FROM dbo.fn_splitpath(14);

-- Getting ancestors using fn_splitpath function
SELECT REPLICATE(' | ', lvl) + empname
FROM dbo.fn_splitpath(14) AS SP
  JOIN dbo.Employees AS E
    ON E.empid = SP.empid
ORDER BY path;

---------------------------------------------------------------------
-- Nested Sets
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Assigning Left and Right Values 
---------------------------------------------------------------------

-- Listing 9-30: Producing Binary Sort Paths Representing Nested Sets Relationships
USE tempdb;
GO
-- Create index to speed sorting siblings by empname, empid
CREATE UNIQUE INDEX idx_unc_mgrid_empname_empid
  ON dbo.Employees(mgrid, empname, empid);
GO

DECLARE @root AS INT;
SET @root = 1;

-- CTE with two numbers: 1 and 2
WITH TwoNumsCTE
AS
(
  SELECT 1 AS n UNION ALL SELECT 2
),
-- CTE with two binary sort paths for each node:
--   One smaller than descendants sort paths
--   One greater than descendants sort paths
SortPathCTE
AS
(
  SELECT empid, 0 AS lvl, n,
    CAST(n AS VARBINARY(MAX)) AS sortpath
  FROM dbo.Employees CROSS JOIN TwoNumsCTE
  WHERE empid = @root

  UNION ALL

  SELECT C.empid, P.lvl + 1, TN.n, 
    P.sortpath + CAST(
      (-1+ROW_NUMBER() OVER(PARTITION BY C.mgrid
                        -- *** determines order of siblings ***
                        ORDER BY C.empname, C.empid))/2*2+TN.n
      AS BINARY(4))
  FROM SortPathCTE AS P
    JOIN dbo.Employees AS C
      ON P.n = 1
      AND C.mgrid = P.empid
    CROSS JOIN TwoNumsCTE AS TN
)
SELECT * FROM SortPathCTE
ORDER BY sortpath;
GO

-- Listing 9-31: CTE Code That Creates Nested Sets Relationships
DECLARE @root AS INT;
SET @root = 1;

-- CTE with two numbers: 1 and 2
WITH TwoNumsCTE
AS
(
  SELECT 1 AS n UNION ALL SELECT 2
),
-- CTE with two binary sort paths for each node:
--   One smaller than descendants sort paths
--   One greater than descendants sort paths
SortPathCTE
AS
(
  SELECT empid, 0 AS lvl, n,
    CAST(n AS VARBINARY(MAX)) AS sortpath
  FROM dbo.Employees CROSS JOIN TwoNumsCTE
  WHERE empid = @root

  UNION ALL

  SELECT C.empid, P.lvl + 1, TN.n, 
    P.sortpath + CAST(
      ROW_NUMBER() OVER(PARTITION BY C.mgrid
                        -- *** determines order of siblings ***
                        ORDER BY C.empname, C.empid, TN.n)
      AS BINARY(4))
  FROM SortPathCTE AS P
    JOIN dbo.Employees AS C
      ON P.n = 1
      AND C.mgrid = P.empid
    CROSS JOIN TwoNumsCTE AS TN
),
-- CTE with Row Numbers Representing sortpath Order
SortCTE
AS
(
  SELECT empid, lvl,
    ROW_NUMBER() OVER(ORDER BY sortpath) AS sortval
  FROM SortPathCTE
),
-- CTE with Left and Right Values Representing
-- Nested Sets Relationships
NestedSetsCTE
AS
(
  SELECT empid, lvl, MIN(sortval) AS lft, MAX(sortval) AS rgt
  FROM SortCTE
  GROUP BY empid, lvl
)
SELECT * FROM NestedSetsCTE
ORDER BY lft;
GO

-- Listing 9-32: Creation Script for the Function fn_empsnestedsets

---------------------------------------------------------------------
-- Function: fn_empsnestedsets, Nested Sets Relationships
--
-- Input   : @root INT: Root of subtree
--
-- Output  : @NestedSets Table: employee id, level in the subtree,
--                              left and right values representing
--                              nested sets relationships
--
-- Process : * Loads subtree into @SortPath,
--             first root, then a level at a time.
--             Note: two instances of each employee are loaded;
--                   one representing left arm (n = 1),
--                   and one representing right (n = 2).
--             For each employee and arm, a binary path is constructed,
--             representing the nested sets poition.
--             The binary path has 4 bytes for each of the employee's
--             ancestors. For each ancestor, the 4 bytes represent
--             its position in the level (calculated with identity).
--             Finally @SortPath will contain a pair of rows for each 
--             employee along with a sort path represeting the arm's
--             nested sets position.
--           * Next, the rows from @SortPath are loaded
--             into @SortVals, sorted by sortpath. After the load,
--             an integer identity column sortval holds sort values
--             representing the nested sets position of each arm.
--           * The data from @SortVals is grouped by employee
--             generating the left and right values for each employee
--             in one row. The resultset is loaded into the
--             @NestedSets table, which is the function's output.
--             
---------------------------------------------------------------------
SET NOCOUNT ON;
USE tempdb;
GO
IF OBJECT_ID('dbo.fn_empsnestedsets') IS NOT NULL
  DROP FUNCTION dbo.fn_empsnestedsets;
GO
CREATE FUNCTION dbo.fn_empsnestedsets(@root AS INT)
  RETURNS @NestedSets TABLE
(
  empid INT NOT NULL PRIMARY KEY,
  lvl   INT NOT NULL,
  lft   INT NOT NULL,
  rgt   INT NOT NULL
)
AS
BEGIN
  DECLARE @lvl AS INT;
  SET @lvl = 0;

  -- @TwoNums: Table Variable with two numbers: 1 and 2
  DECLARE @TwoNums TABLE(n INT NOT NULL PRIMARY KEY);
  INSERT INTO @TwoNums(n) SELECT 1 AS n UNION ALL SELECT 2;
  
  -- @SortPath: Table Variable with two binary sort paths
  -- for each node:
  --   One smaller than descendants sort paths
  --   One greater than descendants sort paths
  DECLARE @SortPath TABLE
  (
    empid    INT            NOT NULL,
    lvl      INT            NOT NULL,
    n        INT            NOT NULL,
    sortpath VARBINARY(900) NOT NULL,
    rownum   INT            NOT NULL IDENTITY,
    UNIQUE(lvl, n, empid)
  );
  
  -- Load root into @SortPath
  INSERT INTO @SortPath(empid, lvl, n, sortpath)
    SELECT empid, @lvl, n,
      CAST(n AS BINARY(4)) AS sortpath
    FROM dbo.Employees CROSS JOIN @TwoNums
    WHERE empid = @root

  WHILE @@rowcount > 0
  BEGIN
    SET @lvl = @lvl + 1;

    -- Load next level into @SortPath
    INSERT INTO @SortPath(empid, lvl, n, sortpath)
      SELECT C.empid, @lvl, TN.n, P.sortpath
      FROM @SortPath AS P
        JOIN dbo.Employees AS C
          ON  P.lvl = @lvl - 1
          AND P.n = 1
          AND C.mgrid = P.empid
        CROSS JOIN @TwoNums AS TN
      -- *** Determines order of siblings ***
      ORDER BY C.empname, C.empid, TN.n;
    
    -- Update sort path to include child's position
    UPDATE @SortPath
      SET sortpath = sortpath + CAST(rownum AS BINARY(4))
    WHERE lvl = @lvl;
  END

  -- @SortVals: Table Variable with row numbers
  -- representing sortpath order
  DECLARE @SortVals TABLE
  (
    empid   INT NOT NULL,
    lvl     INT NOT NULL,
    sortval INT NOT NULL IDENTITY
  )

  -- Load data from @SortPath sorted by sortpath
  -- to generate sort values
  INSERT INTO @SortVals(empid, lvl)
    SELECT empid, lvl FROM @SortPath ORDER BY sortpath;

  -- Load data into @NestedStes generating left and right
  -- values representing nested sets relationships
  INSERT INTO @NestedSets(empid, lvl, lft, rgt)
    SELECT empid, lvl, MIN(sortval), MAX(sortval)
    FROM @SortVals
    GROUP BY empid, lvl

  RETURN;
END
GO

-- Test the fn_empsnestedsets function
SELECT * FROM dbo.fn_empsnestedsets(1)
ORDER BY lft;
GO

-- Listing 9-33: Materializing Nested Sets Relationships in a Table 
SET NOCOUNT ON;
USE tempdb;
GO

DECLARE @root AS INT;
SET @root = 1;

WITH TwoNumsCTE
AS
(
  SELECT 1 AS n UNION ALL SELECT 2
),
SortPathCTE
AS
(
  SELECT empid, 0 AS lvl, n,
    CAST(n AS VARBINARY(MAX)) AS sortpath
  FROM dbo.Employees CROSS JOIN TwoNumsCTE
  WHERE empid = @root

  UNION ALL

  SELECT C.empid, P.lvl + 1, TN.n, 
    P.sortpath + CAST(
      ROW_NUMBER() OVER(PARTITION BY C.mgrid
                        -- *** determines order of siblings ***
                        ORDER BY C.empname, C.empid, TN.n)
      AS BINARY(4))
  FROM SortPathCTE AS P
    JOIN dbo.Employees AS C
      ON P.n = 1
      AND C.mgrid = P.empid
    CROSS JOIN TwoNumsCTE AS TN
),
SortCTE
AS
(
  SELECT empid, lvl,
    ROW_NUMBER() OVER(ORDER BY sortpath) AS sortval
  FROM SortPathCTE
),
NestedSetsCTE
AS
(
  SELECT empid, lvl, MIN(sortval) AS lft, MAX(sortval) AS rgt
  FROM SortCTE
  GROUP BY empid, lvl
)
SELECT E.empid, E.empname, E.salary, NS.lvl, NS.lft, NS.rgt
INTO dbo.EmployeesNS
FROM NestedSetsCTE AS NS
  JOIN dbo.Employees AS E
    ON E.empid = NS.empid;

ALTER TABLE dbo.EmployeesNS ADD PRIMARY KEY NONCLUSTERED(empid);
CREATE UNIQUE CLUSTERED INDEX idx_unc_lft_rgt ON dbo.EmployeesNS(lft, rgt);
GO

---------------------------------------------------------------------
-- Querying
---------------------------------------------------------------------

-- Descendants of a given root
SELECT C.empid, REPLICATE(' | ', C.lvl - P.lvl) + C.empname AS empname
FROM dbo.EmployeesNS AS P
  JOIN dbo.EmployeesNS AS C
    ON P.empid = 3
    AND C.lft >= P.lft AND C.rgt <= P.rgt
ORDER BY C.lft;

-- Descendants of a given root, limiting 2 levels
SELECT C.empid, REPLICATE(' | ', C.lvl - P.lvl) + C.empname AS empname
FROM dbo.EmployeesNS AS P
  JOIN dbo.EmployeesNS AS C
    ON P.empid = 3
    AND C.lft >= P.lft AND C.rgt <= P.rgt
    AND C.lvl - P.lvl <= 2
ORDER BY C.lft;

-- Leaf nodes under a given root
SELECT C.empid, C.empname
FROM dbo.EmployeesNS AS P
  JOIN dbo.EmployeesNS AS C
    ON P.empid = 3
    AND C.lft >= P.lft AND C.rgt <= P.rgt
    AND C.rgt - C.lft = 1;

-- Count of subordinates of each node
SELECT empid, (rgt - lft - 1) / 2 AS cnt,
  REPLICATE(' | ', lvl) + empname AS empname
FROM dbo.EmployeesNS
ORDER BY lft;

-- Ancestors of a given node
SELECT P.empid, P.empname, P.lvl
FROM dbo.EmployeesNS AS P
  JOIN dbo.EmployeesNS AS C
    ON C.empid = 14
    AND C.lft >= P.lft AND C.rgt <= P.rgt;
GO

-- Cleanup
DROP TABLE dbo.EmployeesNS;
GO

---------------------------------------------------------------------
-- Transitive Closure
---------------------------------------------------------------------

---------------------------------------------------------------------
-- Directed Acyclic Graph (DAG)
---------------------------------------------------------------------

-- Listing 9-34: Transitive Closure of BOM (DAG)
WITH BOMTC
AS
(
  -- Return all first-level containment relationships
  SELECT assemblyid, partid
  FROM dbo.BOM
  WHERE assemblyid IS NOT NULL

  UNION ALL

  -- Return next-level containment relationships
  SELECT P.assemblyid, C.partid
  FROM BOMTC AS P
    JOIN dbo.BOM AS C
      ON C.assemblyid = P.partid
)
-- Return distinct pairs that have
-- transitive containment relationships
SELECT DISTINCT assemblyid, partid
FROM BOMTC;
GO

-- Listing 9-35: Creation Script for the fn_BOMTC UDF
IF OBJECT_ID('dbo.fn_BOMTC') IS NOT NULL
  DROP FUNCTION dbo.fn_BOMTC;
GO

CREATE FUNCTION fn_BOMTC() RETURNS @BOMTC TABLE
(
  assemblyid INT NOT NULL,
  partid     INT NOT NULL,
  PRIMARY KEY (assemblyid, partid)
)
AS
BEGIN
  INSERT INTO @BOMTC(assemblyid, partid)
    SELECT assemblyid, partid
    FROM dbo.BOM
    WHERE assemblyid IS NOT NULL

  WHILE @@rowcount > 0
    INSERT INTO @BOMTC
    SELECT P.assemblyid, C.partid
    FROM @BOMTC AS P
      JOIN dbo.BOM AS C
        ON C.assemblyid = P.partid
    WHERE NOT EXISTS
      (SELECT * FROM @BOMTC AS P2
       WHERE P2.assemblyid = P.assemblyid
       AND P2.partid = C.partid);

  RETURN;
END
GO

-- Use the fn_BOMTC UDF
SELECT assemblyid, partid FROM fn_BOMTC();
GO

-- Listing 9-36: All Paths in BOM
WITH BOMPaths
AS
(
  SELECT assemblyid, partid,
    1 AS distance, -- distance in first level is 1
    -- path in first level is .assemblyid.partid.
    '.' + CAST(assemblyid AS VARCHAR(MAX)) +
    '.' + CAST(partid     AS VARCHAR(MAX)) + '.' AS path
  FROM dbo.BOM
  WHERE assemblyid IS NOT NULL

  UNION ALL

  SELECT P.assemblyid, C.partid,
    -- distance in next level is parent's distance + 1
    P.distance + 1,
    -- path in next level is parent_path.child_partid.
    P.path + CAST(C.partid AS VARCHAR(MAX)) + '.'
  FROM BOMPaths AS P
    JOIN dbo.BOM AS C
      ON C.assemblyid = P.partid
)
-- Return all paths
SELECT * FROM BOMPaths;

-- Listing 9-37: Shortest Paths in BOM
WITH BOMPaths -- All paths
AS
(
  SELECT assemblyid, partid,
    1 AS distance,
    '.' + CAST(assemblyid AS VARCHAR(MAX)) +
    '.' + CAST(partid     AS VARCHAR(MAX)) + '.' AS path
  FROM dbo.BOM
  WHERE assemblyid IS NOT NULL

  UNION ALL

  SELECT P.assemblyid, C.partid,
    P.distance + 1,
    P.path + CAST(C.partid AS VARCHAR(MAX)) + '.'
  FROM BOMPaths AS P
    JOIN dbo.BOM AS C
      ON C.assemblyid = P.partid
),
BOMMinDist AS -- Minimum distance for each pair
(
  SELECT assemblyid, partid, MIN(distance) AS mindist
  FROM BOMPaths
  GROUP BY assemblyid, partid
)
-- Shortest path for each pair
SELECT BP.*
FROM BOMMinDist AS BMD
  JOIN BOMPaths AS BP
    ON BMD.assemblyid = BP.assemblyid
    AND BMD.partid = BP.partid
    AND BMD.mindist = BP.distance;
GO

---------------------------------------------------------------------
-- Undirected Cyclic Graph
---------------------------------------------------------------------

-- Listing 9-38: Transitive Closure of Roads (Undirected Cyclic Graph)
WITH Roads2 -- Two rows for each pair (f-->t, t-->f)
AS
(
  SELECT city1 AS from_city, city2 AS to_city FROM dbo.Roads
  UNION ALL
  SELECT city2, city1 FROM dbo.Roads
),
RoadPaths AS
(
  -- Return all first-level reachability pairs
  SELECT from_city, to_city,
    -- path is needed to identify cycles
    CAST('.' + from_city + '.' + to_city + '.' AS VARCHAR(MAX)) AS path
  FROM Roads2

  UNION ALL

  -- Return next-level reachability pairs
  SELECT F.from_city, T.to_city,
    CAST(F.path + T.to_city + '.' AS VARCHAR(MAX))
  FROM RoadPaths AS F
    JOIN Roads2 AS T
      -- if to_city appears in from_city's path, cycle detected
      ON CASE WHEN F.path LIKE '%.' + T.to_city + '.%'
              THEN 1 ELSE 0 END = 0
      AND F.to_city = T.from_city
)
-- Return Transitive Closure of Roads
SELECT DISTINCT from_city, to_city
FROM RoadPaths;
GO

-- Listing 9-39: Creation Script for the fn_RoadsTC UDF
IF OBJECT_ID('dbo.fn_RoadsTC') IS NOT NULL
  DROP FUNCTION dbo.fn_RoadsTC;
GO

CREATE FUNCTION dbo.fn_RoadsTC() RETURNS @RoadsTC TABLE (
  from_city VARCHAR(3) NOT NULL,
  to_city   VARCHAR(3) NOT NULL,
  PRIMARY KEY (from_city, to_city)
)
AS
BEGIN
  DECLARE @added as INT;

  INSERT INTO @RoadsTC(from_city, to_city)
    SELECT city1, city2 FROM dbo.Roads;

  SET @added = @@rowcount;

  INSERT INTO @RoadsTC
    SELECT city2, city1 FROM dbo.Roads

  SET @added = @added + @@rowcount;

  WHILE @added > 0 BEGIN

    INSERT INTO @RoadsTC
      SELECT DISTINCT TC.from_city, R.city2
      FROM @RoadsTC AS TC
        JOIN dbo.Roads AS R
          ON R.city1 = TC.to_city
      WHERE NOT EXISTS
        (SELECT * FROM @RoadsTC AS TC2
         WHERE TC2.from_city = TC.from_city
           AND TC2.to_city = R.city2)
        AND TC.from_city <> R.city2;

    SET @added = @@rowcount;

    INSERT INTO @RoadsTC
      SELECT DISTINCT TC.from_city, R.city1
      FROM @RoadsTC AS TC
        JOIN dbo.Roads AS R
          ON R.city2 = TC.to_city
      WHERE NOT EXISTS
        (SELECT * FROM @RoadsTC AS TC2
         WHERE TC2.from_city = TC.from_city
           AND TC2.to_city = R.city1)
        AND TC.from_city <> R.city1;

    SET @added = @added + @@rowcount;
  END
  RETURN;
END
GO

-- Use the fn_RoadsTC UDF
SELECT * FROM dbo.fn_RoadsTC();
GO

-- Listing 9-40: All paths and distances in Roads (15262 rows)
WITH Roads2
AS
(
  SELECT city1 AS from_city, city2 AS to_city, distance FROM dbo.Roads
  UNION ALL
  SELECT city2, city1, distance FROM dbo.Roads
),
RoadPaths AS
(
  SELECT from_city, to_city, distance,
    CAST('.' + from_city + '.' + to_city + '.' AS VARCHAR(MAX)) AS path
  FROM Roads2

  UNION ALL

  SELECT F.from_city, T.to_city, F.distance + T.distance,
    CAST(F.path + T.to_city + '.' AS VARCHAR(MAX))
  FROM RoadPaths AS F
    JOIN Roads2 AS T
      ON CASE WHEN F.path LIKE '%.' + T.to_city + '.%'
              THEN 1 ELSE 0 END = 0
      AND F.to_city = T.from_city
)
-- Return all paths and distances
SELECT * FROM RoadPaths;

-- Listing 9-41: Shortest paths in Roads
WITH Roads2
AS
(
  SELECT city1 AS from_city, city2 AS to_city, distance FROM dbo.Roads
  UNION ALL
  SELECT city2, city1, distance FROM dbo.Roads
),
RoadPaths AS
(
  SELECT from_city, to_city, distance,
    CAST('.' + from_city + '.' + to_city + '.' AS VARCHAR(MAX)) AS path
  FROM Roads2

  UNION ALL

  SELECT F.from_city, T.to_city, F.distance + T.distance,
    CAST(F.path + T.to_city + '.' AS VARCHAR(MAX))
  FROM RoadPaths AS F
    JOIN Roads2 AS T
      ON CASE WHEN F.path LIKE '%.' + T.to_city + '.%'
              THEN 1 ELSE 0 END = 0
      AND F.to_city = T.from_city
),
RoadsMinDist -- Min distance for each pair in TC
AS
(
  SELECT from_city, to_city, MIN(distance) AS mindist
  FROM RoadPaths
  GROUP BY from_city, to_city
)
-- Return shortest paths and distances
SELECT RP.*
FROM RoadsMinDist AS RMD
  JOIN RoadPaths AS RP
    ON RMD.from_city = RP.from_city
    AND RMD.to_city = RP.to_city
    AND RMD.mindist = RP.distance;
GO

-- Listing 9-42: Load Shortest Road Paths Into a Table
WITH Roads2
AS
(
  SELECT city1 AS from_city, city2 AS to_city, distance FROM dbo.Roads
  UNION ALL
  SELECT city2, city1, distance FROM dbo.Roads
),
RoadPaths AS
(
  SELECT from_city, to_city, distance,
    CAST('.' + from_city + '.' + to_city + '.' AS VARCHAR(MAX)) AS path
  FROM Roads2

  UNION ALL

  SELECT F.from_city, T.to_city, F.distance + T.distance,
    CAST(F.path + T.to_city + '.' AS VARCHAR(MAX))
  FROM RoadPaths AS F
    JOIN Roads2 AS T
      ON CASE WHEN F.path LIKE '%.' + T.to_city + '.%'
              THEN 1 ELSE 0 END = 0
      AND F.to_city = T.from_city
),
RoadsMinDist
AS
(
  SELECT from_city, to_city, MIN(distance) AS mindist
  FROM RoadPaths
  GROUP BY from_city, to_city
)
SELECT RP.*
INTO dbo.RoadPaths
FROM RoadsMinDist AS RMD
  JOIN RoadPaths AS RP
    ON RMD.from_city = RP.from_city
    AND RMD.to_city = RP.to_city
    AND RMD.mindist = RP.distance;

CREATE UNIQUE CLUSTERED INDEX idx_uc_from_city_to_city
  ON dbo.RoadPaths(from_city, to_city);
GO

-- Return shortest path between Los Angeles and New York
SELECT * FROM dbo.RoadPaths 
WHERE from_city = 'LAX' AND to_city = 'JFK';
GO

-- Listing 9-43: Creation Script for the fn_RoadsTC UDF
IF OBJECT_ID('dbo.fn_RoadsTC') IS NOT NULL
  DROP FUNCTION dbo.fn_RoadsTC;
GO
CREATE FUNCTION dbo.fn_RoadsTC() RETURNS @RoadsTC TABLE
(
  uniquifier INT          NOT NULL IDENTITY,
  from_city  VARCHAR(3)   NOT NULL,
  to_city    VARCHAR(3)   NOT NULL,
  distance   INT          NOT NULL,
  route      VARCHAR(MAX) NOT NULL,
  PRIMARY KEY (from_city, to_city, uniquifier)
)
AS
BEGIN
  DECLARE @added AS INT;

  INSERT INTO @RoadsTC
    SELECT city1 AS from_city, city2 AS to_city, distance,
      '.' + city1 + '.' + city2 + '.'
    FROM dbo.Roads;

  SET @added = @@rowcount;

  INSERT INTO @RoadsTC
    SELECT city2, city1, distance, '.' + city2 + '.' + city1 + '.'
    FROM dbo.Roads;

  SET @added = @added + @@rowcount;

  WHILE @added > 0 BEGIN
    INSERT INTO @RoadsTC
      SELECT DISTINCT TC.from_city, R.city2,
        TC.distance + R.distance, TC.route + city2 + '.'
      FROM @RoadsTC AS TC
        JOIN dbo.Roads AS R
          ON R.city1 = TC.to_city
      WHERE NOT EXISTS
        (SELECT * FROM @RoadsTC AS TC2
         WHERE TC2.from_city = TC.from_city
           AND TC2.to_city = R.city2
           AND TC2.distance <= TC.distance + R.distance)
        AND TC.from_city <> R.city2;

    SET @added = @@rowcount;

    INSERT INTO @RoadsTC
      SELECT DISTINCT TC.from_city, R.city1,
        TC.distance + R.distance, TC.route + city1 + '.'
      FROM @RoadsTC AS TC
        JOIN dbo.Roads AS R
          ON R.city2 = TC.to_city
      WHERE NOT EXISTS
        (SELECT * FROM @RoadsTC AS TC2
         WHERE TC2.from_city = TC.from_city
           AND TC2.to_city = R.city1
           AND TC2.distance <= TC.distance + R.distance)
        AND TC.from_city <> R.city1;

    SET @added = @added + @@rowcount;
  END
  RETURN;
END
GO

-- Return shortest paths and distances
SELECT from_city, to_city, distance, route
FROM (SELECT from_city, to_city, distance, route,
        RANK() OVER (PARTITION BY from_city, to_city
                     ORDER BY distance) AS rk
      FROM dbo.fn_RoadsTC()) AS RTC
WHERE rk = 1;
GO

-- Cleanup
DROP TABLE dbo.RoadPaths;
GO