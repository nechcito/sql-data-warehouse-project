/*
Vytvorenie database a vytvorenie Schemas pre EXTRACT,TRANSFORM ,LOAD 
*/
--Vytvorenie database
USE master;
CREATE DATABASE DataWarehouse;
USE DataWarehouse;
--vytvorenie vrstiev
Create Schema bronz
Create Schema silver;
GO
Create Schema golden;
GO
