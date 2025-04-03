/*
==========================================
Create Database and Schemas
==========================================
script purpose:
	This script creates a new database named 'DataWarehouse' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three
	new schemas within the database: 'bronze', 'silver' and 'gold'.

Warning:
	Running this script will drop the entire 'DataWarehouse' database if it exists.

*/


use master;

--Drop and recreate the 'DataWarehouse' database
if exists (select 1 from sys.databases where name='DataWarehouse')
begin
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse;
end;
go


--Create the 'DataWarehouse' database
create database DataWarehouse;
go

use DataWarehouse;
go

--Create Schemas
create schema bronze;
go

create schema silver;
go

create schema gold;
go

