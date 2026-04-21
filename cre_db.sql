/*
================================================================================
Shopee Thailand Data Warehouse Database Creation Script
================================================================================
This script creates the complete database schema for the Shopee Thailand DW
including Staging, DW, Cube, and SystemLog schemas.
================================================================================
*/

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DWShopeeTH')
BEGIN
    PRINT 'Creating database DWShopeeTH...';
    CREATE DATABASE DWShopeeTH;
END

-- Switch to DWShopeeTH database
/*
USE DWShopeeTH;
GO
*/

-- Create main schemas
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Staging')
BEGIN
    PRINT 'Creating schema Staging...';
    EXEC('CREATE SCHEMA Staging AUTHORIZATION dbo');
END
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'DW')
BEGIN
    PRINT 'Creating schema DW...';
    EXEC('CREATE SCHEMA DW AUTHORIZATION dbo');
END
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Cube')
BEGIN
    PRINT 'Creating schema Cube...';
    EXEC('CREATE SCHEMA Cube AUTHORIZATION dbo');
END

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'SystemLog')
BEGIN
    PRINT 'Creating schema SystemLog...';
    EXEC('CREATE SCHEMA SystemLog AUTHORIZATION dbo');
END

/*
================================================================================
Dimension Tables - Core DW Schema
================================================================================
These dimension tables contain descriptive attributes used in analysis.
*/

-- Customer Dimension: Contains customer profile information
DROP TABLE IF EXISTS [DW].[DimCustomer];
CREATE TABLE [DW].[DimCustomer] (
    CustomerID     VARCHAR(255) PRIMARY KEY,
    CustomerName   VARCHAR(255),
    Province        VARCHAR(100),
    City            VARCHAR(100),
    LoadExecutionID  BIGINT
);

-- Session Dimension: Tracks user sessions and device information
DROP TABLE IF EXISTS [DW].[DimSession];
CREATE TABLE [DW].[DimSession] (
    SessionID   VARCHAR(255) PRIMARY KEY,
    UserID      INT,
    DeviceType  VARCHAR(50),
    LoadExecutionID  BIGINT
);

-- Shipment Dimension: Contains shipment types and methods
DROP TABLE IF EXISTS [DW].[DimShipment];
CREATE TABLE [DW].[DimShipment] (
    ShipmentID   VARCHAR(255) PRIMARY KEY,
    ShipmentType VARCHAR(100),
    LoadExecutionID  BIGINT
);

-- Date Dimension: Standard date dimension with temporal attributes
DROP TABLE IF EXISTS [DW].[DimDate];
CREATE TABLE [DW].[DimDate] (
    [DateID]            BIGINT PRIMARY KEY CLUSTERED,
    [DateOfDay]         DATE NOT NULL,
    [DayName]           VARCHAR(20) NOT NULL,
    [DayNbOfTheWeek]    TINYINT NOT NULL,
    [DayNbOfTheMonth]   TINYINT NOT NULL,
    [DayNbOfTheYear]    SMALLINT NOT NULL,
    [MonthName]         VARCHAR(20) NOT NULL,
    [MonthNb]           TINYINT NOT NULL,
    [WeekNbOfTheMonth]  TINYINT NOT NULL,
    [QuarterNb]         TINYINT NOT NULL,
    [YearNb]            SMALLINT NOT NULL,
    [IsCampaignFlag]    BIT NOT NULL,
);

-- Seller Dimension: Seller profile and onboarding information
DROP TABLE IF EXISTS [DW].[DimSeller];
CREATE TABLE [DW].[DimSeller] (
    SellerID   VARCHAR(255) PRIMARY KEY,
    SellerName VARCHAR(255),
    JoinDate   DATE,
    Province    VARCHAR(255),
    City        VARCHAR(255),
    LoadExecutionID  BIGINT
);

-- Product Dimension: Product catalog with categories and commission rates
DROP TABLE IF EXISTS [DW].[DimProduct];
CREATE TABLE [DW].[DimProduct] (
    ProductID        VARCHAR(255) PRIMARY KEY,
    ProductName      VARCHAR(255),
    Category          VARCHAR(100),
    CommissionRate   NUMERIC(5,2),
    MaintenanceRate  NUMERIC(5,2),
    SellerID         VARCHAR(255),
    LoadExecutionID      BIGINT,
    CONSTRAINT fk_product_seller
        FOREIGN KEY (SellerID)
        REFERENCES [DW].[DimSeller](SellerID)
);

-- Campaign Dimension: Marketing campaigns and promotions
DROP TABLE IF EXISTS [DW].[DimCampaign];
CREATE TABLE [DW].[DimCampaign] (
    CampaignID   VARCHAR(255) PRIMARY KEY,
    CampaignName VARCHAR(255),
    CampaignType VARCHAR(100),
    LoadExecutionID  BIGINT
);

/*
================================================================================
Fact Table - Core DW Schema
================================================================================
Central fact table capturing transactional sales data with measures and foreign keys.
*/

-- Sales Fact Table: Main transactional fact table for sales analysis
DROP TABLE IF EXISTS [DW].[FactSales];
CREATE TABLE [DW].[FactSales] (
    ProductID          VARCHAR(255),
    SellerID           VARCHAR(255),
    ShipmentID         VARCHAR(255),
    CampaignID         VARCHAR(255),
    SessionID          VARCHAR(255),
    CustomerID         VARCHAR(255),
    OrderNo            INT,
    OrderLineNo       INT,
    OrderDateID       BIGINT,
    DueDateID         BIGINT,
    ShippedDateID     BIGINT,

    UnitPrice          NUMERIC(10,2),
    Discount           NUMERIC(5,2),
    DiscountAmount     NUMERIC(10,2),
    UnitPriceAfterDiscount NUMERIC(10,2),
    Quantity            INT,
    LineTotal          NUMERIC(12,2),
    CommissionAmount   NUMERIC(12,2),
    MaintenanceAmount  NUMERIC(12,2),
    ShippingFee        NUMERIC(10,2),
    LoadExecutionID      BIGINT,

    -- Composite PK (typical for fact tables)
    PRIMARY KEY (
        ProductID, SellerID, ShipmentID, CampaignID,
        SessionID, OrderDateID, CustomerID
    ),

    -- Foreign Keys
    FOREIGN KEY (ProductID) REFERENCES DW.DimProduct(ProductID),
    FOREIGN KEY (SellerID) REFERENCES DW.DimSeller(SellerID),
    FOREIGN KEY (ShipmentID) REFERENCES DW.DimShipment(ShipmentID),
    FOREIGN KEY (CampaignID) REFERENCES DW.DimCampaign(CampaignID),
    FOREIGN KEY (SessionID) REFERENCES DW.DimSession(SessionID),
    FOREIGN KEY (OrderDateID) REFERENCES DW.DimDate(DateID),
    FOREIGN KEY (DueDateID) REFERENCES DW.DimDate(DateID),
    FOREIGN KEY (ShippedDateID) REFERENCES DW.DimDate(DateID),
    FOREIGN KEY (CustomerID) REFERENCES DW.DimCustomer(CustomerID)
);

/*
================================================================================
Staging Tables - Staging Schema
================================================================================
These tables store raw data from source systems before transformation and loading
into DW tables. They include ModifiedDate and LoadExcutionID for ETL tracking.
*/

-- Staging: Customer data from source
DROP TABLE IF EXISTS [Staging].[StgCustomer];
CREATE TABLE [Staging].[StgCustomer] (
    CustomerID     VARCHAR(255) PRIMARY KEY,
    CustomerName   VARCHAR(255),
    Address         TEXT,
    Province        VARCHAR(100),
    City            VARCHAR(100),
    ModifiedDate    DATETIME CONSTRAINT DF_StgCustomer_ModifiedDate DEFAULT GETDATE() NOT NULL,
    LoadExecutionID  BIGINT
);

-- Staging: Session data from source
DROP TABLE IF EXISTS [Staging].[StgSession];
CREATE TABLE [Staging].[StgSession] (
    SessionID   VARCHAR(255) PRIMARY KEY,
    UserID      INT,
    DeviceType  VARCHAR(50),
    ModifiedDate    DATETIME CONSTRAINT DF_StgSession_ModifiedDate DEFAULT GETDATE() NOT NULL,
    LoadExecutionID  BIGINT
);

-- Staging: Shipment data from source
DROP TABLE IF EXISTS [Staging].[StgShipment];
CREATE TABLE [Staging].[StgShipment] (
    ShipmentID   VARCHAR(255) PRIMARY KEY,
    ShipmentType VARCHAR(100),
    ModifiedDate    DATETIME CONSTRAINT DF_StgShipment_ModifiedDate DEFAULT GETDATE() NOT NULL,
    LoadExecutionID  BIGINT
);

-- Staging: Seller data from source
DROP TABLE IF EXISTS [Staging].[StgSeller];
CREATE TABLE [Staging].[StgSeller] (
    SellerID   VARCHAR(255) PRIMARY KEY,
    SellerName VARCHAR(255),
    JoinDate   DATE,
    Province    VARCHAR(255),
    City        VARCHAR(255),
    ModifiedDate    DATETIME CONSTRAINT DF_StgSeller_ModifiedDate DEFAULT GETDATE() NOT NULL,
    LoadExecutionID  BIGINT
);

-- Staging: Product data from source
DROP TABLE IF EXISTS [Staging].[StgProduct];
CREATE TABLE [Staging].[StgProduct] (
    ProductID        VARCHAR(255) PRIMARY KEY,
    ProductName      VARCHAR(255),
    Category          VARCHAR(100),
    CommissionRate   NUMERIC(5,2),
    MaintenanceRate  NUMERIC(5,2),
    SellerID         VARCHAR(255),
    ModifiedDate    DATETIME CONSTRAINT DF_StgProduct_ModifiedDate DEFAULT GETDATE() NOT NULL,
    LoadExecutionID      BIGINT,
    CONSTRAINT fk_stg_product_seller
        FOREIGN KEY (SellerID)
        REFERENCES [Staging].[StgSeller](SellerID)
);

-- Staging: Campaign data from source
DROP TABLE IF EXISTS [Staging].[StgCampaign];
CREATE TABLE [Staging].[StgCampaign] (
    CampaignID   VARCHAR(255) PRIMARY KEY,
    CampaignName VARCHAR(255),
    CampaignType VARCHAR(100),
    ModifiedDate    DATETIME CONSTRAINT DF_StgCampaign_ModifiedDate DEFAULT GETDATE() NOT NULL,
    LoadExecutionID  BIGINT
);

-- Staging: Order Details data from source (main fact table staging)
DROP TABLE IF EXISTS [Staging].[StgOrderDetails];
CREATE TABLE [Staging].[StgOrderDetails] (
    OrderItemId       INT PRIMARY KEY,
    OrderId           INT,
    ProductId         VARCHAR(255),
    Quantity          INT,
    UnitPrice         NUMERIC(10,2),
    UnitPriceAfterDiscount NUMERIC(10,2),
    Discount          NUMERIC(5,2),
    LineTotal         NUMERIC(12,2),
    CommissionAmount  NUMERIC(12,2),
    MaintenanceAmount NUMERIC(12,2),
    ShippingFee       NUMERIC(10,2),
    CampaignFlag             BIT,
    ModifiedDate    DATETIME CONSTRAINT DF_StgOrderDetails_ModifiedDate DEFAULT GETDATE() NOT NULL,
    LoadExecutionID  BIGINT,
);

-- Staging: Order Header data from source (additional attributes for orders)
DROP TABLE IF EXISTS [Staging].[StgOrderHeader];
CREATE TABLE [Staging].[StgOrderHeader] (
    OrderId         INT PRIMARY KEY,
    CustomerId      VARCHAR(255),
    SessionId       VARCHAR(255),
    OrderDate       DATE,
    DueDate         DATE,
    ShippedDate     DATE,
    SubTotal        NUMERIC(12,2),
    ShippingFeeTotal NUMERIC(10,2),
    CommissionTotal  NUMERIC(12,2),
    MaintenanceTotal NUMERIC(12,2),
    TotalAmount      NUMERIC(12,2),
    CampaignId         VARCHAR(255),
    ModifiedDate    DATETIME CONSTRAINT DF_StgOrderHeader_ModifiedDate DEFAULT GETDATE() NOT NULL,
    LoadExecutionID  BIGINT
);

/*
================================================================================
System Log Tables - SystemLog Schema
================================================================================
These tables track ETL execution, performance metrics, and data quality issues.
Used for monitoring, auditing, and troubleshooting data loads.
*/

-- System Log: Environment configuration (Dev, Test, Prod, etc.)
DROP TABLE IF EXISTS [SystemLog].[LoadEnvironments];
CREATE TABLE [SystemLog].[LoadEnvironments] (
	[LoadEnvironmentId] BIGINT IDENTITY(1,1) NOT NULL  CONSTRAINT [PK_LoadEnvironments] PRIMARY KEY CLUSTERED,
    [EnvironmentName] NVARCHAR(25) NOT NULL,
    [EnvironmentDescription] NVARCHAR(1000) NULL
);

-- System Log: Applications that perform data loads
DROP TABLE IF EXISTS [SystemLog].[LoadApplications];
CREATE TABLE [SystemLog].[LoadApplications] (
    [LoadApplicationId] TINYINT IDENTITY(1,1) NOT NULL  CONSTRAINT [PK_LoadApplications] PRIMARY KEY,
    [ApplicationName] NVARCHAR(100) NOT NULL,
    [ApplicationDescription] NVARCHAR(200) NOT NULL
);

-- System Log: Execution status codes (Success, Failed, Running, etc.)
DROP TABLE IF EXISTS [SystemLog].[ExecutionStatus];
CREATE TABLE [SystemLog].[ExecutionStatus] (
    [ExecutionStatusId] TINYINT IDENTITY(1,1) NOT NULL  CONSTRAINT [PK_ExecutionStatus] PRIMARY KEY,
    [StatusName] NVARCHAR(50) NOT NULL,
    [StatusDescription] NVARCHAR(200) NULL
);

-- System Log: Message types for ETL notifications and alerts
DROP TABLE IF EXISTS [SystemLog].[ExecutionMessageTypes];
CREATE TABLE [SystemLog].[ExecutionMessageTypes] (
    [ExecutionMessageTypeId] TINYINT IDENTITY(1,1) NOT NULL  CONSTRAINT [PK_ExecutionMessageTypes] PRIMARY KEY,
    [MessageTypeName] NVARCHAR(50) NOT NULL,
    [MessageTypeDescription] NVARCHAR(200) NOT NULL,
    [SSISDBMessageType] SMALLINT NULL
);

-- System Log: Load job records
DROP TABLE IF EXISTS [SystemLog].[Loads];
CREATE TABLE [SystemLog].[Loads] (
    [LoadId] BIGINT NOT NULL IDENTITY(1, 1) CONSTRAINT [PK_LoadId] PRIMARY KEY CLUSTERED ,
    [LoadApplicationId] TINYINT NOT NULL ,
    [LoadStartDateTime] DATETIME NOT NULL ,
    [LoadEndDateTime] DATETIME NULL ,
    [LoadStatusId] TINYINT NOT NULL,
    FOREIGN KEY (LoadApplicationId) REFERENCES [SystemLog].[LoadApplications](LoadApplicationId),
    FOREIGN KEY (LoadStatusId) REFERENCES [SystemLog].[ExecutionStatus](ExecutionStatusId)
);

-- System Log: Individual execution records for each load job
DROP TABLE IF EXISTS [SystemLog].[LoadExecutions];
CREATE TABLE [SystemLog].[LoadExecutions] (
    [LoadExecutionId] BIGINT NOT NULL IDENTITY(1, 1) CONSTRAINT [PK_LoadExecutions] PRIMARY KEY CLUSTERED ,
    [SSISServerExecutionId] BIGINT NOT NULL ,
    [LoadId] BIGINT NOT NULL ,
    [ExecutionStatusId] TINYINT NOT NULL,
    [LoadEnvironmentId] BIGINT NOT NULL,
    [ExecutionStartDateTime] DATETIME NOT NULL ,
    [ExecutionEndDateTime] DATETIME NULL,
    FOREIGN KEY (LoadId) REFERENCES [SystemLog].[Loads](LoadId),
    FOREIGN KEY (ExecutionStatusId) REFERENCES [SystemLog].[ExecutionStatus](ExecutionStatusId),
    FOREIGN KEY (LoadEnvironmentId) REFERENCES [SystemLog].[LoadEnvironments](LoadEnvironmentId)
);

-- System Log: Records rejected during load processing (data quality issues)
DROP TABLE IF EXISTS [SystemLog].[ExecutionRejects];
CREATE TABLE [SystemLog].[ExecutionRejects] (
    [ExecutionRejectsId] BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_LoadRejects PRIMARY KEY ,
    [LoadExecutionId] BIGINT NOT NULL ,
    [Code] VARCHAR(100) NOT NULL ,
    [RowValues] VARCHAR(4000) NOT NULL,
    CONSTRAINT [FK_LoadExecutionRejects_To_LoadExecutions]
    FOREIGN KEY ([LoadExecutionId]) REFERENCES [SystemLog].[LoadExecutions] ([LoadExecutionId])
);

-- System Log: Detailed execution statistics and data flow performance metrics
DROP TABLE IF EXISTS [SystemLog].[ExecutionStatictics];
CREATE TABLE [SystemLog].[ExecutionStatictics] (
    [ExecutionStatisticId] BIGINT NOT NULL IDENTITY(1, 1) CONSTRAINT [PK_ExecutionStatistics] PRIMARY KEY CLUSTERED,
    [LoadExecutionId] BIGINT NOT NULL,                   -- Reference to load execution
    [SSISProjectName] NVARCHAR(260) NOT NULL,            -- SSIS project name
    [SSISPackageName] NVARCHAR(260) NOT NULL,            -- SSIS package name
    [SSISPackageStartDateTime] DATETIMEOFFSET NOT NULL,  -- Package execution start time
    [SSISPackageEndDateTime] DATETIMEOFFSET NOT NULL,    -- Package execution end time
    [DataFlowPathIdString] NVARCHAR(4000) NOT NULL,      -- Data flow path identifiers
    [SourceComponentName] NVARCHAR(500) NOT NULL,        -- Source component name
    [DestinationComponentName] NVARCHAR(500) NOT NULL,   -- Destination component name
    [RowsSent] INT NOT NULL,                             -- Row count transferred
    CONSTRAINT [FK_ExecutionStatictics_To_LoadExecutions]
    FOREIGN KEY ([LoadExecutionId]) REFERENCES [SystemLog].[LoadExecutions] ([LoadExecutionId])
);

/*
================================================================================
Database Schema Creation Complete
================================================================================
Schema Summary:
- DW Schema: 7 dimension tables + 1 fact table for analytical queries
- Staging Schema: 8 staging tables for raw data ingestion
- Cube Schema: Reserved for future OLAP implementation
- SystemLog Schema: 8 audit and monitoring tables for ETL tracking

Next Steps:
1. Configure ETL packages to load data into Staging tables
2. Create stored procedures for transforming Staging to DW
3. Implement SSIS packages for automated data loading
4. Set up monitoring and alerting for load execution
================================================================================
*/
