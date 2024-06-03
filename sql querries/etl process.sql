-- Step 1: Drop existing tables if they exist
DROP TABLE IF EXISTS Fact_Bids;
DROP TABLE IF EXISTS Fact_Auctions;
DROP TABLE IF EXISTS Dim_Date;
DROP TABLE IF EXISTS Dim_Bidders;
DROP TABLE IF EXISTS Dim_Cars;
DROP TABLE IF EXISTS Dim_AuctionHouses_SCD;
DROP TABLE IF EXISTS Dim_CarOwners;
DROP TABLE IF EXISTS Dim_CarOwnerships;
DROP TABLE IF EXISTS Dim_Payments;
DROP TABLE IF EXISTS staging_payments;
DROP TABLE IF EXISTS staging_carownership;
DROP TABLE IF EXISTS staging_bids;
DROP TABLE IF EXISTS staging_bidders;
DROP TABLE IF EXISTS staging_cars;
DROP TABLE IF EXISTS staging_auctions;
DROP TABLE IF EXISTS staging_auctionhouses;
DROP TABLE IF EXISTS staging_carowners;
DROP TABLE IF EXISTS staging_carconditions;

-- Step 2: Create tables

-- Dimension Table: Dim_AuctionHouses_SCD (SCD Type 2)
CREATE TABLE IF NOT EXISTS Dim_AuctionHouses_SCD (
    SurrogateKey SERIAL PRIMARY KEY,
    AuctionHouseID INT NOT NULL,
    Name VARCHAR(100),
    Location VARCHAR(100),
    UNIQUE (AuctionHouseID)
);

-- Dimension Table: Dim_Cars
CREATE TABLE IF NOT EXISTS Dim_Cars (
    CarID INT PRIMARY KEY,
    Make VARCHAR(50),
    Model VARCHAR(50),
    Year INT,
    VIN VARCHAR(17),
    Condition VARCHAR(50)
);

-- Dimension Table: Dim_Bidders
CREATE TABLE IF NOT EXISTS Dim_Bidders (
    BidderID INT PRIMARY KEY,
    Name VARCHAR(100),
    Address VARCHAR(255),
    Phone VARCHAR(15),
    Email VARCHAR(100)
);

-- Dimension Table: Dim_Date
CREATE TABLE IF NOT EXISTS Dim_Date (
    DateID SERIAL PRIMARY KEY,
    Date DATE UNIQUE,
    Year INT,
    Month INT,
    Day INT,
    Quarter INT
);

-- Dimension Table: Dim_CarOwners
CREATE TABLE IF NOT EXISTS Dim_CarOwners (
    OwnerID INT PRIMARY KEY,
    OwnerName VARCHAR(100)
);

-- Dimension Table: Dim_CarOwnerships
CREATE TABLE IF NOT EXISTS Dim_CarOwnerships (
    CarID INT,
    OwnerID INT,
    PurchaseDate DATE,
    SaleDate DATE,
    PRIMARY KEY (CarID, OwnerID),
    FOREIGN KEY (CarID) REFERENCES Dim_Cars(CarID),
    FOREIGN KEY (OwnerID) REFERENCES Dim_CarOwners(OwnerID)
);

-- Fact Table: Fact_Auctions
CREATE TABLE IF NOT EXISTS Fact_Auctions (
    FactAuctionID SERIAL PRIMARY KEY,
    AuctionID INT UNIQUE,
    DateID INT,
    AuctionHouseKey INT,
    TotalSales DECIMAL(15, 2),
    NumberOfCars INT,
    FOREIGN KEY (AuctionHouseKey) REFERENCES Dim_AuctionHouses_SCD(SurrogateKey),
    FOREIGN KEY (DateID) REFERENCES Dim_Date(DateID)
);

-- Fact Table: Fact_Bids
CREATE TABLE IF NOT EXISTS Fact_Bids (
    FactBidID SERIAL PRIMARY KEY,
    BidID INT UNIQUE,
    DateID INT,
    CarID INT,
    BidderID INT,
    BidAmount DECIMAL(15, 2),
    FOREIGN KEY (CarID) REFERENCES Dim_Cars(CarID),
    FOREIGN KEY (BidderID) REFERENCES Dim_Bidders(BidderID),
    FOREIGN KEY (DateID) REFERENCES Dim_Date(DateID)
);

-- Dimension Table: Dim_Payments
CREATE TABLE IF NOT EXISTS Dim_Payments (
    PaymentID INT PRIMARY KEY,
    PaymentDate TIMESTAMP,
    Amount DECIMAL(15, 2),
    BidID INT,
    PaymentMethod VARCHAR(50),
    FOREIGN KEY (BidID) REFERENCES Fact_Bids(BidID)
);

-- Step 3: Create temporary staging tables
CREATE TEMP TABLE staging_auctionhouses AS TABLE AuctionHouses WITH NO DATA;
CREATE TEMP TABLE staging_auctions AS TABLE Auctions WITH NO DATA;
CREATE TEMP TABLE staging_cars AS TABLE Cars WITH NO DATA;
CREATE TEMP TABLE staging_bidders AS TABLE Bidders WITH NO DATA;
CREATE TEMP TABLE staging_bids AS TABLE Bids WITH NO DATA;
CREATE TEMP TABLE staging_carowners AS TABLE CarOwners WITH NO DATA;
CREATE TEMP TABLE staging_carownership AS TABLE CarOwnership WITH NO DATA;
CREATE TEMP TABLE staging_payments AS TABLE Payments WITH NO DATA;

-- Step 4: Extract data into staging tables
INSERT INTO staging_auctionhouses SELECT * FROM AuctionHouses;
INSERT INTO staging_auctions SELECT * FROM Auctions;
INSERT INTO staging_cars SELECT * FROM Cars;
INSERT INTO staging_bidders SELECT * FROM Bidders;
INSERT INTO staging_bids SELECT * FROM Bids;
INSERT INTO staging_carowners SELECT * FROM CarOwners;
INSERT INTO staging_carownership SELECT * FROM CarOwnership;
INSERT INTO staging_payments SELECT * FROM Payments;

-- Step 5: Transform data

-- Update Dim_AuctionHouses_SCD for SCD Type 2
INSERT INTO Dim_AuctionHouses_SCD (AuctionHouseID, Name, Location)
SELECT s.AuctionHouseID, s.Name, s.Location
FROM staging_auctionhouses s
LEFT JOIN Dim_AuctionHouses_SCD d ON s.AuctionHouseID = d.AuctionHouseID 
WHERE d.AuctionHouseID IS NULL OR (d.Name != s.Name OR d.Location != s.Location);

-- Load Dim_Cars
INSERT INTO Dim_Cars (CarID, Make, Model, Year, VIN, Condition)
SELECT s.CarID, s.Make, s.Model, s.Year, s.VIN, s.Condition
FROM staging_cars s
LEFT JOIN Dim_Cars d ON s.CarID = d.CarID
WHERE d.CarID IS NULL;

-- Load Dim_Bidders
INSERT INTO Dim_Bidders (BidderID, Name, Address, Phone, Email)
SELECT s.BidderID, s.Name, s.Address, s.Phone, s.Email
FROM staging_bidders s
LEFT JOIN Dim_Bidders d ON s.BidderID = d.BidderID
WHERE d.BidderID IS NULL;

-- Load Dim_CarOwners
INSERT INTO Dim_CarOwners (OwnerID, OwnerName)
SELECT s.OwnerID, d.OwnerName
FROM staging_carowners s
LEFT JOIN Dim_CarOwners d ON s.OwnerID = d.OwnerID
WHERE d.OwnerID IS NULL;

-- Load Dim_CarOwnerships
INSERT INTO Dim_CarOwnerships (CarID, OwnerID, PurchaseDate, SaleDate)
SELECT s.CarID, s.OwnerID, s.PurchaseDate, s.SaleDate
FROM staging_carownership s
LEFT JOIN Dim_CarOwnerships d ON s.CarID = d.CarID AND s.OwnerID = d.OwnerID
WHERE d.CarID IS NULL AND d.OwnerID IS NULL;

-- Populate Dim_Date
INSERT INTO Dim_Date (Date, Year, Month, Day, Quarter)
SELECT
    d::DATE,
    EXTRACT(YEAR FROM d),
    EXTRACT(MONTH FROM d),
    EXTRACT(DAY FROM d),
    CASE
        WHEN EXTRACT(MONTH FROM d) IN (1, 2, 3) THEN 1
        WHEN EXTRACT(MONTH FROM d) IN (4, 5, 6) THEN 2
        WHEN EXTRACT(MONTH FROM d) IN (7, 8, 9) THEN 3
        ELSE 4
    END
FROM GENERATE_SERIES('2020-01-01'::DATE, '2030-12-31'::DATE, '1 day'::INTERVAL) AS d
ON CONFLICT (Date) DO NOTHING;

-- Load Fact_Auctions
INSERT INTO Fact_Auctions (AuctionID, DateID, AuctionHouseKey, TotalSales, NumberOfCars)
SELECT
    s.AuctionID,
    d.DateID,
    h.SurrogateKey,
    COALESCE(SUM(b.BidAmount), 0) AS TotalSales,
    COALESCE(COUNT(DISTINCT c.CarID), 0) AS NumberOfCars
FROM
    staging_auctions s
LEFT JOIN
    staging_bids b ON s.AuctionID = s.AuctionID
LEFT JOIN
    staging_cars c ON b.CarID = c.CarID
JOIN
    Dim_Date d ON s.AuctionDate = d.Date
JOIN
    Dim_AuctionHouses_SCD h ON s.AuctionHouseID = h.AuctionHouseID
GROUP BY
    s.AuctionID, d.DateID, h.SurrogateKey
ON CONFLICT (AuctionID) DO NOTHING;

SELECT * FROM staging_bids;
SELECT * FROM Dim_Date;


-- Load Fact_Bids
INSERT INTO Fact_Bids (BidID, CarID, BidderID, BidAmount)
SELECT
    s.BidID,
    s.CarID,
    s.BidderID,
    s.BidAmount
FROM
    staging_bids s
ON CONFLICT (BidID) DO NOTHING;


SELECT * FROM Fact_Bids

-- Load Dim_Payments
INSERT INTO Dim_Payments (PaymentID, PaymentDate, Amount, BidID, PaymentMethod)
SELECT s.PaymentID, s.PaymentDate, s.Amount, s.BidID, s.PaymentMethod
FROM staging_payments s
LEFT JOIN Dim_Payments d ON s.PaymentID = d.PaymentID
WHERE d.PaymentID IS NULL AND s.BidID IN (SELECT BidID FROM Fact_Bids);

-- Step 6: Drop temporary staging tables
DROP TABLE IF EXISTS staging_auctionhouses;
DROP TABLE IF EXISTS staging_auctions;
DROP TABLE IF EXISTS staging_cars;
DROP TABLE IF EXISTS staging_bidders;
DROP TABLE IF EXISTS staging_bids;
DROP TABLE IF EXISTS staging_carowners;
DROP TABLE IF EXISTS staging_carownership;
DROP TABLE IF EXISTS staging_payments;
