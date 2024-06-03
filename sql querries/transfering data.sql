-- Step 1: Create temporary staging tables

CREATE TEMP TABLE staging_auctionhouses (
    AuctionHouseID INT,
    Name VARCHAR(100),
    Location VARCHAR(100)
);

CREATE TEMP TABLE staging_auctions (
    AuctionID INT,
    AuctionDate DATE,
    AuctionHouseID INT
);

CREATE TEMP TABLE staging_cars (
    CarID INT,
    Make VARCHAR(50),
    Model VARCHAR(50),
    Year INT,
    VIN VARCHAR(17),
    Condition VARCHAR(50),
    AuctionID INT
);

CREATE TEMP TABLE staging_bidders (
    BidderID INT,
    Name VARCHAR(100),
    Address VARCHAR(255),
    Phone VARCHAR(15),
    Email VARCHAR(100)
);

CREATE TEMP TABLE staging_bids (
    BidID INT,
    BidAmount DECIMAL(10, 2),
    BidDate TIMESTAMP,
    CarID INT,
    BidderID INT
);

CREATE TEMP TABLE staging_carowners (
    OwnerID INT,
    Name VARCHAR(100),
    Address VARCHAR(255),
    Phone VARCHAR(15),
    Email VARCHAR(100)
);

CREATE TEMP TABLE staging_carownership (
    CarID INT,
    OwnerID INT,
    PurchaseDate DATE,
    SaleDate DATE
);

CREATE TEMP TABLE staging_payments (
    PaymentID INT,
    PaymentDate TIMESTAMP,
    Amount DECIMAL(10, 2),
    BidID INT,
    PaymentMethod VARCHAR(50)
);

-- Step 2: Load data into staging tables using COPY

COPY staging_auctionhouses FROM 'C:\Work\DB\CourseWork\csv\auctionHouses.csv' DELIMITER ',' CSV HEADER;
COPY staging_auctions FROM 'C:\Work\DB\CourseWork\csv\auctions.csv' DELIMITER ',' CSV HEADER;
COPY staging_cars FROM 'C:\Work\DB\CourseWork\csv\cars.csv' DELIMITER ',' CSV HEADER;
COPY staging_bidders FROM 'C:\Work\DB\CourseWork\csv\bidders.csv' DELIMITER ',' CSV HEADER;
COPY staging_bids FROM 'C:\Work\DB\CourseWork\csv\bids.csv' DELIMITER ',' CSV HEADER;
COPY staging_carowners FROM 'C:\Work\DB\CourseWork\csv\carOwners.csv' DELIMITER ',' CSV HEADER;
COPY staging_carownership FROM 'C:\Work\DB\CourseWork\csv\carOwnerships.csv' DELIMITER ',' CSV HEADER;
COPY staging_payments FROM 'C:\Work\DB\CourseWork\csv\payments.csv' DELIMITER ',' CSV HEADER;

-- Step 3: Insert new records into target tables

-- AuctionHouses
INSERT INTO AuctionHouses (AuctionHouseID, Name, Location)
SELECT s.AuctionHouseID, s.Name, s.Location
FROM staging_auctionhouses s
LEFT JOIN AuctionHouses t ON s.AuctionHouseID = t.AuctionHouseID
WHERE t.AuctionHouseID IS NULL;

-- Auctions
INSERT INTO Auctions (AuctionID, AuctionDate, AuctionHouseID)
SELECT s.AuctionID, s.AuctionDate, s.AuctionHouseID
FROM staging_auctions s
LEFT JOIN Auctions t ON s.AuctionID = t.AuctionID
WHERE t.AuctionID IS NULL;

-- Cars
INSERT INTO Cars (CarID, Make, Model, Year, VIN, Condition, AuctionID)
SELECT s.CarID, s.Make, s.Model, s.Year, s.VIN, s.Condition, s.AuctionID
FROM staging_cars s
LEFT JOIN Cars t ON s.CarID = t.CarID
WHERE t.CarID IS NULL;

-- Bidders
INSERT INTO Bidders (BidderID, Name, Address, Phone, Email)
SELECT s.BidderID, s.Name, s.Address, s.Phone, s.Email
FROM staging_bidders s
LEFT JOIN Bidders t ON s.BidderID = t.BidderID
WHERE t.BidderID IS NULL;

-- Bids
INSERT INTO Bids (BidID, BidAmount, BidDate, CarID, BidderID)
SELECT s.BidID, s.BidAmount, s.BidDate, s.CarID, s.BidderID
FROM staging_bids s
LEFT JOIN Bids t ON s.BidID = t.BidID
WHERE t.BidID IS NULL;

-- CarOwners
INSERT INTO CarOwners (OwnerID, Name, Address, Phone, Email)
SELECT s.OwnerID, s.Name, s.Address, s.Phone, s.Email
FROM staging_carowners s
LEFT JOIN CarOwners t ON s.OwnerID = t.OwnerID
WHERE t.OwnerID IS NULL;

-- CarOwnership
INSERT INTO CarOwnership (CarID, OwnerID, PurchaseDate, SaleDate)
SELECT s.CarID, s.OwnerID, s.PurchaseDate, s.SaleDate
FROM staging_carownership s
LEFT JOIN CarOwnership t ON s.CarID = t.CarID AND s.OwnerID = t.OwnerID
WHERE t.CarID IS NULL AND t.OwnerID IS NULL;

-- Payments
INSERT INTO Payments (PaymentID, PaymentDate, Amount, BidID, PaymentMethod)
SELECT s.PaymentID, s.PaymentDate, s.Amount, s.BidID, s.PaymentMethod
FROM staging_payments s
LEFT JOIN Payments t ON s.PaymentID = t.PaymentID
WHERE t.PaymentID IS NULL;

-- Drop temporary staging tables
DROP TABLE IF EXISTS staging_auctionhouses;
DROP TABLE IF EXISTS staging_auctions;
DROP TABLE IF EXISTS staging_cars;
DROP TABLE IF EXISTS staging_bidders;
DROP TABLE IF EXISTS staging_bids;
DROP TABLE IF EXISTS staging_carowners;
DROP TABLE IF EXISTS staging_carownership;
DROP TABLE IF EXISTS staging_payments;


