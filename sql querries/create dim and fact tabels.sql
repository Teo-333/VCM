-- Dimension Table: Dim_AuctionHouses_SCD (SCD Type 2)
CREATE TABLE Dim_AuctionHouses_SCD (
    SurrogateKey SERIAL PRIMARY KEY,
    AuctionHouseID INT NOT NULL,
    Name VARCHAR(100),
    Location VARCHAR(100),
    StartDate DATE NOT NULL,
    EndDate DATE,
    IsCurrent BOOLEAN,
    UNIQUE (AuctionHouseID, StartDate)
);

-- Dimension Table: Dim_Cars
CREATE TABLE Dim_Cars (
    CarID INT PRIMARY KEY,
    Make VARCHAR(50),
    Model VARCHAR(50),
    Year INT,
    VIN VARCHAR(17),
    Condition VARCHAR(50)
);

-- Dimension Table: Dim_Bidders
CREATE TABLE Dim_Bidders (
    BidderID INT PRIMARY KEY,
    Name VARCHAR(100),
    Address VARCHAR(255),
    Phone VARCHAR(15),
    Email VARCHAR(100)
);

-- Dimension Table: Dim_Date
CREATE TABLE Dim_Date (
    DateID SERIAL PRIMARY KEY,
    Date DATE,
    Year INT,
    Month INT,
    Day INT,
    Quarter INT
);


-- Fact Table: Fact_Auctions
CREATE TABLE Fact_Auctions (
    FactAuctionID SERIAL PRIMARY KEY,
    AuctionID INT,
    DateID INT,
    AuctionHouseKey INT,
    TotalSales DECIMAL(15, 2),
    NumberOfCars INT,
    FOREIGN KEY (AuctionHouseKey) REFERENCES Dim_AuctionHouses_SCD(SurrogateKey),
    FOREIGN KEY (DateID) REFERENCES Dim_Date(DateID)
);

-- Fact Table: Fact_Bids
CREATE TABLE Fact_Bids (
    FactBidID SERIAL PRIMARY KEY,
    BidID INT,
    DateID INT,
    CarID INT,
    BidderID INT,
    BidAmount DECIMAL(15, 2),
    FOREIGN KEY (CarID) REFERENCES Dim_Cars(CarID),
    FOREIGN KEY (BidderID) REFERENCES Dim_Bidders(BidderID),
    FOREIGN KEY (DateID) REFERENCES Dim_Date(DateID)
);

-- Indexes for optimizing queries
CREATE INDEX idx_fact_auctions_dateid ON Fact_Auctions(DateID);
CREATE INDEX idx_fact_bids_dateid ON Fact_Bids(DateID);

-- Procedure to insert or update Auction Houses
CREATE OR REPLACE PROCEDURE Update_AuctionHouse(
    p_AuctionHouseID INT,
    p_Name VARCHAR,
    p_Location VARCHAR,
    p_UpdateDate DATE
) LANGUAGE plpgsql AS $$
DECLARE
    v_CurrentRecord RECORD;
BEGIN
  
    SELECT * INTO v_CurrentRecord
    FROM Dim_AuctionHouses_SCD
    WHERE AuctionHouseID = p_AuctionHouseID AND IsCurrent = TRUE;


    IF FOUND THEN
        IF v_CurrentRecord.Name != p_Name OR v_CurrentRecord.Location != p_Location THEN
            
            UPDATE Dim_AuctionHouses_SCD
            SET EndDate = p_UpdateDate, IsCurrent = FALSE
            WHERE SurrogateKey = v_CurrentRecord.SurrogateKey;
            
        
            INSERT INTO Dim_AuctionHouses_SCD (AuctionHouseID, Name, Location, StartDate, IsCurrent)
            VALUES (p_AuctionHouseID, p_Name, p_Location, p_UpdateDate, TRUE);
        END IF;
    ELSE
    
        INSERT INTO Dim_AuctionHouses_SCD (AuctionHouseID, Name, Location, StartDate, IsCurrent)
        VALUES (p_AuctionHouseID, p_Name, p_Location, p_UpdateDate, TRUE);
    END IF;
END;
$$;

