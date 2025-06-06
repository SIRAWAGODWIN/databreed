CREATE DATABASE global_analysis;
USE global_analysis;

-- DROP DATABASE IF EXISTS global_analysis;

-- Create tables without foreign keys
CREATE TABLE IF NOT EXISTS gdp_2020 (
    Country VARCHAR(100) PRIMARY KEY,
    Nominal_gdp_per_capita DECIMAL(15,3) NOT NULL,
    PPP_gdp_per_capita DECIMAL(15,3) NOT NULL,
    GDP_growth_percentage DECIMAL(5,3),
    Rise_fall_GDP VARCHAR(100)
);
-- DROP TABLE gdp_2020;

CREATE TABLE IF NOT EXISTS covid_19 (
    Country VARCHAR(100) PRIMARY KEY,
    Confirmed INT NOT NULL,
    Deaths INT NOT NULL,
    Recovered INT NOT NULL,
    Active INT NOT NULL,
    New_cases INT,
    New_deaths INT,
    New_recovered INT,
    WHO_Region VARCHAR(50)
);

-- Load GDP data
LOAD DATA INFILE '/var/lib/mysql-files/gdp_2020.csv'
INTO TABLE gdp_2020
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- Load COVID data
LOAD DATA INFILE '/var/lib/mysql-files/covid_19.csv'
INTO TABLE covid_19
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SET SQL_SAFE_UPDATES = 0;

DELETE FROM covid_19
WHERE Country NOT IN (SELECT Country FROM gdp_2020);

ALTER TABLE covid_19
ADD CONSTRAINT fk_covid_country
FOREIGN KEY (Country) REFERENCES gdp_2020(Country)
ON DELETE CASCADE
ON UPDATE CASCADE;

SELECT * 
FROM covid_19
LIMIT 5;

-- Step 1: Clean WHO_Region values
UPDATE covid_19
SET WHO_Region = TRIM(REPLACE(REPLACE(REPLACE(WHO_Region, '\r', ''), '\n', ''), '\t', ''))
WHERE WHO_Region LIKE '%Europe%';

-- Step 2: Check distinct values (to verify cleanup)
SELECT DISTINCT WHO_Region FROM covid_19;

-- Step 3: Count rows for cleaned value
SELECT COUNT(*) FROM covid_19 WHERE WHO_Region = 'Europe';

-- task 3 Solved
SELECT 
    Country,
    Confirmed,
    Deaths,
    ROUND((Deaths * 100.0 / Confirmed), 2) AS death_rate_percentage
FROM 
    covid_19
WHERE 
    WHO_Region = 'Europe'
    AND Confirmed > 10000
ORDER BY 
    death_rate_percentage DESC;
    
-- task 4  
SELECT 
    g.Country,
    g.GDP_growth_percentage,
    c.Deaths AS COVID_Deaths
FROM 
    gdp_2020 g
JOIN 
    covid_19 c ON g.Country = c.Country
WHERE 
    g.Rise_fall_GDP = 'short fall'
ORDER BY 
    c.Deaths DESC;
    

-- task 5
-- Find countries that are either in the top 10% of GDP per capita OR in the top 10% of COVID deaths, but not both.
-- Calculate top 10% threshold for GDP per capita
-- Calculate top 10% threshold for Covid deaths
-- combine both
WITH gdp_rank AS (
    SELECT 
        Country,
        NTILE(10) OVER (ORDER BY Nominal_gdp_per_capita DESC) AS gdp_percentile
    FROM gdp_2020
),
covid_rank AS (
    SELECT 
        Country,
        NTILE(10) OVER (ORDER BY Deaths DESC) AS covid_percentile
    FROM covid_19
),
combined AS (
    SELECT
        g.Country AS Country,
        CASE WHEN g.gdp_percentile = 1 THEN 1 ELSE 0 END AS top_gdp,
        CASE WHEN c.covid_percentile = 1 THEN 1 ELSE 0 END AS top_covid
    FROM gdp_rank g
    INNER JOIN covid_rank c
    ON g.Country = c.Country
)
SELECT
    Country
FROM combined
WHERE (top_gdp = 1 AND top_covid = 0)
   OR (top_gdp = 0 AND top_covid = 1);
   
 --  Task 6
-- Show the average, minimum, and maximum GDP growth percentage by WHO region, only for regions with more than 5 countries in the dataset.
    SELECT
    c.WHO_Region,
    AVG(g.GDP_growth_percentage) AS avg_growth,
    MIN(g.GDP_growth_percentage) AS min_growth,
    MAX(g.GDP_growth_percentage) AS max_growth,
    COUNT(*) AS country_count
FROM
    gdp_2020 g
JOIN
    covid_19 c ON g.Country = c.Country
GROUP BY
    c.WHO_Region
HAVING
    COUNT(*) > 5;
    
-- Task 7
-- Calculate an "economic impact score" for each country based on COVID-19 deaths and GDP drop,
-- rank the top 10 most impacted countries.

-- Step 1: estimate population from COVID deaths data.
-- extract deaths and region for later use.
WITH country_population AS (
    SELECT 
        c.Country,
        c.Deaths,
        c.WHO_Region
    FROM covid_19 c
),

-- Step 2: Extract GDP growth percentages and calculate GDP "drop" (negative growth).
gdp_changes AS (
    SELECT 
        Country,
        GDP_growth_percentage,
        CASE 
            WHEN GDP_growth_percentage < 0 THEN ABS(GDP_growth_percentage)
            ELSE 0 
        END AS gdp_drop_percentage
    FROM gdp_2020
),

-- Step 3: Approximate deaths per million for each country.
-- NULLIF ensures we don't divide by zero if all deaths are 0.
covid_deaths_per_million AS (
    SELECT 
        c.Country,
        (c.Deaths * 1000000) / NULLIF((SELECT MAX(Deaths) FROM covid_19 WHERE Deaths > 0), 0) AS estimated_deaths_per_million
    FROM covid_19 c
    WHERE c.Deaths > 0  -- Exclude countries with no reported deaths
)

-- Step 4: Calculate the economic impact score:
-- We join the CTEs above to combine COVID and GDP data.
SELECT 
    c.Country,
    c.WHO_Region,
    d.estimated_deaths_per_million AS covid_deaths_per_million,
    g.gdp_drop_percentage AS gdp_per_capita_drop_percentage,
    (d.estimated_deaths_per_million * g.gdp_drop_percentage) / 1000 AS economic_impact_score
FROM 
    covid_19 c
JOIN 
    covid_deaths_per_million d ON c.Country = d.Country  -- Match countries with COVID deaths
JOIN 
    gdp_changes g ON c.Country = g.Country  -- Match countries with GDP data
WHERE 
    g.gdp_drop_percentage > 0  -- Only include countries with GDP drops
    AND d.estimated_deaths_per_million > 0  -- Only include countries with COVID deaths
ORDER BY 
    economic_impact_score DESC  
LIMIT 10;  

-- Task 8
-- Identify countries with GDP growth data but missing COVID data, then 
SELECT g.Country
FROM gdp_2020 g
LEFT JOIN covid_19 c ON g.Country = c.Country
WHERE c.Country IS NULL;

-- insert placeholder COVID records for these countries with 0 values for all metrics and 'Unknown' region.
INSERT INTO covid_19 (Country, Confirmed, Deaths, Recovered, Active, New_cases, New_deaths, New_recovered, WHO_Region)
SELECT 
    g.Country,
    0 AS Confirmed,
    0 AS Deaths,
    0 AS Recovered,
    0 AS Active,
    0 AS New_cases,
    0 AS New_deaths,
    0 AS New_recovered,
    'Unknown' AS WHO_Region
FROM gdp_2020 g
LEFT JOIN covid_19 c ON g.Country = c.Country
WHERE c.Country IS NULL;


