
/*****************************************************************************/
/*********************** Data Cleaning in SQL *******************************/
/*****************************************************************************/

-- 08.05.2025
SELECT * FROM world_layoffs.layoffs;

-- Create the header of the staging table
CREATE TABLE world_layoffs.layoffs_staging
LIKE  world_layoffs.layoffs;

-- Add rows to the staging table
INSERT world_layoffs.layoffs_staging
SELECT * FROM world_layoffs.layoffs;

SELECT * FROM world_layoffs.layoffs_staging;

-- 1. Remove duplicates

-- Check if you have any duplicates
WITH duplicate_cte AS
(SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging)

SELECT * FROM duplicate_cte
WHERE row_num>1;

-- Company Casper showed up on the list of duplicates. Take a look at it.
SELECT * FROM world_layoffs.layoffs_staging
WHERE company='Casper';

-- Delete duplicates by creating another table (layoffs_staging2)
CREATE TABLE `world_layoffs.layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO world_layoffs.layoffs_staging2
SELECT *,
ROW_NUMBER () OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;

SELECT * FROM world_layoffs.layoffs_staging2;

DELETE FROM world_layoffs.layoffs_staging2
WHERE row_num>1;

SELECT * FROM world_layoffs.layoffs_staging2;

-- 2. Standardize the data
SELECT company, trim(company) FROM world_layoffs.layoffs_staging2;

-- Remove leading and trailing blanks
UPDATE world_layoffs.layoffs_staging2
SET company=TRIM(company);

SELECT company FROM world_layoffs.layoffs_staging2;

SELECT DISTINCT (industry) FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- Combine Crypto, Crypto Currency, CryptoCurrency
SELECT * FROM world_layoffs.layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE world_layoffs.layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT (industry) FROM world_layoffs.layoffs_staging2
ORDER BY 1;

SELECT DISTINCT (location) FROM world_layoffs.layoffs_staging2
ORDER BY 1;

SELECT DISTINCT (country) FROM world_layoffs.layoffs_staging2
ORDER BY 1; /*there are duplicates*/

SELECT * FROM world_layoffs.layoffs_staging2
WHERE country LIKE 'United States%'
ORDER BY 1;

UPDATE world_layoffs.layoffs_staging2
SET COUNTRY=TRIM(TRAILING '.' FROM country) /*this removes the period at the end */
WHERE country  LIKE 'United States%';

SELECT DISTINCT (country) FROM world_layoffs.layoffs_staging2
ORDER BY 1;

-- Date is currently a text/string and needs to be changed to date format for Time Series Analysis using str_to_date function
SELECT `date`,
str_to_date(`date`, '%m/%d/%Y') /*this formats date in the YYYY-MM-DD format, which is a standard*/
FROM world_layoffs.layoffs_staging2;

UPDATE world_layoffs.layoffs_staging2
SET `date`=str_to_date(`date`, '%m/%d/%Y');

SELECT `date` FROM world_layoffs.layoffs_staging2;

-- Now change data type of column date to DATE
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

-- 3. Null values or blank values
SELECT * FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL;

SELECT industry FROM world_layoffs.layoffs_staging2
WHERE industry IS NULL OR industry='';

-- Try to populate missing fields(like industry for company Airbnb) by looking at other entries
SELECT * FROM world_layoffs.layoffs_staging2
WHERE company='Airbnb'; /*this company's industry is Travel*/

-- Change blanks to NULLS first
UPDATE world_layoffs.layoffs_staging2 
SET industry=NULL
WHERE industry='';

-- Populate 
UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- 4. Remove unnecessary columns or rows

-- Remove rows that have total_laid_off NULL AND percentage_laid_off  NULL
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Drop column row_num
ALTER TABLE world_layoffs.layoffs_staging2
DROP column row_num;
