-- Tables:
-- 1. products - FDA approved drug product information
-- 2. exclusivity - Drug exclusivity records
-- 3. patent - Drug patent information

USE drug_database;

DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS exclusivity;
DROP TABLE IF EXISTS patent;
DROP TABLE IF EXISTS sales;

-- products table (products.csv)
CREATE TABLE products(
	-- id is useful for referencing rows for specific purposes from other tables
	id INT AUTO_INCREMENT PRIMARY KEY,
	appl_no VARCHAR(20) COMMENT 'Application Number',
	appl_type CHAR(1) COMMENT 'Application Type (A or N)',
	ingredient TEXT COMMENT 'Active ingredients',
    dosage VARCHAR(100) COMMENT 'Dosage form',
    form VARCHAR(100) COMMENT 'Physical form',
    route VARCHAR(100) COMMENT 'Administration route',
    trade_name VARCHAR(255) COMMENT 'Brand/trade name',
    applicant VARCHAR(255) COMMENT 'Applicant short name',
    strength VARCHAR(255) COMMENT 'Drug strength/concentration',
    te_code VARCHAR(20) COMMENT 'Therapeutic equivalence code',
    approval_date DATE COMMENT 'FDA approval date',
    rld VARCHAR(10) COMMENT 'Reference Listed Drug (Yes/No)',
    rs VARCHAR(10) COMMENT 'Reference Standard (Yes/No)',
    type VARCHAR(20) COMMENT 'Product type (RX, OTC, DISCN)',
    applicant_full_name TEXT COMMENT 'Full applicant company name',
    number_of_approvals INT COMMENT 'Number of distinct approvals for this ingredient and dosage combination',

-- Indexes help filtering optimization (makes it easier for look ups, etc.)
	INDEX idx_appl_no (appl_no),
    INDEX idx_trade_name (trade_name),
    INDEX idx_ingredient (ingredient(255)),
    INDEX idx_approval_date (approval_date),
    INDEX idx_form (form),
    INDEX idx_route (route),
    INDEX idx_type (type),
	-- Full-text index for searching specific keywords within ingredient and trade name
    FULLTEXT INDEX ft_ingredient_tradename (ingredient, trade_name)
);

-- exclusivity table (exclusivity.csv)
CREATE TABLE exclusivity (
    id INT AUTO_INCREMENT PRIMARY KEY,
    appl_no VARCHAR(20) COMMENT 'Application Number',
    appl_type CHAR(1) COMMENT 'Application Type',
    ingredient TEXT COMMENT 'Active ingredient(s)',
    dosage VARCHAR(100) COMMENT 'Dosage form',
    form VARCHAR(100) COMMENT 'Physical form',
    route VARCHAR(100) COMMENT 'Administration route',
    trade_name VARCHAR(255) COMMENT 'Brand/trade name',
    strength VARCHAR(255) COMMENT 'Drug strength',
    exclusivity_code VARCHAR(50) COMMENT 'Exclusivity code (e.g., RTO, ODE, NS)',
    exclusivity_date DATE COMMENT 'Exclusivity expiration date',
    
    INDEX idx_appl_no (appl_no),
    INDEX idx_trade_name (trade_name),
    INDEX idx_exclusivity_date (exclusivity_date),
    INDEX idx_exclusivity_code (exclusivity_code),
    FULLTEXT INDEX ft_ingredient_tradename (ingredient, trade_name)
);

-- patent table (patent.csv)
CREATE TABLE patent (
    id INT AUTO_INCREMENT PRIMARY KEY,
    appl_no VARCHAR(20) COMMENT 'Application Number',
    appl_type CHAR(1) COMMENT 'Application Type',
    ingredient TEXT COMMENT 'Active ingredient(s)',
    dosage VARCHAR(100) COMMENT 'Dosage form',
    form VARCHAR(100) COMMENT 'Physical form',
    route VARCHAR(100) COMMENT 'Administration route',
    trade_name VARCHAR(255) COMMENT 'Brand/trade name',
    applicant VARCHAR(255) COMMENT 'Applicant short name',
    strength VARCHAR(255) COMMENT 'Drug strength',
    patent_no VARCHAR(50) COMMENT 'Patent number',
    patent_expire_date_text VARCHAR(50) COMMENT 'Patent expiration date',
    drug_substance_flag CHAR(3) COMMENT 'Drug substance patent (Y/N)',
    drug_product_flag CHAR(3) COMMENT 'Drug product patent (Y/N)',
    patent_use_code VARCHAR(50) COMMENT 'Patent use code',
    submission_date DATE COMMENT 'Patent submission date',
    
    INDEX idx_appl_no (appl_no),
    INDEX idx_trade_name (trade_name),
    INDEX idx_patent_no (patent_no),
    INDEX idx_patent_expire (patent_expire_date_text),
    INDEX idx_submission_date (submission_date),
    FULLTEXT INDEX ft_ingredient_tradename (ingredient, trade_name)
);

-- sales table (sales.csv)
CREATE TABLE sales (
	id INT AUTO_INCREMENT PRIMARY KEY,
	appl_no VARCHAR(20) COMMENT 'Application Number',
	ingredient TEXT COMMENT 'Active ingredient(s)',
	route VARCHAR(100) COMMENT 'Administration route',
	route_extended VARCHAR(255) COMMENT 'Extended route description',
	dosage VARCHAR(100) COMMENT 'Dosage form',
	manufacturer VARCHAR(255) COMMENT 'Manufacturer name',
	strength VARCHAR(100) COMMENT 'Drug strength',
	pack_quantity INT COMMENT 'Pack quantity',
	ndc_number VARCHAR(11) COMMENT 'National Drug Code',
	labeler_code VARCHAR(5) COMMENT 'Labeler code',
	product_code VARCHAR(4) COMMENT 'Product code',
	sales DECIMAL(15,2) COMMENT 'Sales amount',
	packs FLOAT COMMENT 'Number of packs sold',
	quantity FLOAT COMMENT 'Total units sold',
	wac DECIMAL(10,2) COMMENT 'Wholesale acquisition cost',
	price DECIMAL (10,2) COMMENT 'Net or reported price',
	number_of_sellers INT COMMENT 'Number of sellers',

	INDEX idx_appl_no (appl_no),
	INDEX idx_manufacturer(manufacturer),
	INDEX idx_ndc (ndc_number),
	INDEX idx_labeler_product (labeler_code, product_code),
	FULLTEXT INDEX ft_ingredient(ingredient)
);

-- Create a view that joins all 3 tables on appl_no and patent_no
-- A view is a virtual table based on the the result of a select query
-- Shows selected product, patent, exclusivity data together (allows user to get information from multiple tables at once)
-- DROP VIEW IF EXISTS product_full_view;

-- CREATE VIEW product_full_view AS
-- SELECT
-- 	p.id as p_id,
-- 	p.appl_no,
-- 	p.appl_type,
-- 	p.ingredient,
-- 	p.dosage,
-- 	p.form,
-- 	p.route,
-- 	p.trade_name,
-- 	p.applicant,
-- 	p.approval_date,
-- 	p.number_of_approvals,
-- 	pt.patent_no, 
-- 	pt.patent_expire_date_text, 
-- 	pt.submission_date AS patent_submission_date,
-- 	e.exclusivity_code,
-- 	e.exclusivity_date
-- FROM products p
-- LEFT JOIN patent pt ON p.appl_no = pt.appl_no
-- LEFT JOIN exclusivity e ON p.appl_no = e.appl_no;
	-- products: appl_no, appl_type, ingredient, dosage, form, route, trade_name, applicant, approval_date, number_of_approvals
	-- patent: appl_no, patent_no, patent_expire_date_text, submission_date
	-- exclusivity: exclusivity_code (ODE*, NCE, CGT), exclusivity_date 


-- sample queries

-- Products approved after 2020 with ORAL route
SELECT * FROM products 
WHERE approval_date >= '2020-01-01' 
AND route = 'ORAL'
ORDER BY approval_date DESC
LIMIT 50;

-- Products containing specific ingredient
SELECT * FROM products 
WHERE ingredient LIKE '%BUDESONIDE%'
ORDER BY trade_name;

-- Products with specific form and type
SELECT * FROM products 
WHERE form = 'TABLET' 
AND type = 'RX'
LIMIT 50;

-- Full-text search on ingredient and trade name
SELECT * FROM products 
-- 'MATCH' 'AGAINST' 'IN NATURAL LANGUAGE MODE' allows searching for keywords in the specified columns
WHERE MATCH(ingredient, trade_name) AGAINST('ASPIRIN' IN NATURAL LANGUAGE MODE);

-- Exclusivity records expiring in 2026
SELECT * FROM exclusivity 
WHERE exclusivity_date BETWEEN '2026-01-01' AND '2026-12-31'
ORDER BY exclusivity_date;

-- Patents by application number
SELECT * FROM patent 
WHERE appl_no = '20610';

-- Count products by route
SELECT route, COUNT(*) as count 
FROM products 
GROUP BY route 
ORDER BY count DESC;

-- Count products by year
SELECT YEAR(approval_date) as year, COUNT(*) as count 
FROM products 
WHERE approval_date IS NOT NULL
GROUP BY YEAR(approval_date) 
ORDER BY year DESC;