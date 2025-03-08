--Milesone 4
--How many stores does the business have and in which countries?
SELECT 
    country_code, 
    COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

--Which locations currently have tghe most stores
SELECT 
    locality, 
    COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC;

--Which months produced the largest amount of sales
SELECT 
    dt.month, 
    SUM(p.product_price * o.product_quantity) AS total_sales
FROM orders_table o
JOIN dim_date_times dt ON o.date_uuid = dt.date_uuid
JOIN dim_card_details c ON o.card_number = c.card_number
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY dt.month
ORDER BY total_sales DESC;

--How many sales are coming from online
SELECT 
	COUNT(p.product_price * o.product_quantity) AS number_of_sales,
    SUM(o.product_quantity) AS product_quantity_count,
	CASE 
		WHEN ds.store_type = 'Web Portal' THEN 'Web'
		ELSE 'Offline'
	END AS LOCATION
FROM orders_table AS o
JOIN dim_store_details AS ds ON o.store_code = ds.store_code
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY LOCATION
ORDER BY number_of_sales DESC;

--What percentage of sales come through each type of store
SELECT 
	SUM(p.product_price * o.product_quantity) AS total_sales,
    COUNT(o.product_quantity) AS product_quantity_count,
	(COUNT(o.product_quantity) * 100.0 /
	(SELECT COUNT(*) FROM orders_table)) AS sales_made
FROM orders_table AS o
JOIN dim_store_details AS ds ON o.store_code = ds.store_code
JOIN dim_products AS p ON o.product_code = p.product_code
GROUP BY ds.store_type

--Which month in each year produced the highest cost of sales
SELECT 
	SUM(p.product_price * o.product_quantity) AS total_sales,
	dt.year,
	dt.month
FROM orders_table AS o
JOIN dim_date_times AS dt ON o.date_uuid = dt.date_uuid
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY dt.year,dt.month
ORDER BY total_sales DESC;

--What is our staff headcount
SELECT 
	SUM(staff_numbers) AS Total_Staff_Numbers,
	country_code
FROM dim_store_details
WHERE LENGTH(country_code) < 3
GROUP BY country_code
ORDER BY Total_Staff_Numbers DESC;

--Which german store is selling the most
SELECT 
	SUM(p.product_price * o.product_quantity) AS total_sales,
	ds.store_type,
	ds.country_code
FROM orders_table AS o
JOIN dim_store_details AS ds ON o.store_code = ds.store_code
JOIN dim_products p ON o.product_code = p.product_code
WHERE country_code = 'DE'
GROUP BY country_code,store_type
ORDER BY total_sales ASC;

--How quickly is the company making sales
WITH CompanySales AS (
    SELECT
        dt.year,
        dt.month,
        dt.day,
        dt.timestamp AS sale_time,
        dt.year || '-' || dt.month || '-' || dt.day || ' ' || dt.timestamp AS full_timestamp,
        LEAD(dt.year || '-' || dt.month || '-' || dt.day || ' ' || dt.timestamp) 
        OVER (ORDER BY dt.year, dt.month, dt.day, dt.timestamp) AS next_sale_time
    FROM
        orders_table AS o
    JOIN dim_date_times AS dt ON o.date_uuid = dt.date_uuid
)
SELECT
    year,
    CONCAT(
        FLOOR(AVG(EXTRACT(EPOCH FROM (next_sale_time::TIMESTAMP - full_timestamp::TIMESTAMP)) / 3600)), ' hours, ',
        FLOOR(AVG(EXTRACT(EPOCH FROM (next_sale_time::TIMESTAMP - full_timestamp::TIMESTAMP)) % 3600) / 60), ' minutes, ',
        FLOOR(AVG(EXTRACT(EPOCH FROM (next_sale_time::TIMESTAMP - full_timestamp::TIMESTAMP)) % 60)), ' seconds'
    ) AS actual_time_taken
FROM
    CompanySales
GROUP BY
    year
ORDER BY
    actual_time_taken ;
