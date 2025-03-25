DROP TABLE IF EXISTS salary_by_country_analysis;

SELECT employee_data.country, AVG(employee_data.salary) AS average_salary
INTO salary_by_country_analysis
FROM employee_data
GROUP BY employee_data.country;