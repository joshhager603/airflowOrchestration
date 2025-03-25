DROP TABLE IF EXISTS salary_to_tenure_analysis;

SELECT employee_data.years_of_experience, AVG(employee_data.salary) AS average_salary
INTO salary_to_tenure_analysis
FROM employee_data
GROUP BY employee_data.years_of_experience
ORDER BY employee_data.years_of_experience ASC;