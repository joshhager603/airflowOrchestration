DROP TABLE IF EXISTS performance_by_salary_analysis;

SELECT employee_data.performance_rating, AVG(employee_data.salary) AS average_salary
INTO performance_by_salary_analysis
FROM employee_data
GROUP BY employee_data.performance_rating;