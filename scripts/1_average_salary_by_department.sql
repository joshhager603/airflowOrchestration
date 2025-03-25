DROP TABLE IF EXISTS salary_to_department_analysis;

SELECT employee_data.department, AVG(employee_data.salary) AS average_salary
INTO salary_to_department_analysis
FROM employee_data
GROUP BY employee_data.department;