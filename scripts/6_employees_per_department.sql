DROP TABLE IF EXISTS employees_per_department;

SELECT employee_data.department, COUNT(employee_data.employee_id) AS num_employees
INTO employees_per_department
FROM employee_data
GROUP BY employee_data.department;