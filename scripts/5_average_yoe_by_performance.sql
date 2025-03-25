DROP TABLE IF EXISTS yoe_by_performance_analysis;

SELECT employee_data.performance_rating, AVG(employee_data.years_of_experience) AS average_yoe
INTO yoe_by_performance_analysis
FROM employee_data
GROUP BY employee_data.performance_rating;