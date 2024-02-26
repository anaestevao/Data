-- Create a new database named 'portfolio'
CREATE DATABASE IF NOT EXISTS portfolio;

-- Use the 'portfolio' database
USE portfolio;

-- Create a table to store project information
CREATE TABLE IF NOT EXISTS projects (
    project_id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(100),
    description TEXT,
    programming_language VARCHAR(50)
);

-- Insert sample data into the 'projects' table
INSERT INTO projects (title, description, programming_language)
VALUES 
    ('Data Analysis Project 1', 'Analyze sales data using Python and Pandas', 'Python'),
    ('Data Visualization Project 1', 'Create interactive dashboards using Tableau', 'Tableau'),
    ('SQL Project 1', 'Optimize database performance using SQL queries', 'SQL'),
    ('Machine Learning Project 1', 'Build predictive models using scikit-learn', 'Python'),
    ('Data Cleaning Project 1', 'Clean and preprocess messy data using Python', 'Python');

-- Query the 'projects' table to verify data insertion
SELECT * FROM projects;
