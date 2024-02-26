-- Query to select Python projects from the 'projects' table
SELECT 
    CONCAT('Project ID: ', project_id) AS Project_ID,
    CONCAT('Title: ', title) AS Title,
    CONCAT('Description: ', description) AS Description,
    CONCAT('Programming Language: ', programming_language) AS Programming_Language
FROM 
    projects
WHERE 
    programming_language = 'Python';
