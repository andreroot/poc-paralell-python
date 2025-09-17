SELECT --s.name as schema_name, t.name as table_name, 
     c.name
FROM sys.columns AS c
INNER JOIN sys.tables AS t ON t.object_id = c.object_id
INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
WHERE t.name = 'operation' AND s.name = 'dbo';

/**
SELECT OBJECT_ID(db.name + '.' + 'operation') 
FROM sys.databases db
WHERE name  = 'BookComercial'
**/
