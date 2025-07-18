

CREATE TABLE test_table (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO test_table (id, name) VALUES
(1, 'Alice'),
(2, 'Bob'),
(3, 'Charlie');


CREATE VIEW test_view AS
SELECT id, name, created_at
FROM test_table
WHERE created_at > NOW() - INTERVAL '1 day';