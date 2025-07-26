SELECT to_jsonb(
               json_build_object(
                       'query', 'SELECT * FROM users WHERE age > 30',
                       'result', (SELECT json_agg(row_to_json(tt)) FROM (SELECT name, syns2 FROM test_table) as tt)
               )
       ) AS query_result;



select to_jsonb(row_to_json(tt)) || jsonb_build_object('tt2', (SELECT json_agg(row_to_json(tt2))
                                                               FROM test_table2 tt2
                                                               WHERE tt2.name = tt.name))

FROM test_table tt
LIMIT 10000;


(SELECT to_jsonb(row_to_json(tt)) ||
        jsonb_build_object('tt2', (SELECT jsonb_agg(row_to_json(tt2))
                                   FROM test_table2 tt2
                                   WHERE tt2.name = tt.name))
 FROM test_table tt
 LIMIT 1000);

select to_jsonb((SELECT to_jsonb(row_to_json(tt)) ||
                        jsonb_build_object('tt2', (SELECT jsonb_agg(row_to_json(tt2))
                                                   FROM test_table2 tt2
                                                   WHERE tt2.name = tt.name))
                 FROM test_table tt
                 LIMIT 1000));


SELECT json_build_object('result', jsonb_agg(result)) AS json_array
FROM (SELECT to_jsonb(row_to_json(tt)) ||
             jsonb_build_object('tt2', (SELECT jsonb_agg(row_to_json(tt2))
                                        FROM test_table2 tt2
                                        WHERE tt2.name = tt.name)) AS result
      FROM test_table tt
      LIMIT 100000) subquery;



SELECT json_build_object('result', json_agg(result)) AS json_array
FROM (SELECT json_build_object('name', tt.name, 'syns2', tt.syns2, 'tt2', (SELECT json_agg(row_to_json(tt2))
                                                                           FROM test_table2 tt2
                                                                           WHERE tt2.name = tt.name)) AS result
      FROM test_table tt
      LIMIT 500000) subquery;



SELECT jsonb_build_object('result', jsonb_agg(
        jsonb_build_object(
                'name', tt.name,
                'syns2', tt.syns2,
            -- add other columns explicitly
                'tt2', COALESCE(tt2_agg.tt2_data, '[]'::jsonb)
        )
                                    )) AS result
FROM test_table tt
         LEFT JOIN (SELECT name,
                           jsonb_agg(jsonb_build_object(
                                   'name', tt2.name,
                                   'syns2', tt2.syns2
                               -- add other tt2 columns explicitly
                                     )) AS tt2_data
                    FROM test_table2 tt2
                    GROUP BY name) tt2_agg ON tt2_agg.name = tt.name
LIMIT 100000;

SELECT row_to_json(tt)
FROM (SELECT name, syns2, json_agg(row_to_json((SELECT name, syns2 FROM test_table2 WHERE name = tt.name)))
      FROM test_table tt
      LIMIT 10) as tt

CREATE INDEX ON test_table (name);

CREATE TABLE test_table2
(
    name  TEXT NOT NULL,
    syns2 TEXT[]
);


INSERT INTO test_table2 (name, syns2)
VALUES (generate_series(1, 10000000), array [repeat('A cool player. ', 2) || 'My number is ' || trunc(random() * 10)]);


SELECT json_build_object('result', json_agg(result), 'count', COUNT(result)) AS json_array
FROM (SELECT json_build_object('name', tt.name, 'syns2', tt.syns2, 'tt2',
                               (SELECT json_build_object('results',
                                                         json_agg(json_build_object('name', tt2.name, 'syns2', tt2.syns2)),
                                                         'count', count(*)) as result
                                FROM test_table2 tt2
                                WHERE tt2.name = tt.name)) AS result
      FROM test_table tt
      LIMIT 11) subquery;


SELECT json_build_object('result', json_agg(result), 'count', COUNT(result)) AS json_array
FROM (SELECT json_build_object('name', tt.name, 'syns2', tt.syns2, 'tt2',
                               json_agg(json_build_object('name', t.name, 'syns2', t.syns2))) AS result
      FROM test_table tt
               INNER JOIN test_table2 t on tt.name = t.name
      GROUP BY tt.name, tt.syns2
      LIMIT 100000) subquery;


SELECT jsonb_build_object('result', jsonb_agg(result)) AS json_array, COUNT(result) as count
FROM (SELECT jsonb_build_object('name', tt.name, 'syns2', tt.syns2) AS result
      FROM test_table tt
      LIMIT 10001) subquery;


SELECT json_build_object('result', json_agg(result)) AS json_array, COUNT(result) as count
FROM (SELECT json_build_object('name', tt.name, 'syns2', tt.syns2) AS result
      FROM test_table tt
      LIMIT 10001) subquery;