import json

"""
SELECT json_build_object('result', json_agg(result)) AS json_array, COUNT(result) as count
FROM (SELECT json_build_object('name', tt.name, 'syns2', tt.syns2, 'tt2', row_to_json(SELECT json_agg(json_build_object('name', tt2.name, 'syns2', tt2.syns2)) as result, count (*) as count
    FROM test_table2 tt2
    WHERE tt2.name = tt.name)) AS result
    FROM test_table tt LIMIT 10001) subquery;

"""

if __name__ == '__main__':
    async def main():
        from asyncpg import connect
        from time import perf_counter_ns
        start = perf_counter_ns()
        conn = await connect(
            dsn="postgresql://postgres:postgres@localhost/postgres"
        )
        await conn.set_type_codec("jsonb", encoder=lambda data: b"\x01" + json.dumps(data),
                                  decoder=lambda data: data[1:], schema="pg_catalog", format="binary")
        async with conn.transaction():

            result = await conn.fetch("""
                                      SELECT jsonb_build_object('result', jsonb_agg(result)) AS json_array,
                                             COUNT(result) as count
                                      FROM (SELECT jsonb_build_object('name', tt.name, 'syns2', tt.syns2) AS result
                                          FROM test_table tt
                                          LIMIT 100000) subquery;
                                      """)
            print(result[0])

        end = perf_counter_ns()
        print(f"Execution time: {(end - start) / 1_000_000} ms")

    import asyncio

    asyncio.run(main())
