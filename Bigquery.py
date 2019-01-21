import click
from google.cloud import bigquery

"""
README

1. For q1:
    I assume that going live is case sensitive

2. For q3:
    I assume that src = dst is not a valid edge. If it is counted in, it will affect page_rank algorithm.

3. For q5:
    I assume that denominator is "#tweets sent by unpopular users"
    
4. For q7:
    The rank should be correct. The score maybe different, because I handle dangling node and reducible graph.
"""

# Test function
def testquery(client):
    q = """select * from `w4111-columbia.graph.tweets` limit 3"""
    job = client.query(q)

    # waits for query to execute and return
    results = job.result()
    return list(results)

# SQL query for Question 1. You must edit this funtion.
# This function should return a list of IDs and the corresponding text.
def q1(client):
    q = """select id, text from `w4111-columbia.graph.tweets` where text like '%going live%' and text like '%www.twitch%'"""
    job = client.query(q)

    results = job.result()
    return list(results)

# SQL query for Question 2. You must edit this funtion.
# This function should return a list of days and their corresponding average likes.
def q2(client):
    q = """
      select SUBSTR(create_time, 0, 3) as dayweek, avg(like_num) as avg_likes 
      from `w4111-columbia.graph.tweets` 
      group by dayweek
      order by avg_likes DESC
      limit 1
    """
    job = client.query(q)

    results = job.result()
    return list(results)

# SQL query for Question 3. You must edit this funtion.
# This function should return a list of source nodes and destination nodes in the graph.
def q3(client):
    q = """
        CREATE OR REPLACE TABLE dataset.edges AS
        select distinct twitter_username as src, REGEXP_EXTRACT(text, r"@([a-zA-Z0-9_]+)") as dst
        from `w4111-columbia.graph.tweets` 
        where REGEXP_EXTRACT(text, r"@([a-zA-Z0-9_]+)") is not null and REGEXP_EXTRACT(text, r"@([a-zA-Z0-9_]+)") != twitter_username
    """
    job = client.query(q)
    return None

# SQL query for Question 4. You must edit this funtion.
# This function should return a list containing the twitter username of the users having the max indegree and max outdegree.
def q4(client):
    q = """with i1 as (
    select src, count(*) as outdegree from `dataset.edges` group by src order by count(*) DESC limit 1
    ), i2 as (
    select dst, count(*) as indegree from `dataset.edges` group by dst order by count(*) DESC limit 1
    )
    select i2.dst as max_indegree, i1.src as max_outdegree from i1, i2
    """
    job = client.query(q)

    results = job.result()
    return list(results)

# SQL query for Question 5. You must edit this funtion.
# This function should return a list containing value of the conditional probability.
def q5(client):
    q1 = """
       CREATE OR REPLACE TABLE dataset.disnamelikenum AS
       select twitter_username, avg(like_num) as avglikenum
       from `w4111-columbia.graph.tweets` as tweets
       group by twitter_username 
       """
    job = client.query(q1)
    results = job.result()
    print("preflight q1")

    q2 = """
       CREATE OR REPLACE TABLE dataset.disnamedegree AS
       select disname.twitter_username, count(distinct edges.src) as indegree
       from dataset.disnamelikenum as disname
       left outer join dataset.edges as edges
       on disname.twitter_username = edges.dst
       group by disname.twitter_username
       """
    job = client.query(q2)
    results = job.result()
    print("preflight q2")

    q3 = """
       CREATE OR REPLACE TABLE dataset.avgindegree AS
       select avg(indegree) as avg
       from dataset.disnamedegree
       """
    job = client.query(q3)
    results = job.result()
    print("preflight q3")

    q4 = """
       CREATE OR REPLACE TABLE dataset.avglikenum AS
       select avg(like_num) as avg 
       from `w4111-columbia.graph.tweets` as tweets
       """
    job = client.query(q4)
    results = job.result()
    print("preflight q4")

    q5 = """CREATE OR REPLACE TABLE dataset.popular AS
               select twitter_username as name 
               from dataset.disnamelikenum as disnamelikenum
               where disnamelikenum.avglikenum > (select avg
                                           from dataset.avglikenum)
               INTERSECT distinct

               select disnamedegree.twitter_username as name 
               from dataset.disnamedegree as disnamedegree
               where disnamedegree.indegree > (select avg 
                                  from dataset.avgindegree)
               """
    job = client.query(q5)
    results = job.result()
    print("preflight q5")

    q6 = """CREATE OR REPLACE TABLE dataset.unpopular AS
               select twitter_username as name 
               from dataset.disnamelikenum as disnamelikenum
               where disnamelikenum.avglikenum < (select avg
                                           from dataset.avglikenum)
               INTERSECT distinct

               select disnamedegree.twitter_username as name 
               from dataset.disnamedegree as disnamedegree
               where disnamedegree.indegree < (select avg 
                                  from dataset.avgindegree)
               """
    job = client.query(q6)
    results = job.result()
    print("preflight q6")

    q7 = """
       select count(case when REGEXP_EXTRACT(tweets.text, r"@([a-zA-Z0-9_]+)") in (select name from dataset.popular) then 1 else null end)/count(*)
       from `w4111-columbia.graph.tweets` as tweets
       where tweets.twitter_username in (select name from dataset.unpopular)
        """
    job = client.query(q7)
    results = job.result()
    return list(results)

# SQL query for Question 6. You must edit this funtion.
# This function should return a list containing the value for the number of triangles in the graph.
def q6(client):
    q = """select count(1)/3 as no_of_triangles
        from `dataset.edges` as G1
        inner join `dataset.edges` as G2
        on G1.dst = G2.src
        inner join `dataset.edges` as G3
        on G2.dst = G3.src
        where G3.dst = G1.src and G1.src != G2.src and G2.src != G3.src and G1.src != G3.src"""

    job = client.query(q)

    results = job.result()
    return list(results)

# SQL query for Question 7. You must edit this funtion.
# This function should return a list containing the twitter username and their corresponding PageRank. CREATE OR REPLACE TABLE dataset.res AS
def q7(client):
    q0 = """
        CREATE OR REPLACE TABLE dataset.itername AS
        select distinct src as name
        from dataset.edges
        union distinct
        select distinct dst as name
        from dataset.edges
    """
    job = client.query(q0)
    results = job.result()
    print("preflight itername")

    q1 = """
        CREATE OR REPLACE TABLE dataset.res0 AS
        SELECT distinct src as twitter_username, cast(1/773350 as FLOAT64) as page_rank_score
        from `dataset.edges`
    """
    job = client.query(q1)
    results = job.result()
    print("preflight res")

    q2 = """
        CREATE OR REPLACE TABLE dataset.outdegree AS
        select 
            itername.name, (CASE WHEN count(distinct dst) = 0 THEN cast(1/773350 as FLOAT64) else (0.85 * 1/ cast(count(distinct dst) as FLOAT64) + 0.15/773350) end) as outdegree 
        from dataset.itername as itername
        left outer join dataset.edges as edges
        on edges.src = itername.name
        group by itername.name
        """
    job = client.query(q2)
    results = job.result()
    print("preflight outdegree")

    for i in range(20):
        print("step %d", i)
        q3 = """
            CREATE OR REPLACE TABLE dataset.res{next_res} AS
            select edges.dst as twitter_username, sum(res.page_rank_score * outdegree.outdegree) + (773350 - 1 - cast(count(*) as FLOAT64)) * 0.15 * 1 / 773350 as page_rank_score
            from dataset.edges as edges
            inner join dataset.outdegree as outdegree
            on edges.src = outdegree.name
            inner join dataset.res{current_res} as res
            on edges.src = res.twitter_username
            group by edges.dst
        """.format(current_res=i,next_res = i + 1)
        job = client.query(q3)
        results = job.result()

    q4 = """
        select * 
        from `dataset.res20`
        order by page_rank_score DESC
        limit 100
    """
    job = client.query(q4)
    results = job.result()
    return list(results)

# Do not edit this function. This is for helping you develop your own iterative PageRank algorithm.
def bfs(client, start, n_iter):

    # You should replace dataset.bfs_graph with your dataset name and table name.
    q1 = """
        CREATE TABLE IF NOT EXISTS dataset.bfs_graph (src string, dst string);
        """
    q2 = """
        INSERT INTO dataset.bfs_graph(src, dst) VALUES
        ('A', 'B'),
        ('A', 'E'),
        ('B', 'C'),
        ('C', 'D'),
        ('E', 'F'),
        ('F', 'D'),
        ('A', 'F'),
        ('B', 'E'),
        ('B', 'F'),
        ('A', 'G'),
        ('B', 'G'),
        ('F', 'G'),
        ('H', 'A'),
        ('G', 'H'),
        ('H', 'C'),
        ('H', 'D'),
        ('E', 'H'),
        ('F', 'H');
        """

    job = client.query(q1)
    results = job.result()
    job = client.query(q2)
    results = job.result()

    # You should replace dataset.distances with your dataset name and table name.
    q3 = """
        CREATE OR REPLACE TABLE dataset.distances AS
        SELECT '{start}' as node, 0 as distance
        """.format(start=start)
    job = client.query(q3)
    # Result will be empty, but calling makes the code wait for the query to complete
    job.result()

    for i in range(n_iter):
        print("Step %d..." % (i+1))
        q1 = """
        INSERT INTO dataset.distances(node, distance)
        SELECT distinct dst, {next_distance}
        FROM dataset.bfs_graph
            WHERE src IN (
                SELECT node
                FROM dataset.distances
                WHERE distance = {curr_distance}
                )
            AND dst NOT IN (
                SELECT node
                FROM dataset.distances
                )
            """.format(
                curr_distance=i,
                next_distance=i+1
            )
        job = client.query(q1)

        t = """select * from dataset.distances"""
        job = client.query(t)
        results = job.result()
        print(list(results))


# Do not edit this function. You can use this function to see how to store tables using BigQuery.
def save_table():
    client = bigquery.Client()
    dataset_id = 'dataset'

    job_config = bigquery.QueryJobConfig()
    # Set use_legacy_sql to True to use legacy SQL syntax.
    job_config.use_legacy_sql = True
    # Set the destination table
    table_ref = client.dataset(dataset_id).table('test')
    job_config.destination = table_ref
    job_config.allow_large_results = True
    sql = """select * from [w4111-columbia.graph.tweets] limit 3"""

    # Start the query, passing in the extra configuration.
    query_job = client.query(
        sql,
        # Location must match that of the dataset(s) referenced in the query
        # and of the destination table.
        location='US',
        job_config=job_config)  # API request - starts the query

    query_job.result()  # Waits for the query to finish
    print('Query results loaded to table {}'.format(table_ref.path))

@click.command()
@click.argument("PATHTOCRED", type=click.Path(exists=True))
def main(pathtocred):
    client = bigquery.Client.from_service_account_json(pathtocred)

    funcs_to_test = [q1, q2, q3, q4, q5, q6, q7]
    for func in funcs_to_test:
        rows = func(client)
        print ("\n====%s====" % func.__name__)
        print(rows)

    # bfs(client, 'A', 5)

if __name__ == "__main__":
  main()