import os
from sqlalchemy import *
from sqlalchemy.pool import NullPool

def create():
    DB_USER = "jason_wu"
    DATABASEURI = "postgresql://localhost"
    engine = create_engine(DATABASEURI)
    return engine

def conn(connection):
    return connection.connect()

def disconn(connection):
    connection.close()

def q5(client):
    q1 = """
    drop table IF EXISTS popular, unpopular;
    """
    q1 = """
    CREATE TABLE disnamelikenum AS
    select twitter_username, avg(like_num) as avglikenum
    from tweets
    group by twitter_username 
    """
    # db.execute(q1)
    # print("1")
    q1 = """
    CREATE TABLE disnamedegree AS
    select disname.twitter_username, count(distinct edges.src) as indegree
    from disnamelikenum as disname
    left outer join edges as edges
    on disname.twitter_username = edges.dst
    group by disname.twitter_username
    """

    q1 = """
    CREATE TABLE avgindegree AS
    select avg(indegree) as avg
    from disnamedegree
    """

    q1 = """
    CREATE TABLE avglikenum AS
    select avg(like_num) as avg 
    from tweets
    """

    q1 = """CREATE TABLE popular AS
            select twitter_username as name 
            from disnamelikenum as disnamelikenum
            where disnamelikenum.avglikenum > (select avg
                                        from avglikenum)
            INTERSECT 
            
            select disnamedegree.twitter_username as name 
            from disnamedegree as disnamedegree
            where disnamedegree.indegree > (select avg 
                               from avgindegree)
            """
    # db.execute(q1)

    q1 = """CREATE TABLE unpopular AS
            select twitter_username as name 
            from disnamelikenum as disnamelikenum
            where disnamelikenum.avglikenum < (select avg
                                        from avglikenum)
            INTERSECT 

            select disnamedegree.twitter_username as name 
            from disnamedegree as disnamedegree
            where disnamedegree.indegree < (select avg 
                               from avgindegree)
            """

    # result = db.execute(q2)
    q3 = """
    with tmp as (
    select * from tweets where tweets.twitter_username in (select name from unpopular)
    )
    select count(case when edges.dst in (select name from popular) then 1 else null end)/count(*)::double precision
    from tmp as tmp
    left outer join edges
    on tmp.twitter_username = edges.src
                """


    q3 = """
    select count(case when REGEXP_EXTRACT(tweets.text, r"@([a-zA-Z0-9_]+)") in (select name from popular) then 1 else null end)/count(*)::double precision
    from tweets as tweets
    where tweets.twitter_username in (select name from unpopular)
            """

    result = db.execute(q3)
    res = [dict(r) for r in result]
    print(res)

def q7(client):
    # for i in range(21):
    #     q0 = """
    #     drop table IF EXISTS res{c},itername, outdegree, havedst, nothavedst;
    #     """.format(c=i)
    #     db.execute(q0)
    # print("0")

    # for i in range(21):
    #     q0 = """
    #     drop table IF EXISTS res{c}, outdegree;
    #     """.format(c=i)
    #     db.execute(q0)
    # print("1")

    q = """
    CREATE table itername AS
    select distinct src as twitter_username
    from edgesnew as edges
    union
    select distinct dst as twitter_username
    from edgesnew as edges
    """
    db.execute(q)
    print("2")

    q = """
        CREATE table res0 AS
        SELECT twitter_username as twitter_username, 1/773350::numeric(100,80) as page_rank_score
        from itername
        """
    db.execute(q)
    print("3")

    q = """
        CREATE table outdegree AS
        select edges.src as twitter_username,  (0.85  / count(distinct dst)  + 0.15/773350)::numeric(100,80)  as outdegree 
        from edgesnew as edges
        group by edges.src
        """
    db.execute(q)
    print("4")

    q = """
    create table havedst as
    select distinct src as twitter_username
    from edgesnew as edges
    """
    db.execute(q)
    print("4")

    q = """
    create table nothavedst as
    select twitter_username as twitter_username
    from itername
    except
    select twitter_username as twitter_username
    from havedst
    """
    db.execute(q)
    print("4")

    for i in range(20):
        q0 = """
        drop table IF EXISTS sumhavedst, sumnothavedst
        """
        db.execute(q0)
        print("step %d", i)

        qm = """
        create table sumhavedst as
        select sum(page_rank_score) * 0.15 / 773350 ::numeric(100,80)
        from res{current_res} as result
        inner join havedst as havedst
        on result.twitter_username = havedst.twitter_username
        """.format(current_res=i)
        db.execute(qm)
        print("middle: sum havedst")

        qm1 = """
        create table sumnothavedst as
        select sum(page_rank_score) / 773350 ::numeric(100,80)
        from res{current_res} as result
        inner join nothavedst as nothavedst
        on result.twitter_username = nothavedst.twitter_username
        """.format(current_res=i)
        db.execute(qm1)
        print("middle: sum not havedst")

        q5 = """
            CREATE table res{next_res} AS
            select res.twitter_username as twitter_username, (sum(res.page_rank_score * (outdegree.outdegree - 0.15 / 773350) ::numeric(100,80)) + (select * from sumhavedst limit 1) + (select * from sumnothavedst limit 1))::numeric(100,80) as page_rank_score
            from res{current_res} as res
            left outer join edgesnew as edges
            on edges.dst = res.twitter_username
            left outer join outdegree as outdegree
            on edges.src = outdegree.twitter_username
            group by res.twitter_username
        """.format(current_res=i, next_res=i + 1)

        db.execute(q5)




if __name__ == '__main__':
    db = conn(create())
    # p = "select * from test"
    # result = db.execute(p).fetchall()

    funcs_to_test = [q7]
    for func in funcs_to_test:
        result = func(db)
        print("\n====%s====" % func.__name__)
        # res = [dict(r) for r in result]
        # print(res)
    

    disconn(db)
    # print(res)