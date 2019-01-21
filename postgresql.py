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
#     q1 = """
#     drop table IF EXISTS res0, res1,res2,res3,res5,res5,res6, tmp, tmpsum, itername;
#     """
#     db.execute(q1)
#     print("1")
#
#
# #cast ((select 1 / count(distinct src) from edges) as float)
#
#     q1 = """
#     CREATE table res0 AS
#     SELECT distinct src as twitter_username, 0.00016055 as page_rank_score
#     from edges
#     """
#     db.execute(q1)
#     print("2")
#
#     q2 = """
#     CREATE table outdegree AS
#     select src, count(distinct dst) as outdegree from edges group by src
#     """
#     # db.execute(q2)
#
#     q3 = """
#     CREATE table itername AS
#     select distinct src
#     from edges
#     order by src
#     """
#     db.execute(q3)
#
#     for i in range(20):
#         print("Step %d..." % (i + 1))
#         a = """
#         create table res{n} (twitter_username text, page_rank_score float)
#         """.format(n=i + 1)
#         db.execute(a)
#
#
#         for j in range(6233):
#             print("Step %d... Offset %d..." % (i, j))
#             # iterate through edges
#             qi = """
#             select src
#             from itername
#             limit 1
#             offset {offset}
#             """.format(offset=j)
#
#             q1 = """
#                drop table IF EXISTS  tmp, tmpsum;
#                """
#             db.execute(q1)
#
#             #get all node point to him
#             e1 = """
#             CREATE table tmp AS
#             select distinct src
#             from edges as e1
#             where e1.dst = ({iter})
#             """.format(iter=qi)
#             db.execute(e1)
#
#
#             e2 = """
#             CREATE table tmpsum AS
#             select sum(res.page_rank_score / degree.outdegree) as score
#             from tmp as point
#             inner join outdegree as degree
#             on point.src = degree.src
#             inner join res{n} as res
#             on point.src = res.twitter_username
#             """.format(n=i)
#             db.execute(e2)
#
#             e3 = """
#             insert into res{n} values
#             (({iter}), (select score from tmpsum limit 1))
#             """.format(n=i+1, iter=qi)
#             db.execute(e3)
    for i in range(21):
        q0 = """
        drop table IF EXISTS res5{c},outdegree, itername, edges2;
        """.format(c = i)
        db.execute(q0)
        print("1")


    q0 = """
    CREATE table edges2 AS
    select *
    from edges
    where src != dst and dst in (select src from edges)
    """
    db.execute(q0)
    print("1.5")

    q1 = """
        CREATE table res50 AS
        SELECT distinct src as twitter_username, 0.00023303 as page_rank_score
        from edges2
        """
    db.execute(q1)
    print("2")


    q2 = """
        CREATE table outdegree AS
        select src, count(distinct dst) as outdegree 
        from edges2
        group by src
        """
    db.execute(q2)
    print("3")
    #
    #
    # q3 = """
    #     CREATE table itername AS
    #     select distinct src
    #     from edges2
    # """
    # db.execute(q3)
    # print("5")

    for i in range(20):
        print("step %d", i)
        q5 = """
            CREATE table res5{next_res} AS
            select edges2.dst as twitter_username, sum(res5{current_res}.page_rank_score / (outdegree.outdegree)) as page_rank_score
            from edges2
            inner join outdegree
            on edges2.src = outdegree.src
            inner join res5{current_res}
            on edges2.src = res5{current_res}.twitter_username
            group by edges2.dst
        """.format(current_res=i,next_res = i + 1)
        db.execute(q5)



def q8(client):
    for i in range(21):
        q0 = """
        drop table IF EXISTS res{c};
        """.format(c = i)
        db.execute(q0)

# , outdegree, itername, matrix
    print("1")
    q0 = """
    CREATE table itername AS
    select distinct src as name
    from edges
    union
    select distinct dst as name
    from edges
    """
    # db.execute(q0)
    print("2")

    q1 = """
        CREATE table res0 AS
        SELECT name as twitter_username, 1/773352::double precision as page_rank_score
        from itername
        """
    db.execute(q1)
    print("3")


    q2 = """
        CREATE table outdegree AS
        select edges.name, (0.85 * cast(1/count(distinct dst) as double precision) + 0.15/773352)  as outdegree 
        from edges as edges
        group by edges.src
        """
    # db.execute(q2)
    print("4")

    # q2 = """
    #     CREATE table outdegree AS
    #     select
    #         itername.name, (CASE WHEN count(distinct dst) = 0 THEN 1/773352::double precision else ( 1/ cast(count(distinct dst) as double precision) ) end) as outdegree
    #     from itername
    #     left outer join edges
    #     on edges.src = itername.name
    #     group by itername.name
    #     """
    # # db.execute(q2)
    print("4")


    # q3 = """
    #     CREATE table matrix AS
    #     select
    #         outdegree.name, (0.85 * outdegree.outdegree + 0.15/773352) as virtual
    #     from outdegree as outdegree
    #     """
    # # db.execute(q3)
    print("5")

    q4 = """
        CREATE table distinctsrc AS
        select distinct src
        from edges as edges
        """
    # db.execute(q4)
    print("5")



    for i in range(20):
        print("step %d", i)
        qm = """
        select sum()
        
        
        """



        q5 = """
            CREATE table res{next_res} AS
            select edges.dst as twitter_username, (sum(res{current_res}.page_rank_score * matrix.virtual) + (6233 - count(edges.src)) * 0.15 / 773352 + 767119 / 773352):: double precision as page_rank_score
            from edges
            inner join matrix as matrix
            on edges.src = matrix.name
            inner join res{current_res}
            on edges.src = res{current_res}.twitter_username
            group by edges.dst
        """.format(current_res=i,next_res = i + 1)
        db.execute(q5)


def q9(client):
    # for i in range(21):
    #     q0 = """
    #     drop table IF EXISTS res{c},itername, outdegree, havedst, nothavedst;
    #     """.format(c=i)
    #     db.execute(q0)
    # print("1")

    for i in range(21):
        q0 = """
        drop table IF EXISTS res{c}, outdegree;
        """.format(c=i)
        db.execute(q0)
    print("1")

    q = """
    CREATE table itername AS
    select distinct src as twitter_username
    from edgesnew as edges
    union
    select distinct dst as twitter_username
    from edgesnew as edges
    """
    # db.execute(q)
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
    # db.execute(q)
    print("4")

    q = """
    create table nothavedst as
    select twitter_username as twitter_username
    from itername
    except
    select twitter_username as twitter_username
    from havedst
    """
    # db.execute(q)
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

    funcs_to_test = [q9]
    for func in funcs_to_test:
        result = func(db)
        print("\n====%s====" % func.__name__)
        # res = [dict(r) for r in result]
        # print(res)
    

    disconn(db)
    # print(res)