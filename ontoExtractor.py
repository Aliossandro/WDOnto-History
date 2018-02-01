import pandas as pd
import psycopg2
import pickle
# from sqlalchemy import create_engine

# -*- coding: utf-8 -*-
import os
import sys

# fileName = '/Users/alessandro/Documents/PhD/OntoHistory/WDTaxo_October2013.csv'


# def get_max_rows(df):
#     B_maxes = df.groupby(['statementId', 'statValue']).revId.transform(min) == df['revId']
#     return df[B_maxes]

# connection parameters
def get_db_params():
    params = {
        'database': 'wikidb',
        'user': 'postgres',
        'password': 'postSonny175',
        'host': 'localhost',
        'port': '5432'
    }
    conn = psycopg2.connect(**params)
    return conn


# df = pd.read_csv(fileName)
#
# df = df[df['statvalue'] != 'deleted']
#
# idx = df.groupby(['statementid'])['revid'].transform(max) == df['revid']
#
# dfClean = df[idx]
# dfClean['statvalue'].value_counts()
# dfClean['statproperty'].value_counts()
# dfClean.groupby('statproperty')['statvalue'].nunique()
#
# cosi = dfClean.groupby('statproperty')['statvalue'].value_counts()
# dfClean.groupby('statproperty')['statvalue'].value_counts()
# df.groupby('statproperty')['statvalue'].apply(lambda x: x.count())
# create table
def create_table():
    ###statement table query
    query_table = """CREATE TABLE IF NOT EXISTS tempData AS (SELECT p.itemId, p.revId, (p.timestamp::timestamp) AS tS, t.statementId, t.statProperty, t.statvalue FROM
(SELECT itemId, revId, timestamp FROM revisionData_201710) p, (SELECT revId, statementId, statProperty, statvalue FROM statementsData_201710 WHERE statProperty = 'P279' OR statProperty = 'P31') t
WHERE p.revId = t.revId)"""



    conn = None

    try:
        conn = get_db_params()
        cur = conn.cursor()
        cur.execute(query_table)
        cur.close()
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



def queryexecutor():
    dictStats = {}
    conn = get_db_params()
    # cur = conn.cursor()

    for i in range(13, 18):
        for j in range(1, 10):
            date = "20" + str(i) + "-0" + str(j) + "-01"
            dictStats[date] = {}
            query = """
                SELECT * FROM tempData WHERE tS < '""" + date + """ 00:00:00';
            """

            # print(query)
            df = pd.DataFrame()
            for chunk in pd.read_sql(query, con=conn, chunksize=500000):
                df = df.append(chunk)

            df = df[df['statvalue'] != 'deleted']
            idx = df.groupby(['statementid'])['revid'].transform(max) == df['revid']
            dfClean = df[idx]
            fileName = "WDHierarchy-" + date + ".csv"
            dfClean.to_csv(fileName, index=False)

            # unique P279 and P31
            uniqueClasses = dfClean['statvalue'].nunique()
            dictStats[date]['uniqueClasses'] = uniqueClasses
            uniqueAll = dfClean.groupby('statproperty')['statvalue'].nunique()
            dictStats[date]['P279'] = uniqueAll['P279']
            dictStats[date]['P31'] = uniqueAll['P31']

            query2 = """
                            SELECT DISTINCT itemId FROM (SELECT itemId, (timestamp::timestamp) FROM revisionData_201710 WHERE timestamp < '""" + date + """ 00:00:00');
                        """
            # print(query)
            dfIndiv = pd.DataFrame()
            for chunk in pd.read_sql(query2, con=conn, chunksize=500000):
                dfIndiv = dfIndiv.append(chunk)

            fileName = "WDIndiv-" + date + ".csv"
            dfIndiv.to_csv(fileName, index=False)

            query3 = """
                                        SELECT DISTINCT itemId FROM (SELECT itemId, (timestamp::timestamp) FROM revisionData_201710 WHERE timestamp < '""" + date + """ 00:00:00') AND itemId ~* 'P[0-9]{1,};
                                    """
            # print(query)
            dfProp = pd.DataFrame()
            for chunk in pd.read_sql(query3, con=conn, chunksize=500000):
                dfProp = dfProp.append(chunk)

            fileName = "WDProp-" + date + ".csv"
            dfProp.to_csv(fileName, index=False)


        for j in range(10, 13):
            date = "20" + str(i) + "-0" + str(j) + "-01"
            query = """
                            SELECT * FROM tempData WHERE tS < '""" + date + """ 00:00:00';
                        """

            df = pd.DataFrame()
            for chunk in pd.read_sql(query, con=conn, chunksize=50000):
                df = df.append(chunk)

            df = df[df['statvalue'] != 'deleted']
            idx = df.groupby(['statementid'])['revid'].transform(max) == df['revid']
            dfClean = df[idx]
            fileName = "WDHierarchy-" + date + ".csv"
            dfClean.to_csv(fileName, index=False)

            # unique P279 and P31
            uniqueClasses = dfClean['statvalue'].nunique()
            dictStats[date]['uniqueClasses'] = uniqueClasses
            uniqueAll = dfClean.groupby('statproperty')['statvalue'].nunique()
            dictStats[date]['P279'] = uniqueAll['P279']
            dictStats[date]['P31'] = uniqueAll['P31']

            query2 = """
                                        SELECT DISTINCT itemId FROM (SELECT itemId, (timestamp::timestamp) FROM revisionData_201710 WHERE timestamp < '""" + date + """ 00:00:00');
                                    """
            # print(query)
            dfIndiv = pd.DataFrame()
            for chunk in pd.read_sql(query2, con=conn, chunksize=500000):
                dfIndiv = dfIndiv.append(chunk)

            fileName = "WDIndiv-" + date + ".csv"
            dfIndiv.to_csv(fileName, index=False)

            query3 = """
                                                    SELECT DISTINCT itemId FROM (SELECT itemId, (timestamp::timestamp) FROM revisionData_201710 WHERE timestamp < '""" + date + """ 00:00:00') AND itemId ~* 'P[0-9]{1,};
                                                """
            # print(query)
            dfProp = pd.DataFrame()
            for chunk in pd.read_sql(query3, con=conn, chunksize=500000):
                dfProp = dfProp.append(chunk)

            fileName = "WDProp-" + date + ".csv"
            dfProp.to_csv(fileName, index=False)

    pickle_out = open("WDdata.pickle", "wb")
    pickle.dump(dictStats, pickle_out)
    pickle_out.close()

def main():
    create_table()
    queryexecutor()


if __name__ == "__main__":
    main()

#
# leafClasses = dfClean['itemid'][(dfClean['statproperty'] == 'P279') & (~dfClean['itemid'].isin(dfClean['statvalue']))]
# for leaf in leafClasses:
#
#
# x = dfClean['itemid'][(dfClean['statproperty'] == 'P279') & (dfClean['statvalue'] == 'Q5')]
#
# uniqueSuperClasses = list(dfClean['statvalue'][(dfClean['statproperty'] == 'P279') & (~dfClean['itemid'].isin(leafClasses))])
# uniqueSuperClasses = set(uniqueSuperClasses)
#
#
# x = dfClean.groupby(['itemid', 'statproperty'])['statvalue'].unique()
#
# uniquePerClass = x.to_frame()
# uniquePerClass.reset_index(inplace= True)
# uniqueSuperClasses = uniquePerClass[uniquePerClass['statproperty'] == 'P279']
# uniqueClasses = uniquePerClass[uniquePerClass['statproperty'] == 'P31']
# uniqueSuperClasses.drop('statproperty', axis = 1, inplace= True)
# uniqueSuperClasses['statvalue'] = uniqueSuperClasses['statvalue'].apply(lambda x : x.tolist())
# uniqueDict = uniqueSuperClasses.set_index('itemid').T.to_dict('list')
# for key in uniqueDict.keys():
#     uniqueDict[key] = uniqueDict[key][0]
#
# dictClasses = {}
# for cl in uniqueSuperClasses:
#     if cl not in leafClasses:
#         listSubClasses = set(dfClean['itemid'][(dfClean['statproperty'] == 'P279') & (dfClean['statvalue'] == cl)])
#         dictClasses[cl] = list(listSubClasses)
#
# dictLeafs = {}
# for leaf in set(leafClasses):
#     counter = 0
#     supClasses = [leaf]
#     supLeaf = None
#
#
#     for key in dictClasses:
#         if leaf in dictClasses[key]:
#             if supLeaf is None:
#                 supLeaf = key
#                 supClasses.append(key)
#             else:
#                 alterHierarchy = [leaf]
#                 alterHierarchy.append(key)
#                 if len(alterHierarchy) > 1:
#                     for key2 in dictClasses:
#                         if alterHierarchy[:-1] in dictClasses[key2]:
#                             alterHierarchy.append(key2)
#                         else:
#                             continue
#
#
#         if len(supClasses) > 1:
#             for key in dictClasses:
#                 if supClasses[:-1] in dictClasses[key]:
#                     supClasses.append(key)
#                 else:
#                     continue
#
#
#     try:
#         dictLeafs[leaf] = (supClasses, alterHierarchy)
#     except:
#         dictLeafs[leaf] = supClasses
#
#


