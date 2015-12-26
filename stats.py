#!/usr/bin/env python3

from sqlalchemy import *

engine = create_engine('mysql+mysqlconnector://root:password@localhost:3306/halcyonnhl')

meta = MetaData(engine)
meta.reflect(bind=engine)

def run(stmt):
	rs = stmt.execute()
	for row in rs:
		print(row)


def getStats_inLastN(stat, team, currGameID, N, statType='Reg', wrapSeason='Yes'):
    """ Returns tubple of length N of stat, which must be an allowed column name (see below), for the last N 
    games for the specified team before currGameID.
   
    Parameters
    ----------å
    stat : string
            column name in TeamStatsByGame table, ie. GF,GA,SF,SA,MF,MA,BF,BA,FOW,FOL,HF,HA,GVF,GVA,TKF,TKA,SOGF,SOGA,PF,PA
    team : string
            three letter short team name for which FOR stats will be returned
    currGameID : int
            ten digit game ID number for game to be predicted (stats won't be returned for this game)
    N : int
            number of games for which count of stat will be returned
    statType : string
            if 'Reg' TeamStatsByGame table is used, if 'Close' CloseTeamStatsByGame table is used, if 'EVReg'
            EVTeamStatsByGame table is used, if 'EVClose' EVCloseTeamStatsByGame table is used
    wrapSeason : string
            if 'Yes' (default) count will include games from previous season if N greater than number of games played
            by team in the current season. if 'No' count will only include games from the current season.
    
    Returns
    -------
    float
    """
    
    games = Table('AltGames', meta, autoload=True)
    if statType=='Reg':
        stats = Table('TeamStatsByGame', meta, autoload=True)
    elif close=='Close':
        stats = Table('CloseTeamStatsByGame', meta, autoload=True)
    elif close=='EVReg':
        stats = Table('EVTeamStatsByGame', meta, autoload=True)
    elif close=='EVClose':
        stats = Table('EVCloseTeamStatsByGame', meta, autoload=True)

    # Find the last N games with the right team
    if wrapSeason=='Yes':
    	g = games.select(and_(games.c.team==team,games.c.gameID<currGameID)).order_by(desc(games.c.gameID)).limit(N)
    else:
        g = games.select(and_(games.c.team==team,games.c.gameID<currGameID,games.c.gameID>currGameID-1230)).order_by(desc(games.c.gameID)).limit(N)

    rg = g.execute()

    count = 0
    tup = ()
    for row in rg:
        s = stats.select(and_(stats.c.gameID==row['gameID'],stats.c.team==team))
        rs = s.execute()
        for r in rs:
            next = r[stat]
            tup = tup + (next,)

    print(tup)
    return tup
'''
def getStatsFor_inLastN(stat, team, currGameID, N, wrapSeason='Yes'):
    """ Returns the count of stat, which must be a table name, over the last N games FOR the given team before currGameID.
   
    Parameters
    ----------
    stat : string
            table name for desired statistic
    team : string
            three letter team name
    currGameID : int
            ten digit game ID number for game to be predicted (stats won't be returned for this game)
    N : int
            number of games for which count of stat will be returned
    wrapSeason : string
            if 'Yes' (default) count will include games from previous season if N greater than number of games played
            by team in the current season. if 'No' count will only include games from the current season.
    
    Returns
    -------
    float
    """
    
    games = Table('AltGames', meta, autoload=True)
    stats = Table(stat, meta, autoload=True)

    # Find the last N games with the right team
    if wrapSeason=='Yes':
    	g = games.select(and_(games.c.team==team,games.c.gameID<currGameID)).order_by(desc(games.c.gameID)).limit(N)
    else:
        g = games.select(and_(games.c.team==team,games.c.gameID<currGameID,games.c.gameID>currGameID-1230)).order_by(desc(games.c.gameID)).limit(N)

    rg = g.execute()

    count = 0
    for row in rg:
        s = select([func.count(stats.c.eventID)],and_(stats.c.team==team,stats.c.gameID==row['gameID'],stats.c.period<5))
        rs = s.execute()
        for row in rs:
            count+=row[0]
            print(count)

    return count

def getStatsAgainst_inLastN(stat, team, currGameID, N, wrapSeason='Yes'):
    """ Returns the count of stat, which must be a table name, over the last N games AGAINST the given team before currGameID.
   
    Parameters
    ----------
    stat : string
            table name for desired statistic
    team : string
            three letter team name
    currGameID : int
            ten digit game ID number for game to be predicted (stats won't be returned for this game)
    N : int
            number of games for which count of stat will be returned
    wrapSeason : string
            if 'Yes' (default) count will include games from previous season if N greater than number of games played
            by team in the current season. if 'No' count will only include games from the current season.
    
    Returns
    -------
    float
    """
    
    games = Table('AltGames', meta, autoload=True)
    stats = Table(stat, meta, autoload=True)

    # Find the last N games with the right team
    if wrapSeason=='Yes':
    	g = games.select(and_(games.c.team==team,games.c.gameID<currGameID)).order_by(desc(games.c.gameID)).limit(N)
    else:
        g = games.select(and_(games.c.team==team,games.c.gameID<currGameID,games.c.gameID>currGameID-1230)).order_by(desc(games.c.gameID)).limit(N)

    rg = g.execute()

    count = 0
    for row in rg:
        s = select([func.count(stats.c.eventID)],and_(stats.c.opponent==team,stats.c.gameID==row['gameID'],stats.c.period<5))
        rs = s.execute()
        for row in rs:
            count+=row[0]
            print(count)

    #return count
'''

getStats_inLastN('GF','VAN', 2015020510, 4)
#getStats_inLastN('GA','VAN', 2015020510, 4)



