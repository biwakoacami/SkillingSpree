import config

from zkillboard import get_killreport, get_killreports, get_killreports_query
from dbactions import DBHandler
#from esi import fetch_missing_kills, fetch_missing_characters

db = DBHandler()
db.connect(config.database_file)

"""
print(get_killreport('68506678'))

print("Kill Report Test Complete")
for data in get_killreports('2116317466', 'character'):
    print(data)
for data in get_killreports_query('https://zkillboard.com/api/systemID/31000382/groupID/25/startTime/202003131100/'):
    print(data)
print(fetch_kill("https://esi.evetech.net/latest/killmails/82262260/ff0504143d819a73711e87c8e50b03958d2eb5f6/?datasource=tranquility"))
"""
#report = get_killreport('82298351')
#report = get_killreports_query('https://zkillboard.com/api/systemID/31000382/groupID/25/startTime/202003131100/')
#print(type(report))
#db.parse_kills(report)
#db.fetch_missing_kills()
#db.parse_spree()
db.fetch_missing_players()
print("Kill Reports Test Complete")
