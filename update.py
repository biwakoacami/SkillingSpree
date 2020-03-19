import config

from zkillboard import get_killreports_query
from dbactions import DBHandler

db = DBHandler()
db.connect(config.database_file)

report = get_killreports_query('https://zkillboard.com/api/systemID/31000382/groupID/25/startTime/202003131100/')
db.parse_kills(report)
db.fetch_missing_kills()
db.parse_spree()
db.fetch_missing_players()
print("Kill Reports Test Complete")