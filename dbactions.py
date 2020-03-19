import logging
import json
import sqlite3

from esi import fetch_kill, fetch_player
from config import logging_file, database_file

# Logging Configuration
logging.basicConfig(filename=logging_file, level=logging.DEBUG)
logger = logging.getLogger(__name__)
console = logging.StreamHandler()
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)


"""
TABLES
======
CREATE TABLE `zkill` (
    `killID` INT UNSIGNED NOT NULL,
    `hash` VARCHAR(40) NOT NULL,
    `value` REAL NOT NULL,
    `killReport` TEXT NOT NULL,
    PRIMARY KEY (`killID`)
);

CREATE TABLE `esi` (
    `killID` INT UNSIGNED NOT NULL,
    `killReport` TEXT NOT NULL,
    PRIMARY KEY (`killID`)
);

CREATE TABLE `spree`(
    `killID` INT UNSIGNED NOT NULL,
    `attackerID` INT UNSIGNED NOT NULL,
    `attacker_shipID` INT UNSIGNED NOT NULL,
    `weaponID` INT UNSIGNED NOT NULL,
    `victimID` INT UNSIGNED NOT NULL,
    `victim_shipID` INT UNSIGNED NOT NULL,
    `valid` INT UNSIGNED NOT NULL,
    `eligible` INT UNSIGNED NOT NULL,
    PRIMARY KEY (`killID`)
);

CREATE TABLE `character` (
    `characterID` INT UNSIGNED NOT NULL,
    `name` TEXT NOT NULL,
    `corporationID` IN UNSIGNED NOT NULL,
    `race` INT UNSIGNED NOT NULL,
    `birthday` DATETIME NOT NULL,
    PRIMARY KEY (`characterID`)
);

CREATE TABLE `ships` (
    `shipID` INT UNSIGNED NOT NULL,
    `name` TEXT NOT NULL,
    PRIMARY KEY (`shipID`)
);

CREATE VIEW `history`
AS
SELECT S.`killID`, A.`name` AS `attacker`, SA.`name` AS `attacker_ship`, V.`name` AS `victim`, SV.`name` AS `victim_ship`, Z.`value`, S.`valid`, S.`eligible` FROM `spree` S
    JOIN `zkill` Z ON S.`killID` = Z.`killID`
    JOIN `character` A ON S.`attackerID` = A.`characterID`
    JOIN `character` V ON S.`victimID` = V.`characterID`
    JOIN `ships` SA ON S.`attacker_shipID` = SA.`shipID`
    JOIN `ships` SV ON S.`victim_shipID` = SV.`shipID`
ORDER BY S.`killID`;

CREATE VIEW `ship_leaderboard` AS
SELECT 
    C.`name` AS `player`
    , S.`name` AS `ship`
    , (SELECT COUNT(*) FROM `spree` WHERE attackerID = P.player AND attacker_shipID = P.shipID) AS `wins`
    , (SELECT COUNT(*) FROM `spree` WHERE victimID = P.player AND victim_shipID = P.shipID) AS `losses`
FROM (
    SELECT DISTINCT attackerID AS `player`, attacker_shipID AS `shipID` FROM `spree`
    UNION
    SELECT DISTINCT victimID AS `player`, victim_shipID AS `shipID` FROM `spree`
) AS P 
JOIN `character` C ON P.`player` = C.`characterID`
JOIN `ships` S ON P.`shipID` = S.`shipID`
ORDER BY wins DESC;

CREATE VIEW `undefeated` AS
SELECT 
    C.`name` AS `player`
    , S.`name` AS `ship`
    , (SELECT COUNT(*) FROM `spree` WHERE attackerID = P.player AND attacker_shipID = P.shipID) AS `wins`
    , (SELECT COUNT(*) FROM `spree` WHERE victimID = P.player AND victim_shipID = P.shipID) AS `losses`
FROM (
    SELECT DISTINCT attackerID AS `player`, attacker_shipID AS `shipID` FROM `spree`
    UNION
    SELECT DISTINCT victimID AS `player`, victim_shipID AS `shipID` FROM `spree`
) AS P 
JOIN `character` C ON P.`player` = C.`characterID`
JOIN `ships` S ON P.`shipID` = S.`shipID`
WHERE `losses` = 0
ORDER BY wins DESC;

CREATE VIEW `player_leaderboard` AS
SELECT 
    C.`name`
    , (SELECT COUNT(*) FROM `spree` WHERE attackerID = P.player) AS `wins`
    , (SELECT COUNT(*) FROM `spree` WHERE victimID = P.player) AS `losses`
FROM (
    SELECT DISTINCT attackerID AS `player` FROM `spree`
    UNION
    SELECT DISTINCT victimID AS `player` FROM `spree`
) AS P 
JOIN `character` C ON P.`player` = C.`characterID`
ORDER BY wins DESC;

CREATE VIEW `player_iskboard` AS
SELECT C.`characterID`, C.`name` AS `player`, SUM(Z.`value`) AS `isk_destroyed` FROM `spree` S
JOIN `character` C ON C.`characterID` = S.`attackerID`
JOIN `zkill` Z ON Z.`killID` = S.`killID`
GROUP BY S.`attackerID`
ORDER BY `isk_destroyed` DESC;

CREATE VIEW `ship_iskboard` AS
SELECT C.`characterID`, C.`name` AS `player`, S.`name` AS `ship`, SUM(Z.`value`) AS `isk_destroyed` FROM `spree` S
JOIN `character` C ON C.`characterID` = S.`attackerID`
JOIN `ships` S ON S.`shipID` = S.`attacker_shipID`
JOIN `zkill` Z ON Z.`killID` = S.`killID`
GROUP BY S.`attackerID`, S.`attacker_shipID`
ORDER BY `isk_destroyed` DESC;


CREATE VIEW player_results AS
SELECT `killid`, `player`, `ship`, `result` FROM (
    SELECT `killid`, `attacker` AS `player`, `attacker_ship` AS `ship`, 'W' AS `result` FROM `history`
    UNION SELECT `killid`, `victim` AS `player`, `victim_ship` AS `ship`, 'L' AS `result` FROM `history`
) ORDER BY `killid`;


CREATE VIEW streaks AS
SELECT 
    `player`
    , `ship`
    , `result`
    , MIN(`killid`) AS `start`
    , MAX(`killid`) AS `end`
    , COUNT(*) AS `matches`
FROM (
    SELECT
        `killid`
        , `player`
        , `ship`
        , `result`
        , (SELECT COUNT(*) FROM player_results R
            WHERE 
            R.player = PR.player
            AND R.ship = PR.ship
            AND R.result <> PR.result
            AND R.killid <= PR.killid) AS `rungroup`
    FROM player_results PR
) A
GROUP BY `player`, `ship`, `result`, `rungroup`
ORDER BY `end`;


"""

class DBHandler(object):
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self, database_file):
        # Create SQL Connection
        try:
            logger.info("Connecting to database file {0}.".format(database_file))
            self.connection = sqlite3.connect(database_file)
            self.cursor = self.connection.cursor()
            logger.debug("Successfully established connection to database ({0}).".format(database_file))
            return self.cursor
        except Exception as e:
            logger.error("Unable to establish connection to database ({0}).".format(database_file))
            logger.exception(e)
            return None

    def __del__(self):
        self.connection.close()

    def parse_kills(self, killReports, commit=True):
        """ Inserts kill reports into `zkill` """

        logger.debug("Received {0} reports".format(len(killReports)))
        for killReport in killReports:
            insert_kill = 'INSERT INTO `zkill` (`killID`, `hash`, `value`, `killReport`)  VALUES ( ?, ?, ?, ?)'

            logger.info("Parsing loss data for killID {0}".format(killReport['killmail_id']))

            if self.kill_exists(killReport['killmail_id']):
                logger.warn("Kill #{0} is already present in the DB.".format(killReport['killmail_id']))
                return None

            try:
                kill = (killReport['killmail_id'], killReport['zkb']['hash'], killReport['zkb']['totalValue'], json.dumps(killReport))
                logger.debug("Executing INSERT on `zkill` for data {0}".format(kill))
                self.cursor.execute(insert_kill, kill)
                logger.info("Successfully parsed kill data for killID {0}".format(killReport['killmail_id']))
                
                if commit: 
                    self.connection.commit()
                
            except Exception as e:
                logger.error("Unexpected error occurred while working with killreport for `zkill`: {0}".format(killReport))
                logger.exception(e)     

    def parse_killdata(self, killReport, commit=True):
        """ Inserts ESI kill report into `esi` """

        insert_kill = 'INSERT INTO `esi` (`killID`, `killReport`)  VALUES ( ?, ?)'

        logger.info("Parsing loss data for killID {0}".format(killReport['killmail_id']))

        try:
            kill = (killReport['killmail_id'], json.dumps(killReport))
            logger.debug("Executing INSERT on `esi` for data {0}".format(kill))
            self.cursor.execute(insert_kill, kill)
            logger.info("Successfully parsed kill data for killID {0}".format(killReport['killmail_id']))
            
            if commit: 
                self.connection.commit()
            
        except Exception as e:
            logger.error("Unexpected error occurred while working with killreport for `esi`: {0}".format(killReport))
            logger.exception(e)     

    def parse_player(self, characterID, playerProfile, commit=True):
        """ Inserts Player into `character` """

        insert_player = 'INSERT INTO `character` (`characterID`, `name`, `corporationID`, `race`, `birthday`) VALUES (?, ?, ?, ?, ?)'

        logger.info("Parsing player {0}".format(characterID))

        try:
            player = (characterID, playerProfile['name'], playerProfile['corporation_id'], playerProfile['race_id'], playerProfile['birthday'])
            logger.debug("Executing INSERT on `character` for data {0}".format(player))
            self.cursor.execute(insert_player, player)
            logger.info("Successfully parsed kill data for killID {0}".format(characterID))
            
            if commit: 
                self.connection.commit()
            
        except Exception as e:
            logger.error("Unexpected error occurred while working with character for `character`: {0}".format(player))
            logger.exception(e)     


    def get_lastkill(self, identifier, base):
        """ Get last known kill based on the identifier/base combination """
        pass

    def kill_exists(self, killID):
        """ Check `zkill` table for this `killID` """

        query = "SELECT `killID` FROM `zkill` WHERE `killID` = {0};".format(killID)
        try:
            logger.debug("Executing Query {0}".format(query))
            self.cursor.execute(query)
            response = self.cursor.fetchone()
            logger.debug("Query Response: {0}".format(response))
            if response is None:
                logger.info("KillID {0} was not found in the database.".format(killID))
                return False
            else:
                logger.info("KillID {0} was found in the database.".format(killID))
                return True
        except Exception as e:
            logger.error("Unexpected error occurred while querying for killID {0}.".format(killID))
            logger.exception(e)
            return True

    def valid_ship(self, shipID):
        """ Check `ships` table for this `shipID` """

        query = "SELECT `shipID` FROM `ships` WHERE `shipID` = {0};".format(shipID)
        try:
            logger.debug("Executing Query {0}".format(query))
            self.cursor.execute(query)
            response = self.cursor.fetchone()
            logger.debug("Query Response: {0}".format(response))
            if response is None:
                logger.info("ShipID {0} was not found in the database.".format(shipID))
                return False
            else:
                logger.info("ShipID {0} was found in the database.".format(shipID))
                return True
        except Exception as e:
            logger.error("Unexpected error occurred while querying for shipID {0}.".format(shipID))
            logger.exception(e)
            return False


    def fetch_missing_kills(self):
        """ Fill `esi` table with missing data """
        query = "SELECT Z.`killID`, Z.`hash` from `zkill` Z LEFT JOIN `esi` E ON Z.`killID` = E.`killID` WHERE E.`killID` IS NULL;"

        try:
            logger.debug("Executing Query {0}".format(query))
            self.cursor.execute(query)

            data = self.cursor.fetchall()

            logger.info("Missing Records to process: {0}".format(len(data)))

            for killID, key in data:
                self.parse_killdata(fetch_kill(killID, key))

        except Exception as e:
            logger.error("Unexpected error occurred while querying for missing kills.")
            logger.exception(e)
            return

    def parse_spree(self, commit=True):
        """Fill `esi` table with missing data """

        query = 'SELECT E.`killID`, E.`killReport` FROM `esi` E LEFT JOIN `spree` S ON E.`killID` = S.`killID` WHERE S.`killID` IS NULL;'
        insert_spree = 'INSERT INTO `spree` (`killID`, `attackerID`, `attacker_shipID`, `weaponID`, `victimID`, `victim_shipID`, `valid`, `eligible`)  VALUES ( ?, ?, ?, ?, ?, ?, ?, ?)'

        try:
            logger.debug("Executing Query {0}".format(query))
            self.cursor.execute(query)

            data = self.cursor.fetchall()
            logger.info("Spree Records to process: {0}".format(len(data)))

        except Exception as e:
            logger.error("Unexpected error occurred while querying for missing kills.")
            logger.exception(e)
            return

        for killID, killReport in data:
            killData = json.loads(killReport)
            for attacker in killData['attackers']:
                if(attacker["final_blow"] == True):
                    killer = attacker

            if self.valid_ship(killer['ship_type_id']) and self.valid_ship(killData['victim']['ship_type_id']):
                kill = (killID, killer['character_id'], killer['ship_type_id'], killer['weapon_type_id'], killData['victim']['character_id'], killData['victim']['ship_type_id'], 0, 0)
                
                try:
                    logger.debug("Executing INSERT on `esi` for data {0}".format(kill))
                    self.cursor.execute(insert_spree, kill)
                    logger.info("Successfully parsed kill data for killID {0}".format(killData['killmail_id']))
                    
                    if commit: 
                        self.connection.commit()

                except Exception as e:
                    logger.error("Unexpected error occurred while working with killreport for `spree`: {0}".format(killID))
                    logger.exception(e)
            else:
                logger.info("Ships were not valid for {0}: {1} vs {2}".format(killID, killer['ship_type_id'], killData['victim']['ship_type_id']))

    def fetch_missing_players(self):
        """Fill `character` table with missing data """
        
        query = """SELECT id FROM (
                    SELECT DISTINCT `attackerID` AS `id` FROM `spree` UNION 
                    SELECT DISTINCT `victimID` AS `id` FROM `spree`
                ) LEFT JOIN `character` C ON C.`characterID` = `id` WHERE C.`characterID` IS NULL;"""
        
        try:
            logger.debug("Executing Query {0}".format(query))
            self.cursor.execute(query)

            data = self.cursor.fetchall()

            logger.info("Missing Records to process: {0}".format(len(data)))
            for characterID in data:
                self.parse_player(characterID[0], fetch_player(characterID[0]))

        except Exception as e:
            logger.error("Unexpected error occurred while querying for missing kills.")
            logger.exception(e)
            return

    def get_query(self, query):
        try:
            logger.debug("Executing Query {0}".format(query))
            self.cursor.execute(query)

            data = self.cursor.fetchall()

            logger.info("Received {0} rows: ".format(len(data)))
            return data

        except Exception as e:
            logger.error("Unexpected error occurred while querying for missing kills.")
            logger.exception(e)
            return
