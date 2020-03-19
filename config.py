database_file = 'spree.db'
logging_file = 'sample.log'
logging_format = '%(asctime)-15s %(clientip)s %(user)-8s %(message)s'

"""
CREATE TABLE `zkill` (
    `killID` INT UNSIGNED NOT NULL,
    `hash` VARCHAR(40) NOT NULL,
    `value` REAL NOT NULL,
    `killReport` TEXT NOT NULL,
    PRIMARY KEY (`killID`)
);
"""