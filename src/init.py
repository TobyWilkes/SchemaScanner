import mysql.connector
import re
import configparser
import difflib
from getpass import getpass
from os import walk, path
from mysql.connector import Error

Config = configparser.ConfigParser()
Config.read("config.ini")

# Setup Configuration
SCHEMA_TEMPLATE = Config.get('Schema', 'Pattern')
SCHEMA_PATH = Config.get('Schema', 'BaseFolder').replace("\\", "/")

SQL_HOST = Config.get('Connection', 'Host')
SQL_PORT = Config.get('Connection', 'Port')
SQL_DATABASE = Config.get('Connection', 'Database')
SQL_USER = Config.get('Connection', 'User')

IGNORE_FILE = Config.get('Preferences', 'IgnoreNoFile')
IGNORE_DB = Config.get('Preferences', 'IgnoreNotInDatabase')

# Get database connection
def get_connection():
    try:
        sqlPass = getpass("Db Password: ")
        connection = mysql.connector.connect(host=SQL_HOST,
                                            port=SQL_PORT,
                                            database=SQL_DATABASE,
                                            user=SQL_USER,
                                            password=sqlPass)
        if connection.is_connected():
            return connection, connection.cursor(dictionary=True)
    except Error as e:
        print("Error while connecting to MySQL", e)
        exit()

# Convert input paths to regex format
def get_regex(path, template):
    escaped = re.escape(path + "/" + template)
    return escaped.replace("\[DB\]", "(?P<DB>.+)").replace("\[SCHEMA\]", "(?P<SCHEMA>.+)")

# Create a map of files to database schemas from files
def parse_files():
    regex = get_regex(SCHEMA_PATH, SCHEMA_TEMPLATE)
    map = {}
    count = 0

    if not path.exists(SCHEMA_PATH):
        exit("Couldnt find schema folder")

    for (dirpath, dirnames, filenames) in walk(path.normpath(SCHEMA_PATH)):
        for file in filenames:
            fullPath = path.normpath(path.join(dirpath, file)).replace("\\", "/")
            hits = re.search(regex, fullPath)
            if hits is None:
                continue
            count += 1
            db = hits.group("DB")
            schema = hits.group("SCHEMA")
            map[db, schema] = fullPath
    print(count, "files found..")
    if count == 0:
        exit()
    return map

map = parse_files()
connection, cursor = get_connection()

# Setup data sets for tracking diff
fileSchemas = set()
dbSchemas = set()
fileMap = {}
differenceCount = 0

# Strip schema names from files
for db, schema in map:
    file = map[db, schema]
    schemaString = "`{}`.`{}`".format(db, schema)
    fileMap[schemaString] = file
    fileSchemas.add(schemaString)

# Strip schema names from database, ignore information schemas
cursor.execute("SELECT `table_schema`, `table_name` FROM INFORMATION_SCHEMA.tables WHERE `table_schema` <> \"information_schema\"")
schemaResult = cursor.fetchall()
for schema in schemaResult:
    schemaString = "`{}`.`{}`".format(schema["table_schema"], schema["table_name"])
    dbSchemas.add(schemaString)

# Find diff, and output
schemaDifference = dbSchemas ^ fileSchemas
for dif in schemaDifference:
    if dif in fileSchemas:
        if not IGNORE_DB == "True":
            print("[NOT IN DATABASE]", dif)
            differenceCount += 1
    else:
        if not IGNORE_FILE == "True":
            print("[NO FILE SCHEMA]", dif)
            differenceCount += 1

# Find matches for schema inspection
schemaMatch = fileSchemas.intersection(dbSchemas)

# Perform diff check on DB schema vs file schema and output
for match in schemaMatch:
    cursor.execute("SHOW CREATE TABLE {}".format(match))
    dbSchema = cursor.fetchone()["Create Table"]
    fileSchema = open(fileMap[match]).read()
    aiResult = re.search("AUTO_INCREMENT\=(?P<AI>[0-9]+)\ ", dbSchema)
    if not aiResult is None:
        ai = "AUTO_INCREMENT=" + aiResult.group("AI") + " "
        dbSchema = dbSchema.replace(ai, "")
    
    fileDiff = difflib.unified_diff(dbSchema.split("\n"), fileSchema.split("\n"), match, fileMap[match])
    for diff in fileDiff:
        print("[TABLE DIFF]", match, ":", diff)
        differenceCount += 1

# Output counts and finish
print("[Check done]", differenceCount, "differences found")