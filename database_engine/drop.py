"""
Module for deleting tables from the database 
"""
from .engine_config import SNOOKER_ENGINE
from .read import get_tables

_all__ = ["drop_all",
"drop_all_formatted"]

def drop_all():
	"""
	Delete all the tables from the database
	"""
	print("Deleting the all tables from the database...")
	SNOOKER_ENGINE.execute("SET FOREIGN_KEY_CHECKS = 0")
	all_tables = get_tables()
	for table in all_tables:
		SNOOKER_ENGINE.execute(f"DROP TABLE {table}")
	SNOOKER_ENGINE.execute("SET FOREIGN_KEY_CHECKS = 1")
	print("All tables have been deleted!\n")

def drop_all_formatted():
	"""
	Delete all the formatted tables from the database
	"""
	print("Deleting all the tables from the database except for the raw_game and raw_tournament tables...")
	SNOOKER_ENGINE.execute("SET FOREIGN_KEY_CHECKS = 0")
	all_tables = get_tables()
	deleted = ""
	for table in all_tables:
		if table != "raw_game" and table != "raw_tournament":
			SNOOKER_ENGINE.execute(f"DROP TABLE {table}")
			deleted = deleted + table + "\n"
	SNOOKER_ENGINE.execute("SET FOREIGN_KEY_CHECKS = 1")
	print(f"The following tables were deleted: {deleted}\n")

