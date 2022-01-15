import sqlite3

from .data import *

class SQLiteDatabase:
	def __init__(self, path):
		self.cursors = []
		self.database = sqlite3.connect(path)

	def __del__(self):
		self.database.commit()
		self.database.close()

	def execute(self, query, *parameters) -> list:
		cursor = self.database.cursor()
		cursor.execute(query, parameters)
		results = cursor.fetchall()
		cursor.close()
		return results

	def prepare(self, sql: str = None, *parameters):
		q = SQLiteQuery(self)
		q.append(sql, parameters)
		return q

	def pragma(self, name, value = None):
		if value != None:
			self.execute(f"PRAGMA {name} = {value}")
		return self.execute(f"PRAGMA {name}")[0][0]

	def version(self, version: int = None):
		return self.pragma("user_version", version)



class SQLiteQuery:
	def __init__(self, db: SQLiteDatabase):
		self.cursor = db.database.cursor()
		self.sql = ""
		self.parameters = []

	def __del__(self):
		self.cursor.close()

	def append(self, sql: str, *parameters):
		if sql:
			self.sql += " " + sql
		if parameters:
			self.parameters.extend(parameters)

	def execute(self):
		self.cursor.execute(self.sql, *self.parameters)

	def fetch_all(self, column: str = None):
		self.execute()
		results = []
		for result in self.cursor.fetchall():
			row = {}
			for i, column_definition in enumerate(self.cursor.description):
				row[column_definition[0]] = result[i]
			results.append(row)
		if column:
			results = array_column(results, column)
		return results

	def fetch_one(self, column: str = None):
		self.execute()
		result = self.cursor.fetchone()
		if not result:
			return result
		row = {}
		for i, column_definition in enumerate(self.cursor.description):
			row[column_definition[0]] = result[i]
		if column:
			row = row[column]
		return row

	def insert_id(self):
		self.execute()
		return self.cursor.lastrowid
