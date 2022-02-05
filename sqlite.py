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
		try:
			cursor.execute(query, parameters)
		except Exception as e:
			print(e)
			print(query)
			print(parameters)
			raise e
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
		self.execute_completed = False
		self.col = None

	def __del__(self):
		self.cursor.close()

	def __iter__(self):
		self.reexecute()
		return self

	def __next__(self):
		row = self.fetch_one(self.col)
		if not row:
			raise StopIteration
		return row

	def column(self, column: str = None):
		self.col = column
		return self

	def append(self, sql: str, *parameters):
		if sql:
			self.sql += " " + sql
		if parameters:
			self.parameters.extend(parameters)
		return self

	def execute(self):
		if not self.execute_completed:
			try:
				self.cursor.execute(self.sql, *self.parameters)
			except Exception as e:
				print(e)
				print(self.sql)
				print(self.parameters)
				raise e
			self.execute_completed = True
		return self

	def reexecute(self):
		self.execute_completed = False
		self.execute()
		return self

	def fetch_all(self, column: str = None):
		self.column(column)
		self.execute()
		results = []
		for result in self.cursor.fetchall():
			row = {}
			for i, column_definition in enumerate(self.cursor.description):
				row[column_definition[0]] = result[i]
			results.append(row)
		if self.col:
			results = array_column(results, self.col)
		return results

	def fetch_one(self, column: str = None):
		self.column(column)
		self.execute()
		result = self.cursor.fetchone()
		if not result:
			return result
		row = {}
		for i, column_definition in enumerate(self.cursor.description):
			row[column_definition[0]] = result[i]
		if self.col:
			row = row[self.col]
		return row

	def insert_id(self):
		self.execute()
		return self.cursor.lastrowid
