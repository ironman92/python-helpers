from datetime import datetime, timedelta



def parse_datetime(source) -> datetime:
	if isinstance(source, datetime):
		return source
	if isinstance(source, str):
		patterns = [
			"%Y-%m-%d %H:%M:%S",
			"%Y-%m-%d %H:%M",
			"%Y-%m-%d",
			"%Y%m%d%H%M%S",
		]
		for pattern in patterns:
			try:
				return datetime.strptime(source, pattern)
			except:
				pass
	if isinstance(source, int) or isinstance(source, str) and source.isnumeric():
		return datetime.fromtimestamp(int(source))
	raise Exception(f"Unknown date format {source}")



def date_difference(t1, t2) -> timedelta:
	t1 = parse_datetime(t1)
	t2 = parse_datetime(t2)
	return t2 - t1



def array_column(array, column) -> list:
	return [row.get(column) for row in array]



def array_unique(array) -> list:
	return list(set(array))
