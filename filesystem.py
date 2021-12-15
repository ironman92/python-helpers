from os import path, listdir

def list_directory(directory_path: str) -> list:
	contents = listdir(directory_path)
	contents.sort()
	return contents



def list_directories(directory_path: str) -> list:
	return [file for file in list_directory(directory_path) if path.isdir(path.join(directory_path, file))]



def list_files(directory_path: str) -> list:
	return [file for file in list_directory(directory_path) if path.isfile(path.join(directory_path, file))]
