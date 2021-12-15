import subprocess
import sys



def execute(command: str, echo: bool = False) -> None:
	if echo:
		print(f" > {command}")
	try:
		subprocess.check_output(command, shell=True, stderr=subprocess.STDOUT)
	except subprocess.CalledProcessError as e:
		print(e.output.decode("ascii"))
		raise e



def eprint(*args, **kwargs):
	print(*args, file=sys.stderr, **kwargs)
