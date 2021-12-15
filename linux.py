from shell import execute
from os import path
import platform

if platform.system() != 'Linux':
	raise Exception(f"linux.py only runs on Linux OS, not {platform.system()} OS")



def unmount(mount_directory: str) -> None:
	if path.ismount(mount_directory):
		execute("sync")
		execute(f"umount {mount_directory}")



def mount_cifs(share: str, user: str, domain: str, password: str, mount_directory: str) -> None:
	execute(f"mkdir -p {mount_directory}")
	unmount(mount_directory)

	password = "'" + password.replace('\'', '\\\'') + "'"
	execute(f"mount -t cifs //{share} -o user={user}@{domain},pass={password} {mount_directory}")
	assert path.ismount(mount_directory)

	open(f"{mount_directory}/.successful_mount_test", "a").close()
	execute(f"rm {mount_directory}/.successful_mount_test")
