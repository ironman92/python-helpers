import multiprocessing
from texttable import Texttable
import threading
import time



class Job:
	def __init__(self, function, *parameters, name: str = "Unnamed"):
		self.function = function
		self.parameters = parameters
		self.exception = None
		self.result = None
		self.dependencies = []
		self.has_run = False
		self.name = name
		self.start = None
		self.finish = None

	def is_ready(self):
		for dependency in self.dependencies:
			if dependency.has_run == False:
				return False
		return True

	def after(self, *preceding_job):
		self.dependencies += preceding_job
		return self

	def before(self, *following_job):
		for job in following_job:
			job.after(self)
		return self

	def status(self) -> dict:
		return {
			"name":		self.name,
			"start":	self.start,
			"duration":	self.runtime(),
			"finish":	self.finish,
			"pending":	[job.name for job in self.dependencies if not job.has_run],
		}

	def run(self):
		try:
			self.start = time.time()
			self.result = self.function(*self.parameters)
			self.finish = time.time()
			self.has_run = True
		except Exception as e:
			print(e)
			self.exception = e


	def log(self, message: str):
		print(f"\t{self.runtime()}\t{self.name}: {message}")

	def runtime(self) -> str:
		finish = time.time() if self.finish == None else self.finish
		start = time.time() if self.start == None else self.start
		return format(finish - start, "0.2f")



class Thread:
	def __init__(self, notify: threading.Condition = None):
		self.run = True
		self.concurrency = threading.Condition()
		self.job = None
		self.notify = notify
		self.thread = threading.Thread(target=self.kernel)
		self.thread.start()

	def kill(self):
		with self.concurrency:
			self.run = False
			self.concurrency.notify_all()
		self.thread.join()

	def ready(self) -> bool:
		with self.concurrency:
			return self.job == None

	def execute(self, job: Job):
		with self.concurrency:
			assert self.job == None, "Thread double scheduled"
			self.job = job
			self.concurrency.notify_all()

	def status(self) -> dict:
		return {
			"active": self.job != None,
			"job": self.job.status() if self.job else None,
		}

	def kernel(self):
		while self.run:
			with self.concurrency:
				if self.job == None:
					self.concurrency.wait()
					continue
			Job.run(self.job)
			self.job = None
			if self.notify:
				with self.notify:
					self.notify.notify_all()



class ThreadPool:
	CPU_COUNT = multiprocessing.cpu_count()

	def __init__(self, thread_count: int = None):
		self.concurrency = threading.Condition()
		if thread_count == None:
			thread_count = CPU_COUNT
		self.pool_enabled = True
		self.thread_pool = []
		self.job_queue = []
		self.completed = []
		self.kernel_thread = threading.Thread(target=self.kernel)
		self.kernel_thread.start()
		for i in range(thread_count):
			self.thread_pool.append(Thread(self.concurrency))

	def wait(self):
		with self.concurrency:
			while len(self.job_queue):
				self.concurrency.wait()

	def busy(self):
		with self.concurrency:
			if len(self.job_queue):
				return True
			for thread in self.thread_pool:
				if not thread.ready():
					return True

	def kill(self):
		self.wait()
		with self.concurrency:
			self.pool_enabled = False
			self.concurrency.notify_all()
		self.kernel_thread.join()
		for thread in self.thread_pool:
			thread.kill()

	def __del__(self):
		self.kill()

	def schedule(self, *job: Job):
		with self.concurrency:
			self.job_queue += job
			self.concurrency.notify_all()

	def find_job(self) -> Job:
		for job in self.job_queue:
			if job.is_ready():
				return job
		return None

	def find_thread(self) -> Thread:
		for thread in self.thread_pool:
			if thread.ready():
				return thread

	def kernel(self):
		while self.pool_enabled:
			with self.concurrency:
				job = self.find_job()
				thread = self.find_thread()
				if job == None or thread == None:
					self.concurrency.wait()
					continue
				self.job_queue.remove(job)
				thread.execute(job)
				self.completed.append(job)

	def status(self) -> dict:
		status = []
		for thread in self.thread_pool:
			status.append(thread.status())
		return status

	def task_list(self) -> str:
		with self.concurrency:
			thread_activity = Texttable()
			thread_activity.header(["Thread", "Job", "Duration"])
			thread_activity.set_cols_align(['l', 'l', 'r'])
			thread_activity.set_cols_dtype(['i', 't', 't'])
			thread_activity.set_deco(Texttable.HEADER)
			for i, thread in enumerate(self.thread_pool):
				thread_status = thread.status()
				job_status = thread_status["job"]
				job_name = job_status["name"] if job_status else "-"
				job_duration = job_status["duration"] if job_status else "-"
				thread_activity.add_row([i, job_name, job_duration])
			job_list = Texttable()
			job_list.header(["Order", "Job", "Pending"])
			job_list.set_cols_align(['l', 'l', 'l'])
			job_list.set_cols_dtype(['i', 't', 't'])
			job_list.set_deco(Texttable.HEADER)
			for i, job in enumerate(self.job_queue):
				job_status = job.status()
				job_list.add_row([i, job_status["name"], ", ".join(job_status["pending"])])
			return f"THREADS\n{thread_activity.draw()}\n\nJOBS\n{job_list.draw()}"
