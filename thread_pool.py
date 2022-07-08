import multiprocessing
import threading



class Job:
	def __init__(self, function, *parameters):
		self.function = function
		self.parameters = parameters
		self.exception = 0
		self.result = None
		self.dependencies = []
		self.has_run = False

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

	def run(self):
		try:
			self.result = self.function(*self.parameters)
			self.has_run = True
		except Exception as e:
			print(e)
			self.exception = e




class ThreadPool:
	def __init__(self, thread_count: int = None):
		self.concurrency = threading.Condition()
		if thread_count == None:
			thread_count = multiprocessing.cpu_count()
		self.pool_enabled = True
		self.thread_pool = []
		self.job_queue = []
		self.completed = []
		self.working_threads = 0
		for i in range(thread_count):
			self.thread_pool.append(threading.Thread(target=self.kernel))
			self.thread_pool[-1].start()

	def wait(self):
		with self.concurrency:
			while len(self.job_queue) or self.working_threads:
				self.concurrency.wait()

	def kill(self):
		self.wait()
		with self.concurrency:
			self.pool_enabled = False
			self.concurrency.notify_all()
		for thread in self.thread_pool:
			thread.join()

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

	def kernel(self):
		while self.pool_enabled:
			with self.concurrency:
				job = self.find_job()
				if job == None:
					self.concurrency.wait()
					continue
				self.working_threads += 1
				self.job_queue.remove(job)
			Job.run(job)
			with self.concurrency:
				self.completed.append(job)
				self.working_threads -= 1
				self.concurrency.notify_all()
