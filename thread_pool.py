import multiprocessing
import threading



class Job:
	def __init__(self, function, *parameters):
		self.function = function
		self.parameters = parameters
		self.exception = 0
		self.result = None

	def run(self):
		try:
			self.result = self.function(*self.parameters)
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

	def kernel(self):
		while self.pool_enabled:
			with self.concurrency:
				if len(self.job_queue) == 0:
					self.concurrency.wait()
					continue
				self.working_threads += 1
				job = self.job_queue.pop(0)
			Job.run(job)
			with self.concurrency:
				self.completed.append(job)
				self.working_threads -= 1
				self.concurrency.notify_all()
