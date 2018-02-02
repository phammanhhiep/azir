class QueuedConentModel:
	'''
	Model: {'docid': xxx, 'tf': [(termid, tf), ...]}
	::field tf:: list of tuple representing term frequency of a term in that content
	'''
	
	def __init__ (self, db, coll_name='queuedcontent'):
		self._coll_name = coll_name
		self._coll = db[self._coll_name]

	def find (self):
		return self._coll.find ()

	def insert_many (self, queues=None):
		if queues is not None and len (queue):
			self._coll.insert_many (queues)

	def drop (self):
		self._coll.drop ()