class IndexModel:
	'''
	Model: {'termid': xxx, 'pl': [[docid, tf, position1, position2, ...], ...]}

	'''

	def __init__ (self, db, coll_name='indexes'):
		self._coll_name = coll_name
		self._coll = db[self._coll_name]

	def insert_many (self, indexes=None):
		if indexes is None:
			raise ValueError ('No indexes is found.')
		if len (indexes):
			self._coll.insert_many (indexes)

	def find_many (self, termids=None):
		'''
		::return:: a cursor or a list if no termids is found
		'''
		results = []
		if termids is None:
			raise ValueError ('No termids is found.')
		if len (termids):
			results = self._coll.find ({'termid': {'$in': termids}})
		return results	

	def update_many (self, termids=None, updates=None):
		'''
		Update many indexes by their term ids.
		Insert if termids not found.
		'''

		if termids is None and updates is None:
			raise ValueError ('No termids or updates are found.')
		if len (termids) and len (updates):
			for t,u in zip (termids, updates):
				self._coll.update_one ({'termid': t}, {'$set': u}, upsert=True)		

	def drop (self):
		self._coll.drop ()