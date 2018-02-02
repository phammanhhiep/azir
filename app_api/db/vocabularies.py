class VocabularyModel:
	'''
	A collection of all terms in all documents in the system.
	schema: {'termid': xx, 'term': yy, 'df': zz}
	::field df:: document frequency of the term. 
	'''
	def __init__ (self, db, coll_name='vocabulary'):
		self._coll_name = coll_name
		self._coll = db[self._coll_name]

	def find (self):
		'''
		Find all 
		''' 
		return self._coll.find ()

	def insert_many (self, vocabulary=None):
		if vocabulary is not None and len (vocabulary):
			self._coll.insert_many (vocabulary)			

	def update_many (self, termids=None, updates=None):
		'''
		Update many terms by their ids.
		Insert if termids not found.
		
		::params updates:: a list of a complete instance of the model of vocabulary. 
		'''
		if termids is None and updates is None:
			raise ValueError ('No termids or updates are found.')
		if len (termids) and len (updates):
			for t,u in zip (termids, updates):
				self._coll.update_one ({'termid': t}, {'$set': u}, upsert=True)

	def drop (self):
		self._coll.drop ()