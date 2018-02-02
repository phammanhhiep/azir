class ContentVectorModel:
	'''
	Model: {'docid': xxx, 'tf': [(termid, tf), ...]}
	::field tf:: list of tuple representing term frequency of a term in that content
	'''
	
	def __init__ (self, db, coll_name='contentvectors'):
		self._coll_name = coll_name
		self._coll = db[self._coll_name]

	def insert_many (self, doc_vectors=None):
		if doc_vectors is None:
			raise ValueError ('No doc_vectors are found.')
		if len (doc_vectors):
			self._coll.insert_many (doc_vectors)

	def update_many (self, doc_vectors=None):
		if doc_vectors is None:
			raise ValueError ('No doc_vectors are found.')
		if len (doc_vectors):
			for dv in doc_vectors:
				docid = dv['docid']
				self._coll.update_one ({'docid': docid}, {'$set': dv}, upsert=True)		

	def find_many (self, docids=None):
		'''
		Fetch and return doc vector in the same order of docids.
		Assume that the docids are sorted in ascending order.
		'''
		docvs = []
		if docids is None:
			raise ValueError ('No docids are found.')
		if len (docids):
			docvs = self._coll.find ({'docid': {'$in': docids}}).sort ('docid', 1)
		return docvs

	def drop (self):
		self._coll.drop ()