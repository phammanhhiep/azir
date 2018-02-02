class ContentModel:
	'''
	Model: {'_id': xxxx, 'content': xxxxxx}
	'''

	def __init__ (self, db, coll_name='contents'):
		self._coll_name = coll_name
		self._coll = db[self._coll_name]

	def insert_one (self, content=None):
		if content is None:
			raise ValueError ('No content is provided.')
		self._coll.insert_one ({'content': content})

	def insert_many (self, contents=None):
		if contents is None:
			raise ValueError ('No contents are provided.')
		contents = [{'content': c} for c in contents]
		self._coll.insert_many (contents)

	def find_one (self, cid=None):
		'''
		Find one content by its id
		'''
		if cid is None:
			raise ValueError ('No content id is provided.')
		return self._coll.find_one ({'_id': cid})

	def find_many (self, cids=None): 
		'''
		Find many contents by their ids
		'''
		results = []
		if cids is None:
			results = self._coll.find ()
		elif len (cids):
			results = self._coll.find ({'_id': {'$in': cids}})
		return results

	def drop (self):
		self._coll.drop ()