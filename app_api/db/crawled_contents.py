class CrawledContentModel:
	'''
	Model = {
		'_id': xxxx, # assign by the system
		'id': xxx, # relevant to facebook post only. Facebook post id.
		'content': xxxxxx, 
		'pageid': xxx, # id of page assign by the system
		'author': {
			'_id': xxx,
			'id': xxx,
		}, 
		'createdAt': xxxx, 
		'crawledAt': xxxx,
		comments: [{id: xx, _id: xxx}], # only the outmost ids, and ordered in the same in the posts. 
	...}
	
	::field content:: could contain text and tag, which be process later.
	::field commnent.commentContent:: could contain inner comment (or reply). Later will process.
	::field author.id:: id the website assign the author. 
	::field author.typeid:: messenger id or other id.
	::field ...:: There are other fields which are details about the content. This is helpful for machine learning later
	'''

	def __init__ (self, db, coll_name='crawled_contents'):
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

	def find_by_author (self, messengerid=None): pass

	def find_by_page (self, pageid=None): pass

	def drop (self):
		self._coll.drop ()


class CrawledUserModel:
	'''
	Model = {
		_id: xxx, # assigned by the system
		id: xxx, # assigned by other system. Relevant to Facebook only. Messenger id.
		fullname: xxxx,
		photo: xxxx, # link

	}
	'''

class CrawledCommentModel:
	'''
	Model = {
		_id: xxx, # assigned by the system
		id: xxx, # assigned by other system. Relevant to Facebook only. Comment id.
		commenterid: xxx, # assigned by other system. Relevant to Facebook only. Messenger id.
		contentid: {id: xxx, _id: xxxx}, 
		content: xxx,
		reply: [{id: xxx, _id: xxx}], # only the right below the comment only.
		createdAt: xxx,
	}

	'''

class CrawledPageModel:
	'''
	Model = {
		_id: xxx, # assign by the system
		id: xxx, # id assign by other system. Relevant to facebook groups and pages only.
		url: xxxx,
		pageType: xxxx, # {1: facebook group, 2: facebook fanpage, 3: other}
		description: xxx,
		createdAt: xxx,
		memberNum: xxx, # to facebook groups and pages only.
		statistics: [
			{contentNumber: xxx, lasttime: xxx}
		],
		active: True,
	}

	Store page urls, and its statistic for analyze which pages should be crawled or not.	

	'''

	def __init__ (self, db, coll_name='crawled_pages'):
		self._coll_name = coll_name
		self._coll = db[self._coll_name]

	def insert_many (self, pages=None):
		if pages is None:
			raise ValueError ('No contents are provided.')
		[p.update ({
			'createdAt': datetime.datetime.now (), 
			'active': True, 
			'statistics': []}) for p in pages]
		self._coll.insert_many (pages)

	def find_one (self, pid=None):
		'''
		Find one content by its id
		'''
		if pid is None:
			raise ValueError ('No content id is provided.')
		return self._coll.find_one ({'_id': pid})

	def find_many (self, pids=None): 
		'''
		Find many contents by their ids
		'''
		results = []
		if pids is None:
			results = self._coll.find ()
		elif len (pids):
			results = self._coll.find ({'_id': {'$in': pids}})
		return results

	def updateStatistic (self, statistics=None):	
		if statistics is None: 
			return
		for s in statistics:
			self._coll.update ({'_id': s['_id']}, {'$push': {'statistics': s['statistics']}})

	def deactive (self, pids=None):
		if pids is None:
			return
		self._coll.update ({'_id': {'$in': pids}}, {'$set': {'active': False}})

	def drop (self):
		self._coll.drop ()
