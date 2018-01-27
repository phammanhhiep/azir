import os, sys
sys.path.insert (0, os.path.abspath ('./'))
from app_ir.retrieve.retrieval import Retrieval
from app_ir.indexing.indexing import Indexing
from app_ir.preprocess.preprocess import Preprocessing

import pytest, pymongo
from collections import defaultdict
from bson.objectid import ObjectId

@pytest.mark.retrievalTest
@pytest.mark.skip
def test__fetch_postings_lists ():
	'''
	Test: Fetch existing terms
	'''

	vocabulary = [
		{'termid': 0, 'term': 'xx', 'df': 1},
		{'termid': 1, 'term': 'yy', 'df': 1},
		{'termid': 2, 'term': 'zz', 'df': 1},
		{'termid': 3, 'term': 'tt', 'df': 1},
	]

	index = [
		{'termid': 0, 'pl': [[0,1,10], [1,1,20]]},
		{'termid': 1, 'pl': [[0,1,11]]},
		{'termid': 2, 'pl': [[0,1,12], [1,1,12], [2,1,12], [3,1,12]]},
		{'termid': 3, 'pl': [[0,1,14]]},
	]

	tokens = ['xx', 'yy', 'zz', 'tt']

	expected_pl = [
		[[0,1,11]],
		[[0,1,14]],
		[[0,1,10], [1,1,20]],
		[[0,1,12], [1,1,12], [2,1,12], [3,1,12]]
	]

	try:
		indexing = Indexing ('test')
		preprocessing = Preprocessing ()		
		indexing.vocabulary_coll.insert_many (vocabulary)
		indexing.index_coll.insert_many (index)
		indexing.create_cache ()
		r = Retrieval ('test', indexing=indexing, preprocessing=preprocessing)
		pl = r._fetch_postings_lists (tokens)

		assert len (pl) == len (expected_pl)
		for a,b in zip (pl, expected_pl):
			assert len (a) == len (b)
			for c,d in zip (a,b):
				assert len (c) == len (d)
				for e,f in zip (c,d):
					assert e == f

	except Exception as ex:
		print (ex)
		indexing.vocabulary_coll.drop ()
		indexing.index_coll.drop ()
		assert False
	else:
		indexing.vocabulary_coll.drop ()
		indexing.index_coll.drop ()		

@pytest.mark.retrievalTest
@pytest.mark.skip
def test__merge_postings_lists (): pass

@pytest.mark.retrievalTest
@pytest.mark.skip
def test__fetch_docs (): pass

@pytest.mark.retrievalTest
@pytest.mark.skip
def test_retrieve (): pass

