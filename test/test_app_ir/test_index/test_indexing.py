import os, sys
sys.path.insert (0, os.path.abspath ('./'))
from app_ir.indexing.indexing import Indexing, BSBIndexing, SPIMIndexing

import pytest, pymongo
from bson.objectid import ObjectId

@pytest.mark.indexingTest
@pytest.mark.skip
def test__parse ():
	tokens = [
		[
			['xx', 'yy', 'zz'],
			['xx', 'tt']
		],
		[
			['yy', 'yy', 'zz'],
			['zz', 'tt']
		],	
	]

	docIDs = [0,1]

	b = Indexing ('test')
	postings = b._parse (tokens, docIDs)
	vocabulary = b.get_vocabulary ()
	assert len (vocabulary) == 4
	assert vocabulary['xx']['df'] == 1
	assert vocabulary['yy']['df'] == 2
	assert vocabulary['zz']['df'] == 2
	assert vocabulary['tt']['df'] == 2

	assert len (postings) == 10
	expected_postings = [
		[0, 0, 0], [1, 0, 1],[2, 0, 2],[0, 0, 3],[3, 0, 4],
		[1, 1, 0], [1, 1, 1], [2, 1, 2], [2, 1, 3], [3, 1, 4]
	]

	for i in range (len (postings)):
		p = postings[i]
		exp = expected_postings[i] 
		assert p[0] == exp[0]
		assert p[1] == exp[1]
		assert p[2] == exp[2]

@pytest.mark.indexingTest
@pytest.mark.skip
def test__index ():
	postings = [
		[0, 0, 0], [1, 0, 1], [2, 0, 2], [0, 0, 3], [3, 0, 4], 
		[1, 1, 0], [1, 1, 1], [2, 1, 2], [2, 1, 3], [3, 1, 4]]

	b = Indexing ('test')
	index = b._index (postings)
	expected_index = [
		[0, [[0, 2, 0, 3]]], [1, [[0, 1, 1], [1, 2, 0, 1]]],
		[2, [[0,1,2], [1, 2, 2, 3]]], [3, [[0,1,4], [1,1,4]]]
	]
	assert len (index) == len (expected_index)
	for j in range (4):
		i = index[j]
		ei = expected_index[j]
		assert i[0] == ei[0]
		assert len (i[1]) == len (ei[1])
		for k,t in zip (i[1], ei[1]):
			assert len (k) == len (t)
			for m in range (len (k)):
				assert k[m] == t[m]

@pytest.mark.indexingTest
# @pytest.mark.skip
def test__update_indexes (): pass

@pytest.mark.indexingTest
# @pytest.mark.skip
def test__index_new_docs (): pass

@pytest.mark.indexingTest
# @pytest.mark.skip
def test__inverse_left_join_postings_lists (): pass

@pytest.mark.indexingTest
# @pytest.mark.skip
def test__merge_indexes (): pass

@pytest.mark.indexingTest
# @pytest.mark.skip
def test_index (): pass

@pytest.mark.bsbindexingTest
@pytest.mark.skip
def test__insert_block (): pass

@pytest.mark.bsbindexingTest
@pytest.mark.skip
def test__merge_blocks ():
	index0 = [
		{'termid': 0, 'pl': [[0,1,0]]},
		{'termid': 1, 'pl': [[0,1,1], [1,1,5]]},
	]

	index1 = [
		{'termid': 0, 'pl': [[2,1,10]]},
		{'termid': 2, 'pl': [[2,2,5,100], [3,1,1]]},
	]

	index2 = [
		{'termid': 2, 'pl': [[4,1,11]]},
		{'termid': 3, 'pl': [[4,1,5], [5,1,1]]},
	]	

	index3 = [
		{'termid': 4, 'pl': [[6,1,6], [7,1,7]]},
		{'termid': 5, 'pl': [[8,1,6], [9,1,7]]}
	]

	b = BSBIndexing ('test')
	b.db[b.temp_index_coll_template.format (0)].insert_many (index0)
	b.db[b.temp_index_coll_template.format (2)].insert_many (index1)
	b.db[b.temp_index_coll_template.format (4)].insert_many (index2)
	b.db[b.temp_index_coll_template.format (6)].insert_many (index3)

	expected_index = [
		{'termid': 0, 'pl': [[0,1,0], [2,1,10]]},
		{'termid': 1, 'pl': [[0,1,1], [1,1,5]]},
		{'termid': 2, 'pl': [[2,2,5,100], [3,1,1], [4,1,11]]},
		{'termid': 3, 'pl': [[4,1,5], [5,1,1]]},
		{'termid': 4, 'pl': [[6,1,6], [7,1,7]]},
		{'termid': 5, 'pl': [[8,1,6], [9,1,7]]}
	]

	dnum = 8
	bsize_max = 2

	try:
		b._merge_blocks (dnum, bsize_max)
		index = list (b.index_coll.find ().sort ('termid', 1))		
		assert len (index) == len (expected_index)
		for j in range (len (index)):
			i = index[j]
			ei = expected_index[j]
			assert i['termid'] == ei['termid']
			assert len (i['pl']) == len (ei['pl'])
			for k,t in zip (i['pl'], ei['pl']):
				assert len (k) == len (t)
				for z in range (len (k)):
					assert k[z] == t[z]	
	except Exception as ex:
		print (ex)
		b.db[b.temp_index_coll_template.format (0)].drop ()
		b.db[b.temp_index_coll_template.format (2)].drop ()
		b.db[b.temp_index_coll_template.format (4)].drop ()
		b.db[b.temp_index_coll_template.format (6)].drop ()		
		b.index_coll.drop ()
		assert False
	else:
		b.index_coll.drop ()		

@pytest.mark.bsbindexingTest
@pytest.mark.skip
# RETEST after complete the update index method.
def test_create_index ():
	collection = [
		[0, [['xx', 'yy', 'zz'],['xx', 'tt']]],
		[1, [['yy', 'yy', 'zz'],['zz', 'tt']]],	
	]

	b = BSBIndexing ('test')
	b.create_index (collection, bsize_max=1)

	expected_vocabulary = [
		{'term': 'xx', 'termid': 0, 'df': 1},
		{'term': 'yy', 'termid': 1, 'df': 2},
		{'term': 'zz', 'termid': 2, 'df': 2},
		{'term': 'tt', 'termid': 3, 'df': 2},
	];

	expected_index = [
		{'termid': 0, 'pl': [[0, 2, 0, 3]]},
		{'termid': 1, 'pl': [[0, 1, 1], [1, 2, 0,1]]},
		{'termid': 2, 'pl': [[0, 1, 2], [1, 2, 2, 3]]},
		{'termid': 3, 'pl': [[0, 1, 4], [1, 1, 4]]},
	]

	vocabulary = list (b.vocabulary_coll.find ().sort ('termid', 1))
	assert len (vocabulary) == len (expected_vocabulary)
	for i in range (len (vocabulary)):
		v = vocabulary[i]
		ev = expected_vocabulary[i]
		assert v['term'] == ev['term']
		assert v['termid'] == ev['termid']
		assert v['df'] == ev['df']

	index = list (b.index_coll.find ().sort ('termid', 1))

	assert len (index) == len (expected_index)
	for j in range (len (index)):
		i = index[j]
		ei = expected_index[j]
		assert i['termid'] == ei['termid']
		assert len (i['pl']) == len (ei['pl'])
		for k,t in zip (i['pl'], ei['pl']):
			assert len (k) == len (t)
			for z in range (len (k)):
				assert k[z] == t[z]	

	b.index_coll.drop ()
	b.vocabulary_coll.drop ()

@pytest.mark.bsbindexingTest
@pytest.mark.skip
def test__fetch_index_by_termid (): pass

@pytest.mark.bsbindexingTest
@pytest.mark.skip
def test__index_new_docs ():
	collection = [
		[0, [['xx', 'yy', 'zz'],['xx', 'tt']]],
		[1, [['yy', 'yy', 'zz'],['zz', 'tt']]],	
	]

	disk_index = [
		{'termid': 1, 'pl': [[9, 1, 1], [7,1,1]]},
		{'termid': 20, 'pl': [[8, 1, 1]]},
	]

	try:
		b = BSBIndexing ('test')
		b._cache_index = {
			0: [[2, 1, 10]],
			10: [[12, 1, 10]],
		}		

		b.index_coll.insert_many (disk_index)
		index = b._index_new_docs (collection)
		expected_index = [
			[0, [[0, 2, 0, 3], [2, 1, 10]]], [1, [[0, 1, 1], [1, 2, 0, 1], [7,1,1], [9, 1, 1]]],
			[2, [[0,1,2], [1, 2, 2, 3]]], [3, [[0,1,4], [1,1,4]]]
		]

		assert len (index) == len (expected_index)
		for j in range (4):
			i = index[j]
			ei = expected_index[j]
			assert i[0] == ei[0]
			assert len (i[1]) == len (ei[1])
			for k,t in zip (i[1], ei[1]):
				assert len (k) == len (t)
				for m in range (len (k)):
					assert k[m] == t[m]		

	except Exception as ex:
		print (ex)
		b.index_coll.drop ()
		assert False
	else:
		b.index_coll.drop ()

@pytest.mark.bsbindexingTest
@pytest.mark.skip
def test_ (): pass

