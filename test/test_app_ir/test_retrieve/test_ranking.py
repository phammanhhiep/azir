import os, sys
sys.path.insert (0, os.path.abspath ('./'))
from app_ir.retrieve.ranking import Ranking, WeightedZoneScoring, TermFrequencyScoring, TFIDFScoring, CosineScoring
import math

import pytest

@pytest.mark.zoneScoreTesting
@pytest.mark.skip
def test_zs_score (): 
	pl = [
		[0, (0,1)], 
		[2, (0,1,2)],
		[4, (2,)]
	]

	zone_weights = {
		0: 0.4,
		1: 0.1,
		2: 0.5
	}

	expected_scores = [[2, 1], [0, 0.5], [4, 0.5]]

	s = WeightedZoneScoring (zone_weights)
	scores = s.score (pl)

	assert len (scores) == len (expected_scores)
	for a,b in zip (expected_scores, scores):
		len (a) == len (b)
		for c,d in zip (a,b):
			assert c == d

@pytest.mark.zoneScoreTesting
@pytest.mark.skip
def test_zs_train ():
	assert False

@pytest.mark.termFrequencyTesting
@pytest.mark.skip
def test_tf_score ():
	pl = [
		[0, (1,5,2)], 
		[2, (2,3,11)],
		[4, (1,1)]
	]

	expected_scores = [[2, 16], [0, 8], [4, 2]]

	t = TermFrequencyScoring ()
	scores = t.score (pl)

	assert len (scores) == len (expected_scores)
	for a,b in zip (expected_scores, scores):
		len (a) == len (b)
		for c,d in zip (a,b):
			assert c == d	

@pytest.mark.tfidfTesting
@pytest.mark.skip
def test_tfidf_score ():
	def idf (df,N):
		return math.log (N/df, 10)

	pl = [
		[0, (100,(20,1),(10,5),(5,2))], 
		[2, (100,(32,2),(12,3),(13,11))],
		[4, (100,(11,1),(10,5),(50,2))],
	]

	expected_scores = [
		[2, (2 * idf(32,100)) + (3 * idf(12,100)) + (11 * idf(13,100))], 
		[0, (1 * idf(20,100)) + (5 * idf(10,100)) + (2 * idf(5,100))], 
		[4, (1 * idf(11,100)) + (5 * idf(10,100)) + (2 * idf(50,100))]
	]

	t = TFIDFScoring ()
	scores = t.score (pl)

	assert len (scores) == len (expected_scores)
	for a,b in zip (expected_scores, scores):
		len (a) == len (b)
		for c,d in zip (a,b):
			assert c == d


@pytest.mark.cosineScoringTesting
# @pytest.mark.skip
def test_cosine_score ():
	def idf (df,N):
		return math.log (N/df, 10)

	pl = [
		[None, (100, None, (20,1), (11,1), (20,1), (4,2))],
		[0, (100, 20, (20,0), (11,15), (20,0), (4,1))], 
		[2, (100, 10, (20,1), (11,0), (20,0), (4,20))],
		[4, (100, 100, (20,0), (11,0), (20,5), (4,7))],
	]

	expected_scores = [
		[2, ((1 * 1 * idf(20,100)* idf(20,100)) + 0 + 0 + (2 * 20 * idf(4,100)* idf(4,100))) / 10],
		[0, (0 + (15 * 1 * idf(11,100) * idf(11,100)) + 0 + (1 * 2 * idf(4,100) * idf(4,100))) / 20],
		[4, (0 + 0 + ( 1 * 5 * idf(20,100)* idf(20,100)) + (2 * 7 * idf(4,100)* idf(4,100))) / 100],
	]

	t = CosineScoring ()
	scores = t.score (pl)

	assert len (scores) == len (expected_scores)
	for a,b in zip (expected_scores, scores):
		len (a) == len (b)
		for c,d in zip (a,b):
			assert c == d


