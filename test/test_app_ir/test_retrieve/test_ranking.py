import os, sys
sys.path.insert (0, os.path.abspath ('./'))
from app_ir.retrieve.ranking import Ranking, WeightedZoneScoring, TermFrequencyScoring, TFIDFScoring
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
# @pytest.mark.skip
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




