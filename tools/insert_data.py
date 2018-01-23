import pymongo

def connect_db (dbname='jobs'):
	db = pymongo.MongoClient ()[dbname]
	return db

def insert_job_postings (db, data):
	jp_coll = db['job_postings']
	jp_coll.insert_many (data)

if __name__ == '__main__': pass
