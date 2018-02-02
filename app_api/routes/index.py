import app_api.controllers.retrieve as retrieveCtrl 
import app_api.controllers.index as indexCtrl
import app_api.controllers.crawl as crawlCtrl

def route (app, irsys, db):
	@app.route ('/api/search', methods=['GET'])
	def retrieve ():
		return retrieveCtrl.retrieve (irsys)

	@app.route ('/api/index', methods=['POST'])
	def index ():
		return indexCtrl.index (irsys)
	
	@app.route ('/api/crawl/save/jobposting', methods=['POST'])
	def save_job_posting ():
		return crawlCtrl.save_job_posting (db)
