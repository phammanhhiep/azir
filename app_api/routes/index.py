import app_api.controllers.retrieve as retrieveCtrl 
import app_api.controllers.index as indexCtrl

def route (app, irsys):
	@app.route ('/api/search', methods=['GET'])
	def retrieve ():
		return retrieveCtrl.retrieve (irsys)

	@app.route ('/api/index', methods=['POST'])
	def index ():
		return indexCtrl.index (irsys)

	


