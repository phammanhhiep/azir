'''
Define routes
'''

from flask import request

import app_api.controllers.search as searchCtrl 

def route (app):
	@app.route ('/api/search', methods=['GET'])
	def search ():
		return searchCtrl.search (request)


