/*
	The process to extract data:
		- Get last states from server includng website urls, last crawling time.
		- For each website, call content script to extract.
		- After each successful extract, save their page states, and statistics. 
		Statistics include job posting number, 

*/

EXTRACT = 0;
FGGROUP_LIST = [ // USE TEMPORARILY
	// 'https://www.facebook.com/groups/vieclamitvietnam/',
	// 'https://www.facebook.com/groups/1444315562541612/',	
	// 'https://www.facebook.com/groups/timvieclam20/',
	'C:/Users/Acer/Desktop/vieclamitvietnam.html',
	'C:/Users/Acer/Desktop/vieclam.html',
];

OTHER_PAGE_LIST = []
STATE_URL = 'http://127.0.0.1:5000/api/crawl/state';

listenToContentReadyMessage ();
// getStates (STATE_URL); // TEMPORARILY NOT USE.

function extract (){
	var target_page = FGGROUP_LIST [0];
	if (!target_page){
		return;
	} 
	var url = _getFbGroupUrl (target_page);
	_setExtractStatus (1);
	console.log ('Start openning new tab ...');
	chrome.tabs.create ({url: url}, 
		function (tab){
			// extract (other_pages);
			FGGROUP_LIST = FGGROUP_LIST.slice (1);
		});

};

function listenToContentReadyMessage (){
	chrome.runtime.onConnect.addListener(function(port) {
		console.assert(port.name == 'content_state');
		port.onMessage.addListener(function(msg) {
			if (msg.content_state == 1 && EXTRACT == 1){
				msg = {action: 1, last_page: false};
				if (FGGROUP_LIST.length == 1){
					msg.last_page = true;
				}
				port.postMessage(msg);
			}
			else if (msg.content_state == 1 && EXTRACT == 0){
				port.postMessage({action: 0});
			}
			else if (msg.extract_status){
				console.log (msg);
				_setExtractStatus (0);
				extract (FGGROUP_LIST);
			}
			else if (msg.error){
				console.log (msg.error);
				record_extract_errors (msg.error);
				_setExtractStatus (0);
				extract (FGGROUP_LIST);
			}

		});
	});	
}

function _getFbGroupUrl (url){
	sorted_setting = '?sorting_setting=CHRONOLOGICAL';
	return url + sorted_setting;
}

function _setExtractStatus (status){
	EXTRACT = status;
}

function recordErrors (){
	// save information somewhere ...
}

function getStates (data, url){
	/*
	Get facebook group states:
		- List of fbgroup urls and their corresponding last crawling time.	
	Get other website states:
		- List of website urls and their corresponding last crawling time.		
	*/

	$.ajax ({
		method: 'GET',
		url: STATE_URL,
	}).done (function (data){
		console.log ('Successfully receiving data!');
		FGGROUP_LIST = data['fbgroups'];
		OTHER_PAGE_LIST = data['other'];

	}).fail (function (err, status){
		console.log ('err ' + status)
	})
};

