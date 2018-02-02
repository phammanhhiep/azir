$(document).ready(function(){

	var CRAWL_SAVE_URL = 'http://127.0.0.1:5000/api/crawl/save/jobposting';
	var CRAWL_MAX_WAIT_TIME = 2;
	var CRAWL_MAX_WAIT_NUM = 3; // Number of time waiting new data being fetched.
	var CRAWL_MAX_SCROLL_NUM = 3;
	var CRAWL_MAX_SCROLL_TIME = 20000; // millisecond
	var CRAWL_DATA_WAIT_TIME = 3000;
	var CRAWL_MAX_POST_NUM = 1000; 
	var CRAWL_CLOSE_WINDOW = false;
	var CRAWL_SCROLL_JUMP = 1;
	var CRAWL_CURRENT_TIME = new Date ();
	var CRAWL_DAY_NUM = 1;
	var CRAWL_LAST_TIME = new Date (CRAWL_CURRENT_TIME); CRAWL_LAST_TIME.setDate (CRAWL_LAST_TIME.getDate () - CRAWL_DAY_NUM);

	var DOM_IDS = {
		/*
		ids, classes, or pattern of ids or classes of elements in the page
		*/
		postList: {id: 'pagelet_group_mall'},
		post: {idPattern: 'mall_post_'},
	} 


	running (CRAWL_CLOSE_WINDOW);

	function running (closeWindow){
		var port = chrome.runtime.connect({name: "content_state"});
		port.postMessage({content_state: 1});
		port.onMessage.addListener(function(msg) {
			if (msg.action == 1){
				extractData ()
					.then (function success (data){
						console.log ('after extracting ...')
						saveData (data, CRAWL_SAVE_URL)
						port.postMessage({extract_status: 1});
						if (closeWindow){
							window.close ();
						}
					})
					.catch (function error (err){
						cosole.log ('Cannot extracting. Something goes wrong ...')
						console.log (err);
						port.postMessage({error: err});
					})
			}
		});	
	};

	function _extract_author (){
		/*
		Things about the author are needed to extracted:
			- user id
			- photo
		*/
	}

	function _extract_content (){
	}

	function _extract_comments (){

	}

	function _extract_page_statistic (){

	}

	function _extractData (){
		/*
		There exist normal post and special posts, which include header that specifies price and location.

		The wrapper of all contents within a post has a class called "userContentWrapper". It contain both post content and comments ...

		 Note about the contents needed to extracted:
			May be more than one contents like text, photo, link, and share of other post. 
			+ user content		
				- The wrapper class is "userContent". Below the class, and still contain all content is class "text_exposed_root". 
				- header: for some special type of posts like a "sell and buy" post. If the header exits, the normal userContent class is empty, and the user content is often put into a class "mtm". If exist other content like share, link, or photo, each are put into the other class "mtm". Note that there still exits another "userContent" that contains the content, but not the header, and the content and the header are their sibling. So far, I observered only posts with one user content and one or zero other type of content, and thus exits only two class "mtm".
				- Without header, the "userContent" is between user profile and the other content like a photo, a link, or a shared post.  
				- Contain complicate structure of html tags, not just text. 
				- Within class "text_exposed_root", for long text, the some part is exposed. Other is hidden and is store in different element, whose class is name "text_exposed_hide".
			+ photo: 
				- 
				- If have only photo and user content, they are often put into a 
				- keep them but put in seperate from user content for later processing.
			+ Link: Found in second class "mtm"
			+ Shared post: 
				- The content within the shared is pretty complicated, and so far I see they DO NOT follow the structure about the user content above.
				- << NOTICE >> Should collect but add a status of not showing the posts until having a good treatment for them.
		
		Approach for normal post:
			+ get the element "userContent first". 
			+ determine it is a normal post or special post.
			+ with a normal post, just pick two sibling above and below the userContent. Pretty sure the first is about the author, and the other is photo, link, or shared post.


		Approach for special post (sell-buy):		
		

		*/


		return {'author': 'pmh', 'content': 'xxxx x xx x '}
	}



	function _extractOldestPostingTime (){
		/*
		Extract the posting time of the oldest post so far
		*/

		return -1
	}

	function _prepare_saved_data (data){
		/*
		Add some statistics before save data
		*/
		return data;
	}	

	function _get_posting_list (){
		var postWrapper = document.getElementById (DOM_IDS.postList.id);
		var posts = postWrapper.querySelectorAll('[id^=' + DOM_IDS.post.idPattern + ']');
		return posts		
	}

	function extractData (){
		/*
		Scrolling down to fetch data and stop when at least one of the conditions are met. 
		Then extract data and return.
		The conditions to stop scrolling:
			+ scrolling time reaches a limit
			+ the oldest post being fetched is older than the last time extracting data. Doing so prevent the extractor from extract the already-extracted data.
		
		How to scroll:
			+ scroll in the same pace until reach the end of the page.
			+ after reach the end of a page, check if more data is fetched. If fetched, the length of the post list should be greater than the last one, and thus reset scrolling params and go on with normal flow. 
			+ Otherwise, sleep a while and check again. Try several times before give waiting, and thus stop scrolling.
			+ Time to scroll does not include time to wait for new data being fetched.
		*/

		// console.log ('extracting data ...');
		return new Promise (async function (resolve, reject){
			var currPostNum = _get_posting_list ().length;
			var lastPostNum = currPostNum;
			var scrollTime = 0;
			var currScrollNum = 0;
			var currBodyHeight = document.body.scrollHeight;
			var currTempHeight = currTempHeight;
			var lastBodyHeight = 0;
			var currHeight = 0;
			var waitNum = 0;
			var oldestPostingTime = -1;
			while (true) {
				// console.log ('Starting a new loop')
				if (scrollTime >= CRAWL_MAX_SCROLL_TIME || oldestPostingTime > CRAWL_LAST_TIME){
					// console.log ('scrolling time reached the limit or reach the post extracted before')
					break;	
				}

				if (currScrollNum >= CRAWL_MAX_SCROLL_NUM){
					currPostNum = _get_posting_list ().length;
				}

				if (currScrollNum >= CRAWL_MAX_SCROLL_NUM && currPostNum > lastPostNum){
					// Need both conditions to indicate that new data is fetch when scroll to the end of the page.
					console.log ('The new data was fetched. Reset scrolling parameters.');
					currTempHeight = document.body.scrollHeight;
					lastBodyHeight = currBodyHeight;
					currBodyHeight = currTempHeight;
					currScrollNum = 0;
					lastPostNum = currPostNum;
				}
				else if (currScrollNum >= CRAWL_MAX_SCROLL_NUM && currPostNum == lastPostNum && waitNum <= CRAWL_MAX_WAIT_NUM){
					// To the end but no data is fetched. Wait.
					// console.log ('Waiting: ' + waitNum);
					await sleep (CRAWL_DATA_WAIT_TIME);
					waitNum++;
					continue;	
				}
				else if (currScrollNum >= CRAWL_MAX_SCROLL_NUM && currPostNum == lastPostNum && waitNum > CRAWL_MAX_WAIT_NUM){
					// No hope new data being fetched. Stop scrolling.
					// console.log ('No more data. Stop after: ' + waitNum);
					break;
				}

				// console.log ('Start sleeping before scrolling ...')

				t = randomWaitTime (CRAWL_MAX_WAIT_TIME);
				await sleep (t);

				currHeight = lastBodyHeight + (currBodyHeight - lastBodyHeight) / (CRAWL_MAX_SCROLL_NUM - currScrollNum);
				window.scrollTo (0, currHeight);

				// console.log ('Scrolling ...')

				scrollTime += t;
				currScrollNum += CRAWL_SCROLL_JUMP;
				oldestPostingTime = _extractOldestPostingTime ();
			}

			// console.log ('Start extracting ...');

			var data = _extractData ();
			data = _prepare_saved_data (data)
			resolve (data)
		})
	}

	function saveState (){
		/*
			Save state of content in the current running.

		*/
	}

	function getState (){
		/*
			Get state of content script in the last running
		*/
	}

	function saveData (data, url){
		$.ajax ({
			method: 'POST',
			url: url,
		    data: JSON.stringify(data),
		    contentType: "application/json; charset=UTF-8",
		}).done (function (msg){
			console.log ('done sending data ...');
			console.log (msg);

		}).fail (function (err, status){
			console.log ('err ' + status)
		})
	};

	function sleep (ms) {
		return new Promise(resolve => setTimeout(resolve, ms));
	}

	function randomWaitTime (maxtime){
		// maxtime is an integer.
		maxtime = maxtime ? maxtime : 3
		t = Math.random () * maxtime * 1000 + 1000;
		return t
	}	

});


