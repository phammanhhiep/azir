if (jQuery){
	document.addEventListener('DOMContentLoaded', function() {
		$('#crawlAllbBtn').click (function (e){
			chrome.extension.getBackgroundPage ().extract ();
		});
	});
}
else{
	console.log ('not found jquery') 
}







