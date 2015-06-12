


function callIngestorService()
{

    chrome.tabs.query({active: true, currentWindow: true}, function(arrayOfTabs) {

         // since only one tab should be active and in the current window at once
         // the return variable should only have one entry
         var activeTab = arrayOfTabs[0];
         getCurrentTabUrl(activeTab.url);

      });

function getCurrentTabUrl(url)
{
     getUrl(url);
}

}

function getUrl(url)
{
    $.ajax({
      type:'POST',
      contentType: "application/text",
      //url: url,
      data: {url:url,username:"aman", eshost: "localhost", esport: "9200",esindex:"dig-ingest01",esdoctype:"WebPage", esprotocol:"http",esusername:"",espassword:""},
      url: "http://localhost:5000/ingest/webpage/"
    })
      .done(function( data ) {
        processResponse(data)
      })
      .fail(function(data){
        processResponse(data)
      });
}


function processResponse(httpResponse)
{
    document.getElementById("results").innerHTML = httpResponse;
}


document.addEventListener('DOMContentLoaded', function()
{
	callIngestorService();
});

