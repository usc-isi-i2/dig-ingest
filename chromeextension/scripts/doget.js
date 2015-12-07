


function callIngestorService()
{

    chrome.tabs.query({active: true, currentWindow: true}, function(arrayOfTabs) {

         // since only one tab should be active and in the current window at once
         // the return variable should only have one entry
         var activeTab = arrayOfTabs[0];

         ingestCurrentTabUrl(activeTab.url,activeTab.id);

      });
}


function ingestCurrentTabUrl(url,tabId)
{

    $.ajax({
      type:'POST',
      contentType: "application/text",
      //dataType:"jsonp",
      data: '{"url":"' + url+ '","username":"aman", "eshost": "localhost", "esport": "9200","esindex":"dig-ingest01","esdoctype":"WebPage", "esprotocol":"http","esusername":"","espassword":""}',
      url: "http://localhost:5000/ingest/webpage"
    })
      .done(function( data ) {
        processResponse(data,"OK",tabId)
      })
      .fail(function(data){
        processResponse(data,"ERROR",tabId)
      });
}


function processResponse(httpResponse,messageType,tabId)
{
    document.getElementById("hiddenresults").innerHTML = httpResponse;

    var strResponse = document.getElementById("hiddenresults").innerHTML;


    if(messageType = "OK")
    {
       $( ".results" ).append( "<p>INFO - " + document.getElementById("hiddenresults").innerHTML +  "</p>" );
    }
    else
    {
       $( ".results" ).append( "<p>ERROR - " + document.getElementById("hiddenresults").innerHTML +  "</p>" );
    }

    chrome.browserAction.getBadgeText({}, updateTabBadgeText);
}


document.addEventListener('DOMContentLoaded', function()
{
    chrome.browserAction.setBadgeBackgroundColor({color:[0,128,255,255]});

    var clear = document.getElementById('clear');
    clear.addEventListener('click', function() {
                                                btnClearOnClick();
                                                });
    callIngestorService();
});


function updateTabBadgeText(text)
{

    if(text === undefined || text == "")
    {
        num = 1;
    }
    else
    {
        var num = parseInt(text) + 1;
    }
    chrome.browserAction.setBadgeText({text: num.toString()});
}


function btnClearOnClick()
{
    chrome.browserAction.setBadgeText({text: ""});
}

