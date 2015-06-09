function callIngestorService()
{
	alert('SUCCESS')
	$.ajax({
                type : 'GET',
                url : 'http://localhost:5000/ingest/webpage/?url=http://www.my2centsreviews.com/921080/index&user=aman'
            });
}

document.addEventListener('DOMContentLoaded', function()
{
	callIngestorService();
});

