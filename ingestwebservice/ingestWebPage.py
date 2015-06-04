#!/usr/bin/python






from flask import Flask
from flask import request
from bs4 import BeautifulSoup


from ingestor import Ingestor

import requests
import json
import os
import sys

app = Flask(__name__)

@app.route('/ingest/webpage/')
def gethtml():

    try:
        url=request.args.get('url')

        #get username from the url parameters
        username=request.args.get('user')

        #default username, CHECK LATER IF WE NEED THIS
        if username is None:
            username='memex'


        r = requests.get(url)

        if(r.status_code == 200):
            soup = BeautifulSoup(r._content)

            #get the title form the web page
            webpagetitle=''
            if soup.title.text:
                webpagetitle=soup.title.text



            for s in soup.findAll('script'):
                s.extract()


            bodyText=soup.get_text()
            text = os.linesep.join([s for s in bodyText.splitlines() if s])

            i=Ingestor()
            print "this"
            jsonDocument=i.extractFeatures(text)
            print "that"

            print jsonDocument

            jsonDocument['images']=i.extractImages(soup)
            jsonDocument['title']=webpagetitle
            jsonDocument['bodytext']=text
            jsonDocument['url']=url
            jsonDocument['username']=username
            jsonDocument['screenshot']=i.getwebpagescreenshot(url)

            print json.dumps(jsonDocument)

            jsonld = i.generateJSON(jsonDocument)

            esresponse = i.publishtoes(jsonld)

            return json.dumps(esresponse)

        else:
            return r

    except Exception as e:
        print >> sys.stderr, e
        raise e


if __name__ == '__main__':
    app.run()
