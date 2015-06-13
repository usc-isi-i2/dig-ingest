#!/usr/bin/python

from flask import Flask
from flask import request
from bs4 import BeautifulSoup
import requests
import json
import os
import sys
import logging
from logging.handlers import RotatingFileHandler
import time

from ingestor import Ingestor

app = Flask(__name__)

@app.route('/ingest/webpage/',methods=['POST'])
def gethtml():

    try:

        jsonForm = json.loads(str(request.get_data()))

        url = jsonForm['url']

        username = jsonForm['username']

        if username == '':
            username='memex'

        r = requests.get(url)

        if r.status_code == requests.codes.ok:
            soup = BeautifulSoup(r.content)

            #get the title form the web page
            webpagetitle=''
            if soup.title is not None:
                webpagetitle=soup.title.text

            for s in soup.findAll('script'):
                s.extract()

            bodyText=soup.get_text()
            text = os.linesep.join([s for s in bodyText.splitlines() if s])

            i=Ingestor(jsonForm)


            if not(i.checkIfUrlExists(url)):
                jsonDocument=i.extractFeatures(text)
                #print jsonDocument
                jsonDocument['images']=i.extractImages(soup)

                jsonDocument['title']=webpagetitle
                jsonDocument['bodytext']=text
                jsonDocument['url']=url
                jsonDocument['username']=username

                jsonDocument['screenshot']=i.getwebpagescreenshot(url)

                #print json.dumps(jsonDocument)

                jsonld = i.generateJSON(jsonDocument)

                esresponse=''
                if jsonld:
                    esresponse = i.publishtoes(jsonld)

                return json.dumps(esresponse)

            else:
                logi("Url: " + url + " already exists")
                return "Url: " + url + " already exists"
        else:
            return r

    except Exception as e:
        print >> sys.stderr, e
        loge(str(e))
        #raise e


def loge(message):

    app.logger.error('Error:' + message)

def logi(message):

    app.logger.info('INFO:' + message)

if __name__ == '__main__':
    todaydate = time.strftime("%m-%d-%Y")
    filename = 'logs/' + str(todaydate) + '.log'
    handler = RotatingFileHandler(filename, mode='a',backupCount=1)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)
    app.run()
