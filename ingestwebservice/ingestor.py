#!/usr/bin/python

__author__ = 'amandeep'

import ConfigParser
import requests
import json
from time import gmtime, strftime
from collections import OrderedDict
import hashlib
from boto.s3.connection import S3Connection
import time
import sys

from flask import Flask
import logging
from logging.handlers import RotatingFileHandler
import time

sys.path.append('/Users/amandeep/Github/memexorg/ist/pyht/common/extraction/')
sys.path.append('/Users/amandeep/Github/memexorg/ist/pyht/common')
sys.path.append('/Users/amandeep/Github/dig-features/dig/phone')
sys.path.append('/Users/amandeep/Github/dig-ingest')

from config import Config
import logging



from extract_address import ExtractAddress
from extract_age import ExtractAge
from extract_email import ExtractEmail
from extract_ethnicity import ExtractEthnicity
from extract_height import ExtractHeight
from extract_name import ExtractName
from extract_rate import ExtractRate
from extract_website import ExtractWebsite
from extract_weight import ExtractWeight
from matchphone import extractPhoneNumbers
from elasticsearch import Elasticsearch

app = Flask(__name__)

class Ingestor (object):

    def __init__(self,formParameters):

        configuration = ConfigParser.RawConfigParser()
        configuration.read('config_real.properties')
        self.karmaHostName = configuration.get('KarmaRestServer', 'hostname')
        self.karmaPort = configuration.get('KarmaRestServer', 'port')
        self.karmaProtocol = configuration.get('KarmaRestServer', 'protocol')
        self.karmaRestPath = configuration.get('KarmaRestServer', 'restpath')
        self.R2rmlURI = configuration.get('KarmaRestServer', 'R2rmlURI')
        self.ContentType = configuration.get('KarmaRestServer', 'ContentType')
        self.ContextURL = configuration.get('KarmaRestServer', 'ContextURL')
        self.BaseURI = configuration.get('KarmaRestServer', 'BaseURI')

        self.esHostName = formParameters['eshost']
        self.esPort = formParameters['esport']
        self.esIndexName = formParameters['esindex']
        self.esDocType = formParameters['esdoctype']
        self.esProtocol = formParameters['esprotocol']
        self.esUserName = formParameters['esusername']
        self.esPassword = formParameters['espassword']

        self.s3KeyID = configuration.get('AWSS3', 'AWS_ACCESS_KEY_ID')
        self.s3SecretKey = configuration.get('AWSS3', 'AWS_SECRET_ACCESS_KEY')
        self.s3ImageUrlPrefix = configuration.get('AWSS3', 's3ImageUrlPrefix')
        self.s3Bucket = configuration.get('AWSS3', 'bucketname')

        self.splashHostName = configuration.get('SplashServer', 'hostname')
        self.splashPort = configuration.get('SplashServer', 'port')
        self.splashProtocol = configuration.get('SplashServer', 'protocol')
        self.splashImageWidth = configuration.get('SplashServer', 'imgWidth')
        self.splashImageHeight = configuration.get('SplashServer', 'imgHeight')




    def generateJSON(self,jsonDocument):

        karmaurl=self.karmaProtocol + '://' + self.karmaHostName + ':' + self.karmaPort + self.karmaRestPath

        payload={}
        payload['R2rmlURI'] = self.R2rmlURI
        payload['ContentType'] = self.ContentType
        payload['RawData'] = json.dumps(jsonDocument)
        payload['ContextURL'] = self.ContextURL
        payload['BaseURI'] = self.BaseURI

        self.logi("Calling karma rest service to generate json ld...")
        response = requests.post(karmaurl, data=payload)

        if response.status_code == requests.codes.ok:
            self.logi("Json ld generation succeeded")
            return response.content
        else:
            self.loge("Bad response from karma rest server:" + str(response))
            return None

    def publishtoes(self,jsondoc):

        esUrl = self.getESObject()

        es = Elasticsearch([esUrl], show_ssl_warnings=False)
        jsonarray = json.loads(jsondoc)
        jsonobj=jsonarray[0]
        objkey = jsonobj['uri']

        self.logi("Uploading json to ElasticSearch, id:" + objkey + "...")
        res = es.index(index=self.esIndexName, doc_type=self.esDocType, body=json.dumps(jsonobj), id=objkey)
        self.logi("Uploaded json to ElasticSearch, id:" + objkey)

        return res

    def getESObject(self):

        if self.esUserName.strip() != '' and self.esPassword.strip() != '':
            esUrl = self.esProtocol+'://' + self.esUserName + ":" + self.esPassword + "@" + self.esHostName + ':' + str(self.esPort)
        else:
            esUrl = self.esProtocol+'://' + self.esHostName + ':' + str(self.esPort)

        return esUrl

    def extractImages(self,soup):
        images = soup.findAll("img")

        imagejsonarray=[]

        s3imageurlprefix = self.s3ImageUrlPrefix

        for image in images:
            imagejsonobject={}

            if image.has_attr('src'):

                imgurl=image['src']

                if imgurl.startswith('http'):
                    imagejsonobject['imageurl']=imgurl

                    imghash=hashlib.sha1(imgurl.encode('utf-8')).hexdigest().upper()
                    imgext = imgurl[imgurl.rindex('.')+1:]
                    imagename=imghash+'.'+imgext
                    imagejsonobject['s3imageurl'] = s3imageurlprefix+imagename

                    response = requests.get(imgurl, stream=False)

                    if response.status_code == requests.codes.ok:

                        imagejsonarray.append(imagejsonobject)

                        self.uploadImagetoS3(response.content,imagename)

                    del response

        return imagejsonarray

    def uploadImagetoS3(self,imageobject,imagename,bucket=None):

        from boto.s3.key import Key

        AWS_ACCESS_KEY_ID=self.s3KeyID
        AWS_SECRET_ACCESS_KEY=self.s3SecretKey

        bucketName=''
        if bucket:
            bucketName=bucket
        else:
            bucketName=self.s3Bucket

        conn = S3Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

        self.logi("Uploading image to s3:" + imagename + "...")
        s3bucket=conn.get_bucket(bucketName)
        s3bucketkey=Key(s3bucket)
        s3bucketkey.key=imagename
        s3bucketkey.set_contents_from_string(imageobject)

        self.logi("Uploaded image to s3:" + imagename)

        conn.close()



    def getwebpagescreenshot(self,url):

        splashserverhost = self.splashHostName
        splashserverport=str(self.splashPort)

        splashurl =  self.splashProtocol + '://' +  splashserverhost + ':' + splashserverport +'/'

        splashurl += 'render.png?url=' + url + '&width=' + self.splashImageWidth + '&height=' + self.splashImageHeight

        imagename=hashlib.sha1(url.encode('utf-8')).hexdigest().upper() +'_screenshot.png'

        self.logi("Getting the screenshot from splash server:" + splashurl + "...")
        response=requests.get(splashurl,stream=False)

        if response.status_code == requests.codes.ok:
            #self.loImagetoS3(response.content,imagename)
            self.logi("Screenshot image name:" + imagename + " for url:" + url)
            return self.s3ImageUrlPrefix + imagename
        else:
            self.loge("Bad response from splash server:" + response.content)
            return ''

    def extractFeatures(self,bodyText):

            c=Config()
            processedJson={}

            self.logi("Begin feature extraction...")

            epoch = int(time.mktime(time.strptime(strftime("%Y-%m-%d %H:%M:%S", gmtime()),"%Y-%m-%d %H:%M:%S")))
            processedJson['importime']=epoch

            address = ExtractAddress(c).test(bodyText)
            addressarray=[]
            if len(address) > 0:
                for ad in address:
                    #print "Address", ad["value"]
                    addressarray.append(ad["value"])
            processedJson['address']=addressarray

            age = ExtractAge(c).test(bodyText)
            agearray=[]
            if len(age)>0:
                for ag in age:
                    #print "age", ag["value"]
                    agearray.append(ag["value"])
            processedJson['age']=agearray

            email = ExtractEmail(c).test(bodyText)
            emailarray=[]
            if len(email)>0:
                for em in email:
                    #print "email", em["value"]
                    emailarray.append(em["value"])
            processedJson['email']=emailarray

            ethinicity = ExtractEthnicity(c).test(bodyText)
            ethinicityarray=[]
            if len(ethinicity) > 0:
                for et in ethinicity:
                    ethinicityarray.append(et["value"])
            processedJson['ethnicity']=ethinicityarray

            height =  ExtractHeight(c).test(bodyText)
            heightarray=[]
            if len(height)>0:
                for he in height:
                    #print "height",he["value"]
                    heightarray.append(he["value"])
            processedJson['height']=heightarray

            name = ExtractName(c).test(bodyText)
            namearray=[]
            if len(name)>0:
                for na in name:
                    #print "name", na["value"]
                    namearray.append(na["value"])
            processedJson['name']=namearray

            rate = ExtractRate(c).test(bodyText)
            ratearray=[]
            if len(rate)>0:
                for ra in rate:
                    #print "rate", ra["value"].encode('ascii', 'replace')
                    ratearray.append(ra["value"])
            processedJson['rate']=ratearray

            website = ExtractWebsite(c).test(bodyText)
            websitearray = []
            if len(website) > 0:
                for we in website:
                    #print "website", we["value"].encode('utf8')
                    websitearray.append(we["value"])
            processedJson['website']=websitearray

            weight=ExtractWeight(c).test(bodyText)
            weightarray=[]
            if len(weight)>0:
                for wei in weight:
                    #print "weight", wei["value"].encode('utf8')
                    weightarray.append(wei["value"])
            processedJson['weight']=weightarray

            phone = extractPhoneNumbers(bodyText)
            phonearray=[]
            dedupphone=list(OrderedDict.fromkeys(phone))
            if len(phone)>0:
                for pho in dedupphone:
                    #print "Andrew phone",pho
                    phonearray.append(pho)
            processedJson['phone']=phonearray

            self.logi("End feature extraction.")

            return processedJson

    def checkIfUrlExists(self,url):

        try:
            esUrl = self.getESObject()
            esUrl = esUrl + "/" + self.esIndexName + "/_search"
            termquery = '{ "filter": { "term": {"url": "' + url + '"}}}'
            response = requests.post(esUrl,data=termquery)

            if response.status_code == requests.codes.ok:
                responsejson = json.loads(response.content)
                totalhits = responsejson['hits']['total']
                if totalhits:
                    if int(totalhits) == 0:
                        return False
                    else:
                        self.logi("The url:" + url + " already exists in ElasticSearch")
                        return True
                else:
                    self.loge("Bad ElasticSearch query response while checking if the url exists:" + str(response.content))
                    return False
            else:
                self.loge("Bad http response while querying ElasticSearch:" + str(response.content))
                return False
        except Exception as e:
            print >> sys.stderr, e
            self.loge(str(e))
            #raise e

    def loge(self,message):
        app.logger.error('Error:' + message)

    def logi(self,message):
        app.logger.info("INFO:" + message)

def main():
    i=Ingestor()
    todaydate = time.strftime("%m-%d-%Y")
    filename = 'logs/' + str(todaydate) + '.log'
    handler = RotatingFileHandler(filename, mode='a',backupCount=1)
    handler.setLevel(logging.INFO)
    app.logger.addHandler(handler)



if __name__ == '__main__':
    main()