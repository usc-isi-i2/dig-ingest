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


sys.path.append('/Users/amandeep/Github/memexorg/ist/pyht/common/extraction/')
sys.path.append('/Users/amandeep/Github/memexorg/ist/pyht/common')
sys.path.append('/Users/amandeep/Github/dig-features/dig/phone')
sys.path.append('/Users/amandeep/Github/dig-ingest')

from config import Config



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


class Ingestor (object):

    def __init__(self):

        configuration = ConfigParser.RawConfigParser()
        configuration.read('config.properties')
        self.karmaHostName = configuration.get('KarmaRestServer', 'hostname')
        self.karmaPort = configuration.get('KarmaRestServer', 'port')
        self.karmaProtocol = configuration.get('KarmaRestServer', 'protocol')
        self.karmaRestPath = configuration.get('KarmaRestServer', 'restpath')
        self.R2rmlURI = configuration.get('KarmaRestServer', 'R2rmlURI')
        self.ContentType = configuration.get('KarmaRestServer', 'ContentType')
        self.ContextURL = configuration.get('KarmaRestServer', 'ContextURL')
        self.BaseURI = configuration.get('KarmaRestServer', 'BaseURI')

        self.esHostName = configuration.get('ElasticSearch', 'hostname')
        self.esPort = configuration.get('ElasticSearch', 'port')
        self.esIndexName = configuration.get('ElasticSearch', 'index')
        self.esDocType = configuration.get('ElasticSearch', 'doctype')
        self.esProtocol = configuration.get('ElasticSearch', 'protocol')

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

        response = requests.post(karmaurl, data=payload)

        return response.content

    def publishtoes(self,jsondoc):

        es = Elasticsearch([self.esProtocol+'://' + self.esHostName + ':' + str(self.esPort)], show_ssl_warnings=False)

        jsonarray = json.loads(jsondoc)
        jsonobj=jsonarray[0]
        objkey = jsonobj['uri']
        res = es.index(index=self.esIndexName, doc_type=self.esDocType, body=json.dumps(jsonobj), id=objkey)

        return res


    def extractImages(self,soup):
        images = soup.findAll("img")

        imagejsonarray=[]

        #cacheimageurls=[]
        #s3imageurls=[]

        s3imageurlprefix = self.s3ImageUrlPrefix

        for image in images:
            imagejsonobject={}

            imgurl=image['src']

            imagejsonobject['imageurl']=imgurl

            imghash=hashlib.sha1(imgurl.encode('utf-8')).hexdigest().upper()
            imgext = imgurl[imgurl.rindex('.')+1:]
            imagename=imghash+'.'+imgext
            imagejsonobject['s3imageurl'] = s3imageurlprefix+imagename

            response = requests.get(imgurl, stream=False)

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

        s3bucket=conn.get_bucket(bucketName)
        s3bucketkey=Key(s3bucket)
        s3bucketkey.key=imagename
        s3bucketkey.set_contents_from_string(imageobject)

        conn.close()



    def getwebpagescreenshot(self,url):

        splashserverhost = self.splashHostName
        splashserverport=str(self.splashPort)

        splashurl =  self.splashProtocol + '://' +  splashserverhost + ':' + splashserverport +'/'

        splashurl += 'render.png?url=' + url + '&width=' + self.splashImageWidth + '&height=' + self.splashImageHeight

        imagename=hashlib.sha1(url.encode('utf-8')).hexdigest().upper() +'_screenshot.png'

        response=requests.get(splashurl,stream=False)

        self.uploadImagetoS3(response.content,imagename)

        return self.s3ImageUrlPrefix + imagename

    def extractFeatures(self,bodyText):

            c=Config()
            processedJson={}
            print "this"

            epoch = int(time.mktime(time.strptime(strftime("%Y-%m-%d %H:%M:%S", gmtime()),"%Y-%m-%d %H:%M:%S")))
            processedJson['importime']=epoch
            #print epoch

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

            #return processedJson


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
                    print "ethinicity", et["value"]
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

            return processedJson


def main():
    i=Ingestor()
    print i.s3ImageUrlPrefix

print

if __name__ == '__main__':
  main()