# -*- coding: utf-8 -*-
"""
Created on Tue Apr  9 18:56:20 2019

@author: siddh
"""

import requests,csv

class KeyNotFoundError(Exception):
    pass

class LocationDetails(KeyNotFoundError):
    
    def __init__(self,city=None,zipcode=None):
        self.city=city
        self.zipcode=zipcode
    
    def getLocationDetails(self):
        if self.city!=None:
            locationURL='http://apidev.accuweather.com/locations/v1/search?q='+self.city+'&apikey=hoArfRosT1215'
        elif self.zipcode!=None:
            locationURL='http://apidev.accuweather.com/locations/v1/search?q='+self.zipcode+'&apikey=hoArfRosT1215'
        locationDetails=requests.get(locationURL) 
        if locationDetails.status_code != 200:
                # This means something went wrong.
                raise KeyNotFoundError('Rest Call Unsuccessful')
    
        else:
            locationDict={}
            jsonResponse=locationDetails.json() #Remove 0 to get all entries: for cities query
    # =============================================================================
    #         print('############## LOCATION DETAILS ###############')
    #         print(jsonResponse)
    #         print('###############################################')
    # =============================================================================
            jsonResponse=jsonResponse[0]
            locationDict['key']=jsonResponse['Key']
            locationDict['locType']=jsonResponse['Type']
            locationDict['primaryPostalCode']=jsonResponse['PrimaryPostalCode']
            locationDict['locName']=jsonResponse['EnglishName']
            locationDict['country']=jsonResponse['Country']['ID']
            locationDict['lat']=jsonResponse['GeoPosition']['Latitude']
            locationDict['long']=jsonResponse['GeoPosition']['Longitude']
            
            return locationDict





    


        

 
class CurrentWeatheDetails(KeyNotFoundError):  
    def __init__(self,locationDetailsDict=None):
        self.locationDetailsDict=locationDetailsDict
        
    def writeCSV(self,locDetailsDictList=None,identifier=None):
        try:
            if locDetailsDictList==None:
                raise KeyNotFoundError
            else:
                fileName='currentWeatherData_'+str(identifier)+'.csv'
                with open(fileName,mode='w')as csv_file:
                    fieldnames=['location','locationType','postalCode','country','lat','long','time','summary','temperature','unit']
                    writer = csv.DictWriter(csv_file,fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for locDetailDict in locDetailsDictList:
                        locName=locDetailDict['locName']
                        locType=locDetailDict['locType']
                        postalCode=locDetailDict['primaryPostalCode']
                        country=locDetailDict['country']
                        lat=locDetailDict['lat']
                        long=locDetailDict['long']
                        time=locDetailDict['time']
                        summary=locDetailDict['weatherText']
                        temperature=locDetailDict['Value']
                        unit=locDetailDict['Unit']
                        writer.writerow({'location': locName,'locationType':locType,'postalCode':postalCode, 'country': country,'lat':lat,'long':long,'time':time,'summary':summary,'temperature': temperature,'unit':unit})
                
        except KeyNotFoundError:
            print('Weather data unavailable')
        
    def getcurrentWeatherDetails(self):
        try:
            if self.locationDetailsDict['key']==None:
                raise KeyNotFoundError
        except:
            print('Key is empty')
        
        try:
            key=self.locationDetailsDict['key']
            currentWeatherURL='http://apidev.accuweather.com/currentconditions/v1/'+key+'.json?language=en&apikey=hoArfRosT1215'
            currentWeatherDetails=requests.get(currentWeatherURL)
           
            if currentWeatherDetails.status_code!=200:
                raise KeyNotFoundError
            else:
                locDetailsDictList=[]
                currWeather=currentWeatherDetails.json()[0]
                #print('############## CURRENT WEATHER #############')
                #print(currWeather)
                #print('##########################################')
                
                del(self.locationDetailsDict['key'])
                self.locationDetailsDict['time']=currWeather['LocalObservationDateTime']
                self.locationDetailsDict['weatherText']=currWeather['WeatherText']
                temperature=currWeather['Temperature']['Metric']
                self.locationDetailsDict.update(temperature)
                print('locationDetailsDict=',self.locationDetailsDict)
                locDetailsDictList.append(self.locationDetailsDict)
                
                if self.locationDetailsDict['locType']=="City":
                    identifier=self.locationDetailsDict['locName']
                else:
                    identifier=self.locationDetailsDict['primaryPostalCode']
                self.writeCSV(locDetailsDictList,identifier)
        except KeyNotFoundError:
            print('Invalid key entered')

class WeatherDataService:
    def main(self):
        choice=''
        while choice!='exit':
            print('Get weather by zipcode or city?("exit to quit application"):',end="")
            choice=input()
            print('choice=',choice)
            choice=str(choice).lower()
            if choice=='zipcode':
                print('Enter zipcode:',end="")
                zipcode=str(input())
                ldObj = LocationDetails(zipcode=zipcode)
                locationDetailsDict=ldObj.getLocationDetails()
                cwdObj=CurrentWeatheDetails(locationDetailsDict)
                cwdObj.getcurrentWeatherDetails()
            elif choice=='city':
                print('Enter city:',end="")
                city=str(input())
                ldObj = LocationDetails(city=city)
                locationDetailsDict=ldObj.getLocationDetails()
                cwdObj=CurrentWeatheDetails(locationDetailsDict)
                cwdObj.getcurrentWeatherDetails()
            elif choice=='exit':
                return
            else:
                print('Enter correct choice!')
        
        
    
if __name__=='__main__':
    wdsObj=WeatherDataService()
    wdsObj.main()

    

    
    