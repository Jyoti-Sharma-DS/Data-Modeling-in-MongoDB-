#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  1 20:57:44 2020

@author: jyotivashishth
"""

import pandas as pd
import os
import re
import unicodedata

_input_dir = os.path.abspath(os.path.dirname(__file__))

def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    only_ascii = nfkd_form.encode('ASCII', 'ignore')
    #chck if the begining of string consist of b
    if ((len(only_ascii) > 15  ) and  (only_ascii[0] == "b")):
        only_ascii = only_ascii[1:]
        
    return only_ascii


#Replace / by , || Replace ¬† by '' || Replace > by '' 

#create a clean function 
def function_clean(stringToBeCleaned):

    new_string=""   
    new_string=re.sub("[¬†>‚Ä¢]", " ", stringToBeCleaned)
    new_string=re.sub("/", ",", new_string)
    new_string=new_string.strip()
    new_string= remove_accents(str(new_string))
    return new_string

#Clean the job type field
def function_City(LocationString):
    str_ret=""
    str_ret= LocationString.split(',')
    
    #check if the string is in corect format
    if(len(str_ret)==2):
        return str_ret[0]
    else:
        return 'nan'
     
#Clean the job type field
def function_State(LocationString):
    str_ret=""
    str_ret_1=""
    str_ret= LocationString.split(',')
    
    #check if the string is in corect format
    if(len(str_ret)==2):
        #check the length of string 
        if (len(str_ret[1])>=1):
            
            str_ret[1]=str_ret[1].replace(u'\xa0', u' ')
            str_ret[1]= str_ret[1].strip()
   
            #check if the regular expression is matched
            matched = re.match("[A-Z]{2}[0-9]{0,}" , str(str_ret[1]))
            boolVal=bool(matched)
            # print(boolVal)
            if(boolVal):
                str_ret_1 = str_ret[1].split(' ')

                return str_ret_1[0]
            else:
                #return the null value
                return 'nan'
        else:
            #return the null value
            return 'nan'
    else:
        #return the null value
        return 'nan'
    
#monsterjob file name 
MonsterJobFile = "monster_com-job_sample.csv"

#Loading the data
pd_Data_MonsterJob = pd.read_csv(os.path.join(_input_dir , MonsterJobFile))

#create an empty dataframe
pd_Data_MonsterJob_Clean=pd.DataFrame()

#Add the countyrcode 
pd_Data_MonsterJob_Clean['country_code'] = pd_Data_MonsterJob['country_code']
pd_Data_MonsterJob_Clean['Date_added']=pd_Data_MonsterJob['date_added']
pd_Data_MonsterJob_Clean['has_expired']=pd_Data_MonsterJob['has_expired']

#add the jobBoardName
pd_Data_MonsterJob_Clean['job_board']=\
    pd_Data_MonsterJob['job_board'].map(lambda job_board: job_board.split('.')[1])
   



#add the  job description summary 
pd_Data_MonsterJob_Clean['job_description_Abstract']=\
  pd_Data_MonsterJob['job_description'].map(lambda job_description: job_description[:150]+'...')
 
#add the  job description
pd_Data_MonsterJob_Clean['job_description']=\
 pd_Data_MonsterJob['job_description'].map(lambda job_description: function_clean(str(job_description)))


#add the  job title
pd_Data_MonsterJob_Clean['job_title']= pd_Data_MonsterJob['job_title']




#Clean the job type field
pd_Data_MonsterJob_Clean['job_type']=\
    pd_Data_MonsterJob['job_type'].map(lambda job_type: function_clean(str(job_type)))
 


pd_Data_MonsterJob_Clean['City'] =\
  pd_Data_MonsterJob['location'].map(lambda location: function_City(str(location)))

    
pd_Data_MonsterJob_Clean['State'] =\
  pd_Data_MonsterJob['location'].map(lambda location: function_State(str(location)))
  
#fetch the organization
pd_Data_MonsterJob_Clean['JobArea']  =\
  pd_Data_MonsterJob['organization'].map(lambda organization: function_clean(str(organization)))
  
#fetch the organization
pd_Data_MonsterJob_Clean['JobPostingLink'] = pd_Data_MonsterJob['page_url']

#salary
pd_Data_MonsterJob_Clean['salary'] = pd_Data_MonsterJob['salary']

#Experience Level
pd_Data_MonsterJob_Clean['sector'] = pd_Data_MonsterJob['sector']
 

##TopSkills,RequiredQual
pd_Data_MonsterJob_Clean['TopSkills'] = ""
pd_Data_MonsterJob_Clean['MinRequiredQual'] = ""
pd_Data_MonsterJob_Clean['CompanyName'] = ""

#Read the Second File  
#DataScientist file name 
DSJobFile = "data_scientist_united_states_job_postings_jobspikr.csv"

#Loading the data
pd_Data_DSJob = pd.read_csv(os.path.join(_input_dir , DSJobFile))

#create dataframe for clean data 
pd_Data_DSJob_Clean = pd.DataFrame()

#Clean the data scientist file 

#Add the countyrcode 
pd_Data_DSJob_Clean['country_code'] =\
    pd_Data_DSJob['country'].map(lambda country: str(country)[:2].upper())

#add the Date for job added 
pd_Data_DSJob_Clean['Date_added']=pd_Data_DSJob['post_date']

#add data for expiry
pd_Data_DSJob_Clean['has_expired']= ""


#add the jobBoardName
pd_Data_DSJob_Clean['job_board']= pd_Data_DSJob['job_board']

#add the  job description summary 
pd_Data_DSJob_Clean['job_description_Abstract']=\
    pd_Data_DSJob['job_description'].values[1][:150] + '...'

pd_Data_DSJob_Clean['job_description_Abstract']=\
  pd_Data_DSJob_Clean['job_description_Abstract'].map(lambda job_description_Abstract: function_clean(str(job_description_Abstract)))
    
#add the  job description
pd_Data_DSJob_Clean['job_description']=\
    pd_Data_DSJob['job_description'].map(lambda job_description: function_clean(str(job_description)))


#add the  job title remove the special caharcters
pd_Data_DSJob_Clean['job_title']=\
 pd_Data_DSJob['job_title'].map(lambda job_title: function_clean(str(job_title)))
 

#Clean the job type field
pd_Data_DSJob_Clean['job_type']=\
 pd_Data_DSJob['job_type'].map(lambda job_type: function_clean(str(job_type)))
 

#fetch the city
pd_Data_DSJob_Clean['City'] =\
  pd_Data_DSJob['inferred_city'].map(lambda inferred_city: function_clean(str(inferred_city)))
  
#fetch the State
pd_Data_DSJob_Clean['State'] =\
  pd_Data_DSJob['inferred_state'].map(lambda inferred_state: function_clean(str(inferred_state)))

#fetch the job area
pd_Data_DSJob_Clean['JobArea']  =\
  pd_Data_DSJob['category'].map(lambda category: function_clean(str(category)))
  

#fetch the organization
pd_Data_DSJob_Clean['JobPostingLink'] = pd_Data_DSJob['url']


#salary
pd_Data_DSJob_Clean['salary'] = pd_Data_DSJob['salary_offered']

#Experience Level
pd_Data_DSJob_Clean['sector'] = ""
 

##TopSkills,RequiredQual
pd_Data_DSJob_Clean['TopSkills'] = ""
pd_Data_DSJob_Clean['MinRequiredQual'] = ""
pd_Data_DSJob_Clean['CompanyName'] =\
    pd_Data_DSJob['company_name'].map(lambda company_name: function_clean(str(company_name)))
    
    
#Merge  the two data sets into one file and the write it in an excel

frames = [pd_Data_MonsterJob_Clean, pd_Data_DSJob_Clean]

CleanDataSet = pd.concat(frames)



CleanDataSet.to_csv("CleanDataSet.csv")