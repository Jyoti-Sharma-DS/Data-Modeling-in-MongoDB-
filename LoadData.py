#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  6 12:13:48 2020

@author: jyotivashishth
"""

import pandas as pd
import os
import re



_input_dir = os.path.abspath(os.path.dirname(__file__))

#monsterjob file name 
Cleanfile = "CleanDataSet.csv"

#Loading the data
pd_Data = pd.read_csv(os.path.join(_input_dir , Cleanfile))

#rename the column
pd_Data = pd_Data.rename(columns={"Unnamed: 0": "IdValue"})
pd_Data["IdValue"] = pd_Data["IdValue"] + 1

#Fetch the Subset of 
pd_Data_subset = pd_Data.iloc[0:30,]

JobDescription='{"_id":"#ID#","JodDesc": #Desc#,"schema_version": "1_1"}'

#create id insert list
Id_inserted =[]
#create jobDescrption List
JobDescList =[]

#traverse the dataset
for x in range(0,len(pd_Data_subset)):
#for x in range(0,2):
    #fetch the data row 
    Y= pd_Data_subset.iloc[x,]
    #create the 
    JobDescription.replace("#ID#" , '"' + str(Y['IdValue']) + '"')
    #fetch the description
    val = JobDescription.replace("#ID#" , str(Y['IdValue']))
    DescValue = Y['job_description'][1:]
    
    #check if it's single quote string
    if DescValue[0] == "\'":
       DescValue= DescValue.replace("\'","")
       DescValue= DescValue.replace('\"','')
       DescValue= DescValue.replace('\\','')
       DescValue= '"' + DescValue + '"'
       
    
    #check if document is in reject list 
    if DescValue.find('{') == -1 :
        val=val.replace("#Desc#" ,  DescValue)
        val = val.replace("b\'" ," ")
        val = val.replace("\'" ," ")
        Id_inserted.append(str(Y['IdValue']))
        JobDescList.append(val)
    else:
        print("Addding document in reject list!!")
        print("ID :" + str(Y['IdValue']) )
        
        
    
    
#create job description insert file
with open('JobDescription_new_insert.json', 'w') as f:
    for item in JobDescList:
        f.write("%s\n" % item)


         
print("JobDescription records inserted in JobDescription_new_insert.json file !!")        
       
#create empty list
JobinfoList=[]

#Create job info document 
jobinfotag_1='"_id": #ID#, "job_title": #job_title#, "job_description_Abstract": #job_description_Abstract#, "JobPostingLink":  #JobPostingLink#", "schema_version": "1_1"'
JobCategoryTag='"JobCategory": { "sector":#sector#, "Job_type": #Job_type# }'
LocationInfoTag= '"LocationInfo": { "Country_code": #Country_code# ,"state": #state#,"city": #city#}'
JobPostingInfoTag='"JobPostingInfo": { "Job_Board": #Job_Board#, "has_Expired": #has_Expired#}'


dict_noOfTags= { "jobinfotag_1":4, "JobCategoryTag" : 2,"LocationInfoTag":3,"JobPostingInfoTag":2 } 

#traverse the dataset
for idval in Id_inserted:
#ffor idval in Id_inserted[:1]:
    x=int(idval)-1
    #fetch the data row 
    Y= pd_Data_subset.iloc[x,]
    #set the values as null
    jobinfotag_1_Val =""
    JobCategoryTag_Val=""
    LocationInfoTag_Val=""
    JobPostingInfoTag_Val=""
    tempVal=""
    
    #Finals 
    strDocument="{ "
    #create the jobinfotag
    jobinfotag_1_Val= jobinfotag_1.replace("#ID#" , '"' + str(Y['IdValue']) + '"')
    jobinfotag_1_Val= jobinfotag_1_Val.replace("#job_title#" , '"' + str(Y['job_title']) + '"')
    jobinfotag_1_Val= jobinfotag_1_Val.replace("#job_description_Abstract#" , '"' + str(Y['job_description_Abstract']) + '"')
    jobinfotag_1_Val= jobinfotag_1_Val.replace("#JobPostingLink#" , '"' + str(Y['JobPostingLink']) )
    
    ##check the null value 
    
    
    # print("jobinfotag_1_Val")
    # print(jobinfotag_1_Val)
    
    #both null
    _NulltagCount=0
    JobCategoryTag_Val = JobCategoryTag
    
    #'"JobCategory": { "sector":#sector#, "Job_type": #Job_type# }'

    #check tag are not null 
    if Y['sector']== "":
        JobCategoryTag_Val = JobCategoryTag_Val.replace('"sector":#sector#,', '')
        _NulltagCount = _NulltagCount +1
    else:
        JobCategoryTag_Val = JobCategoryTag_Val.replace('#sector#', '"' + str(Y['sector']) + '"')
        
    #check for job type
    if Y['job_type']== "":
        if _NulltagCount == 0:
            JobCategoryTag_Val = JobCategoryTag_Val.replace(', "Job_type": #Job_type#', '')
        else:
            JobCategoryTag_Val = JobCategoryTag_Val.replace('"Job_type": #Job_type#', '')
        _NulltagCount = _NulltagCount +1
    else:
        ##check 
        
        if str(Y['job_type'])[0] == "b":
            tempVal = str(Y['job_type'])[1:]
            tempVal = tempVal.replace("'" , "")
        else:
            tempVal = str(Y['job_type'])
            #replace the value
        JobCategoryTag_Val = JobCategoryTag_Val.replace('#Job_type#', '"' + str(tempVal) + '"')
        
    #check for the value
    if int(dict_noOfTags["JobCategoryTag"]) == _NulltagCount:
        JobCategoryTag_Val=""
            
    # print("JobCategoryTag_Val")
    # print(JobCategoryTag_Val)
    
    #both null
    _NulltagCount=0
   
    LocationInfoTag_Val = LocationInfoTag

    #check for country code
    if Y['country_code'] == "":
        _NulltagCount = _NulltagCount +1
        LocationInfoTag_Val = LocationInfoTag_Val.replace('"Country_code": #Country_code# ,', '')
    else:
        LocationInfoTag_Val = LocationInfoTag_Val.replace('#Country_code#','"'+ str(Y['country_code'])+ '"')
        
    #check for state
    if Y['State'] == "":
        _NulltagCount = _NulltagCount +1
        LocationInfoTag_Val = LocationInfoTag_Val.replace('"state": #state#,' , '')
    else:
        #replace the value
        LocationInfoTag_Val = LocationInfoTag_Val.replace('#state#', '"'+ str(Y['State'])+'"')
        
    #check for city
    if Y['City'] == "":
        _NulltagCount = _NulltagCount +1
        LocationInfoTag_Val = LocationInfoTag_Val.replace('"city": #city#' , '')
    else:
        #replace the value
        LocationInfoTag_Val = LocationInfoTag_Val.replace('#city#', '"' + str(Y['City'])+ '"')
        
    #check for the value
    if int(dict_noOfTags["LocationInfoTag"]) == _NulltagCount:
        LocationInfoTag_Val=""
        
    # print("LocationInfoTag_Val")
    # print(LocationInfoTag_Val)
        
    #both null
    _NulltagCount=0
   
    JobPostingInfoTag_Val = JobPostingInfoTag
    #'"JobPostingInfo": { "Job_Board": #Job_Board#, "has_Expired": #has_Expired#}'
    
    #check for job board
    if Y['job_board'] == "":
        _NulltagCount = _NulltagCount +1
        JobPostingInfoTag_Val = JobPostingInfoTag_Val.replace('"Job_Board": #Job_Board#,', '')
    else:
        JobPostingInfoTag_Val= JobPostingInfoTag_Val.replace('#Job_Board#',  '"' + str(Y['job_board'])+ '"')
        
    #check for has expired
    if Y['has_expired'] == "":
        if _NulltagCount == 0:
            JobPostingInfoTag_Val = JobPostingInfoTag_Val.replace(', "has_Expired": #has_Expired#', '')
        else:
            JobPostingInfoTag_Val = JobPostingInfoTag_Val.replace('"has_Expired": #has_Expired#', '')
       
        _NulltagCount = _NulltagCount +1
    else:
        JobPostingInfoTag_Val= JobPostingInfoTag_Val.replace('#has_Expired#', '"' + str(Y['has_expired']) + '"')
    
    #check for the value
    if int(dict_noOfTags["JobPostingInfoTag"]) == _NulltagCount:
        JobPostingInfoTag_Val=""
        
    # print("JobPostingInfoTag_Val")
    # print(JobPostingInfoTag_Val)
    
    strDocument = strDocument + " " + jobinfotag_1_Val
    
    #check if the tag is null 
    if JobCategoryTag_Val != "":
        strDocument = strDocument + " , " + JobCategoryTag_Val
    
    #check if the tag value is null
    if LocationInfoTag_Val != "":
        strDocument = strDocument + " , " + LocationInfoTag_Val
        
    #check if the tag value is null
    if JobPostingInfoTag_Val != "":
        strDocument = strDocument + " , " + JobPostingInfoTag_Val
        
    # close the tag
    strDocument = strDocument + " }"
    
    #append 
    JobinfoList.append(strDocument)
    
#create job description insert file
with open('JobinfoList_new_insert.json', 'w') as f:
    for item in JobinfoList:
        f.write("%s\n" % item)
         
print("JobInfo records inserted in JobinfoList_new_insert.json file !!")