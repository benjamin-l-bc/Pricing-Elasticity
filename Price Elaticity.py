# -*- coding: utf-8 -*-

'''
Created on Mon May 15 14:48:46 2017

@author: Benjamin Shi
Run it under python 3.0
'''


import pyodbc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math as m
from statsmodels.formula.api import ols
from statsmodels.regression.linear_model import RegressionResults as rr
#import statistics tools in python not only for regression model but also can see the confidence interval.

def timeformat(a):
    b=str(a)
    time='{a}-{b}-{c}'.format(a=b[0:4],b=b[4:6],c=b[6:8])
    return time
#change the time format

def login(uid,password,conf,Rsquare):
    global VM_hours
    con = pyodbc.connect('DRIVER={Server};SERVER=Server;DATABASE=DB;UID={id};PWD={pas}'.format(Server='SQL Server',id=uid,pas=password))
    VM_hours=pd.read_sql('with loc as (SELECT weekkey,AI_offertype,serviceinstancename,os,family,series,core,customeryear,sum(TotalUnits) as hours from v_VM_Hours h join VM_type t on h.serviceinstancename=t.Instance join datekey d on h.DateKey=d.datekey JOIN MC_MNC_EA m on h.CloudCustomerGuid=m.CloudCustomerGuid where m.CloudCustomerGuid not in (\'8A526C00-E0AB-4FCD-909F-85B5DA47356B\',\'5248E23C-A420-4DA4-B8B2-3C69DFB32955\',\'FBFA6D69-654E-458F-8559-8D16F3D3629E\',\'2E634FFB-8C57-439B-B91E-3696191C5386\',\'B5DA9F98-98D7-4937-A41D-B4A478A835C2\',\'3BD360D0-757A-49E1-9C8E-3386B6EAC942\',\'CBF13497-70E0-4DE1-9891-B9F04BFA9C03\',\'8FC2D77F-2952-4504-A043-748784C1DEF5\',\'E3D33C3F-9ACC-43D9-ABDB-33340255CE19\') and m.MNC=0 group by customeryear,weekkey,serviceinstancename,os,family,Series,core,AI_offertype) select weekkey,serviceinstancename as Instance,customeryear,AI_offertype,os,family,series,core*hours as core_hours from loc',con)
    #connect to the sqlDB and pull raw data
    VM_discount=pd.read_sql('select * from [discount%]',con)
    VM_hours=pd.merge(VM_hours,VM_discount)
    VM_hours['Yunqi_Dis']=np.where(VM_hours['weekkey']<=20161001,100,VM_hours['Yunqi_Discount'])
    VM_hours['MC_Feb_Dis']=np.where(VM_hours['weekkey']<=20170210,100,VM_hours['MC_Feb_Discount'])
    #apply the price cut value
    VM_hours=VM_hours.drop(['Yunqi_Discount','MC_Feb_Discount','series','Instance'],axis=1)
    VM_hours=VM_hours[(VM_hours['weekkey']!=20160131)&(VM_hours['weekkey']!=20170611)]
    VM_hours['timeperiod']=VM_hours.weekkey.apply(timeformat)  
    VM_hours=VM_hours[(VM_hours['family']!='A0 Basic')&(VM_hours['family']!='A0 Std')&(VM_hours['family']!='Av2')]
    #VM_hours=VM_hours[VM_hours['customeryear']=='FY17']
    #Ali and MC doesn't have A0 Basic and A0 std price cut after Feb 2016. Delete the useless data.
    
    for i in VM_hours[(VM_hours['family']!='F')&(VM_hours['family']!='A2-4 Std')&(VM_hours['family']!='A2-4 Basic')&(VM_hours['family']!='A5-7 Std')].family.unique():
        VM_p2=VM_hours[VM_hours['family']==i]
        VM_p2=VM_p2.drop('weekkey',axis=1)
        for j in VM_hours.os.unique():
            VM_p2_os=VM_p2[VM_p2['os']==j]
            #by different operation systems
            for k in VM_p2_os.AI_offertype.unique():
                #by different offertype
                VM_p2_os_type=VM_p2_os[VM_p2_os['AI_offertype']==k]
                VM_p2_os_type=VM_p2_os_type.groupby(['timeperiod','Yunqi_Dis','MC_Feb_Dis']).sum()
                VM_p2_os_type.to_csv('VM_p2_os_type.csv')
                VM_p2_os_type=pd.read_csv('VM_p2_os_type.csv')
                VM_p2_os_type['time']=VM_p2_os_type.index+1
                VM_p2_os_type['lntime']=VM_p2_os_type['time'].apply(m.log)
                VM_p2_os_type['lntime_2']=VM_p2_os_type['lntime']**2
                VM_p2_os_type['lntime_3']=VM_p2_os_type['lntime']**3
                VM_p2_os_type['lntime_4']=VM_p2_os_type['lntime']**4
                VM_p2_os_type['lntime_5']=VM_p2_os_type['lntime']**5
                VM_p2_os_type['P_ali']=(VM_p2_os_type['Yunqi_Dis']).apply(m.log)
                VM_p2_os_type['P_mc']=(VM_p2_os_type['MC_Feb_Dis']).apply(m.log)
                VM_p2_os_type['lncore_hours']=VM_p2_os_type['core_hours'].apply(m.log)
                VM_p2_os_type.to_csv('VM/{VM}_{os}_{of}.csv'.format(VM=i,os=j,of=k))
                model=ols('lncore_hours~lntime+P_ali+P_mc',VM_p2_os_type).fit()
                model2=ols('lncore_hours~lntime+lntime_2+P_ali+P_mc',VM_p2_os_type).fit()
                model3=ols('lncore_hours~lntime+lntime_2+lntime_3+P_ali+P_mc',VM_p2_os_type).fit()
                model4=ols('lncore_hours~lntime+lntime_2+lntime_3+lntime_4+P_ali+P_mc',VM_p2_os_type).fit()
                model5=ols('lncore_hours~lntime+lntime_2+lntime_3+lntime_4+lntime_5+P_ali+P_mc',VM_p2_os_type).fit()
                #fit the data with different models of different Polynomial Orders
                if rr.rsquared(model)>Rsquare:
                #use rsquared>0.6 VM
                    print('VM/{VM}_{os}_{of} 1 order'.format(VM=i,os=j,of=k))
                    print(model.params)
                    print(rr.rsquared(model))
                    print(model.conf_int(conf))
                    #print(model.pvalues)
                elif rr.rsquared(model2)>Rsquare:
                    print('VM/{VM}_{os}_{of} 2 orders'.format(VM=i,os=j,of=k))
                    print(model2.params)
                    print(rr.rsquared(model2))
                    print(model2.conf_int(conf))
                    #print(model2.pvalues)
                elif rr.rsquared(model3)>Rsquare:
                    print('VM/{VM}_{os}_{of} 3 orders'.format(VM=i,os=j,of=k))
                    print(model3.params)
                    print(rr.rsquared(model3))
                    print(model3.conf_int(conf))
                    #print(model3.pvalues)
                elif rr.rsquared(model4)>Rsquare:
                    print('VM/{VM}_{os}_{of} 4 orders'.format(VM=i,os=j,of=k))
                    print(model4.params)     
                    print(rr.rsquared(model4))
                    print(model4.conf_int(conf))
                    #print(model4.pvalues)
                elif rr.rsquared(model5)>Rsquare:
                    print('VM/{VM}_{os}_{of} 5 orders'.format(VM=i,os=j,of=k))
                    print(model5.params)
                    print(rr.rsquared(model5))
                    print(model5.conf_int(conf))
                    #print(model5.pvalues)
                else:
                    print('VM/{VM}_{os}_{of} use 1 order'.format(VM=i,os=j,of=k))
                    print(model.params)
                    print(rr.rsquared(model))
                    print(model.conf_int(conf))
                    #print(model.pvalues)
                
                #choose the best simple model with R^2>0.6 and show the statistic summary
                draw=VM_p2_os_type[['timeperiod','core_hours']]
                draw.index=draw['timeperiod']
                draw=draw.drop('timeperiod',axis=1)
                draw.plot(title='VM/{VM}_{os}_{of} use 1 order'.format(VM=i,os=j,of=k))
    for i in VM_hours[VM_hours['family']=='F'].family.unique():
        #F series VM is launched after Oct 2016 which is after Ali's yunqi price cut
        VM_p2=VM_hours[VM_hours['family']==i]
        VM_p2=VM_p2.drop('weekkey',axis=1)
        for j in VM_hours.os.unique():
            VM_p2_os=VM_p2[VM_p2['os']==j]
            for k in VM_p2_os.AI_offertype.unique():
                VM_p2_os_type=VM_p2_os[VM_p2_os['AI_offertype']==k]
                VM_p2_os_type=VM_p2_os_type.groupby(['timeperiod','MC_Feb_Dis']).sum()
                VM_p2_os_type.to_csv('VM_p2_os_type.csv')
                VM_p2_os_type=pd.read_csv('VM_p2_os_type.csv')
                VM_p2_os_type['time']=VM_p2_os_type.index+1
                VM_p2_os_type['lntime']=VM_p2_os_type['time'].apply(m.log)
                VM_p2_os_type['lntime_2']=VM_p2_os_type['lntime']**2
                VM_p2_os_type['lntime_3']=VM_p2_os_type['lntime']**3
                VM_p2_os_type['lntime_4']=VM_p2_os_type['lntime']**4
                VM_p2_os_type['lntime_5']=VM_p2_os_type['lntime']**5
                VM_p2_os_type['P_mc']=(VM_p2_os_type['MC_Feb_Dis']).apply(m.log)
                VM_p2_os_type['lncore_hours']=VM_p2_os_type['core_hours'].apply(m.log)
                VM_p2_os_type.to_csv('VM/{VM}_{os}_{of}.csv'.format(VM=i,os=j,of=k))
                model=ols('lncore_hours~lntime+P_mc',VM_p2_os_type).fit()
                model2=ols('lncore_hours~lntime+lntime_2+P_mc',VM_p2_os_type).fit()
                model3=ols('lncore_hours~lntime+lntime_2+lntime_3+P_mc',VM_p2_os_type).fit()
                model4=ols('lncore_hours~lntime+lntime_2+lntime_3+lntime_4+P_mc',VM_p2_os_type).fit()
                model5=ols('lncore_hours~lntime+lntime_2+lntime_3+lntime_4+lntime_5+P_mc',VM_p2_os_type).fit()
                if rr.rsquared(model)>Rsquare:
                    print('VM/{VM}_{os}_{of} 1 order'.format(VM=i,os=j,of=k))
                    print(model.params)
                    print(rr.rsquared(model))
                    print(model.conf_int(conf))
                    #print(model.pvalues)
                elif rr.rsquared(model2)>Rsquare:
                    print('VM/{VM}_{os}_{of} 2 orders'.format(VM=i,os=j,of=k))
                    print(model2.params)
                    print(rr.rsquared(model2))
                    print(model2.conf_int(conf))
                    #print(model2.pvalues)
                elif rr.rsquared(model3)>Rsquare:
                    print('VM/{VM}_{os}_{of} 3 orders'.format(VM=i,os=j,of=k))
                    print(model3.params)
                    print(rr.rsquared(model3))
                    print(model3.conf_int(conf))
                    #print(model3.pvalues)
                elif rr.rsquared(model4)>Rsquare:
                    print('VM/{VM}_{os}_{of} 4 orders'.format(VM=i,os=j,of=k))
                    print(model4.params)     
                    print(rr.rsquared(model4))
                    print(model4.conf_int(conf))
                    #print(model4.pvalues)
                elif rr.rsquared(model5)>Rsquare:
                    print('VM/{VM}_{os}_{of} 5 orders'.format(VM=i,os=j,of=k))
                    print(model5.params)
                    print(rr.rsquared(model5))
                    print(model5.conf_int(conf))
                    #print(model5.pvalues)
                else:
                    print('VM/{VM}_{os}_{of} use 1 order'.format(VM=i,os=j,of=k))
                    print(model.params)
                    print(rr.rsquared(model))
                    print(model.conf_int(conf))
                    #print(model.pvalues)
                #draw=VM_p2_os_type[['timeperiod','core_hours']]
                #draw.index=draw['timeperiod']
                #draw=draw.drop('timeperiod',axis=1)
                #draw.plot(title='VM/{VM}_{os}_{of} use 1 order'.format(VM=i,os=j,of=k))
    for i in VM_hours[(VM_hours['family']=='A2-4 Std')|(VM_hours['family']=='A2-4 Basic')|(VM_hours['family']=='A5-7 Std')].family.unique():
        #MC didn't have those VMs price cut after Feb 2016
        VM_p2=VM_hours[VM_hours['family']==i]
        VM_p2=VM_p2.drop('weekkey',axis=1)
        for j in VM_hours.os.unique():
            VM_p2_os=VM_p2[VM_p2['os']==j]
            for k in VM_p2_os.AI_offertype.unique():
                VM_p2_os_type=VM_p2_os[VM_p2_os['AI_offertype']==k]
                VM_p2_os_type=VM_p2_os_type.groupby(['timeperiod','Yunqi_Dis']).sum()
                VM_p2_os_type.to_csv('VM_p2_os_type.csv')
                VM_p2_os_type=pd.read_csv('VM_p2_os_type.csv')
                VM_p2_os_type['time']=VM_p2_os_type.index+1
                VM_p2_os_type['lntime']=VM_p2_os_type['time'].apply(m.log)
                VM_p2_os_type['lntime_2']=VM_p2_os_type['lntime']**2
                VM_p2_os_type['lntime_3']=VM_p2_os_type['lntime']**3
                VM_p2_os_type['lntime_4']=VM_p2_os_type['lntime']**4
                VM_p2_os_type['lntime_5']=VM_p2_os_type['lntime']**5
                VM_p2_os_type['P_ali']=(VM_p2_os_type['Yunqi_Dis']).apply(m.log)
                VM_p2_os_type['lncore_hours']=VM_p2_os_type['core_hours'].apply(m.log)
                VM_p2_os_type.to_csv('VM/{VM}_{os}_{of}.csv'.format(VM=i,os=j,of=k))
                model=ols('lncore_hours~lntime+P_ali',VM_p2_os_type).fit()
                model2=ols('lncore_hours~lntime+lntime_2+P_ali',VM_p2_os_type).fit()
                model3=ols('lncore_hours~lntime+lntime_2+lntime_3+P_ali',VM_p2_os_type).fit()
                model4=ols('lncore_hours~lntime+lntime_2+lntime_3+lntime_4+P_ali',VM_p2_os_type).fit()
                model5=ols('lncore_hours~lntime+lntime_2+lntime_3+lntime_4+lntime_5+P_ali',VM_p2_os_type).fit()
                if rr.rsquared(model)>Rsquare:
                    print('VM/{VM}_{os}_{of} 1 order'.format(VM=i,os=j,of=k))
                    print(model.params)
                    print(rr.rsquared(model))
                    print(model.conf_int(conf))
                    #print(model.pvalues)
                elif rr.rsquared(model2)>Rsquare:
                    print('VM/{VM}_{os}_{of} 2 orders'.format(VM=i,os=j,of=k))
                    print(model2.params)
                    print(rr.rsquared(model2))
                    print(model2.conf_int(conf))
                    #print(model2.pvalues)
                elif rr.rsquared(model3)>Rsquare:
                    print('VM/{VM}_{os}_{of} 3 orders'.format(VM=i,os=j,of=k))
                    print(model3.params)
                    print(rr.rsquared(model3))
                    print(model3.conf_int(conf))
                    #print(model3.pvalues)
                elif rr.rsquared(model4)>Rsquare:
                    print('VM/{VM}_{os}_{of} 4 orders'.format(VM=i,os=j,of=k))
                    print(model4.params) 
                    print(rr.rsquared(model4))
                    print(model4.conf_int(conf))
                    #print(model4.pvalues)
                elif rr.rsquared(model5)>Rsquare:
                    print('VM/{VM}_{os}_{of} 5 orders'.format(VM=i,os=j,of=k))
                    print(model5.params)
                    print(rr.rsquared(model5))
                    print(model5.conf_int(conf))
                    #print(model5.pvalues)
                else:
                    print('VM/{VM}_{os}_{of} use 1 order'.format(VM=i,os=j,of=k))
                    print(model.params)
                    print(rr.rsquared(model))  
                    print(model.conf_int(conf))
                    #print(model.pvalues)
                #draw=VM_p2_os_type[['timeperiod','core_hours']]
                #draw.index=draw['timeperiod']
                #draw=draw.drop('timeperiod',axis=1)
                #draw.plot(title='VM/{VM}_{os}_{of} use 1 order'.format(VM=i,os=j,of=k))
