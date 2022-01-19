import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import math
import glob
import warnings
import datetime as dt
from itertools import groupby
from datetime import timedelta, date
from dateutil.relativedelta import relativedelta
import re

def get_datef_mms(x):
	try:
		
		if "/" in str(x):
			
			dat = str(x).split("/")
			dd = int(dat[1])
			mm = int(dat[0])
			yy = int(dat[2])
			return dt.date(yy,mm,dd)
		else:
			dat = str(x).split(" ")[0].split("-")
			dd = int(dat[2])
			mm = int(dat[1])
			yy = int(dat[0])
			return dt.date(yy,mm,dd)
	except:
		return np.nan

def daterange(start_date, end_date,group='d'):
    if group == 'd':
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)
    if group == 'm':
        for n in range(int(((end_date.year-start_date.year)*12)+(end_date.month-start_date.month))+1):
            yield start_date + relativedelta(years=(n//12),months=(n%12))

def get_datetime_mms(x,f="date"):
    
    try:
        dat = str(x).split(" ")[0]
        tam = str(x).split(" ")[1]
        x2 = dat.split("-")
        y2 = tam.split(":")
        
        yy= int(x2[0])
        mm=int(x2[1])
        dd = int(x2[2])
        
        hr = int(y2[0])
        
        mi = int(y2[1])
        
        ss = int(float(y2[2]))
        
        if f=="date":
            
            return dt.date(yy,mm,dd)
        if f=="datetime":
            return dt.datetime(yy,mm,dd,hr,mi,ss)
    except:
        return np.nan

def get_date(x):
    yy= int(x[6:10])
    mm=int(x[0:2])
    dd = int(x[3:5])
    
    return dt.date(yy,mm,dd)

def clean_npi(x):
    return str(x).replace('\t','').replace('\n','').replace('.0','')

def clean_phone_number(x):
    return x.replace("(","").replace(")","").replace("-","").replace("/","").replace("\\","")

def get_nic_datetime(x,f="date"):
    try:
        datefield = str(x).split(" ")[0]
        timefield =str(x).split(" ")[1]
        
        daf = datefield.split("/")
        yy = int(daf[0])
        mm =int(daf[1])
        dd = int(daf[2])
        
        taf = timefield.split(":")
        hr = int(taf[0])
        mi = int(taf[1])
        ss = int(taf[2])
        
        if f=="date":
            return dt.date(yy,mm,dd)
        if f=="datetime":
            return dt.datetime(yy,mm,dd,hr,mi,ss)
    except:
        return np.nan

def get_max_trackrx_date(x,f="date"):
	datesplit = str(x).split(" ")
	try:
		if len(datesplit)==3:
			return get_trackrx_date(x,f)
		if len(datesplit)>3:
			dar = []
			#dar.append()
			for x in [datesplit[i:i+3] for i in range(0,len(datesplit),3)]:
				dar.append(get_trackrx_date(" ".join(x),f))
			return max(dar)
	except:
		return np.nan


def get_trackrx_date(x,f="date"):
    
    
    try:
        datefield = str(x).split(" ")[0]
        timefield = str(x).split(" ")[1]
        try:
            ampm = str(x).split(" ")[2]
        except:
            ampm = ""

        daf = datefield.split("/")
        yy= int(daf[2])
        mm=int(daf[0])
        dd = int(daf[1])

        taf = timefield.split(":")
        hour = int(taf[0])
        if (ampm=="PM") or (ampm=="PM."):
            if hour <12:
                hour = hour+12
        
        if (ampm=="AM") or (ampm=="AM."):
            if hour==12:
                hour = 0
        
        
        mi = int(taf[1])
        try:
            ss = int(taf[2])
        except:
            ss=0

        if f=="date":
            return dt.date(yy,mm,dd)
        if f=='time':
            return dt.time(hour,mi,ss)
        if f=="datetime":
            return dt.datetime(yy,mm,dd,hour,mi,ss)
        else:
            return dt.datetime(yy,mm,dd,hour,mi,ss).strftime(f)
    except:
        return np.nan

def get_status_flow(pdf):
   
    flowvals = [str(x) for x in pdf.values]
    
    #print(flowvals)
    #print(len(flowvals))
    
    flowstr = "->".join(flowvals)
    flowarr_ = flowstr.split("->")
    
    flowarr = [i[0] for i in groupby(flowarr_)]
    
    nj = [flowarr[j:j+2] for j in range(0,len(flowarr),2)]
    
    return [p[0] for p in groupby(["->".join(l) for l in nj])]
    
def get_last_trackrx_value(pdf):
    
    xr = get_status_flow(pdf)
    yr = "->".join(xr)
    
    jr = yr.split("->")
    
    return jr[-1]

def get_onfleet_zipcode(x):
    
	try:
		#print(x)
		zval = (re.findall('[0-9]{5}',x))
		zipvalue = zval[len(zval)-1]
		return zipvalue
	except:
		#print('error zipcode')
		return np.nan

def find_total_time_by_tagid(df,tagname):
    
    if not isinstance(tagname,list):
        tagname = [tagname]
    
    sdf = df[(df['FIELD CHANGED']=='TAGID')&(df['NEW VALUE'].isin(tagname))].sort_values(['change_datetime'])['change_datetime'].values
    edf = df[(df['FIELD CHANGED']=='TAGID')&(df['OLD VALUE'].isin(tagname))].sort_values(['change_datetime'])['change_datetime'].values
    
    incomplete_flag = False
    if len(sdf)<len(edf):
        edf = edf[1:]
        incomplete_flag = True
        times_set = "{} - Incomplete flow".format(len(sdf))
    
    if len(sdf)==0:
        returnval = 0,"Not Set"
        return returnval
    
    if len(edf)==0:
        returnval = "In Progress",len(sdf)
        return returnval
    
    tt = []
    
    if len(sdf)>=len(edf):
        
        if not incomplete_flag:
            times_set = len(sdf)
        
        for i in range(0,len(edf)):
            tt.append(pd.to_datetime(edf[i]) - pd.to_datetime(sdf[i]))
    
    totaltime = tt[0]
    
    for i in range(1,len(tt)):
        totaltime = totaltime + tt[i]
    
    return totaltime,times_set


def get_nic_data(nicdf,rx_code,rphdate,pickeddate,batchno,pid):
    drange = []
    for d in (daterange(rphdate.date(),pickeddate.date()+timedelta(1))):
        drange.append(d)
    nicdf['contact_start_date'] = nicdf['contact_start_datetime'].apply(lambda x: x.date())
    nicdf['contact_end_date'] = nicdf['contact_end_datetime'].apply(lambda x: x.date())
    
    wdf = (nicdf[nicdf['contact_start_date'].isin(drange)])
    
    if len(wdf)>0:
        pid = wdf['mapped_patient_id'].values[0]
        wdf['cid'] = wdf['Contact ID'].astype('int64')
        
        wstart_datetime = wdf['contact_start_datetime'].min()
        wend_datetime = wdf['contact_end_datetime'].max()
        
        odf = (pd.pivot_table(wdf,index=['mapped_patient_id'],aggfunc={'call_time':lambda x: x.sum(),\
                                                                     'Contact ID': lambda x: '|'.join((x.astype('int64').astype('str'))),\
                                                                      'cid': lambda x: (x).astype('int64').sum()})\
                                                                       .reset_index())
        
        odf['Contact ID'] = (odf['Contact ID']).astype(str).str.replace(".0","")
        odf['rx_code_key'] = rx_code
        #print(batchno)
        odf['batch_no'] = batchno
        odf['contact_start_datetime'] = wstart_datetime
        odf['contact_end_datetime'] = wend_datetime
        odf.rename(columns={'Contact ID':'combination_key',\
                           'call_time':'total_call_time',\
                           'cid':'nic_id'},inplace=True)
        #print(odf)
        return odf
    else:
        return pd.DataFrame([[pid,'','',rx_code,batchno,'','','']],\
                            columns=['mapped_patient_id','nic_id','total_call_time',\
                                     'rx_code_key','batch_no','combination_key',\
                                    'contact_start_datetime','contact_end_datetime'])


def px_data_compilation(pid,rxflow,nicdf,batchdf,deliverydf):
    
    wdf = rxflow[rxflow['PATIENTNO']==pid].sort_values(['track_rx_pickedup'])
    ndf = nicdf[nicdf['mapped_patient_id']==pid]
    
    if len(wdf)==0:
        return False
       
    odf = pd.DataFrame() 
    tdf = pd.DataFrame()
    
    for i,r in wdf.iterrows():
        
        delivery_date = (r['track_rx_delivered']).date()
        rcode = r['rx_code_key']
        
        try:
            batchno = batchdf[(batchdf['batch_delivery_date']==delivery_date)&\
                         (batchdf['rx_code_key_location']==r['rx_code_key_location'])]['BATCHNO'].values[0]
        except:
            batchno = ''
        
        rphdate = (r['track_rph_picks'])
        if pd.isnull(rphdate):
            rphdate = (r['final_rx_rph_picks'])
            
        pickeddate = (r['track_rx_pickedup'])
        if pd.isnull(pickeddate):
            pickeddate = (r['final_rx_pickedup'])
            
        ddf = pd.DataFrame([[r['rx_code_key'],rphdate.date(),pickeddate.date(),'',batchno]],\
                           columns=['rx_code_key','rph_date','picked_date','px_remark','batch_no'])
        kdf = pd.DataFrame()
        if pd.isnull(pickeddate):
            ddf['px_remark'] = "not pickedup"
            pickeddate = rphdate
        if pd.isnull(rphdate):
            ddf['px_remark'] = "rph verfication pending"
        if rphdate.date()>pickeddate.date():
            ddf['px_remark'] = "exception"
        
        if (rphdate.date()<=pickeddate.date()):
            xdf = get_nic_data(ndf,rcode,rphdate,pickeddate,batchno,pid)
            tdf = tdf.append(xdf)
            
        
        odf = odf.append(ddf)
        
    
    return odf,tdf
        
def onfleet_match_on_contact(onfleet,contact,deliverydate,method_identifier_tag):
    
    onfdf = onfleet[(onfleet['patient_phone']==contact)&\
                   (onfleet['delivery_date']==deliverydate)]
    
    if len(onfdf)>1:
        onfdf_success = onfdf[onfdf['didSucceed']==True]
        if len(onfdf_success)==1:
            onfdf_success['method_identifier'] = method_identifier_tag+"-Success selected"
            return onfdf_success
    
    onfdf['method_identifier'] = method_identifier_tag
    return onfdf

def onfleet_match_on_name(onfleet,pfname,plname,pzip,deliverydate,method_identifier_tag):
    pfname = pfname.upper()
    plname = plname.upper()
    
    onfdf = onfleet[(onfleet['recipientsNames'].str.upper().str.contains(pfname,na=False))&\
                  (onfleet['recipientsNames'].str.upper().str.contains(plname,na=False))&\
                  (onfleet['delivery_date']==deliverydate)&(onfleet['patient_zipcode']==pzip)]
    
    if len(onfdf)>1:
        onfdf_success = onfdf[onfdf['didSucceed']==True]
        if len(onfdf_success)==1:
            onfdf_success['method_identifier'] = method_identifier_tag+"-Success selected"
            return onfdf_success
    
    onfdf['method_identifier'] = method_identifier_tag
    return onfdf
        
