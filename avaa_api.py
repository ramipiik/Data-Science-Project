# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 07:44:58 2021

@author: pkolari

Download data and metadata from AVAA SmartSMEAR via API
"""

import pandas as pd

import requests

try:
    from StringIO import StringIO # Python 2
except ImportError:
    from io import StringIO # Python 3

basepath='https://smear-backend.rahtiapp.fi/'

def _toList(args):
    if not isinstance(args,list) and not isinstance(args,tuple):
        args=[args]
    return args

def _getMeta(url,queryparams,format):
    metadata={}
    response=requests.get(url,params=queryparams)
    if response.status_code!=200:
        print('ERROR:')
        print(response.content)
    else:
        if format=='json':
            metadata=response.json()
        else:
            metadata=pd.read_csv(StringIO(response.text))
    return metadata

def getData(fdate,ldate,tablevariables=[],quality='ANY',interval=1,aggregation='NONE',timeout=60):
    """
    Function for downloading data through AVAA SMEAR API.
    Give fdate and ldate as datetime or pandas timestamp,
    tablevariables as list of table.column strings.
    API may return data in column order that is different from the tablevariable list in the API query.
    Data columns are reordered to ensure consistency with the tablevariable list. 
    """
    url='{}search/timeseries/csv'.format(basepath) 
    queryparams={'from':fdate.strftime('%Y-%m-%dT%H:%M:%S'),'to':ldate.strftime('%Y-%m-%dT%H:%M:%S'),
                 'tablevariable':_toList(tablevariables),'quality':quality,'interval':interval,'aggregation':aggregation}
    data0=[]
    data=[]
    try:
        # reading directly from url with pandas.read_csv would be faster with big datasets 
        # but this gives more informative error messages and possibility to set timeout    
        response=requests.get(url,params=queryparams,timeout=timeout)
        if response.status_code==200:
            data0=pd.read_csv(StringIO(response.text))
        else:
            print(response.reason)
            print(response.text.replace('\r','\n'))
    except requests.exceptions.Timeout:
        print('The request could not be completed in {} seconds.'.format(timeout))
    except pd.errors.EmptyDataError:
        print('No data.')
    if len(data0)>0:
        # convert date&time columns to datetime
        data0['Datetime']=pd.to_datetime(data0[['Year','Month','Day','Hour','Minute','Second']])
        # check if all tablevariables were returned
        hdr0=list(data0.columns)
        hdr=list(v for v in tablevariables if v in hdr0)
        missing=list(v for v in tablevariables if v not in hdr0)
        if len(missing)>0:
            print('WARNING! Temporal coverage of some table(s) is outside the given time span.')
            print('These columns will be missing:')
            for ms in missing:
                print('  ',ms)
        # drop date & time columns, reorder data columns to match the tablevariables list
        data=data0.reindex(columns=['Datetime']+hdr)
    return data
    
def getMetadata(stations=[],tables=[],tablevariables=[],format='json'):
    """
    Function for downloading metadata through AVAA SMEAR API.
    Call the function with just one of these arguments: stations, tables or tablevariables.
    Give station(s) or table(s) as string or list/tuple of string(s)
    tablevariables as table.column string or list/tuple of table.column string(s).
    Format options: 'json' (dict), 'csv' (data frame)
    """
    formatstr=''
    if format!='json':
        formatstr='/csv'
    url='{}search/variable{}'.format(basepath,formatstr)
    queryparams={}
    if len(tablevariables)>0:
        queryparams={'tablevariable':_toList(tablevariables)}
    elif len(tables)>0:
        queryparams={'table':_toList(tables)}
    elif len(stations)>0:
        queryparams={'station':_toList(stations)}
    metadata={}
    if len(queryparams)>0:
        metadata=_getMeta(url,queryparams,format)
    return metadata

def getEvents(tablevariables=[]):
    """
    Function for downloading data lifecycle events through AVAA SMEAR API.
    Give tablevariables as table.column string or list/tuple of several table.column strings.
    Note that the result does not contain information on which variable each event belongs to,
    in some cases it's better to retrieve the events for one variable at a time. 
    """
    format='json'
    url='{}search/event'.format(basepath)
    metadata={}
    if len(tablevariables)>0:
        queryparams={'tablevariable':_toList(tablevariables)}
        metadata=_getMeta(url,queryparams,format)
    return metadata

    
if __name__ == '__main__':
    
    tablevariables = [
        'HYY_META.VOC_M137_gradient_flux', #monoterpene
        'HYY_META.VOC_M33_gradient_flux', #methanol
        'HYY_META.VOC_M59_gradient_flux', #acetone
        'HYY_META.VOC_M45_gradient_flux' #acetaldehyde
        ]

    for i in range(10, 23):
        fdate = pd.Timestamp(f'20{i}-01-01 00:00:00')
        ldate = pd.Timestamp(f'20{i}-12-31 23:59:00')
        print(fdate, ldate)

        data = getData(fdate, ldate, tablevariables, quality='CHECKED')
        data = data.dropna(subset=tablevariables, how='all')

        data = data.rename(columns={
            'HYY_META.VOC_M137_gradient_flux': 'monoterpene_flux',
            'HYY_META.VOC_M33_gradient_flux': 'methanol_flux',
            'HYY_META.VOC_M59_gradient_flux': 'acetone_flux',
            'HYY_META.VOC_M45_gradient_flux': 'acetaldehyde_flux'
            })

        data.to_csv(f'~/project/VOC/voc_20{i}.csv')

    tablevariables = [
        'HYY_EDDY233.NEE', #net ecosystem exchange gapfilled
        'HYY_EDDY233.Qc_gapf_NEE', #NEE gapfilling method
        'HYY_EDDY233.GPP', #gross primary production
        ]
    
    for i in range(1, 23):
        fdate = pd.Timestamp(f'20{str(i).zfill(2)}-01-01 00:00:00')
        ldate = pd.Timestamp(f'20{str(i).zfill(2)}-12-31 23:59:00')
        print(fdate, ldate)

        data = getData(fdate, ldate, tablevariables, quality='CHECKED')
        data = data.dropna(subset=tablevariables, how='all')

        data = data.rename(columns={
            'HYY_EDDY233.NEE': 'NEE',
            'HYY_EDDY233.Qc_gapf_NEE': 'NEE_gapfilling_method',
            'HYY_EDDY233.GPP': 'GPP'
            })

        data.to_csv(f'~/project/flux/GPP_NEE_flux_20{str(i).zfill(2)}.csv')

    tablevariables = [
        'HYY_EDDY233.H_gapf', #sensible heat flux gapfilled
        'HYY_EDDY233.Qc_gapf_H', #sensible heat flux gapfilling method
    ]

    for i in range(1, 23):
        fdate = pd.Timestamp(f'20{str(i).zfill(2)}-01-01 00:00:00')
        ldate = pd.Timestamp(f'20{str(i).zfill(2)}-12-31 23:59:00')
        print(fdate, ldate)

        data = getData(fdate, ldate, tablevariables, quality='CHECKED')
        data = data.dropna(subset=tablevariables, how='all')

        data = data.rename(columns={
            'HYY_EDDY233.H_gapf': 'sensible_heat_flux',
            'HYY_EDDY233.Qc_gapf_H': 'sensible_heat_flux_gapfilling_method'
            })

        data.to_csv(f'~/project/flux/sensible_heat_flux_20{str(i).zfill(2)}.csv')

    tablevariables = [
        'HYY_EDDY233.LE', #latent heat flux (before 4/2018)
        'HYY_EDDY233.Qc_LE' #latent heat flux quality flag (before 4/2018)
    ]

    for i in range(1, 19):
        fdate = pd.Timestamp(f'20{str(i).zfill(2)}-01-01 00:00:00')
        if i == 18:
            ldate = pd.Timestamp('2018-03-31 23:59:00')
        else:
            ldate = pd.Timestamp(f'20{str(i).zfill(2)}-12-31 23:59:00')
        print(fdate, ldate)

        data = getData(fdate, ldate, tablevariables, quality='CHECKED')
        data = data.dropna(subset=tablevariables, how='all')

        data = data.rename(columns={
            'HYY_EDDYMAST.LE_270': 'latent_heat_flux_before_4-2018',
            'HYY_EDDYMAST.Qc_LE_270': 'latent_heat_flux_quality_flag_before_4-2018'
            })

        data.to_csv(f'~/project/flux/latent_heat_flux_before_4-2018_20{str(i).zfill(2)}.csv')

    tablevariables = [
        'HYY_EDDYMAST.LE_270', #latent heat flux (after 4/2018)
        'HYY_EDDYMAST.Qc_LE_270', #latent heat flux quality flag (after 4/2018)
    ]

    for i in range(18, 23):
        if i == 18:
            fdate = pd.Timestamp('2018-04-01 00:00:00')
        else:
            fdate = pd.Timestamp(f'20{str(i).zfill(2)}-01-01 00:00:00')
        ldate = pd.Timestamp(f'20{str(i).zfill(2)}-12-31 23:59:00')
        print(fdate, ldate)

        data = getData(fdate, ldate, tablevariables, quality='CHECKED')
        data = data.dropna(subset=tablevariables, how='all')

        data = data.rename(columns={
            'HYY_EDDYMAST.LE_270': 'latent_heat_flux_after_4-2018',
            'HYY_EDDYMAST.Qc_LE_270': 'latent_heat_flux_quality_flag_after_4-2018'
            })

        data.to_csv(f'~/project/flux/latent_heat_flux_after_4-2018_20{str(i).zfill(2)}.csv')
    
    

    #stress periods:
    #2006 july to end of august: drought
    #2018 june to end of august: hot+dry
    #2010 end of june to end of july: hot, no drought, VOC unclear
    #any period with frost + high light (monoterpenes increase exponentially as a function of temperature. 
    #Hence, if you see any peaks in the VOC flux data for spring time then that is most probably caused by 
    #co-occurrence of frost and high radiation (you should of course check that there is frost and high radiation 
    #at the same time as those peaks to make sure).

