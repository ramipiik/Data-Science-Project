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

"""our project specific code"""

def divide_timespan(start_timestamp, end_timestamp):
    
    current_date = pd.Timestamp('2024-02-13')
    
    if end_timestamp > current_date:
        end_timestamp = current_date

    intervals = []

    current_year = start_timestamp.year
    end_year = end_timestamp.year

    while current_year <= end_year:
        start_interval = pd.Timestamp(f'{current_year}-01-01T00:00:00')
        end_interval = pd.Timestamp(f'{current_year}-12-31T23:59:59')

        if current_year == start_timestamp.year:
            start_interval = max(start_interval, start_timestamp)

        if current_year == end_timestamp.year:
            end_interval = min(end_interval, end_timestamp)

        intervals.append((start_interval, end_interval))
        current_year += 1

    return intervals
    
if __name__ == '__main__':

    tablevariables = [
        'HYY_EDDY233.ET_gapf',  #Evapotranspiration gapfilled
        'HYY_EDDY233.GPP',  #Gross primary production
        'HYY_EDDY233.H_gapf',   #Sensible heat flux gapfilled
        'HYY_EDDY233.NEE',  #NEE gapfilled
        'HYY_EDDY233.Qc_gapf_NEE',  #NEE gapfilling method
        'HYY_META.CO2672',  #CO₂ concentration 67.2 m
        'HYY_META.CO2icos672',  #CO₂ 67.2 m (ICOS AS)
        'HYY_META.diffGLOB',    #Diffuse radiation
        'HYY_META.diffPAR', #Diffuse PAR
        'HYY_META.Glob',    #Global radiation 18/35 m
        'HYY_META.Glob67',  #Global radiation 67 m
        'HYY_META.H2O270icos',  #H₂O 27 m (ICOS ES)
        'HYY_META.H2O672',  #H₂O concentration 67.2 m
        'HYY_META.H2Oicos672',  #H₂O 67.2 m (ICOS AS)
        'HYY_META.maaPAR',  #PAR below canopy
        'HYY_META.O3672',   #O₃ concentration 67.2 m
        'HYY_META.O384',    #O₃ concentration 8.4 m
        'HYY_META.O3tower', #O₃ concentration 35 m
        'HYY_META.Pamb0',   #Air pressure (ground)
        'HYY_META.PAR', #PAR
        'HYY_META.PAR_plot1',   #PAR below canopy (plot1)
        'HYY_META.PAR_plot2',   #PAR below canopy (plot2)
        'HYY_META.PAR_plot3',   #PAR below canopy (plot3)
        'HYY_META.PAR_plot4',   #PAR below canopy (plot4)
        'HYY_META.Precip',  #Rainfall
        'HYY_META.Precipacc',   #Precipitation
        'HYY_META.Precipacc_gpm',   #Precipitation (GPM field)
        'HYY_META.SD_gpm',  #Snow depth (GPM field)
        'HYY_META.T336',    #Air temperature 33.6 m
        'HYY_META.T672',    #Air temperature 67.2 m
        'HYY_META.Tmm672',  #Air temperature 67.2 m (2)
        'HYY_META.tsoil_A', #Soil temperature A
        'HYY_META.VOC_M137_220',    #monoterpenes 22 m
        'HYY_META.VOC_M137_336',    #monoterpenes 33.6 m
        'HYY_META.VOC_M137_gradient_flux',  #monoterpene flux
        'HYY_META.VOC_M33_220',  #methanol 22 m,
        'HYY_META.VOC_M33_336',  #methanol 33.6 m,
        'HYY_META.VOC_M33_gradient_flux', #methanol flux
        'HYY_META.wpsoil_A',    #Soil water potential A
        'HYY_META.wpsoil_B',    #Soil water potential B
        'HYY_META.wsoil_A',     #Soil water content A
        'HYY_META.wsoil_B1_p50' #Soil water content B1 (2)
    ]

    timestamps = [
        (pd.Timestamp('1997-01-01T00:15:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:15:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:15:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:15:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:15:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:18:00'), pd.Timestamp('2017-12-12T15:14:00')),
        (pd.Timestamp('2011-11-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2000-03-15T11:00:00'), pd.Timestamp('2010-03-15T00:00:00')),
        (pd.Timestamp('2009-12-15T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2017-10-05T12:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2018-04-20T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:18:00'), pd.Timestamp('2017-12-12T15:14:00')),
        (pd.Timestamp('2011-11-01T00:00:00'), pd.Timestamp('2021-06-09T15:00:00')),
        (pd.Timestamp('2003-11-20T15:07:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:18:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:03:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2018-06-20T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2019-05-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2019-05-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2019-05-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2019-05-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2005-04-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2014-01-15T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2014-01-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2012-09-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-01T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2006-06-12T00:00:00'), pd.Timestamp('2009-06-26T00:00:00')),
        (pd.Timestamp('2010-05-28T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2010-05-28T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2006-06-12T00:00:00'), pd.Timestamp('2009-06-26T00:00:00')),
        (pd.Timestamp('2010-05-28T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2010-05-28T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2005-06-17T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2005-06-17T00:00:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('1997-01-02T03:32:00'), pd.Timestamp('9999-09-09T00:00:00')),
        (pd.Timestamp('2007-02-01T00:00:00'), pd.Timestamp('2023-04-04T00:00:00'))
    ]

    for var, ts in zip(tablevariables, timestamps):
        fdate = ts[0]
        ldate = ts[1]
        
        cols = ['Datetime', var]

        df = pd.DataFrame(columns=cols)

        print(fdate, ldate)

        intervals = divide_timespan(fdate, ldate)

        for interval in intervals:
            data = getData(interval[0], interval[1], [var], quality='CHECKED')

            if data.empty:
                continue

            data = data.dropna(subset=[var], how='all')

            data = data.rename(columns={var: var})

            df = pd.concat([df, data], ignore_index=True)

        df.to_csv(f'./data/{var}_{str(ts[0]).split(" ")[0]}--{str(ts[1]).split(" ")[0]}.csv')


    #stress periods:
    #2006 july to end of august: drought
    #2018 june to end of august: hot+dry
    #2010 end of june to end of july: hot, no drought, VOC unclear
    #any period with frost + high light (monoterpenes increase exponentially as a function of temperature. 
    #Hence, if you see any peaks in the VOC flux data for spring time then that is most probably caused by 
    #co-occurrence of frost and high radiation (you should of course check that there is frost and high radiation 
    #at the same time as those peaks to make sure).

