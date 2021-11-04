#!/usr/bin/env python
# coding: utf-8

# In[4]:


from ib_insync import *
import pandas as pd
import numpy as np
from config.settings import *
import datetime as dt
ib = IB()
util.startLoop()

def get_fills(api = True):
    if api: #true znamená, že načte obchody přes API jinak ze souboru trades csv v tomto adresáři
        ib.connect(setIB['IP'], setIB['port'], setIB['clientID'])
        exekuce = (e for e in ib.fills())
        exekuce = [(e.time,e.contract.symbol,e.execution.side,e.execution.shares,e.execution.avgPrice,e.commissionReport.commission,e.execution.orderRef) 
               for e in exekuce]
        '''exekuce = [(e.time,e.execution.acctNumber,e.execution.clientId,e.contract.symbol,e.execution.permId,e.execution.side,e.execution.shares,e.execution.avgPrice,e.commissionReport.commission,e.execution.orderRef) 
               for e in exekuce]'''
        exe=pd.DataFrame(exekuce, columns=['cas','ticker','smer','pocet','cena','komise','orderRef'])
    else:
        exe = pd.read_csv('trades.csv', sep=';')
    exe.cas = pd.to_datetime(exe.cas) 
    exe['cas'] = exe['cas'].dt.strftime('%d.%m.%Y') #přepíše datum na správný formát a vymaže čas (prohazuje den a měsíc)
    #exe.set_index("cas",inplace=True)
    ib.disconnect()
    return exe


# In[5]:


def fill(strategy,api = False):
    '''
    - funkce načte otervřené obchody z databáze 
    - porovná, jestli se z nějakých obchodů vystoupilo, případně je zapíše
    - načte dnes otevřené obchody a zapíše do databáze
    '''
    #data = pd.read_excel('LiveReport_dev.xlsx',sheet_name = strategy)
    #str_opened = data[data.Výstup.isnull()]
    str_opened = pd.read_csv(f'{strategy}_open.csv', sep = ';')
    str_opened.index = np.arange(0, len(str_opened))
    fills = get_fills(api)
    today = dt.date.today().strftime('%d.%m.%Y')
    fills.to_csv(f'fills_{today}.csv', sep = ';', mode = 'w')
    
    aggr = {'cas': 'first', 'ticker': 'first', 'smer': 'first' , 'pocet': 'sum', 'cena': 'mean', 'komise' : 'sum', 'orderRef' : 'first' }

    str_short_fills = fills[(fills.orderRef == strategy) & (fills.smer == 'SLD')]
    str_short_fills = str_short_fills.groupby(['ticker']).aggregate(aggr)
    str_short_fills.index = np.arange(0, len(str_short_fills))
    
    str_long_fills = fills[(fills.orderRef == strategy) & (fills.smer == 'BOT')]
    str_long_fills = str_long_fills.groupby(['ticker']).aggregate(aggr)
    str_long_fills.index = np.arange(0, len(str_long_fills))
    
    for i in range(len(str_opened.Trh)):
        for j in range(len(str_short_fills)):
            if str_opened.Trh[i] == str_short_fills.ticker[j]:
                    str_opened.Vystup_cena[i] = str_short_fills.cena[j]
                    str_opened.Vystup[i] = str_short_fills.cas[j]
                    str_opened.Komise[i] += str_short_fills.komise[j]
    str_closed = str_opened[str_opened.Vystup.notnull()]
    if len(str_closed) != len(str_short_fills):
        print('Výstup nepřiřazen!')
    str_closed.to_csv(f'{strategy}_hist.csv', encoding = 'utf-8', sep = ';', mode = 'a', header = False, index = False)
    str_opened = str_opened[str_opened.Vystup.isnull()]
    str_opened.index = np.arange(0, len(str_opened))
    for i in range(len(str_long_fills)):
        col = len(str_opened) + i
        str_opened.loc[col] = [str_long_fills.cas[i], str_long_fills.ticker[i], str_long_fills.pocet[i], str_long_fills.cena[i], 'NaN', 'NaN',str_long_fills.komise[i], 'NaN']
    str_opened.to_csv(f'{strategy}_open.csv', mode = 'w',sep = ';',encoding = 'utf-8', index = False)
    return str_opened
    return str_closed
        
    #str_closed['Vstup'] = str_closed['Vstup'].dt.tz_localize(None)


# In[6]:


fill('Mopul', True)

