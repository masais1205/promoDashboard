import pandas as pd
import numpy as np
from distutils.util import strtobool
from distutils.util import strtobool
from TM1py.Services import TM1Service
from TM1py.Utils import Utils
import datetime, re, os, sys, math


def getPeriod(day, path):
    try:
        promoPeriodDF = pd.read_excel(path+'Promotion period.xlsx', sheet_name='TE AU & NZ')
    except:
        sys.exit('Error. NO "Promotion period.xlsx" file found, or NO "TE AU & NZ" sheet found in the file!')

    promoPeriodDF[['Start', 'End']] = pd.to_datetime(promoPeriodDF[['Start', 'End']].stack()).unstack()
    flag = False
    for idx, row in promoPeriodDF.iterrows():
        if day >= row['Start'] and day <= row['End']:
            flag = True
            startDate = row['Start'].strftime("%Y%m%d")
            endDate = row['End'].strftime("%Y%m%d")
            break

    if flag:
        return row['Period'], startDate, endDate
    else:
        sys.exit('Error. Yesterday did not belong to any promotion period!')


def getDate(MDX):
    with TM1Service(
        address='au-rpt-001.au.lsaspac.internal', # Address or "localhost"
        port=20002, # HTTP Port (Default 5000)
        user='',
        password='',
        namespace='CXMD', # CAM Namespace, Str, Default None
        gateway='http://au-rpt-002.au.lsaspac.internal:80/analytics/bi/v1/disp', # ClientCAMURI, Str, Default None
        ssl=True) as tm1:
        
        data = tm1.cubes.cells.execute_mdx(MDX)
        df = Utils.build_pandas_dataframe_from_cellset(data, multiindex = False)

    return df


class PromoDash():

    def __init__(self, path, promoPeriod, startDate, endDate):
        self.path = path
        self.promoPeriod = promoPeriod
        self.startDate = startDate
        self.endDate = endDate
        self.analysisDF = pd.DataFrame()
        self.briefDF = pd.DataFrame()
        self.promoDict = {}
        self.briefDict = {}


    def readBrief(self, file, country):
        try:
            analysis = pd.read_excel(self.path+self.promoPeriod+'/'+file, sheet_name='Analysis', skiprows=1)
        except:
            sys.exit('Error. NO "Anaysis" sheet in the '+country+' Brief file!')
        try:
            brief = pd.read_excel(self.path+self.promoPeriod+'/'+file, sheet_name='Promo Brief - TEC '+country, skiprows=3)
        except:
            sys.exit('Error. NO "Promo Brief - TEC '+country+'" sheet in the '+country+' Brief file!')

        return analysis, brief


    def processBrief(self):
        files = os.listdir(self.path+self.promoPeriod)
        flagAU, flagNZ = False, False
        for file in files:
            if 'au' in file.lower() and 'brief' in file.lower():
                flagAU = True
                AUAnalysis, AUBrief = self.readBrief(file, 'AU')
            if 'nz' in file.lower() and 'brief' in file.lower():
                flagNZ = True
                NZAnalysis, NZBrief = self.readBrief(file, 'NZ')

        if not flagAU:
            sys.exit('Error. NO AU Brief file found!')
        if not flagNZ:
            sys.exit('Error. NO NZ Brief file found!')

        self.analysisDF = pd.concat([AUAnalysis[['Promotion','Deal or Promo?']], NZAnalysis[['Promotion','Deal or Promo?']]])
        self.briefDF = pd.concat([AUBrief, NZBrief])


    def genPromoDict(self):
        try:
            promoCodeDf = pd.read_csv(self.path+'Promotion Description List.csv', index_col=0)
        except:
            sys.exit('Error. NO "Promotion Description List.csv" found!')
        for idx, row in promoCodeDf.iterrows():
            self.promoDict[row['Promotion']] = row['Description']


    def setBriefDict(self):
        offerCols = ['Promo Code', 'Promotion', 'Disclaimer', 'Signage Required? (Y/N)', 'Rebates\n(Yes / No)',
                     'Forecasted Uplift', 'Category', 'Vendor No', 'Vendor Name', 'Distributor']
        cols = self.briefDF.columns
        cols = [x for x in cols if x not in offerCols]

        for idx, row in self.briefDF.iterrows():
            promoCode = row['Promo Code']
            promo = row['Promotion']

            isDeal = self.analysisDF[self.analysisDF['Promotion'] == promo]['Deal or Promo?']
            if len(isDeal) > 0:
                isDeal = isDeal.values[0]
            else:
                isDeal = None
                
            self.briefDict[promoCode] = {'Deal or Promo?': isDeal, 'Stores': {}}
            tmpRows = self.briefDF[self.briefDF['Promo Code'] == promoCode]
            
            if len(tmpRows) < 1:
                for col in cols:
                    self.briefDict[promoCode]['Stores'][col] = False
            else:
                for col in cols:
                    isPromoed = tmpRows[col].values[0]
                    if str(isPromoed) == 'nan':
                        self.briefDict[promoCode]['Stores'][col] = False
                    else:
                        self.briefDict[promoCode]['Stores'][col] = True


    def process(self, df):
        df['isDeal'] = [None] * len(df)
        removeRow = []
        for idx, row in df.iterrows():
            promoCode = row['Promotion']
            store = row['Company and Cost Centre']
            try:
                isPromoed = self.briefDict[promoCode]['Stores'][store]
                df.at[idx, 'isDeal'] = self.briefDict[promoCode]['Deal or Promo?']
            except:
                isPromoed = False
                df.at[idx, 'isDeal'] = None
                
            if not isPromoed:
                df.at[idx, 'Values'] = None
            else:
                if str(df.loc[idx]['Values']) == 'nan':
                    df.at[idx, 'Values'] = 0
                
            if str(df.loc[idx]['Values']) == 'nan':
                removeRow.append(idx)
                
        df = df.drop(removeRow)
        return df


if __name__ == '__main__':
    t0 = datetime.datetime.now()

    path = 'V:/8-Sales & Marketing/00 - Commercial Analytics/10. Promotion Scanning Automated Dashboard/'
    yesterday = datetime.date.today()  - datetime.timedelta(days=1)
    promoPeriod, startDate, endDate = getPeriod(yesterday, path)

    period = pd.DataFrame({'Period': [promoPeriod], 'Start': [startDate], 'End': [endDate]})
    period.to_csv(path+promoPeriod+'/Promotion period.csv', index=False)
    period.to_csv(path+'currentPeriod/Promotion period.csv', index=False)

    promoDash = PromoDash(path, promoPeriod, startDate, endDate)
    promoDash.processBrief()

    promoCodeBrief = promoDash.briefDF['Promo Code'].values.tolist()

    dateFormat = ('[Date Detail].[{}].[{}]:[Date Detail].[{}].[{}]').format(startDate[:6], startDate, endDate[:6], endDate)
    promoMDX = """
        SELECT
            NON EMPTY 
            {""" + dateFormat + """}*
            {[Promotion].[All Sales].[Total Promotions].Children} on ROWS,
            NON EMPTY 
            {[RPT Sales and Margin Measure].[POS Gross Sales]} on COLUMNS 
        FROM [Sales and Margin Reporting by SKU]
        WHERE ([Currency].[AUD],[Version].[Actual],
               [RPT Sales and Margin Measure].[POS Net Sales],
               [Company].[All Companies],[Business Unit].[All Business Units],
               [Terminal].[All Terminals],[Cluster].[All Clusters],[City].[All Cities],
               [Concept].[All Concepts],[Brand].[All Brands],[Vendor].[All Vendors],
               [Product Category].[Total Product Divisions],[Flight].[All Flights],
               [Promotion].[All Sales])
           """
    promoDF = getDate(promoMDX)
    promoCodeDB = promoDF['Promotion'].values.tolist()
    promoCodeDB = [re.findall(r'(PR.*)', x)[0] for x in promoCodeDB if re.findall(r'(PR.*)', x)]

    promoCodeBrief = promoDash.briefDF['Promo Code'].values.tolist()
    promoCode = list(set(promoCodeBrief) & set(promoCodeDB))
    promoStr = ''.join(['[Promotion].[All Sales].[Total Promotions].[%s],' % (x) for x in promoCode])[:-1]


    MDX = """
        SELECT
            NON EMPTY 
            {""" + dateFormat + """}*
            {""" + promoStr + """}*
            {[Product Category].[Total Product Divisions].Children} on ROWS,
            NON EMPTY 
            {[Company and Cost Centre].[All Companies and Cost Centres].Children} on COLUMNS
        FROM [Sales and Margin Reporting by SKU]
        WHERE ([Currency].[AUD],[Version].[Actual],
               [RPT Sales and Margin Measure].[POS Net Sales],
               [Company].[All Companies],[Business Unit].[All Business Units],
               [Terminal].[All Terminals],[Cluster].[All Clusters],[City].[All Cities],
               [Concept].[All Concepts],[Brand].[All Brands],[Vendor].[All Vendors],
               [Product Category].[Total Product Divisions],[Flight].[All Flights],
               [Promotion].[All Sales]))
           """
    raw = getDate(MDX)

    excldCol = ['Currency', 'Version', 'Business Unit', 'Terminal', 'Cluster', 'City', 'Concept', 'SKU', 
            'Brand', 'Vendor', 'Flight', 'RPT Sales and Margin Measure']
    raw.drop(excldCol, axis=1, inplace=True)
    raw[['Company','Company and Cost Centre']] = raw['Company and Cost Centre'].str.split('.',expand=True,)
    raw.to_csv(path+promoPeriod+'/dailyRpt.csv', index=False)
    raw.to_csv(path+'currentPeriod/dailyRpt.csv', index=False)


    promoDash.genPromoDict()
    promoDash.setBriefDict()
    df = promoDash.process(raw)

    df.rename(columns={'Promo Code': 'Promotion', 'Promo Type': 'isDeal', 'Store': 'Company and Cost Centre',
                  'Net Sales': 'Values'}, inplace=True)
    df['Date Detail'] = pd.to_datetime(df['Date Detail'])
    df.to_csv(path+'currentPeriod/dailyRpt_processed.csv', index=False)
    df.to_csv(path+promoPeriod+'/dailyRpt_processed.csv', index=False)

    print('ALL DONE. Using ' + str(datetime.datetime.now()-t0) + 's.')