'''This is the script to run on Dumbo and generate the outputs required. There
are 25 functions in total:
1. one is used for checking the type of enrty;
2. the rest 24 are for checking the validity of 24 columns.'''


from __future__ import print_function
from operator import add
from csv import reader
from pyspark import SparkContext
import datetime
from dateutil.parser import parse


def check_type(x):
    '''Given an entry, it detects the entry is Float/Int, Date, Time, Coordinate or String'''
    if x=='':
        return None
    else:
        try:
            a=float(x)
            if a.is_integer():
                return 'Int'
            else:
                return 'Float'
        except ValueError:
            pass
    
        try:
            if ':' not in x:
                a=parse(x)
                return('Date')
            else:
                datetime.datetime.strptime(x, "%H:%M:%S").time()
                return('Time')
        except ValueError:
            pass
        try:
            if (x[0]!='(')|(x[-1]!=')'):
                raise ValueError
            else:
                k=x[1:-1].split(',')
                if len(k)!=2:
                    raise ValueError
                elif (float(k[0]),float(k[1])):
                    return 'Coordinate'
        except ValueError:
            pass
        return 'String'





def check_valid_col_1(x):
    '''CMPLNT_NUM'''
    '''the validity is determined as follow:
    1. length of the entry is 9 and the type is int > Valid
    2. empty entry > Null
    3. else > Invalid'''
    if x=='':
        return 'Null'
    elif len(x)!=9:
        return 'Invalid'
    elif check_type(x) != 'Int':
        return 'Invalid'
    else:
        return 'Valid'

def check_valid_col_2(x):
    '''CMPLNT_FR_DT'''
    '''the validity is determined as follow:
    1.type is Date> Valid
    2. empty entry > Null
    3. else > Invalid'''
    if x=='':
        return 'Null'
    elif check_type(x)!='Date':
        return 'Invalid'
    else:
        return 'Valid'


def check_valid_col_3(x):
    '''CMPLNT_FR_TM'''
    '''the validity is determined as follow:
    1. type is Time> Valid
    2. empty entry > Null
    3. else > Invalid'''
    if x=='':
        return 'Null'
    elif check_type(x) != 'Time':
        return 'Invalid'
    else:
        return 'Valid'

def check_valid_col_4(x,y): #map two columns
    '''CMPLNT_TO_DT'''
    '''the validity is determined as follow:
    1. the type is Date, it is no later than the from date/when FM_DT is not missing but this is missing > Valid
    2. empty entry and FROM_DT is also missing > Null
    3. else > Invalid'''
    if (y=='') & (x==''):
        return 'Null'
    elif (y=='') &(x!=''):
        return 'Valid'
    elif check_type(y)!='Date':
        return 'Invalid'
    elif check_type(x)=='Date':
        from_dt=parse(x)
        to_dt=parse(y)
        if from_dt>to_dt:
            return 'Invalid'
        else:
            return 'Valid'
    else:
        return 'Valid'

def check_valid_col_5(date1,date2,x,y):
    '''CMPLNT_TO_TM'''
    '''the validity is determined as follow:
    1. the type is Time, it is no later than the from time(when they are on the same day)/when FM_TM is not missing but this is missing > Valid
    2. empty entry and FROM_TM is also missing > Null
    3. else > Invalid'''
    if (y=='') & (x==''):
        return 'Null'
    elif (y=='') &(x!=''):
        return 'Valid'
    elif check_type(y)!='Time':
        return 'Invalid'
    elif (date1==date2)& (date1 != '') & (check_type(x)=='Time'):
        if datetime.datetime.strptime(x, "%H:%M:%S").time() >= datetime.datetime.strptime(y, "%H:%M:%S").time():
            return 'Invalid'
        else:
            return 'Valid'
    else:
        return 'Valid'


def check_valid_col_6(x):
    '''RPT_DT'''
    '''the validity is determined as follow:
    1.type is Date and the date is between 2005 and 2015 > Valid
    2. empty entry > Null
    3. else > Invalid'''   
    if x=='':
        return 'Null' 
    elif check_type(x)!='Date':
        return 'Invalid'
    elif (parse(x).year<2005)|(parse(x).year>=2016):
        return 'Invalid'
    else:
        return 'Valid'

def check_valid_col_7(x):
    '''KY_CD'''
    '''the validity is determined as follow:
    1.type is three digits int > Valid
    2. empty entry > Null 
    3. else > Invalid'''   
    if x=='':
        return 'Null' 
    elif len(x)!=3:
        return 'Invalid'
    elif check_type(x)!='Int':
        return 'Invalid'
    else:
        return 'Valid'

def check_valid_col_8(x):
    '''OFNS_DESC'''
    '''the validity is determined as follow:
    1.type is String > Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x=='':
        return 'Null' 
    elif check_type(x)!='String':
        return 'Invalid'
    else:
        return 'Valid'


def check_valid_col_9(x):
    '''PD_CD'''
    '''the validity is determined as follow:
    1.type is three digits int > Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x=='':
        return 'Null'
    elif len(x)!=3:
        return 'Invaid'
    elif check_type(x)!='Int':
        return 'Invalid'
    else:
        return 'Valid'



def check_valid_col_10(x):
    '''PD_DESC'''
    '''the validity is determined as follow:
    1.type is String> Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x=='':
        return 'Null'
    elif check_type(x)!='String':
        return 'Invalid'
    else:
        return 'Valid'


def check_valid_col_11(x):
    '''CRM_ATPT_CPTD_CD'''
    '''the validity is determined as follow:
    1. Whether it is equal to COMPLETED and ATTEMPTED> Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x=='':
        return 'Null' 
    elif (x!='COMPLETED')&(x!='ATTEMPTED'):
        return 'Invalid'
    else:
        return 'Valid'


def check_valid_col_12(x):
    '''LAW_CAT_CD'''
    '''the validity is determined as follow:
    1.Whether it is equal to FELONY, MISDEMEANOR and VIOLATION > Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x=='':
        return 'Null' 
    elif x not in ['FELONY','MISDEMEANOR','VIOLATION']:
        return 'Invalid'
    else:
        return 'Valid' 


def check_valid_col_13(x):
    '''JURIS_DESC'''
    '''the validity is determined as follow:
    1.type is String > Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x=='':
        return 'Null' 
    elif check_type(x)!='String':
        return 'Invalid'
    else:
        return 'Valid'


def check_valid_col_14(x):
    '''BORO_NM'''
    '''the validity is determined as follow:
    1.Whether it is equal to MANHATTAN, QUEENS, STATEN ISLAND, BROOKLYN, BRONX > Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x=='':
        return 'Null'
    elif x in ['MANHATTAN','QUEENS','STATEN ISLAND','BROOKLYN','BRONX']:
        return 'Valid'
    else:
        return 'Invalid'

Precinct_Boro={'MANHATTAN':[1,5,6,7,9,10,13,17,19,20,23,24,25,26,28,30,32,33,34],'QUEENS':range(110,116),'STATEN ISLAND':[120,121,122,123],'BRONX':[40,41,42,43,44,45,46,47,48,49,50,52],'BROOKLYN':[60,61,62,63,66,67,68,69,70,71,72,73,75,76,77,78,79,81,83,84,88,90,94]}        
def check_valid_col_15(x,y):
    '''ADDR_PCT_CD'''
    '''the validity is determined as follow:
    1. the type is an int and the int matches the boro_name (according to the dictionary found via Internet)> Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if y == '':
        return 'Null'
    elif check_type(y) != 'Int':
        return 'Invalid'
    elif check_valid_col_14(x)=='Valid':
        if int(y) not in Precinct_Boro[x]:
            return 'Invalid'
        else:
            return 'Valid'
    else:
        return 'Valid'

def check_valid_col_16(x):
    '''LOC_OF_OCCUR_DESC'''
    '''the validity is determined as follow:
    1. Whether it is equal to REAR OF, OUTSIDE, 'OPPOSITE OF', 'FRONT OF' ,'INSIDE' > Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x== '':
        return 'Null'
    elif x not in['REAR OF','OUTSIDE','OPPOSITE OF', 'FRONT OF' ,'INSIDE']:
        return 'Invalid'
    else:
        return 'Valid'

def check_valid_col_17(x):
    '''PREM_TYP_DESC'''
    '''the validity is determined as follow:
    1. the type is an String> Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x== '':
        return 'Null'
    elif check_type(x)!='String':
        return 'Invalid'
    else:
        return 'Valid'

    
def check_valid_col_18(x):
    '''PREM_TYP_DESC'''
    '''the validity is determined as follow:
    1. the type is String > Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x== '':
        return 'Null'
    elif check_type(x)!='String':
        return 'Invalid'
    else:
        return 'Valid'



def check_valid_col_19(x):
    '''PREM_TYP_DESC'''
    '''the validity is determined as follow:
    1. the type is String> Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x== '':
        return 'Null'
    elif check_type(x)!='String':
        return 'Invalid'
    else:
        return 'Valid'


def check_valid_col_20(x):
    '''X_COORD_CD,Y_COORD_CD,Latitude,Longitude'''
    '''the validity is determined as follow:
    1. the type is an int/float > Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x== '':
        return 'Null'    
    elif (check_type(x) != 'Int') & (check_type(x) != 'Float'):
        return 'Invalid'
    else:
        return 'Valid'


def check_valid_col_24(a,b,x):
    '''Lat_Lon'''
    '''the validity is determined as follow:
    1. the type is in the format of coordinate, the left(right) coordinate is equal to the value of col_22(col_23)> Valid
    2. empty entry > Null 
    3. else > Invalid''' 
    if x=='':
        return 'Null'
    elif check_type(x) != 'Coordinate':
        return 'Invalid'
    else:
        k=x[1:-1].split(',')
        left=float(k[0])
        right=float(k[1])
        if (check_valid_col_20(a)=='Valid') & (check_valid_col_20(b)=='Valid'):
            if (left != float(a)) | (right !=float(b)):
                return 'Invalid'
            else:
                return 'Valid'
        else:
            return 'Valid'









if __name__ == "__main__":
    sc = SparkContext()
    data=sc.textFile('NYPD_Complaint_Data_Historic.csv')
    header=data.filter(lambda l: 'CMPLNT_NUM' in l)
    Noheader=data.subtract(header).mapPartitions(lambda x: reader(x))


    col_li={}
    col_li['col_1'] = Noheader.map(lambda x: x[0]).map(lambda x: (x, 'Int','ID',check_valid_col_1(x)))
    col_li['col_2'] = Noheader.map(lambda x: x[1]).map(lambda x: (x, 'Date','Occurring date',check_valid_col_2(x)))
    col_li['col_3'] = Noheader.map(lambda x: x[2]).map(lambda x: (x, 'Time','Occurring Time',check_valid_col_3(x)))
    col_li['col_4'] = Noheader.map(lambda x: (x[1],x[3])).map(lambda x: (x[1], 'Date','Ending date ',check_valid_col_4(x[0],x[1])))
    col_li['col_5'] = Noheader.map(lambda x: (x[1],x[3],x[2],x[4])).map(lambda x: (x[3], 'Time','Ending time',check_valid_col_5(x[0],x[1],x[2],x[3])))
    col_li['col_6'] = Noheader.map(lambda x: x[5]).map(lambda x: (x, 'Date','Report date',check_valid_col_6(x)))
    col_li['col_7'] = Noheader.map(lambda x: x[6]).map(lambda x: (x, 'Int','Offense classification code',check_valid_col_7(x)))
    col_li['col_8'] = Noheader.map(lambda x: x[7]).map(lambda x: (x, 'String','Offense code description',check_valid_col_8(x)))
    col_li['col_9'] = Noheader.map(lambda x: x[8]).map(lambda x: (x, 'Int','Internal classification code',check_valid_col_9(x)))
    col_li['col_10'] = Noheader.map(lambda x: x[9]).map(lambda x: (x, 'String','Internal classification code description',check_valid_col_10(x)))
    col_li['col_11'] = Noheader.map(lambda x: x[10]).map(lambda x: (x, 'String','Crime status',check_valid_col_11(x)))
    col_li['col_12'] = Noheader.map(lambda x: x[11]).map(lambda x: (x, 'String','Level of offense',check_valid_col_12(x)))
    col_li['col_13'] = Noheader.map(lambda x: x[12]).map(lambda x: (x, 'String','Responsible Jurisdiction',check_valid_col_13(x)))
    col_li['col_14'] = Noheader.map(lambda x: x[13]).map(lambda x: (x, 'String','Borough',check_valid_col_14(x)))
    col_li['col_15'] = Noheader.map(lambda x: (x[13],x[14])).map(lambda x: (x[1], 'Int','Precinct code',check_valid_col_15(x[0],x[1])))
    col_li['col_16'] = Noheader.map(lambda x: x[15]).map(lambda x: (x, 'String','Location of event',check_valid_col_16(x)))
    col_li['col_17'] = Noheader.map(lambda x: x[16]).map(lambda x: (x, 'String','Premise Type Description',check_valid_col_17(x)))
    col_li['col_18'] = Noheader.map(lambda x: x[17]).map(lambda x: (x, 'String','Park name',check_valid_col_18(x)))
    col_li['col_19'] = Noheader.map(lambda x: x[18]).map(lambda x: (x, 'String','Housing Development',check_valid_col_19(x)))
    col_li['col_20'] = Noheader.map(lambda x: x[19]).map(lambda x: (x, 'Int','NY X-coordinate',check_valid_col_20(x)))
    col_li['col_21'] = Noheader.map(lambda x: x[20]).map(lambda x: (x, 'Int','NY Y-coordinate',check_valid_col_20(x)))
    col_li['col_22'] = Noheader.map(lambda x: x[21]).map(lambda x: (x, 'Float','Latitude',check_valid_col_20(x)))
    col_li['col_23'] = Noheader.map(lambda x: x[22]).map(lambda x: (x, 'Float','Longitude',check_valid_col_20(x)))
    col_li['col_24'] = Noheader.map(lambda x: (x[21],x[22],x[23])).map(lambda x: (x[2], 'Coordinate','Latitude, Longitude',check_valid_col_24(x[0],x[1],x[2])))


    for i in col_li.keys():
        col_li[i].saveAsTextFile('{0}.out'.format(i))

    

    
    sc.stop()






