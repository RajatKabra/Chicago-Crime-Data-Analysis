from pymongo import MongoClient, errors

host = '192.168.249.103'
port = 27017
dbname = 'project'
colname = 'projectdb'
timeout = 4000 
np = 10
ex = -1
num = 16
val = 0

def pl(ps, n):
    i = 0
    split_lines = ps.split('\n')
    while i < n and i < len(split_lines):
        print(split_lines[i])
        i += 1
    return '\n'.join(split_lines[i:])

ol = [
    '[0] Show this message again',
    '[1] Where does the crime happen most often? (Street, Home, etc)',
    '[2] Max crime location wherein the police could make an arrest',
    '[3] Crime in which most arrests were made',
    '[4] Number of Domestic violence cases in 2017 and the location where it occurred.',
    '[5] List of crime records where crime took place on Dec 25:',
    '[6] Which year saw the maximum number of crimes in Chicago? Also, what are the number of crimes that happened in Chicago categorized by year?',
    '[7] What is the IUCR code for the crime where the Primary Type is Homicide and Secondary Type is First Degree Murder. This IUCR code can be used to locate the number of Homicide- First degree murders per year.',
    '[8] How many First Degree Homicide murders (based on the IUCR code that we found in the previous step) were reported in which an arrest was made, per year.',
    '[9] Year with the most number of crimes',
    '[10] The most common type of secondary crime in descending order with the primary crime category specified (“CRIMINAL DAMAGE”)',
    '[11] Find case number of all the crimes that happened in the location range given.',
    '[12] Most unsafe districts in descending order.',
    '[13] first 10 wards of police that made least number of arrest.',
    '[14] Find distinct IUCR code of all the crime under ‘OTHER OFFENSE’ category',
    '[15] Type and number of Theft crime involving money specified in amount.',
    '[16] Community areas with with least number of Theft Crime involving amount of money in ascending order.'
]

def pl(ps, n):
    i = 0
    split_lines = ps.split('\n')
    while i < n and i < len(split_lines):
        print(split_lines[i])
        i += 1
    return '\n'.join(split_lines[i:])
def p_l(l):
    po = ''
    for elem in l:
        po += str(elem) + '\n'
    return po
def gql():
    return p_l(ol)
def bi(index):
    return 'Error: ' + str(index) + ' is not valid!'

def query_1(col):
    pipeline=[( [ { $group : { _id : "$loc_desc", No_of_Crimes : { $sum : 1 } } }, { $sort : { No_of_Crimes: -1 } } ] )]
    return 'Query 1' +p_l(col.aggregate(pipeline))
def query_2(col):
    pipeline = [ { $match: { Arrest: "true" } }, { $group: { _id: "$Location Description", Arrest_made: { $sum: 1 } } }, { $sort : { Arrest_made: -1 } } ]  # pipeline = pretty much exactly you would use in in normal "db.<col>.aggregate()" call in mongo shell
    return 'Query 2:'+ p_l(col.aggregate(pipeline))
def query_3(col):
    pipeline =[ { $match : { Arrest: "true" } }, { $group : { _id : "$Primary Type", Arrest_Made : { $sum : 1 } } }, { $sort : { Arrest_Made: -1 } } ]
    return 'Query 3' + p_l(col.aggregate(pipeline))
def query_4(col):
    pipeline=[ { $match: { $and: [ { Date: { $regex : '/2017' } }, { Domestic: "true" } ] } }, { $group : { _id : "$Location Description", No_of_Crimes: { $sum: 1 } } }, { $sort: { No_of_Crimes: -1 } } ]
    return 'Query 4'+p_l(col.aggregate(pipeline))
def query_5(col):
    return 'Query 5: Find all reported crimes with the primary type of "NARCOTICS"\n' + \
           p_l(col.find({ Date: { $regex: '12/25/' } } ).limit( 10 ).pretty())
def query_6(col):
    pipeline=[ { $group: { _id: "$Year", No_of_Crimes: { $sum: 1 } } }, { $sort: { No_of_Crimes: -1 } } ]
    return 'Query 6: Find the top 10 locations where the most "THEFT" happens\n' +p_l(col.aggregate(pipeline))
def query_7(col):
    return 'Query 7: Find the crime in which most arrests were made\n' + p_l(col.find({ Date: { $regex: '12/25/' } } ).limit( 10 ).pretty())
def query_8(col):
    pipeline=[ { $match: { $and: [ { IUCR: 110 }, { Arrest: "true" } ] } }, { $group: { _id: "$Year", No_of_Crimes: { $sum: 1 } } }, { $sort: { No_of_Crimes: -1 } } ]
    return 'Query 8'+p_l(col.aggregate(pipeline))
def query_9(col):
    pipeline=[ { $group: { _id: "$Year", Crimes: { $sum: 1 } } }, { $sort: { Crimes: -1 } }, { $limit:1 } ]
    return 'Query 9'+p_l(col.aggregate(pipeline))
def query_10(col):
    pipeline=[ { $match: { "Primary Type": "CRIMINAL DAMAGE" } }, { $group: { _id: "$Description", Number_crime: { $sum: 1 } } }, { $sort: { Number_crime: -1 } } ]
    return 'Query 10'+p_l(col.aggregate(pipeline))
def query_11(col):
    return 'Query 11: Find the crime in which most arrests were made\n' + p_l(col.find({ Latitude: { $gte: 41.79922968 }, Longitude: { $lte: -87.766148551 } }, { "Case Number":1 } ).pretty())
def query_12(col):
    pipeline=[ { $group: { _id: "$District", Crimes: { $sum: 1 } } }, { $sort: { Crimes: -1 } } ]
    return 'Query 12'+p_l(col.aggregate(pipeline))
def query_13(col):
    pipeline=[ { $match: { Arrest: "true" } }, { $group: { _id: "$Ward", Arrest_made: { $sum: 1 } } }, { $sort: { Arrest_made: 1 } }, { $limit:10 } ]
    return 'Query 13'+p_l(col.aggregate(pipeline))
def query_14(col):
    return 'Query 14: Find the crime in which most arrests were made\n' + p_l(col.distinct( "IUCR", { "Primary Type": "OTHER OFFENSE" } ))
def query_15(col):
    pipeline=[ { $match: { $and: [ { "Primary Type": "THEFT" }, { Description: /00/ } ] } }, { $group: { _id : "$Description", Number: { $sum: 1 } } }, { $sort: { Number: -1 } } ]
    return 'Query 15'+p_l(col.aggregate(pipeline))
def query_16(col):
    pipeline=[ { $match: { $and: [ { "Primary Type": "THEFT" }, { Description: /00/ } ] } }, { $group: { _id: "$Community Area", Number: { $sum: 1 } } }, { $sort: { Number: 1 } } ]
    return 'Query 16'+p_l(col.aggregate(pipeline))
def query_17(col):
    pipeline=[ { $match: { $and: [ { "Primary Type": "WEAPONS VIOLATION" }, { Description: /HANDGUN/ } ] } }, { $group: { _id: "$Location Description", Number: { $sum: 1 } } }, { $sort: { Number: -1 } }, { $limit: 1 } ]
    return 'Query 17'+p_l(col.aggregate(pipeline))
def query_18(col):
    pipeline=[ { $group: { _id: "$Block", No_of_Crimes: { $sum: 1 } } }, { $sort: { No_of_Crimes: -1 } }, { $limit:10 } ]
    return 'Query 18'+p_l(col.aggregate(pipeline))

def run_query(index, col):
    if type(index) is int:  # Make sure that index is an integer
        if index == 0:
            return gql()
        elif index == 1:
            return query_1(col)
        elif index == 2:
            return query_2(col)
        elif index == 3:
            return query_3(col)
        elif index == 4:
            return query_4(col)
        elif index == 5:
            return query_5(col)
        elif index == 6:
            return query_6(col)
        elif index == 7:
            return query_7(col)
        elif index == 8:
            return query_8(col)
        elif index == 9:
            return query_9(col)
        elif index == 10:
            return query_10(col)
        elif index == 11:
            return query_11(col)
        elif index == 12:
            return query_12(col)
        elif index == 13:
            return query_13(col)
        elif index == 14:
            return query_14(col)
        elif index == 15:
            return query_15(col)
        elif index == 16:
            return query_16(col)
        elif index == 17:
            return query_17(col)
        elif index == 18:
            return query_18(col)
        else:
            bi(index)
    return 'Type of index must be int, not ' + str(type(index)) + ' for run_query'

def main():
    print('Chicago Crime Data Analysis\n')
    print('Attempting to connect to MongoDB host ' + host + ' on port ' + str(port))
    try:
        client = MongoClient(host, port,serverSelectionTimeoutMS=timeout)
        client.server_info()
    except errors.ServerSelectionTimeoutError:
        print('Connection timed out')
        return
    print('Connection successful!')
    db = client[dbname]
    col = db[colname]
    print('\nHere are the current query ops in the program:')
    print(run_query(0, col))
    op = 0
    while op != ex:
        print('\nWhat op would you like to use? (-1: exit;0:show query options)')
        op_text = input('-> ')
        try:
            op = int(op_text)
        except ValueError:
            op = -10

        # Check for valid ops being used
        if op < ex or op > num:
            print('Please enter a valid number')
            op = 1
        elif op == ex:
            break
        else:            
            if op != val:
                result = pl(run_query(op, col), np)
                op = 'it'
                while result.find('\n') != -1 and op == 'it':
                    print('Enter "it" to view more results (or anything else to choose another op)')
                    op = input('--> ')
                    if op != 'it':
                        break
                    result = pl(result, np)
            else:
                print(run_query(op, col))
    client.close()
    print('Termicating')

main()
