import pyodbc

class db_connect:

    def __init__(self, driver, server, database, trusted):

        self.driver = driver
        self.server = server
        self.database = database
        self.trusted = trusted

        self.conn = pyodbc.connect(f'Driver={self.driver};Server={self.server};Database={self.database};Trusted_Connection={self.trusted};')
        
    def initiate_connection(self):

        return self.conn.cursor()

my_work = db_connect( 'SQL Server', 'PW0B6TR5\MSSQL_2022', 'TrainingDB', 'yes') #10.249.32.152,1433\MSSQL_2022 ipaddress,port/sql_instance

my_work_cursor = my_work.initiate_connection()


##################### Insert into DataBase #####################
my_work_cursor.execute("INSERT INTO dbo.client(client_id, birth_number, district_id) VALUES (?, ?, ?)", 12, 348909, 78)
my_work_cursor.commit()
################################################################

###################### Read from DataBase ######################
my_work_cursor.execute('SELECT * FROM dbo.client')

columns = [column[0] for column in my_work_cursor.description]
print(columns)

for row in my_work_cursor:
    # print(row)
    row_to_list = [elem for elem in row]

    print(row_to_list)
################################################################

my_work_cursor.close()

##################### pyodbc implementation no class ######################
###########################################################################
############################## init connection ############################
# driver ='SQL Server'
# server = 'PW0B6TR5\MSSQL_2022'
# database = 'WorkTrain'
# trusted = 'yes'

# conn = pyodbc.connect(f'Driver={driver};Server={server};Database={database};Trusted_Connection={trusted};')
# cursor = conn.cursor()

########################################## insert into db ##########################################
# cursor.execute("INSERT INTO dbo.client(client_id, birth_number, district_id) VALUES (?, ?, ?)", 12, 348909, 78)

# conn.commit()
###################################################################################################

########################################## select from db ##########################################
# cursor.execute('SELECT * FROM dbo.client')

# for row in cursor:
#     # print(row)
#     row_to_list = [elem for elem in row]

# print(row_to_list)  
###################################################################################################