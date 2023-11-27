import pyodbc

class db_connect:

    def __init__(self, driver, server, database, trusted):

        self.driver = driver
        self.server = server
        self.database = database
        self.trusted = trusted

        self.conn = pyodbc.connect(f'Driver={self.driver};Server={self.server};Database={self.database};Trusted_Connection={self.trusted};')

    def db_commit(self):

        return self.conn.commit()
        
    def initiate_connection(self):

        return self.conn.cursor()
        
    def end_connection(self):

        return self.conn.close()


##################### insert to db ######################
# cursor = conn.cursor()
# cursor.execute("INSERT INTO HumanResources.Department(Name, GroupName, ModifiedDate) VALUES (?, ?, ?)", 'Access', 'AC', '2023-08-10 23:00:00.000')

# conn.commit()
#########################################################

##################### select rom db #####################
# cursor.execute('SELECT * FROM stg.accounts')

# for row in cursor:
#     # print(row)
#     row_to_list = [elem for elem in row]

# print(row_to_list)
#########################################################

# my_work = db_connect( 'SQL Server', 'E352012', 'MyWork', 'yes')

# my_work_cursor = my_work.initiate_connection()

# my_work_cursor.execute('SELECT * FROM stg.accounts')

# columns = [column[0] for column in my_work_cursor.description]
# print(columns)

# for row in my_work_cursor:
#     # print(row)
#     row_to_list = [elem for elem in row]

#     print(row_to_list)
