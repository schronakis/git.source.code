import random
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, exists
from sqlalchemy.orm import relationship, declarative_base, session, scoped_session, sessionmaker
from sqlalchemy.sql import text
import uuid
from faker import Faker
from datetime import datetime
from _calcStats import  uniqueIDs, categoricalWeights, quantitiveWeights
import time


def unique_id(session, tableName, columnName, csvFilepath):

    random_id = random.randint(1000, 99999999)
    uniqueIdsList = uniqueIDs(csvFilepath, columnName if columnName!='relation_id' else 'disp_id')
    uniqueColumn = getattr(tableName, columnName)

    while session.query(exists().where(uniqueColumn == random_id)).scalar() and random_id in uniqueIdsList:
        random_id = random.randint(1000, 99999999)

    return random_id


def pick_bin(bins, weights, column):
    
    bin = random.choices(bins.get(column), weights.get(column))
    interval = bin[0]

    return interval

def setup_database_and_generate_data():

    engine = create_engine(f'sqlite:///sqlitedbs/bank_synthetic_data.db') 
    session = scoped_session(sessionmaker(bind=engine))
    Base = declarative_base()

    class Client(Base):
        __tablename__ = "client_table"
        client_id = Column(String, primary_key=True)
        sex = Column(String(10))
        date = Column(String(50))
        age = Column(Integer)
        social = Column(String(50))
        first_name = Column(String(50))
        middle_name = Column(String(50))
        last_name = Column(String(50))
        phone = Column(String(50))
        email = Column(String(100))
        address = Column(String(100))
        city = Column(String(50))
        state = Column(String(10))
        zipcode = Column(Integer)
        district_id = Column(Integer)

        relations = relationship('Relations', back_populates='client')
        
    class Accounts(Base):
        __tablename__ = "accounts_table"
        account_id = Column(String, primary_key=True)
        district_id = Column(Integer)
        frequency = Column(String(150))
        parsedate = Column(String(50))

        relations = relationship('Relations', back_populates='accounts')
        loans =relationship('Loans', back_populates='accounts')

    class Relations(Base):
        __tablename__ = "relations_table"
        relation_id = Column(String, primary_key=True)
        client_id = Column(String(50), ForeignKey('client_table.client_id'))
        account_id = Column(String(50), ForeignKey('accounts_table.account_id'))
        type = Column(String(50))

        client = relationship('Client', back_populates='relations')
        accounts = relationship('Accounts', back_populates='relations')

    class Loans(Base):
        __tablename__ = "loans_table"
        loan_id = Column(String(50), primary_key=True)
        account_id = Column(String(50), ForeignKey('accounts_table.account_id'))
        amount = Column(Integer)
        duration = Column(Integer)
        payments = Column(Integer)
        status = Column(String(50))
        fulldate = Column(String(50))
        purpose = Column(String(100))

        accounts = relationship('Accounts', back_populates='loans')


    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()

    session.execute(text('DELETE FROM relations_table'))
    session.execute(text('DELETE FROM client_table'))
    session.execute(text('DELETE FROM accounts_table'))
    session.execute(text('DELETE FROM loans_table'))
    session.commit()
    

    fake = Faker()

    num_of_records = 5000  # Number of generated records

    for _ in range(num_of_records):

        # Create fake data and add to the session
        csvClientPath = './bank.refined/completedclient.csv'
        ClientCatValues, ClientCatProbs = categoricalWeights(csvClientPath, ['sex','age'])

        firstname = fake.first_name()
        lastname = fake.last_name()
        age = str(random.choices(ClientCatValues.get('age'), ClientCatProbs.get('age'))[0])
        birthDate = fake.date_between(start_date=datetime(2024,1,1), end_date=datetime(2024,12,31))

        client = Client(
            client_id = f"C{unique_id(session, Client, 'client_id', csvClientPath):08d}",
            sex = str(random.choices(ClientCatValues.get('sex'), ClientCatProbs.get('sex'))[0]),
            date = f"{datetime.now().year-int(age)}-{birthDate.month:02d}-{birthDate.day:02d}",
            age = int(age),   
            social = str(uuid.uuid4()),
            first_name = firstname,
            middle_name = fake.random_element([fake.first_name_male(), fake.first_name_female()]),
            last_name = lastname,
            phone =  fake.phone_number(),
            email = f'{firstname}.{lastname}@{fake.domain_name()}',
            address = fake.address(),
            city = fake.city(),
            state = fake.state(),
            zipcode = fake.zipcode(),
            district_id = fake.random_int(min=1, max=100),
        )


        csvAccPath = './bank.refined/completedacct.csv'
        AccCatValues,AccCatProbs =  categoricalWeights(csvAccPath, ['frequency'])
        
        accounts = Accounts(
            account_id = f"A{unique_id(session, Accounts, 'account_id', csvAccPath):08d}",
            district_id = fake.random_int(min=1, max=100),
            frequency = str(random.choices(AccCatValues.get('frequency'), AccCatProbs.get('frequency'))[0]),
            parsedate = fake.date(),
        )
        

        csvRelPath = './bank.refined/completeddisposition.csv'
        RelCatValues, RelCatProbs = categoricalWeights(csvRelPath, ['type'])

        relations = Relations(
            relation_id = f"R{unique_id(session, Relations, 'relation_id', csvRelPath):08d}",
            client_id = client.client_id,
            account_id = accounts.account_id,
            type = str(random.choices(RelCatValues.get('type'), RelCatProbs.get('type'))[0]),
        )
        

        csvLoansPath = './bank.refined/completedloan.csv'
        LoanCatValues, LoanCatProbs = categoricalWeights(csvLoansPath, ['duration', 'status', 'purpose'])
        LoanBins, LoanWeights = quantitiveWeights(csvLoansPath, ['amount', 'payments'])
        
        # duration = str(random.choices(LoanCatValues.get('duration'), LoanCatProbs.get('duration'))[0])

        loans = Loans(
            loan_id = f"L{unique_id(session, Loans, 'loan_id', csvLoansPath):08d}",
            account_id = accounts.account_id,
            amount = int(random.uniform(pick_bin(LoanBins, LoanWeights, 'amount').left, pick_bin(LoanBins, LoanWeights, 'amount').right)),
            duration = int(str(random.choices(LoanCatValues.get('duration'), LoanCatProbs.get('duration'))[0])),
            payments = int(random.uniform(pick_bin(LoanBins, LoanWeights, 'payments').left, pick_bin(LoanBins, LoanWeights, 'payments').right)),
            status = str(random.choices(LoanCatValues.get('status'), LoanCatProbs.get('status'))[0]),
            fulldate = fake.date_between(start_date=datetime(2004,1,1), end_date=datetime.now()),
            purpose = str(random.choices(LoanCatValues.get('purpose'), LoanCatProbs.get('purpose'))[0])
        )


        try:
            session.add_all([client,accounts,relations,loans])
            session.commit()
        except:
            session.rollback()
            print("IntegrityError occurred. Retrying with a new ID.")
           
        
    session.close()


if __name__ == "__main__":

    start = time.perf_counter()

    setup_database_and_generate_data()

    print(f'Running time: {time.perf_counter() - start}')

