from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date
from sqlalchemy.orm import relationship, declarative_base, scoped_session, sessionmaker
from sqlalchemy.sql import text
import uuid
from faker import Faker

def setup_database_and_generate_data():
    project_path = 'C:/Users/schronak/Downloads/git/git.source.code/python.project/synthetic_data/faker_gen/'
    engine = create_engine(f'sqlite:///{project_path}bank_synthetic_data.db') 
    session = scoped_session(sessionmaker(bind=engine))
    Base = declarative_base()

    fake = Faker()

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
    
    class Relations(Base):
        __tablename__ = "relations_table"
        relation_id = Column(String, primary_key=True)
        client_id = Column(String(50), ForeignKey('client_table.client_id'))
        account_id = Column(String(50), ForeignKey('accounts_table.account_id'))
        type = Column(String(50))

        client = relationship('Client', back_populates='relations')
        accounts = relationship('Accounts', back_populates='relations')

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    session.execute(text('DELETE FROM relations_table'))
    session.execute(text('DELETE FROM client_table'))
    session.execute(text('DELETE FROM accounts_table'))
    session.commit()
    
    num_of_records = 10  # Change this to the desired number of records

    for _ in range(num_of_records):
        # Create fake data and add to the session
        client_genders = ['Male', 'Female']
        accounts_frequency = ['Monthly Issuance', 'Issuance After Transaction', 'Weekly Issuance']
        relations_type = ['Owner', 'User']
        firstname = fake.first_name()
        lastname = fake.last_name()

        client = Client(
            client_id = f'C{fake.random_int(min=1, max=10000)}',
            sex = fake.random_element(client_genders),
            date = fake.date(),
            age = fake.random_int(min=1, max=99),
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
        accounts = Accounts(
            account_id = f'A{fake.random_int(min=1, max=10000)}',
            district_id = fake.random_int(min=1, max=100),
            frequency = fake.random_element(accounts_frequency),
            parsedate = fake.date(),
        )
        relations = Relations(
            relation_id = f'R{fake.random_int(min=1, max=10000)}',
            client_id = client.client_id,
            account_id = accounts.account_id,
            type = fake.random_element(relations_type),
        )
  
        session.add(client)
        session.add(accounts)
        session.add(relations)

    session.commit()
    session.close()

if __name__ == "__main__":
    setup_database_and_generate_data()
