import enum

from sqlalchemy import create_engine, Table, DateTime, Float, Enum, Numeric, BigInteger
from sqlalchemy import Column, Integer, String, ForeignKey, SmallInteger
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.ext.declarative import declarative_base

import pandas as pd
import numpy as np
import re

# Base class used by my classes (my entities)
from sqlalchemy.sql.ddl import CreateTable

Base = declarative_base()  # Required


class Country(Base):
    __tablename__ = 'country'
    ctr_id = Column(Integer, primary_key=True)
    ctr_name = Column(String(42))


class Sondage(Base):
    __tablename__ = 'sondage'
    sdg_id = Column(Integer, primary_key=True)
    sdg_year = Column(SmallInteger)


class LargestCity(Base):
    __tablename__ = 'largest_city'
    pop_id = Column(Integer, primary_key=True)
    pop_name = Column(String(50))


class Education(Base):
    __tablename__ = 'education'
    edu_id = Column(Integer, primary_key=True)
    edu_title = Column(String(50))


class LookingJob(Base):
    __tablename__ = 'looking_job'
    look_id = Column(Integer, primary_key=True)
    look_job = Column(String(100))


class CarreerPlan(Base):
    __tablename__ = 'carreer_plan'
    cap_id = Column(Integer, primary_key=True)
    cap_plan = Column(String(50))


class Certification(Base):
    __tablename__ = 'certification'
    cert_id = Column(Integer, primary_key=True)
    cert_name = Column(String(50))


class HowManyCompanies(Base):
    __tablename__ = 'how_many_companies'
    mcp_id = Column(Integer, primary_key=True)
    mcp_many_companies = Column(String(150))


class EmploymentStatus(Base):
    __tablename__ = 'employment_status'
    emp_id = Column(Integer, primary_key=True)
    emp_status = Column(String(150))


class EmploymentSector(Base):
    __tablename__ = 'employment_sector'
    sec_id = Column(Integer, primary_key=True)
    sec_name = Column(String(50))


class Task(Base):
    __tablename__ = 'task'
    tas_id = Column(Integer, primary_key=True)
    tas_name = Column(String(255))


association_table_tp = Table('task_perfomed', Base.metadata,
                             Column('tas_id', Integer, ForeignKey('task.tas_id')),
                             Column('sgi_id', Integer, ForeignKey('sondage_item.sgi_id'))
                             )


class Job(Base):
    __tablename__ = 'job'
    job_id = Column(Integer, primary_key=True)
    job_name = Column(String(150))


association_table_ot = Table('other_duties', Base.metadata,
                             Column('job_id', Integer, ForeignKey('job.job_id')),
                             Column('sgi_id', Integer, ForeignKey('sondage_item.sgi_id'))
                             )


class Database(Base):
    __tablename__ = 'db'
    db_id = Column(Integer, primary_key=True)
    db_name = Column(String(100))


association_table_odb = Table('other_db', Base.metadata,
                              Column('db_id', Integer, ForeignKey('db.db_id')),
                              Column('sgi_id', Integer, ForeignKey('sondage_item.sgi_id'))
                              )


class SondageItem(Base):
    __tablename__ = 'sondage_item'
    sgi_id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    salary_usd = Column(Numeric(10, 2))
    postal_code = Column(String(50))
    years_with_db = Column(Integer)
    manage_staff = Column(SmallInteger)
    years_with_job = Column(SmallInteger)
    other_people = Column(Integer)
    company_employee = Column(String(150))
    database_servers = Column(Integer)
    education_computer = Column(SmallInteger)
    hours_worked = Column(Integer)
    telecommute = Column(Integer)
    newest_version = Column(String(100))
    oldest_version = Column(String(100))
    gender = Column(String(100))
    ctr_id = Column(Integer, ForeignKey('country.ctr_id'))
    sdg_id = Column(Integer, ForeignKey('sondage.sdg_id'))
    pop_id = Column(Integer, ForeignKey('largest_city.pop_id'))
    edu_id = Column(Integer, ForeignKey('education.edu_id'))
    look_id = Column(Integer, ForeignKey('looking_job.look_id'))
    cap_id = Column(Integer, ForeignKey('carreer_plan.cap_id'))
    cert_id = Column(Integer, ForeignKey('certification.cert_id'))
    mcp_id = Column(Integer, ForeignKey('how_many_companies.mcp_id'))
    emp_id = Column(Integer, ForeignKey('employment_status.emp_id'))
    sec_id = Column(Integer, ForeignKey('employment_sector.sec_id'))
    job_id = Column(Integer, ForeignKey('job.job_id'))
    db_id = Column(Integer, ForeignKey('db.db_id'))


# The main part
if __name__ == '__main__':
    engine = create_engine('mysql+pymysql://martin:simplon!21M@localhost/survey', echo=False)

    data = pd.read_excel('jeux_donnees/Data_Professional_Salary_Survey_Responses_1.xls')
    print(data)

    print("--- Construct all tables for the database (here just one table) ---")
    Base.metadata.create_all(engine)  # Only for the first time

    print("_________test insertion ________")


    def insert_country():
        column_country = pd.read_sql_query("select * from country", engine).columns
        country = pd.DataFrame(columns=column_country)
        country['ctr_name'] = data['Country']
        country.drop_duplicates(inplace=True)
        country.to_sql('country', if_exists='append', con=engine, index=False)
        country_table = pd.read_sql_query("select * from country", engine)
        print(country_table)


    def insert_sondage():
        column_sondage = pd.read_sql_query("select * from sondage", engine).columns
        sondage = pd.DataFrame(columns=column_sondage)
        sondage['sdg_year'] = data['Survey Year']
        sondage.drop_duplicates(inplace=True)
        sondage.to_sql('sondage', if_exists='append', con=engine, index=False)
        sondage_table = pd.read_sql_query("select * from sondage", engine)
        print(sondage_table)


    def insert_largest_city():
        column_largest_c = pd.read_sql_query("select * from largest_city", engine).columns
        largest_city = pd.DataFrame(columns=column_largest_c)
        largest_city['pop_name'] = data['PopulationOfLargestCityWithin20Miles']
        largest_city.drop_duplicates(inplace=True)
        largest_city = largest_city[largest_city['pop_name'] != 'Not Asked']
        largest_city.to_sql('largest_city', if_exists='append', con=engine, index=False)
        largest_city_table = pd.read_sql_query("select * from largest_city", engine)
        print(largest_city_table)


    def insert_education():
        column_education = pd.read_sql_query("select * from education", engine).columns
        education = pd.DataFrame(columns=column_education)
        education['edu_title'] = data['Education']
        education.drop_duplicates(inplace=True)
        education = education[education['edu_title'] != 'Not Asked']
        education.to_sql('education', if_exists='append', con=engine, index=False)
        education_table = pd.read_sql_query("select * from education", engine)
        print(education_table)


    def insert_looking_job():
        column_looking_job = pd.read_sql_query("select * from looking_job", engine).columns
        looking_job = pd.DataFrame(columns=column_looking_job)
        looking_job['look_job'] = data['LookingForAnotherJob']
        looking_job.drop_duplicates(inplace=True)
        looking_job = looking_job[looking_job['look_job'] != 'Not Asked']
        looking_job.to_sql('looking_job', if_exists='append', con=engine, index=False)
        looking_job_table = pd.read_sql_query("select * from looking_job", engine)
        print(looking_job_table)


    def insert_carreer_plan():
        column_carreer_plan = pd.read_sql_query("select * from carreer_plan", engine).columns
        carreer_plan = pd.DataFrame(columns=column_carreer_plan)
        carreer_plan['cap_plan'] = data['CareerPlansThisYear']
        carreer_plan.drop_duplicates(inplace=True)
        carreer_plan = carreer_plan[carreer_plan['cap_plan'] != 'Not Asked']
        carreer_plan.to_sql('carreer_plan', if_exists='append', con=engine, index=False)
        carreer_plan_table = pd.read_sql_query("select * from carreer_plan", engine)
        print(carreer_plan_table)


    def insert_certification():
        column_certification = pd.read_sql_query("select * from certification", engine).columns
        certification = pd.DataFrame(columns=column_certification)
        certification['cert_name'] = data['Certifications']
        certification.drop_duplicates(inplace=True)
        certification = certification[certification['cert_name'] != 'Not Asked']
        certification.to_sql('certification', if_exists='append', con=engine, index=False)
        certification_table = pd.read_sql_query("select * from certification", engine)
        print(certification_table)


    def insert_how_many_companies():
        column_how_many_companies = pd.read_sql_query("select * from how_many_companies;", engine).columns
        how_many_companies = pd.DataFrame(columns=column_how_many_companies)
        how_many_companies['mcp_many_companies'] = data['HowManyCompanies']
        how_many_companies.drop_duplicates(inplace=True)
        how_many_companies = how_many_companies[how_many_companies['mcp_many_companies'] != 'Not Asked']
        how_many_companies.to_sql('how_many_companies', if_exists='append', con=engine, index=False)
        how_many_companies_table = pd.read_sql_query("select * from how_many_companies;", engine)
        print(how_many_companies_table)


    def insert_employment_status():
        column_employment_status = pd.read_sql_query("select * from employment_status;", engine).columns
        employment_status = pd.DataFrame(columns=column_employment_status)
        employment_status['emp_status'] = data['EmploymentStatus']
        employment_status.drop_duplicates(inplace=True)
        employment_status.to_sql('employment_status', if_exists='append', con=engine, index=False)
        employment_status_table = pd.read_sql_query("select * from employment_status;", engine)
        print(employment_status_table)


    def insert_employment_sector():
        column_employment_sector = pd.read_sql_query("select * from employment_sector;", engine).columns
        employment_sector = pd.DataFrame(columns=column_employment_sector)
        employment_sector['sec_name'] = data['EmploymentSector']
        employment_sector.drop_duplicates(inplace=True)
        employment_sector.to_sql('employment_sector', if_exists='append', con=engine, index=False)
        employment_sector_table = pd.read_sql_query("select * from employment_sector;", engine)
        print(employment_sector_table)


    def insert_task():
        column_task = pd.read_sql_query("SELECT * FROM task", engine).columns
        task = pd.DataFrame(columns=column_task)
        task['tas_name'] = data["KindsOfTasksPerformed"]
        task.drop_duplicates(inplace=True)
        task = task[task['tas_name'] != "Not Asked"]
        a = task['tas_name'].values.tolist()
        s = ', '.join(map(str, a))
        l = s.split(', ')
        task_df = pd.DataFrame(l)
        task_df.drop_duplicates(inplace=True)
        task_df = task_df[task_df[task_df.columns[0]] != "nan"]
        task = pd.read_sql_query("SELECT * FROM task", engine)
        task['tas_name'] = task_df[0]
        task.to_sql('task', if_exists='append', con=engine, index=False)
        task_table = pd.read_sql_query("select * from task", engine)
        print(task_table)


    def insert_job():
        column_job = pd.read_sql_query("select * from job", engine).columns
        job = pd.DataFrame(columns=column_job)
        job['job_name'] = data['JobTitle']
        job.drop_duplicates(inplace=True)
        job.to_sql('job', if_exists='append', con=engine, index=False)
        job_table = pd.read_sql_query("select * from job", engine)
        print(job_table)


    def insert_db():
        column_db = pd.read_sql_query("select * from db", engine).columns
        db = pd.DataFrame(columns=column_db)
        db['db_name'] = data['PrimaryDatabase']
        db.drop_duplicates(inplace=True)
        db.to_sql('db', if_exists='append', con=engine, index=False)
        db_table = pd.read_sql_query("select * from db", engine)
        print(db_table)


    def insert_sondage_item():
        table_sondage_item = pd.read_sql_query("select * from sondage_item", engine)
        dic = {
            'timestamp': 'Timestamp',
            'salary_usd': 'SalaryUSD',
            'postal_code': 'PostalCode',
            'years_with_db': 'YearsWithThisDatabase',
            'manage_staff': 'ManageStaff',
            'years_with_job': 'YearsWithThisTypeOfJob',
            'other_people': 'OtherPeopleOnYourTeam',
            'company_employee': 'CompanyEmployeesOverall',
            'database_servers': 'DatabaseServers',
            'education_computer': 'EducationIsComputerRelated',
            'hours_worked': 'HoursWorkedPerWeek',
            'telecommute': 'TelecommuteDaysPerWeek',
            'newest_version': 'NewestVersionInProduction',
            'oldest_version': 'OldestVersionInProduction',
            'gender': 'Gender',
            "sdg_id": "Survey Year",
            "sec_id": "EmploymentSector",
            "look_id": "LookingForAnotherJob",
            "cap_id": "CareerPlansThisYear",
            "ctr_id": "Country",
            "db_id": "PrimaryDatabase",
            "emp_id": "EmploymentStatus",
            "job_id": "JobTitle",
            "mcp_id": "HowManyCompanies",
            "edu_id": "Education",
            "cert_id": "Certifications",
            "pop_id": "PopulationOfLargestCityWithin20Miles"
        }

        dic_df = {"sdg_id": pd.read_sql_query("SELECT * FROM sondage", engine),
                  "sec_id": pd.read_sql_query("SELECT * FROM employment_sector", engine),
                  "look_id": pd.read_sql_query("SELECT * FROM looking_job", engine),
                  "cap_id": pd.read_sql_query("SELECT * FROM carreer_plan", engine),
                  "ctr_id": pd.read_sql_query("SELECT * FROM country", engine),
                  "db_id": pd.read_sql_query("SELECT * FROM db", engine),
                  "emp_id": pd.read_sql_query("SELECT * FROM employment_status", engine),
                  "job_id": pd.read_sql_query("SELECT * FROM job", engine),
                  "mcp_id": pd.read_sql_query("SELECT * FROM how_many_companies", engine),
                  "edu_id": pd.read_sql_query("SELECT * FROM education", engine),
                  "cert_id": pd.read_sql_query("SELECT * FROM certification", engine),
                  "pop_id": pd.read_sql_query("SELECT * FROM largest_city", engine)}

        data['HowManyCompanies'] = data['HowManyCompanies'].apply(str)
        pd.set_option('mode.chained_assignment', None)

        def trouver_index(valeur, table):
            return int(table[table.columns[0]][(table[table.columns[1]] == valeur) == True])

        for cle, colonne in dic.items():
            print(colonne)
            if cle[-2:] == "id":
                for i in data[colonne].index:
                    print(i)
                    if data[colonne][i] == "Not Asked":
                        table_sondage_item[cle][i] = "Not Asked"
                    else:
                        table_sondage_item[cle][i] = trouver_index(data[colonne][i], dic_df[cle])
            else:
                table_sondage_item[cle] = data[colonne]

        table_sondage_item = table_sondage_item.replace("Not Asked", np.nan)
        table_sondage_item['manage_staff'] = table_sondage_item['manage_staff'].replace("Yes", 1)
        table_sondage_item['manage_staff'] = table_sondage_item['manage_staff'].replace("No", 0)

        table_sondage_item['education_computer'] = table_sondage_item['education_computer'].replace("No", 0)
        table_sondage_item['education_computer'] = table_sondage_item['education_computer'].replace("Yes", 1)

        table_sondage_item['telecommute'] = table_sondage_item['telecommute'].replace(
            "None, or less than 1 day per week", 0)

        table_sondage_item['telecommute'] = table_sondage_item['telecommute'].replace("5 or more", 5)

        table_sondage_item['other_people'] = table_sondage_item['other_people'].replace('None', 0)
        table_sondage_item['other_people'] = table_sondage_item['other_people'].replace("More than 5", 5)

        table_sondage_item.to_sql('sondage_item', if_exists='append', con=engine, index=False)
        sondage_item = pd.read_sql_query("select * from sondage_item", engine)
        print(sondage_item)


    def insert_task_performed():
        sondage_item = pd.read_sql_query("select * from sondage_item", engine)
        df_id_sondage_task = pd.DataFrame(columns=['sgi_id', 'task'])
        df_id_sondage_task['sgi_id'] = sondage_item['sgi_id']
        df_id_sondage_task['task'] = data['KindsOfTasksPerformed']
        df_id_sondage_task = df_id_sondage_task.replace(np.nan, None)
        dct_id_sondage_task = {}

        for i in range(len(df_id_sondage_task)):
            dct_id_sondage_task[df_id_sondage_task['sgi_id'][i]] = df_id_sondage_task['task'][i]

        list_tas_id = []
        list_sgi_id = []
        for key, value in dct_id_sondage_task.items():
            if value != 'Not Asked':
                print(key + 1, value)
                l = value.split(', ')
                for j in l:
                    list_sgi_id.append(key)
                    list_tas_id.append(
                        pd.read_sql_query("select tas_id from task where tas_name = '%s'" % j, engine)['tas_id'][0])

        task_performed = pd.DataFrame({'tas_id': list_tas_id, 'sgi_id': list_sgi_id})
        column_task_perfomed = pd.read_sql_query("select * from task_perfomed;", engine)
        df_task_performed = pd.DataFrame(columns=column_task_perfomed)
        df_task_performed['sgi_id'] = task_performed['sgi_id']
        df_task_performed['tas_id'] = task_performed['tas_id']

        df_task_performed.to_sql('task_perfomed', if_exists='append', con=engine, index=False)
        table_task_performed = pd.read_sql_query("select * from task_perfomed", engine)
        print(table_task_performed)


    def insert_other_duties():
        sondage_item = pd.read_sql_query("select * from sondage_item", engine)
        df_id_sondage_job = pd.DataFrame(columns=['sgi_id', 'job'])
        df_id_sondage_job['sgi_id'] = sondage_item['sgi_id']
        df_id_sondage_job['job'] = data['OtherJobDuties']
        df_id_sondage_job = df_id_sondage_job.replace(np.nan, 'Not Asked')

        dct_id_sondage_job = {}

        for i in range(len(df_id_sondage_job)):
            dct_id_sondage_job[df_id_sondage_job['sgi_id'][i]] = df_id_sondage_job['job'][i]

        list_job_id = []
        list_sgi_id = []
        for key, value in dct_id_sondage_job.items():
            if value != 'Not Asked':
                l = re.split("(,(?=[^\)]*(?:\(|$)) )", value)
                for n, i in enumerate(l):
                    if i == ', ':
                        l.remove(l[n])

                print(key + 1, value)
                for j in l:
                    list_sgi_id.append(key)
                    list_job_id.append(
                        pd.read_sql_query("select job_id from job where job_name = '%s'" % j, engine)['job_id'][0])

        other_duties = pd.DataFrame({'job_id': list_job_id, 'sgi_id': list_sgi_id})

        other_duties.to_sql('other_duties', if_exists='append', con=engine, index=False)
        table_other_duties = pd.read_sql_query("select * from other_duties", engine)
        print(table_other_duties)


    def insert_other_db():
        pd.set_option('display.width', 1000)
        sondage_item = pd.read_sql_query("select * from sondage_item", engine)
        df_id_sondage_o_db = pd.DataFrame(columns=['sgi_id', 'db'])
        df_id_sondage_o_db['sgi_id'] = sondage_item['sgi_id']
        df_id_sondage_o_db['db'] = data['OtherDatabases']

        df_id_sondage_o_db['db'] = df_id_sondage_o_db['db'].replace(np.nan, 'Not Asked')
        df_id_sondage_o_db = df_id_sondage_o_db.applymap(lambda x: x.replace('"', '') if (isinstance(x, str)) else x)
        dct_id_sondage_db = {}

        for i in range(len(df_id_sondage_o_db)):
            dct_id_sondage_db[df_id_sondage_o_db['sgi_id'][i]] = df_id_sondage_o_db['db'][i]

        list_db_id = []
        list_sgi_id = []
        for key, value in dct_id_sondage_db.items():
            if value != 'Not Asked':
                print(key + 1, value)
                l = re.split("(,(?=[^\)]*(?:\(|$)) )", value)
                for n, i in enumerate(l):
                    if i == ', ':
                        l.remove(l[n])

                for j in l:
                    list_sgi_id.append(key)
                    test = pd.read_sql_query('select db_id from db where db_name = "%s"' % j, engine)['db_id']
                    if test.empty:
                        list_db_id.append(3)
                    else:
                        list_db_id.append(test[0])

        other_db = pd.DataFrame({'db_id': list_db_id, 'sgi_id': list_sgi_id})

        other_db.to_sql('other_db', if_exists='append', con=engine, index=False)
        table_other_db = pd.read_sql_query("select * from other_db", engine)
        print(table_other_db)


    insert_country()
    insert_sondage()
    insert_largest_city()
    insert_education()
    insert_looking_job()
    insert_carreer_plan()
    insert_certification()
    insert_how_many_companies()
    insert_employment_status()
    insert_employment_sector()
    insert_task()
    insert_job()
    insert_db()
    insert_sondage_item()
    insert_task_performed()
    insert_other_duties()
    insert_other_db()
