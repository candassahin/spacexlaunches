from sqlalchemy import create_engine
import psycopg2

from launches import Launches
from launchpads import LaunchPads

# Connecting to PostgreSQL database.
conn_string= 'postgresql+psycopg2://postgres:107145@127.0.0.1:5432/postgres'
db = create_engine(conn_string)
conn = db.connect()
conn1 = psycopg2.connect(
    database="postgres",
  user='postgres',
  password='000000',
  host='127.0.0.1',
  port= '5432')
conn1.autocommit = True
cursor = conn1.cursor()

# Creating the database schema.
cursor.execute('DROP SCHEMA public CASCADE;')
cursor.execute('CREATE SCHEMA public;')

launchpads = LaunchPads()

# Creating the launchpads table.
df_table_launchpads = launchpads.df_table_launchpads
sql = '''
CREATE TABLE launchpads
(id int,
 launchpad_service_id varchar(100),
 name varchar(100),
 full_name varchar(100),
 locality varchar(100),
 region varchar(100),
 latitude decimal,
 longitude decimal,
 launch_attempts int,
 launch_successes int,
 timezone varchar(100),
 status varchar(100),
 details varchar(100),
 image varchar(100)
 );
'''
cursor.execute(sql)
df_table_launchpads.to_sql('launchpads', conn,index= False, if_exists= 'replace')

sql = '''
ALTER TABLE public.launchpads ADD CONSTRAINT launchpads_pk PRIMARY KEY (id);
'''
cursor.execute(sql)


