from sqlalchemy import create_engine
import psycopg2

from launches import Launches
from launchpads import LaunchPads


def build_database():
    # Connecting to PostgreSQL database.
    conn_string = 'postgresql+psycopg2://postgres:107145@127.0.0.1:5432/postgres'
    db = create_engine(conn_string)
    conn = db.connect()
    conn1 = psycopg2.connect(
        database="postgres",
        user='postgres',
        password='000000',
        host='127.0.0.1',
        port='5432')
    conn1.autocommit = True
    cursor = conn1.cursor()

    # Creating the database schema.
    cursor.execute('DROP SCHEMA public CASCADE;')
    cursor.execute('CREATE SCHEMA public;')

    launchpads = LaunchPads()
    launches = Launches()

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
    df_table_launchpads.to_sql('launchpads', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.launchpads ADD CONSTRAINT launchpads_pk PRIMARY KEY (id);
    '''
    cursor.execute(sql)

    # Creating the launchpad_rockets table.
    df_table_launchpad_rockets = launchpads.df_table_launchpad_rockets
    sql = '''
    CREATE TABLE launchpad_rockets
    (id int,
     launchpad_id int,
     rocket varchar(100)
     );
    '''
    cursor.execute(sql)
    df_table_launchpad_rockets.to_sql('launchpad_rockets', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.launchpad_rockets ADD CONSTRAINT launchpad_rockets_pk PRIMARY KEY (id),
    ADD CONSTRAINT launchpad_id_fk FOREIGN KEY (launchpad_id) REFERENCES launchpads(id) ON UPDATE CASCADE ON DELETE CASCADE;
    '''
    cursor.execute(sql)

    # Creating the launches table.
    df_table_launches = launches.df_table_launches
    sql = '''
    CREATE TABLE launches
    (id int,
     launch_service_id varchar(100),
     static_fire_date_utc date,
     static_fire_date_unix decimal,
     net bool,
     window_ int,
     rocket varchar(100),
     success bool,
     details varchar(100),
     launchpad varchar(100),
     flight_number int,
     name varchar(100),
     date_utc date,
     date_unix int,
     date_local date,
     date_precision varchar(100),
     upcoming bool,
     auto_update bool,
     launch_library_id varchar(100) 
     );
    '''
    cursor.execute(sql)
    df_table_launches.to_sql('launches', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.launches ADD CONSTRAINT launches_pk PRIMARY KEY (id);
    '''
    cursor.execute(sql)

    # Creating the flickr_links table.
    df_table_flickr_links = launches.df_table_flickr_links
    sql = '''
    CREATE TABLE flickr_links
    (id int,
     launch_id int,
     link varchar(100),
     type varchar(100)
     );
    '''
    cursor.execute(sql)
    df_table_flickr_links.to_sql('flickr_links', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.flickr_links ADD CONSTRAINT flickr_links_pk PRIMARY KEY (id),
    ADD CONSTRAINT launch_id_fk FOREIGN KEY (launch_id) REFERENCES launches(id) ON UPDATE CASCADE ON DELETE CASCADE;'''
    cursor.execute(sql)

    # Creating the launch_links table.
    df_table_launch_links = launches.df_table_launch_links
    sql = '''
    CREATE TABLE launch_links
    (id int,
     launch_id int,
     small_patch varchar(100),
     large_patch varchar(100),
     reddit_campaign varchar(100),
     reddit_launch varchar(100),
     reddit_media varchar(100),
     reddit_recovery varchar(100),
     presskit varchar(100),
     webcast varchar(100),
     youtube_id varchar(100),
     article varchar(100),
     wikipedia varchar(100)
        
     );
    '''
    cursor.execute(sql)
    df_table_launch_links.to_sql('launch_links', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.launch_links ADD CONSTRAINT launch_links_pk PRIMARY KEY (id),
    ADD CONSTRAINT launch_id_fk FOREIGN KEY (launch_id) REFERENCES launches(id) ON UPDATE CASCADE ON DELETE CASCADE;'''
    cursor.execute(sql)

    # Creating the fairing_details table.
    df_table_fairing_details = launches.df_table_fairing_details
    sql = '''
    CREATE TABLE fairing_details
    (id int,
     launch_id int,
     reused bool,
     recovery_attempt bool,
     recovered bool
    
     );
    '''
    cursor.execute(sql)
    df_table_fairing_details.to_sql('fairing_details', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.fairing_details ADD CONSTRAINT fairing_details_pk PRIMARY KEY (id),
    ADD CONSTRAINT launch_id_fk FOREIGN KEY (launch_id) REFERENCES launches(id) ON UPDATE CASCADE ON DELETE CASCADE;'''
    cursor.execute(sql)

    # Creating the fairing_ships table.
    df_table_fairing_ships = launches.df_table_fairing_ships
    sql = '''
    CREATE TABLE fairing_ships
    (id int,
     launch_id int,
     ship varchar(100)
    
     );
    '''
    cursor.execute(sql)
    df_table_fairing_ships.to_sql('fairing_ships', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.fairing_ships ADD CONSTRAINT fairing_ships_pk PRIMARY KEY (id),
    ADD CONSTRAINT launch_id_fk FOREIGN KEY (launch_id) REFERENCES launches(id) ON UPDATE CASCADE ON DELETE CASCADE;'''
    cursor.execute(sql)

    # Creating the launch_cores table.
    df_table_launch_cores = launches.df_table_launch_cores
    sql = '''
    CREATE TABLE launch_cores
    (id int,
     launch_id int,
     core varchar(100),
     flight int,
     gridfins bool,
     legs bool,
     reused bool,
     landing_attempt bool,
     landing_success bool,
     landing_type varchar(50),
     landpad varchar(100)
    
     );
    '''
    cursor.execute(sql)
    df_table_launch_cores.to_sql('launch_cores', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.launch_cores ADD CONSTRAINT launch_cores_pk PRIMARY KEY (id),
    ADD CONSTRAINT launch_id_fk FOREIGN KEY (launch_id) REFERENCES launches(id) ON UPDATE CASCADE ON DELETE CASCADE;'''
    cursor.execute(sql)

    # Creating the launch_payloads table.
    df_table_launch_payloads = launches.df_table_launch_payloads
    sql = '''
    CREATE TABLE launch_payloads
    (id int,
     launch_id int,
     payload varchar(100)
    
     );
    '''
    cursor.execute(sql)
    df_table_launch_payloads.to_sql('launch_payloads', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.launch_payloads ADD CONSTRAINT launch_payloads_pk PRIMARY KEY (id),
    ADD CONSTRAINT launch_id_fk FOREIGN KEY (launch_id) REFERENCES launches(id) ON UPDATE CASCADE ON DELETE CASCADE;'''
    cursor.execute(sql)

    # Creating the launch_capsules table.
    df_table_launch_capsules = launches.df_table_launch_capsules
    sql = '''
    CREATE TABLE launch_capsules
    (id int,
     launch_id int,
     capsule varchar(100)
    
     );
    '''
    cursor.execute(sql)
    df_table_launch_capsules.to_sql('launch_capsules', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.launch_capsules ADD CONSTRAINT launch_capsules_pk PRIMARY KEY (id),
    ADD CONSTRAINT launch_id_fk FOREIGN KEY (launch_id) REFERENCES launches(id) ON UPDATE CASCADE ON DELETE CASCADE;'''
    cursor.execute(sql)

    # Creating the launch_ships table.
    df_table_launch_ships = launches.df_table_launch_ships
    sql = '''
    CREATE TABLE launch_ships
    (id int,
     launch_id int,
     ship varchar(100)
    
     );
    '''
    cursor.execute(sql)
    df_table_launch_ships.to_sql('launch_ships', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.launch_ships ADD CONSTRAINT launch_ships_pk PRIMARY KEY (id),
    ADD CONSTRAINT launch_id_fk FOREIGN KEY (launch_id) REFERENCES launches(id) ON UPDATE CASCADE ON DELETE CASCADE;'''
    cursor.execute(sql)

    # Creating the launch_crew table.
    df_table_launch_crew = launches.df_table_launch_crew
    sql = '''
    CREATE TABLE launch_crew
    (id int,
     launch_id int,
     crew varchar(100),
     role varchar(100)
    
     );
    '''
    cursor.execute(sql)
    df_table_launch_crew.to_sql('launch_crew', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.launch_crew ADD CONSTRAINT launch_crew_pk PRIMARY KEY (id),
    ADD CONSTRAINT launch_id_fk FOREIGN KEY (launch_id) REFERENCES launches(id) ON UPDATE CASCADE ON DELETE CASCADE;'''
    cursor.execute(sql)

    # Creating the launch_failures table.
    df_table_launch_failures = launches.df_table_launch_failures
    sql = '''
    CREATE TABLE launch_failures
    (id int,
     launch_id int,
     time int,
     altitude int,
     reason varchar(100)
    
     );
    '''
    cursor.execute(sql)
    df_table_launch_failures.to_sql('launch_failures', conn, index=False, if_exists='replace')

    sql = '''
    ALTER TABLE public.launch_failures ADD CONSTRAINT launch_failures_pk PRIMARY KEY (id),
    ADD CONSTRAINT launch_id_fk FOREIGN KEY (launch_id) REFERENCES launches(id) ON UPDATE CASCADE ON DELETE CASCADE;'''
    cursor.execute(sql)


if __name__ == '__main__':
    build_database()
