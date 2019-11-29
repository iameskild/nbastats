import psycopg2
import getpass

# get user name (already superuser in PostgreSQL)
user = getpass.getuser()
# enter password
pw = getpass.getpass()

# database host address (socket), found by:
# postgres=# \conninfo
host = '/var/run/postgresql'

# name of nba stats database
nba_db = 'nba'

with psycopg2.connect(dbname=nba_db, user=user, password=pw, host=host) as conn:
    with conn.cursor() as cur:

        league_data = '''
                        INSERT INTO league (name) VALUES ('NBA')
                        ON CONFLICT (name) DO NOTHING
                        RETURNING *;
                      '''
        cur.execute(league_data)
        print(cur.fetchall())


        conference_data = '''
                            INSERT INTO conference (league_id, name)
                            VALUES ((
                                    SELECT league_id
                                    FROM league
                                    WHERE name='NBA'
                                    ),
                                    'Western Conference'),
                                   ((
                                    SELECT league_id
                                    FROM league
                                    WHERE name='NBA'
                                    ),
                                    'Eastern Conference')
                            ON CONFLICT (name) DO NOTHING
                            RETURNING *;
                          '''
        cur.execute(conference_data)
        print(cur.fetchall())

        division_data = '''
                        INSERT INTO division (conference_id, name)
                        VALUES ((
                                SELECT conference_id
                                FROM conference
                                WHERE name='Western Conference'
                                ),
                                'Northwest Division'),
                                ((
                                SELECT conference_id
                                FROM conference
                                WHERE name='Western Conference'
                                ),
                                'Pacific Division'),
                                ((
                                SELECT conference_id
                                FROM conference
                                WHERE name='Western Conference'
                                ),
                                'Southwest Division'),
                                ((
                                SELECT conference_id
                                FROM conference
                                WHERE name='Eastern Conference'
                                ),
                                'Atlantic Division'),
                                ((
                                SELECT conference_id
                                FROM conference
                                WHERE name='Eastern Conference'
                                ),
                                'Central Division'),
                                ((
                                SELECT conference_id
                                FROM conference
                                WHERE name='Eastern Conference'
                                ),
                                'Southeast Division')
                        ON CONFLICT (name) DO NOTHING
                        RETURNING *;
                    '''

        cur.execute(division_data)
        print(cur.fetchall())    
