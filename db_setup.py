import psycopg2
import getpass

# get user name (already superuser in PostgreSQL)
user = getpass.getuser()
# enter password
pw = getpass.getpass()

# database host address (socket), found by:
# postgres=# \conninfo
host = '/var/run/postgresql'

# name of new database
nba_db = 'nba'

# function to determine if the table or database already exists in the PostgreSQL
def exists(name, cursor, type='table'):
    if type == 'table':
        e = '''
            SELECT EXISTS
            (
                SELECT relname FROM pg_catalog.pg_class
                WHERE relname = '{}'
            );
            '''.format(name)
    elif type == 'database':
        e = '''
            SELECT EXISTS
            (
                SELECT datname FROM pg_catalog.pg_database
                WHERE datname = '{}'
            );
            '''.format(name)
    else:
        print('"type" paramater must be either "table" (default) or "database"')

    cursor.execute(e)
    if cursor.fetchone()[0]:
        print('{t} "{n}" exists...'.format(t=type, n=name))
        return True
    else:
        print('{t} "{n}" does NOT exists...'.format(t=type, n=name))
        return False


# connect to the database with the same name as 'user'
with psycopg2.connect(user=user, password=pw, host=host) as conn:
    with conn.cursor() as cur:

        # autocommit on to execute commands requiring to be run outside a transaction
        # such as CREATE DATABASE
        conn.autocommit = True

        # check to see if the database already exists
        # replacement for 'CREATE DATABASE IF NOT EXISTS...' (unavailable in PostgreSQL)
        if exists(nba_db, cur, 'database'):
            pass
        else:
            print('database "{}" is being created...'.format(nba_db))
            create_db = '''
                        CREATE DATABASE {db} OWNER {user};
                        '''.format(db=nba_db, user=user)
            cur.execute(create_db)
            print('database "{}" exists...'.format(nba_db))


# connects to the newly created 'nba' database
with psycopg2.connect(dbname=nba_db, user=user, password=pw, host=host) as conn_nba:
    with conn_nba.cursor() as cur_nba:

        conn_nba.autocommit = True

        # create tables without foreign keys
        # (see database schema diagram for table details)

        # LEAGUE
        league_table = 'league'
        create_league = '''
                        CREATE TABLE IF NOT EXISTS {}
                        (
                            league_id SERIAL PRIMARY KEY,
                            name TEXT
                        );
                        '''.format(league_table)
        cur_nba.execute(create_league)
        # verify table was created
        exists(league_table, cur_nba)

        # FRANCHISE
        franchise_table = 'franchise'
        create_franchise = '''
                            CREATE TABLE IF NOT EXISTS {}
                            (
                                franchise_id SERIAL PRIMARY KEY,
                                name TEXT
                            );
                            '''.format(franchise_table)
        cur_nba.execute(create_franchise)
        # verify table was created
        exists(franchise_table, cur_nba)

        # STADIUM
        stadium_table = 'stadium'
        create_stadium = '''
                            CREATE TABLE IF NOT EXISTS {}
                            (
                                stadium_id SERIAL PRIMARY KEY,
                                city TEXT,
                                state TEXT,
                                country TEXT,
                                name TEXT
                            );
                            '''.format(stadium_table)
        cur_nba.execute(create_stadium)
        # verify table was created
        exists(stadium_table, cur_nba)

        # PERSON INFO
        person_info_table = 'person_info'
        create_person_info = '''
                             CREATE TABLE IF NOT EXISTS {}
                             (
                                 person_id SERIAL PRIMARY KEY,
                                 first_name TEXT,
                                 last_name TEXT,
                                 date_of_birth DATE
                             );
                             '''.format(person_info_table)
        cur_nba.execute(create_person_info)
        # verify table was created
        exists(person_info_table, cur_nba)

        # create tables with foreign keys
        # (see database schema diagram for table details)

        # CONFERENCE
        conference_table = 'conference'
        create_conference = '''
                            CREATE TABLE IF NOT EXISTS {}
                            (
                                conference_id SERIAL PRIMARY KEY,
                                league_id INTEGER REFERENCES league(league_id),
                                name TEXT
                            );
                            '''.format(conference_table)
        cur_nba.execute(create_conference)
        # verify table was created
        exists(conference_table, cur_nba)

        # DIVISION
        division_table = 'division'
        create_division = '''
                          CREATE TABLE IF NOT EXISTS {}
                          (
                              division_id SERIAL PRIMARY KEY,
                              conference_id INTEGER REFERENCES conference(conference_id),
                              name TEXT
                          );
                          '''.format(division_table)
        cur_nba.execute(create_division)
        # verify table was created
        exists(division_table, cur_nba)

        # TEAM
        team_table = 'team'
        create_team = '''
                      CREATE TABLE IF NOT EXISTS {}
                      (
                            team_id SERIAL PRIMARY KEY,
                            division_id INTEGER REFERENCES division(division_id),
                            franchise_id INTEGER REFERENCES franchise(franchise_id),
                            city TEXT,
                            name TEXT
                      );
                      '''.format(team_table)
        cur_nba.execute(create_team)
        # verify table was created
        exists(team_table, cur_nba)

        # GAME
        game_table = 'game'
        create_game = '''
                      CREATE TABLE IF NOT EXISTS {}
                      (
                            game_id SERIAL PRIMARY KEY,
                            game_date DATE,
                            stadium_id INTEGER REFERENCES stadium(stadium_id),
                            length_min INTEGER,
                            completion BOOLEAN,
                            notes TEXT
                      );
                      '''.format(game_table)
        cur_nba.execute(create_game)
        # verify table was created
        exists(game_table, cur_nba)

        # TEAM APPEARANCE
        team_appearance_table = 'team_appearance'
        create_team_appearance = '''
                                 CREATE TABLE IF NOT EXISTS {}
                                 (
                                     team_id INTEGER,
                                     game_id INTEGER,
                                     home BOOLEAN,
                                     PRIMARY KEY (team_id, game_id),
                                     FOREIGN KEY (team_id) REFERENCES team(team_id),
                                     FOREIGN KEY (game_id) REFERENCES game(game_id)
                                 );
                                 '''.format(team_appearance_table)
        cur_nba.execute(create_team_appearance)
        # verify table was created
        exists(team_appearance_table, cur_nba)

        # PERSON APPEARANCE
        person_appearance_table = 'person_appearance'
        create_person_appearance = '''
                                   CREATE TABLE IF NOT EXISTS {}
                                   (
                                       apperance_id SERIAL PRIMARY KEY,
                                       team_id INTEGER REFERENCES team(team_id),
                                       game_id INTEGER REFERENCES game(game_id),
                                       person_id INTEGER REFERENCES person_info(person_id)
                                 );
                                 '''.format(person_appearance_table)
        cur_nba.execute(create_person_appearance)
        # verify table was created
        exists(person_appearance_table, cur_nba)

        # PLAYER_GAME_STATS
        player_game_stats_table = 'person_game_stats'
        create_player_game_stats = '''
                                   CREATE TABLE IF NOT EXISTS {}
                                   (
                                       person_id INTEGER,
                                       game_id INTEGER,
                                       start BOOLEAN,
                                       MP INTEGER,
                                       FG INTEGER,
                                       FGA INTEGER,
                                       THRP INTEGER,
                                       THRPA INTEGER,
                                       FT INTEGER,
                                       FTA INTEGER,
                                       ORB INTEGER,
                                       DRB INTEGER,
                                       AST INTEGER,
                                       STL INTEGER,
                                       BLK INTEGER,
                                       TOV INTEGER,
                                       PF INTEGER,
                                       PTS INTEGER,
                                       BPM INTEGER,
                                       PRIMARY KEY (person_id, game_id),
                                       FOREIGN KEY (person_id) REFERENCES person_info(person_id),
                                       FOREIGN KEY (game_id) REFERENCES game(game_id)
                                  );
                                  '''.format(player_game_stats_table)
        cur_nba.execute(create_player_game_stats)
        # verify table was created
        exists(player_game_stats_table, cur_nba)
