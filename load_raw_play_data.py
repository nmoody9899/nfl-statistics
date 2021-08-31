import psycopg2
import csv


SCHEMA = 'raw'
TABLE_PREFIX = 'play_by_play_'
CSV_PREFIX = './extract/play-by-play_data_'
YEARS = ['2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']


# Checks if a table for the specified year already exists in the database/schema
def does_table_exist(cursor_: 'psycopg2.extensions.cursor', year_: str) -> bool:
    cursor_.execute(f'''
        SELECT
            table_name
        FROM information_schema.tables
        WHERE table_schema = '{SCHEMA}'
            AND table_name = '{TABLE_PREFIX}{year_}'
    ''')

    tables = cursor_.fetchall()

    if len(tables) > 0:
        print(f"The following table already exists: {SCHEMA}.{TABLE_PREFIX}{year_}")
        return True
    else:
        print(f"The following table does not exist: {SCHEMA}.{TABLE_PREFIX}{year_}")
        return False


# Creates a table if one does not already exist for the specified year
def create_table(connection_: 'psycopg2.extensions.connection', 
                    cursor_: 'psycopg2.extensions.cursor', 
                    year_: str) -> None:
    print("Creating new table... ", end="", flush=True)

    cursor_.execute(f'''
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE_PREFIX}{year_}(
            game_id                 integer     NOT NULL,
            game_date               date        NOT NULL,
            quarter                 smallint    NOT NULL,
            minute                  smallint    NOT NULL,
            second                  smallint    NOT NULL,
            offense_team            text        NULL,
            defense_team            text        NULL,
            down                    smallint    NOT NULL,
            yards_to_go             smallint    NOT NULL,
            yard_line               smallint    NOT NULL,
            blank_one               text        NULL,
            is_first_down           smallint    NOT NULL,
            blank_two               text        NULL,
            next_score              smallint    NOT NULL,
            description             text        NOT NULL,
            team_win                smallint    NOT NULL,
            blank_three             text        NULL,
            blank_four              text        NULL,
            season_year             smallint    NOT NULL,
            yards                   smallint    NOT NULL,
            formation               text        NULL,
            play_type               text        NULL,
            is_rush                 smallint    NOT NULL,
            is_pass                 smallint    NOT NULL,
            is_incompletion         smallint    NOT NULL,
            is_touchdown            smallint    NOT NULL,
            pass_type               text        NULL,
            is_sack                 smallint    NOT NULL,
            is_challenge            smallint    NOT NULL,
            is_challenge_success    smallint    NOT NULL,
            challenger              text        NULL,
            is_measurement          smallint    NOT NULL,
            is_interception         smallint    NOT NULL,
            is_fumble               smallint    NOT NULL,
            is_penalty              smallint    NOT NULL,
            is_two_pt_conversion    smallint    NOT NULL,
            is_two_pt_conv_success  smallint    NOT NULL,
            rush_direction          text        NULL,
            yard_line_fixed         smallint    NOT NULL,
            yard_line_direction     text        NOT NULL,
            is_penalty_accepted     smallint    NOT NULL,
            penalty_team            text        NULL,
            is_no_play              smallint    NOT NULL,
            penalty_type            text        NULL,
            penalty_yards           smallint    NOT NULL
        )
    ''')

    connection_.commit()

    print("New table successfully created.")


# Loads the raw, unaltered data into a staging table
def load_raw_data(connection_: 'psycopg2.extensions.connection',
                    cursor_: 'psycopg2.extensions.cursor', 
                    year_: str) -> None:
    print("Loading data... ", end="", flush=True)

    with open(f'{CSV_PREFIX}{year_}.csv', 'r') as file:
        '''
        next(file)
        cursor_.copy_from(file, f'{TABLE_PREFIX}{year_}', sep=',')
        '''
        reader = csv.reader(file)
        next(reader)

        for row in reader:
            cursor_.execute(f'''
                INSERT INTO {SCHEMA}.{TABLE_PREFIX}{year_}
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''',
            row)

    connection_.commit()

    print(f"Data successfully loaded in {TABLE_PREFIX}{year_}")


if __name__ == "__main__":
    connection = psycopg2.connect(f"host={HOST} dbname={DBNAME} user={USER} password={PASSWORD}")
    cursor = connection.cursor()

    #for year in YEARS:
    if does_table_exist(cursor, '2013'):
        quit()

    create_table(connection, cursor, '2013')

    load_raw_data(connection, cursor, '2013')

    print("Hello World")