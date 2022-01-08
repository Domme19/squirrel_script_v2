import pandas as pd
import numpy as np
from datetime import datetime
from connect_db import connect_to_db

# get date and time 
def get_date_time():
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")

# clean file_name and set up table
# return file name and the clean data frame
def clean_file_and_dataset():
    file = '2018_Central_Park_Squirrel_Census_-_Squirrel_Data.csv'
    new_file = file.partition('.')[0]
    table_name = new_file.lower().replace("-", "_")
    data = pd.read_csv('2018_Central_Park_Squirrel_Census_-_Squirrel_Data.csv')
    # refactor column_names
    data.columns = [x.lower().replace(" ", "_").replace(r"/", "_") for x in data.columns.to_list()]
    column_string = get_table_schema(data)
    data.dropna()
    return (table_name, data, column_string)

# load a clean dataset to the database
def master_data(conn, table_name, data, column_string):
    curs = conn.cursor()
    drop_query = "drop table if exists %s" %table_name
    create_query = "create table %s( %s )" %(table_name, column_string)
    curs.execute(drop_query)
    curs.execute(create_query)

    #load the clean csv dataset to the table
    data.to_csv('squirrel_census.csv', header=data.columns, index=False, encoding='utf-8')
    # csv_file = open("squirrel_census.csv")
    sql = "load data infile 'C://Users//Domme Mabounda//OneDrive//Desktop//Projects//squirrel_script_V3//squirrel_census.csv'\
            into table %s fields terminated by ','\
            enclosed by '\"' " %table_name

    curs.execute(sql)
    curs.close()
    print("successfully loaded into the database")


# get squirrel count dataframe
def get_squirrel_count_df(conn, df, table_name):
    sql_query = "select highlight_fur_color,count(*) from %s\
        group by highlight_fur_color" %table_name
    # convert to a dataframe
    df = pd.read_sql(sql_query, con=conn, index_col=None)
    df_transposed = df.set_index('highlight_fur_color').transpose()
    del df_transposed['highlight_fur_color']
    del df_transposed['']
    df_transposed.columns = [x.lower().replace(' ', '').replace(',', '_') for x in df_transposed.columns.to_list()]
    return df_transposed

# schema of the table
def get_table_schema(df):
    replacements = {
        'object': 'varchar(200)',
        'float64': 'varchar(200)',
        'int64': 'varchar(200)',
        'bool': 'varchar(200)'
    }
    col_string = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))
    return col_string
    
# squirrel_count table
def create_squirrel_count_table(conn, col_string):
    curs = conn.cursor()
    table_name = "squirrel_count"
    create_query = "create table if not exists %s(id int not null auto_increment,\
                    %s,\
                    date_time varchar(30) not null,\
                    primary key(id))" %(table_name, col_string)

    curs.execute(create_query)
    curs.close()
    print("squirrel_count table created")

def insert_to_squirrel_count(conn, df):
    date_time = get_date_time()
    curs = conn.cursor()
    insert_query = "insert into squirrel_count(black, black_cinnamon, black_cinnamon_white, black_white,\
        cinnamon, cinnamon_white, gray, gray_black, gray_white, white,\
        date_time) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    for row in df.itertuples(index=False):
        count_list = [str(x) for x in list(row)]
        print(count_list)
        final_values = tuple(count_list  + [date_time])
        curs.execute(insert_query, final_values)

    curs.close()
    print('insertion successful')


def execute():
    conn = connect_to_db()
    (table_name, data, column_string) = clean_file_and_dataset()
    master_data(conn, table_name, data, column_string)
    df_transposed = get_squirrel_count_df(conn, data, table_name)
    col_string = get_table_schema(df_transposed)
    create_squirrel_count_table(conn, col_string)
    insert_to_squirrel_count(conn, df_transposed)
    print("Operation done.")



if __name__ == "__main__":
    execute()