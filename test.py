# clean columns
# create sql table with all the columns name
# different type of object should be mapped
import pandas as pd
import numpy as np
from connect_db import connect_to_db

# replacement is used to map data type of the tables
replacements = {
        'object': 'varchar(400)',
        'float64': 'varchar(400)',
        'int64': 'varchar(400)',
        'bool': 'varchar(400)'
}

data = pd.read_csv('2018_Central_Park_Squirrel_Census_-_Squirrel_Data.csv')
data = data.replace(np.nan, 'none', regex=True)
table_name = 'squirrel_census_table'
data.columns = [x.lower().replace(' ','_').replace('/', '_') for x in list(data.columns)]
col_string = ", ".join("{} {}".format(n, d) for (n, d) in zip(data.columns, data.dtypes.replace(replacements)))
print(data['other_interactions'])


conn = connect_to_db()
curs = conn.cursor()
sql_query = "create table if not exists %s(%s)" %(table_name, col_string)
insert_query = '''insert into squirrel_census_table(x, y, unique_squirrel_id, hectare,
shift, date, hectare_squirrel_number, age, primary_fur_color, highlight_fur_color, combination_of_primary_and_highlight_color,
color_notes, location, above_ground_sighter_measurement, specific_location, running, chasing, climbing, eating, foraging, other_activities,
kuks, quaas, moans, tail_flags, tail_twitches, approaches, indifferent, runs_from, other_interactions, lat_long)values(%s,%s,%s,%s,%s,
%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
curs.execute(sql_query)

for row in data.itertuples(index=False):
    values = [str(x) for x in list(row)]
    curs.execute(insert_query, tuple(values))


curs.close()