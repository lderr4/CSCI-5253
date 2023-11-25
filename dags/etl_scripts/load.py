import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
import os

def load_data(table_file, table_name, key):

    # def insert_on_conflict_nothing(table, conn, keys, data_iter):
    #     data = [dict(zip(keys, row)) for row in data_iter]
    #     stmt = insert(table.table).values(data).on_conflict_do_nothing(index_elements=[key])
    #     result = conn.execute(stmt)
    #     return result.rowcount


    db_url = os.environ['DB_URL']
    conn = create_engine(db_url)
    
    pd.read_parquet(table_file).to_sql(table_name, conn, if_exists="append", index=False)
    print(f"{table_name} table loaded")

def load_fact_data(table_file, table_name):
    
    db_url = os.environ['DB_URL']
    connection = create_engine(db_url)


    animal_fk_query = "SELECT animal_id FROM animal_dim"
    animal_fk = pd.read_sql_query(animal_fk_query, con=connection)
    animal_fk = animal_fk.rename(
        columns={animal_fk.columns[0]: 'animal_id_fk'})

    outcome_fk_query = "SELECT outcome_id FROM outcome_dim"
    outcome_fk = pd.read_sql_query(outcome_fk_query, con=connection)
    outcome_fk = outcome_fk.rename(
        columns={outcome_fk.columns[0]: 'outcome_id_fk'})

    time_fk_query = "SELECT time_id FROM time_dim"
    time_fk = pd.read_sql_query(time_fk_query, con=connection)
    time_fk = time_fk.rename(columns={time_fk.columns[0]: 'time_id_fk'})

    fact_table_df = pd.concat([animal_fk, outcome_fk, time_fk], axis=1)

    fact_table_df.to_sql(table_name, con=connection,
                         if_exists="replace", index=False)

    print("fact table loaded")