import pandas as pd
import numpy as np
import argparse
from sqlalchemy import create_engine


def extract_data(source):
    df = pd.read_csv(source)
    return df

def convert_age(age): # standardize the ages which are all in different units
    try:
        num = int(''.join(filter(str.isdigit, age)))
    except:
        return np.nan
    if "day" in age:
        return num/365
    elif "week" in age:
        return num / 52
    elif "month" in age:
        return num / 12
    elif "year" in age:
        return num
    else:
        print("error: ", age, "\n\n\n\n")
        return num
    


def transform_data(df):
    new_df = df.copy()
    new_df[['Month', 'Year']] = new_df.MonthYear.str.split(" ", expand=True)
    new_df['Sex'] = new_df['Sex upon Outcome'].replace("Unkown", np.nan)
    new_df['Age upon Outcome'] = new_df['Age upon Outcome'].apply(convert_age)
    new_df.drop(columns=["MonthYear", "Sex upon Outcome"], inplace=True)
   
    col_changes = {}
    for col in new_df.columns:
        new_col = col.lower().replace(" ", "_")
        col_changes[col] = new_col
    new_df = new_df.rename(columns=col_changes)
    return new_df


def load_data(df):

    db_url = "postgresql+psycopg2://lucas:lucaspassword@db:5432/shelter"
    connection = create_engine(db_url)

    animal = df[["animal_id", "name", "date_of_birth",
                 "animal_type", "breed", "color", "sex"]].drop_duplicates()
    animal.to_sql(
        "animal_dim", connection, if_exists="append", index=False)
    df[['outcome_type', "outcome_subtype", "age_upon_outcome"]].to_sql(
        "outcome_dim", connection, if_exists="append", index=False)
    df[['datetime', "month", "year"]].to_sql(
        "time_dim", connection, if_exists="append", index=False)

    animal_fk = df['animal_id']
    animal_fk = animal_fk.rename("animal_id_fk")

    outcome_fk_query = "SELECT outcome_id FROM outcome_dim"
    outcome_fk = pd.read_sql_query(outcome_fk_query, con=connection)
    outcome_fk = outcome_fk.rename(
        columns={outcome_fk.columns[0]: 'outcome_id_fk'})

    time_fk_query = "SELECT time_id FROM time_dim"
    time_fk = pd.read_sql_query(time_fk_query, con=connection)
    time_fk = time_fk.rename(columns={time_fk.columns[0]: 'time_id_fk'})

    fact_table_df = pd.concat([animal_fk, outcome_fk, time_fk], axis=1)

    fact_table_df.to_sql("outcomes_fact", con=connection,
                         if_exists="append", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help="source csv")

    args = parser.parse_args()

    print("Starting...")

    df = extract_data(args.source)

    new_df = transform_data(df)

    load_data(new_df)
    print("Finished.")
