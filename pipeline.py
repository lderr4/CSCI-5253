import pandas as pd
import numpy as np
import argparse


def extract_data(source):
    df = pd.read_csv(source)
    return df


def transform_data(df):
    new_df = df.copy()
    new_df['is_male'] = df['Sex'].apply(lambda x: 1 if x == "male" else 0) # change 'male' and 'female' to 1 and 0
    new_df["log_SibSp"] = np.log(df["SibSp"] + 1) # apply log transform to "SibSp" variable
    new_df["last_name"] = df['Name'].apply(lambda x: x.split(",")[0]) # Extract each last name

    return new_df

def load_data(df, target):
    df.to_csv(target)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help="source csv")
    parser.add_argument('target', help="target csv")

    args = parser.parse_args()

    print("Starting...")

    df = extract_data(args.source)

    new_df = transform_data(df)

    load_data(new_df, args.target)
    print("Finished.")


