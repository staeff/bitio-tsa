import bitdotio
import os
import pandas as pd
import requests

from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine

def get_tsa_data() -> pd.DataFrame:
    """Attempts to download TSA Checkpoint Throughtput data"""
    url = "https://www.tsa.gov/coronavirus/passenger-throughput"

    header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        r = requests.get(url, headers=header)
    except Exception as e:
        raise e

    dfs = pd.read_html(r.text)
    return dfs[0]


def format_tsa_data(tsa_data: pd.DataFrame) -> pd.DataFrame:
    """Formats the Data for Uploading to Database"""
    tsa_data_out = tsa_data
    tsa_data_out.columns = ["date", "year_2022", "year_2021", "year_2020", "year_2019"]
    tsa_data_out["date"] = tsa_data_out["date"].apply(
        lambda x: datetime.strptime(x, "%m/%d/%Y").date()
    )
    return tsa_data_out.astype(
        {"year_2022": float, "year_2021": float, "year_2020": float, "year_2019": float}
    )


def initialize_repo(client, repo_name="tsa_throughput"):
    """Creates the empty bit.io repository"""
    repo = bitdotio.model.repo.Repo(name=repo_name,
                                    is_private=True)
    client.create_repo(repo=repo)

def initialize_data(repo_name="tsa_throughput", table_name="tsa_throughput"):
    """Creates the repository and initial data table if they do not already exist"""
    schema = f"{os.getenv('USERNAME')}/{repo_name}"
    engine = create_engine(
        os.getenv("PG_STRING"), connect_args={"options": "-c statement_timeout=120s"}
    )
    bitio = bitdotio.bitdotio(os.getenv("BITIO_KEY"))

    if not engine.dialect.has_schema(engine.connect(), schema):
        print(f'Initializing repo {repo_name}')
        initialize_repo(bitio, repo_name=repo_name)

    if not engine.dialect.has_table(
        connection=engine.connect(), table_name=table_name, schema=schema
    ):
        print(f'Creating table {table_name}')
        tsa_table = format_tsa_data(get_tsa_data())
        tsa_table.to_sql(
            name=table_name,
            con=engine.connect(),
            schema=f"{os.getenv('USERNAME')}/{repo_name}",
            index=False,
        )

# Query most recent date in bit.IO
def get_db_most_recent(repo_name="tsa_throughput", table_name="tsa_throughput"):
    schema = f"{os.getenv('USERNAME')}/{repo_name}"

    engine = create_engine(
        os.getenv("PG_STRING"), connect_args={"options": "-c statement_timeout=120s"}
    )

    query = f"""
    SELECT date
    FROM "{schema}"."{table_name}"
    ORDER BY date DESC
    limit 1;
    """
    with engine.connect() as con:
        most_recent = con.execute(query)

    return most_recent.all()[0][0]


def get_tsa_most_recent() -> pd.DataFrame:
    """Downloads first row of TSA Data"""
    url = "https://www.tsa.gov/coronavirus/passenger-throughput"

    header = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    try:
        r = requests.get(url, headers=header)
    except Exception as e:
        raise e

    most_recent = pd.read_html(r.text, skiprows=range(2, 365))[0].loc[
        0, ["Date", "2022"]
    ]
    most_recent_date = datetime.strptime(most_recent.at["Date"], "%m/%d/%Y").date()
    most_recent_value = most_recent.at["2022"]
    return most_recent_date, most_recent_value


def update_tsa_on_bitdotio(repo_name="tsa_throughput", table_name="tsa_throughput"):
    most_recent_date, most_recent_value = get_tsa_most_recent()
    if most_recent_date > get_db_most_recent():
        schema = f"{os.getenv('USERNAME')}/{repo_name}"
        engine = create_engine(
            os.getenv("PG_STRING"), connect_args={"options": "-c statement_timeout=120s"}
        )

        query = f"""
        UPDATE "{schema}"."{table_name}"
        SET date = '{most_recent_date.strftime('%Y/%m/%d')}'
        ,
        year_2021 = {most_recent_value}
        WHERE date = '{most_recent_date.replace(2020).strftime('%Y/%m/%d')}'
        """
        with engine.connect() as con:
            con.execute(query)
    else:
        print("Data Already Up to Date!")
if __name__ == '__main__':
    load_dotenv()
    initialize_data()
    update_tsa_on_bitdotio()
