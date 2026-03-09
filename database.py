import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()


def get_engine():
    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "3306")
    name     = os.getenv("DB_NAME", "bcrm")
    user     = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}?charset=utf8mb4"
    return create_engine(url)


@st.cache_data(ttl=300, show_spinner="Loading data...")
def fetch_data(sql: str) -> pd.DataFrame:
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()
