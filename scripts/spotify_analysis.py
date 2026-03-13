import os
from dotenv import load_dotenv
import oracledb
import pandas as pd
import matplotlib.pyplot as plt

# Load environment variables from .env
load_dotenv()

# Read credentials
user = os.getenv("ORACLE_USER")
password = os.getenv("ORACLE_PASS")
host = os.getenv("ORACLE_HOST")
port = os.getenv("ORACLE_PORT")
service = os.getenv("ORACLE_SERVICE")

# Create DSN 
dsn = oracledb.makedsn(host, port, service_name=service)

# Connect to Oracle
connection = oracledb.connect(
    user=user,
    password=password,
    dsn=dsn)

cursor = connection.cursor()