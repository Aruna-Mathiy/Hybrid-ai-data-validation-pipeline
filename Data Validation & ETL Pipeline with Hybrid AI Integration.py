import pandas as pd
import logging
import os
from openai import OpenAI
from sqlalchemy import create_engine

#Configuration

OPENAI_API = os.getenv("your_key_here")

SNOWFLAKE_CONFIG = {
    "user": "YOUR_USER",
    "password": "YOUR_PASSWORD",
    "account": "YOUR_ACCOUNT",
    "warehouse": "YOUR_WAREHOUSE",
    "database": "YOUR_DATABASE",
    "schema": "PUBLIC",
    "role": "YOUR_ROLE"
}

client = OpenAI(api_code="your key here")

#Logging Details

logging.basicConfig(
    filename="validation_pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

#Clean the data
def clean_data(df):
    df = df.copy()

    string_cols = df.select_dtypes(include="object").columns
    df[string_cols] = df[string_cols].apply(lambda col: col.str.strip())

    df.replace("", pd.NA, inplace=True)

    if "name" in df.columns:
        df["name"] = df["name"].str.title()

    if "email" in df.columns:
        df["email"] = df["email"].str.replace(" ", "", regex=False)

    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce")

    if "salary" in df.columns:
        df["salary"] = pd.to_numeric(df["salary"], errors="coerce")

    if "joining_date" in df.columns:
        df["joining_date"] = pd.to_datetime(df["joining_date"], errors="coerce")

    return df

#Basic Validation
def basic_validation(row):
    results = []

    # ID check
    results.append("ID VALID" if pd.notna(row["employee_id"]) else "ID INVALID")

    # Name check
    results.append("Name VALID" if pd.notna(row["name"]) else "Name INVALID")

    # Email check
    if pd.notna(row["email"]) and "@" in str(row["email"]):
        results.append("Email VALID")
    else:
        results.append("Email INVALID")

    # Age check
    if pd.notna(row["age"]) and row["age"] > 18:
        results.append("Age VALID")
    else:
        results.append("Age INVALID")

    # Salary check
    if pd.notna(row["salary"]) and row["salary"] > 0:
        results.append("Salary VALID")
    else:
        results.append("Salary INVALID")

    # Phone check (simple)
    if pd.notna(row["phone_number"]) and len(str(row["phone_number"])) >= 10:
        results.append("Phone VALID")
    else:
        results.append("Phone INVALID")

    return results

#LLM Validation

LLM_RULES = [
    "Check if name looks realistic",
    "Check if email looks suspicious",
    "Check if comment is meaningful and professional"
]

def llm_validation(row):
    row_text = f"""
    Name: {row['name']}
    Email: {row['email']}
    Comments: {row['comments']}
    """

    results = []

    for rule in LLM_RULES:
        prompt = f"""
        Validate the following data:

        {row_text}

        Rule: {rule}

        Reply strictly:
        VALID - reason
        or
        INVALID - reason
        """

        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}]
            )

            result = resp.choices[0].message.content.strip()
            results.append(f"{rule} → {result}")

        except Exception as e:
            logging.error(f"LLM error: {e}")
            results.append(f"{rule} → ERROR")

    return results

#Hybrid Validation

def hybrid_validation(row):
    basic_results = basic_validation(row)

    # Only run LLM if basic checks pass
    if all("VALID" in r for r in basic_results):
        llm_results = llm_validation(row)
    else:
        llm_results = ["LLM skipped"]

    return " | ".join(basic_results + llm_results)

#Snowflake Connection

def get_snowflake_engine():
    conn_str = (
        f"snowflake://{SNOWFLAKE_CONFIG['user']}:{SNOWFLAKE_CONFIG['password']}"
        f"@{SNOWFLAKE_CONFIG['account']}/{SNOWFLAKE_CONFIG['database']}/{SNOWFLAKE_CONFIG['schema']}"
        f"?warehouse={SNOWFLAKE_CONFIG['warehouse']}&role={SNOWFLAKE_CONFIG['role']}"
    )
    return create_engine(conn_str)

#Loading the data to Snowflake
def load_to_snowflake(df, table_name="validated_data"):
    engine = get_snowflake_engine()
    df.to_sql(table_name, engine, index=False, if_exists="replace", method="multi")
    return engine

#Creating Star Schema
def run_post_sql(engine):
    queries = [

        # DIM DEPARTMENT
        """CREATE OR REPLACE TABLE dim_department AS
           SELECT DISTINCT 
               ROW_NUMBER() OVER (ORDER BY department) AS department_id,
               department
           FROM validated_data""",

        # FACT TABLE
        """CREATE OR REPLACE TABLE fact_employee_data AS
           SELECT 
               employee_id,
               age,
               salary,
               validation_result,
               CURRENT_TIMESTAMP() AS load_timestamp,
               d.department_id
           FROM validated_data v
           LEFT JOIN dim_department d ON v.department = d.department""",

        # MATERIALIZED VIEW
        """CREATE OR REPLACE MATERIALIZED VIEW mv_validation_summary AS
           SELECT 
               validation_result,
               COUNT(*) AS total_records
           FROM fact_employee_data
           GROUP BY validation_result"""
    ]

    with engine.connect() as conn:
        for q in queries:
            conn.execute(q)

#Pipeline Stars Here
def run_pipeline(df):
    logging.info("Pipeline started")

    df = clean_data(df)

    df["validation_result"] = df.apply(hybrid_validation, axis=1)

    engine = load_to_snowflake(df)

    run_post_sql(engine)

    logging.info("Pipeline completed")
    git
    init
    return df

# Sample Data Used
data = [
    {
        "employee_id": 101,
        "name": "John",
        "email": "john@email.com",
        "age": 25,
        "department": "Analytics",
        "salary": 70000,
        "joining_date": "2022-01-15",
        "phone_number": "1234567890",
        "comments": "Strong analytical and communication skills"
    },
    {
        "employee_id": 102,
        "name": "Sarah",
        "email": "",
        "age": 17,
        "department": "HR",
        "salary": -5000,
        "joining_date": "2023-05-10",
        "phone_number": "123",
        "comments": "Good"
    }
]

df = pd.DataFrame(data)

# RUN PIPELINE
result = run_pipeline(df)
print(result)

