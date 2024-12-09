import openai
import logging
from sqlalchemy import text, inspect
from pymongo import MongoClient
from app.database import get_sql_engine, get_mongo_client

# Set up OpenAI API key
openai.api_key = "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"

# Initialize SQL and MongoDB connections
sql_engine = get_sql_engine()
mongo_client = get_mongo_client()
mongo_db = mongo_client["datasets"]

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def query_llm(prompt: str):
    """Send a query to ChatGPT and get the response."""
    try:
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",  # Or 'gpt-4' for more complex tasks
            messages=[
                {"role": "system", "content": "You are an ETL assistant for inventory management datasets. Generate SQL or NoSQL queries tailored to the dataset structure. Ensure SQL queries are compatible with MSSQL."},
                {"role": "user", "content": prompt}
            ],
            temperature=0,
            max_tokens=500
        )
        return response['choices'][0]['message']['content'].strip()
    except Exception as e:
        logging.error(f"Error querying LLM: {e}")
        raise e

def analyze_sql_table(table_name: str):
    """Analyze SQL table structure and generate sample prompts."""
    queries = []
    with sql_engine.connect() as conn:
        inspector = inspect(sql_engine)
        columns = inspector.get_columns(table_name)
        column_names = [col['name'] for col in columns]

        if "brand" in column_names and "inventory_count" in column_names:
            queries.append(f"SELECT TOP 5 brand, SUM(inventory_count) AS total_inventory FROM {table_name} GROUP BY brand ORDER BY total_inventory DESC;")
        if "price" in column_names and "inventory_count" in column_names:
            queries.append(f"SELECT brand, SUM(price * inventory_count) AS total_value FROM {table_name} GROUP BY brand;")
        queries.append(f"SELECT COUNT(*) AS total_rows FROM {table_name};")
        queries.append(f"SELECT DISTINCT category FROM {table_name};")
    return queries

def analyze_mongo_collection(collection_name: str):
    """Analyze MongoDB collection structure and generate sample prompts."""
    collection = mongo_db[collection_name]
    sample = collection.find_one()
    queries = []
    if sample:
        keys = sample.keys()
        if "brand" in keys:
            queries.append(f"Find the top 5 brands by inventory count in the {collection_name} collection.")
        if "price" in keys and "inventory_count" in keys:
            queries.append(f"Find the total inventory value (price * inventory_count) for each brand in the {collection_name} collection.")
        queries.append(f"Count the total number of documents in the {collection_name} collection.")
        queries.append(f"List all distinct categories in the {collection_name} collection.")
    return queries

def generate_prompts_for_dataset(dataset_info, dataset_type):
    """Generate sample prompts for the dataset based on its type."""
    if dataset_type == "sql":
        table_name = dataset_info.get("table_name")
        return analyze_sql_table(table_name)
    elif dataset_type == "nosql":
        collection_name = dataset_info.get("collection_name")
        return analyze_mongo_collection(collection_name)
    return []


def perform_etl(prompt: str):
    """Perform ETL operations based on the LLM response."""
    try:
        # Query ChatGPT for instructions
        llm_response = query_llm(prompt)
        logging.info(f"LLM Response: {llm_response}")

        # Ensure the response is not empty
        if not llm_response:
            raise ValueError("LLM returned an empty response.")

        # Remove backticks or prefix like `sql`
        if llm_response.startswith("```") and llm_response.endswith("```"):
            llm_response = llm_response.strip("```").strip()
        if llm_response.lower().startswith("sql"):
            llm_response = llm_response[3:].strip()  # Remove the `sql` prefix

            # Execute the SQL query
            logging.info(f"Executing SQL query: {llm_response}")
            with sql_engine.connect() as conn:
                result = conn.execute(text(llm_response))
                # Process rows
                rows = [dict(row._mapping) for row in result]  # Use _mapping for row conversion
                return {"rows": rows}

    except Exception as e:
        logging.error(f"Error performing ETL: {e}")
        raise e

