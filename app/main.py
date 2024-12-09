from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.llm_service import perform_etl, generate_prompts_for_dataset
from app.microservice import process_dataset
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../")))

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from fastapi import FastAPI, UploadFile, HTTPException
from app.microservice import process_dataset, store_in_sql
from app.llm_service import generate_prompts_for_dataset
from fastapi.responses import JSONResponse
import logging

app = FastAPI()

@app.post("/upload/")
async def upload_dataset(file: UploadFile):
    try:
        file_ext = file.filename.split('.')[-1].lower()
        if file_ext == 'pdf':
            logging.info(f"Processing PDF file: {file.filename}")
            dataset_info = await store_in_sql(file, file_ext)
            dataset_type = "sql"
        else:
            dataset_info = await process_dataset(file)
            dataset_type = "sql" if "table_name" in dataset_info else "nosql"

        prompts = generate_prompts_for_dataset(dataset_info, dataset_type)
        return {"message": "Dataset processed successfully.", "dataset_info": dataset_info, "prompts": prompts}
    except Exception as e:
        logging.error(f"Error processing upload: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/etl/")
async def etl_endpoint(prompt:str):
    try:
        result = perform_etl(prompt)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
