import os
import uvicorn


from typing import Optional
from pydantic import BaseModel
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from src.chat_assistant import ProcurementAssistant
from src.data_manager import ProcurementDataManager

load_dotenv()

app = FastAPI(
    title="Procurement Chatbot API",
    description="AI-powered chatbot for procurement data analysis",
    version="1.0.0"
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

data_manager = ProcurementDataManager()
assistant = ProcurementAssistant(data_manager.db)


class ChatMessage(BaseModel):
    message: str

class ChatResponse(BaseModel):
    success: bool
    response: str
    data: Optional[list] = None
    error: Optional[str] = None
    type: Optional[str] = None

@app.post("/load-data")
async def load_data():
    try:
        dataset_path = os.path.join('/app/dataset', 'purchase.csv')
        
        if not os.path.exists(dataset_path):
            raise HTTPException(
                status_code=404, 
                detail=f"Dataset file not found at {dataset_path}"
            )
            
        records_count = data_manager.load_dataset(dataset_path)
        
        return {
            "success": True,
            "message": f"Successfully loaded {records_count} records into database",
            "records_count": records_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/chat", response_model=ChatResponse)
async def chat(message: ChatMessage):
    try:
        result = await assistant.process_message(message.message)
        
        if not result["success"]:
            raise HTTPException(status_code=500, detail=result["error"])
            
        return ChatResponse(**result)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    return {"status": "healthy", "version": "1.0.0", "message": "Service is running"}

if __name__ == "__main__":
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)