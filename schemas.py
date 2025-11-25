from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict

class SearchRequest(BaseModel):
    content: str = Field(..., min_length=1)
    input_mode: Optional[str] = Field("AUTO", pattern="^(AUTO|TEXT|ID|SEQUENCE)$")
    db_scope: Optional[List[str]] = None
    filters: Optional[Dict[str, Any]] = None

class JobResponse(BaseModel):
    task_id: str
    status: str
    queue_position: int

class StatusResponse(BaseModel):
    task_id: str
    status: str
    progress: int
    queue_position: int
    search_meta: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
