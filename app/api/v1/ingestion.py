from typing import List, Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.schemas import DataSourceResponse, UploadResponse
from app.services.ingestion import IngestionService

router = APIRouter()


@router.post("/upload/csv", response_model=UploadResponse)
async def upload_csv(
    file: UploadFile = File(...),
    use_case: Optional[str] = Query(None, description="Use case: 'revenue' or 'marketing'"),
    db: AsyncSession = Depends(get_db),
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    service = IngestionService(db)
    return await service.ingest_csv(file, use_case=use_case)


@router.post("/upload/excel", response_model=UploadResponse)
async def upload_excel(
    file: UploadFile = File(...),
    use_case: Optional[str] = Query(None, description="Use case: 'revenue' or 'marketing'"),
    db: AsyncSession = Depends(get_db),
):
    valid_extensions = (".xlsx", ".xls")
    if not file.filename.endswith(valid_extensions):
        raise HTTPException(status_code=400, detail="File must be Excel (.xlsx or .xls)")

    service = IngestionService(db)
    return await service.ingest_excel(file, use_case=use_case)


@router.get("/sources", response_model=List[DataSourceResponse])
async def list_sources(limit: int = Query(10, ge=1, le=100), db: AsyncSession = Depends(get_db)):
    service = IngestionService(db)
    return await service.list_sources(limit=limit)


@router.get("/sources/{source_id}", response_model=DataSourceResponse)
async def get_source(source_id: str, db: AsyncSession = Depends(get_db)):
    service = IngestionService(db)
    source = await service.get_source(source_id)
    if not source:
        raise HTTPException(status_code=404, detail="Source not found")
    return source
