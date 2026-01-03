import io
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, File, HTTPException, Query, UploadFile
from pydantic import BaseModel
from sqlalchemy import desc, select

from app.core.database import get_db
from app.models.report import Report, ReportStatus, ReportType
from app.services.reports.generator import get_report_generator
from app.services.reports.templates import get_template, list_templates

router = APIRouter()


class GenerateReportRequest(BaseModel):
    template_type: str
    user_id: Optional[str] = "default"


class ReportResponse(BaseModel):
    report_id: str
    template_type: str
    status: str
    generated_at: datetime
    metrics: Dict[str, Any]
    narratives: Dict[str, str]
    metadata: Dict[str, Any]


class ReportListItem(BaseModel):
    report_id: str
    template_type: str
    status: str
    generated_at: datetime
    user_id: str


class TemplateInfo(BaseModel):
    type: str
    name: str
    description: str
    required_metrics: List[str]
    required_columns: List[str]


@router.get("/templates", response_model=List[Dict[str, str]])
async def get_templates():
    return list_templates()


@router.get("/templates/{template_type}", response_model=TemplateInfo)
async def get_template_info(template_type: str):
    try:
        template = get_template(template_type)
        return TemplateInfo(
            type=template.template_type,
            name=template.display_name,
            description=template.description,
            required_metrics=template.required_metrics,
            required_columns=template.required_columns,
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/generate", response_model=ReportResponse)
async def generate_report(
    template_type: str = Query(..., description="Report template type"),
    user_id: str = Query("default", description="User ID"),
    file: UploadFile = File(..., description="CSV file with your data"),
):
    try:
        contents = await file.read()
        df = pd.read_csv(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read CSV file: {str(e)}")

    if df.empty:
        raise HTTPException(status_code=400, detail="CSV file is empty")

    generator = get_report_generator()

    try:
        report = await generator.generate(df=df, template_type=template_type, user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Report generation failed: {str(e)}")

    db_report = Report(
        id=report.report_id,
        user_id=user_id,
        report_type=ReportType(template_type),
        status=ReportStatus.COMPLETED,
        generated_at=report.generated_at,
        metrics=report.metrics,
        narratives=report.narratives,
        metadata_=report.metadata,
    )

    async for session in get_db():
        session.add(db_report)
        await session.commit()
        await session.refresh(db_report)
        break

    return ReportResponse(
        report_id=report.report_id,
        template_type=report.template_type,
        status="completed",
        generated_at=report.generated_at,
        metrics=report.metrics,
        narratives=report.narratives,
        metadata=report.metadata,
    )


@router.get("", response_model=List[ReportListItem])
async def list_reports(
    user_id: str = Query("default", description="User ID"),
    limit: int = Query(10, description="Max reports to return"),
):
    async for session in get_db():
        result = await session.execute(
            select(Report)
            .where(Report.user_id == user_id)
            .order_by(desc(Report.generated_at))
            .limit(limit)
        )
        reports = result.scalars().all()

        return [
            ReportListItem(
                report_id=r.id,
                template_type=r.report_type.value,
                status=r.status.value,
                generated_at=r.generated_at,
                user_id=r.user_id,
            )
            for r in reports
        ]


@router.get("/{report_id}", response_model=ReportResponse)
async def get_report(report_id: str):
    async for session in get_db():
        result = await session.execute(select(Report).where(Report.id == report_id))
        report = result.scalar_one_or_none()

        if not report:
            raise HTTPException(status_code=404, detail="Report not found")

        return ReportResponse(
            report_id=report.id,
            template_type=report.report_type.value,
            status=report.status.value,
            generated_at=report.generated_at,
            metrics=report.metrics or {},
            narratives=report.narratives or {},
            metadata=report.metadata_ or {},
        )


@router.delete("/{report_id}")
async def delete_report(report_id: str):
    async for session in get_db():
        result = await session.execute(select(Report).where(Report.id == report_id))
        report = result.scalar_one_or_none()

        if not report:
            raise HTTPException(status_code=404, detail="Report not found")

        await session.delete(report)
        await session.commit()

        return {"message": "Report deleted successfully", "report_id": report_id}
