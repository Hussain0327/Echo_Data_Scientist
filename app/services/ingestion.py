import uuid
from io import BytesIO
from typing import List, Optional

import pandas as pd
from fastapi import UploadFile
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.data_source import DataSource, SourceType
from app.models.schemas import DataSourceResponse, UploadResponse, ValidationError
from app.services.data_validator import DataValidator
from app.services.schema_detector import SchemaDetector


class IngestionService:
    MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB

    def __init__(self, db: AsyncSession):
        self.db = db

    async def ingest_csv(self, file: UploadFile, use_case: Optional[str] = None) -> UploadResponse:
        content = await file.read()

        if len(content) > self.MAX_FILE_SIZE:
            return self._error_response(
                file.filename,
                SourceType.CSV,
                "File too large",
                f"Maximum file size is {self.MAX_FILE_SIZE // (1024*1024)}MB",
            )

        try:
            df = pd.read_csv(BytesIO(content))
        except Exception as e:
            return self._error_response(
                file.filename,
                SourceType.CSV,
                f"Failed to parse CSV: {str(e)}",
                "Ensure file is valid CSV format",
            )

        return await self._process_dataframe(
            df=df,
            source_type=SourceType.CSV,
            file_name=file.filename,
            file_size=len(content),
            use_case=use_case,
        )

    async def ingest_excel(
        self, file: UploadFile, use_case: Optional[str] = None
    ) -> UploadResponse:
        content = await file.read()

        if len(content) > self.MAX_FILE_SIZE:
            return self._error_response(
                file.filename,
                SourceType.EXCEL,
                "File too large",
                f"Maximum file size is {self.MAX_FILE_SIZE // (1024*1024)}MB",
            )

        try:
            df = pd.read_excel(BytesIO(content))
        except Exception as e:
            return self._error_response(
                file.filename,
                SourceType.EXCEL,
                f"Failed to parse Excel: {str(e)}",
                "Ensure file is valid Excel format (.xlsx or .xls)",
            )

        return await self._process_dataframe(
            df=df,
            source_type=SourceType.EXCEL,
            file_name=file.filename,
            file_size=len(content),
            use_case=use_case,
        )

    async def _process_dataframe(
        self,
        df: pd.DataFrame,
        source_type: SourceType,
        file_name: str,
        file_size: int,
        use_case: Optional[str] = None,
    ) -> UploadResponse:
        source_id = str(uuid.uuid4())

        detector = SchemaDetector(df)
        schema_info = detector.detect()

        validator = DataValidator(df, use_case=use_case)
        validation_errors = validator.validate()

        has_errors = any(e.severity == "error" for e in validation_errors)
        has_warnings = any(e.severity == "warning" for e in validation_errors)

        if has_errors:
            status = "invalid"
            message = "File has validation errors that must be fixed"
        elif has_warnings:
            status = "valid"
            message = "File uploaded with warnings"
        else:
            status = "valid"
            message = "File uploaded and validated successfully"

        data_source = DataSource(
            id=source_id,
            user_id="default",
            source_type=source_type,
            file_name=file_name,
            file_size=file_size,
            schema_info=schema_info.model_dump(),
            validation_status=status,
            validation_errors=[e.model_dump() for e in validation_errors],
            row_count=len(df),
        )

        self.db.add(data_source)
        await self.db.commit()

        return UploadResponse(
            id=source_id,
            source_type=source_type,
            file_name=file_name,
            status=status,
            message=message,
            schema_info=schema_info,
            validation_errors=validation_errors if validation_errors else None,
        )

    def _error_response(
        self, file_name: str, source_type: SourceType, message: str, suggestion: str
    ) -> UploadResponse:
        return UploadResponse(
            id=str(uuid.uuid4()),
            source_type=source_type,
            file_name=file_name,
            status="error",
            message=message,
            validation_errors=[
                ValidationError(
                    severity="error", field="file", message=message, suggestion=suggestion
                )
            ],
        )

    async def list_sources(self, limit: int = 10) -> List[DataSourceResponse]:
        result = await self.db.execute(
            select(DataSource).order_by(DataSource.upload_timestamp.desc()).limit(limit)
        )
        sources = result.scalars().all()
        return [self._to_response(s) for s in sources]

    async def get_source(self, source_id: str) -> Optional[DataSourceResponse]:
        result = await self.db.execute(select(DataSource).where(DataSource.id == source_id))
        source = result.scalar_one_or_none()
        if source:
            return self._to_response(source)
        return None

    def _to_response(self, source: DataSource) -> DataSourceResponse:
        return DataSourceResponse(
            id=source.id,
            user_id=source.user_id,
            source_type=source.source_type,
            file_name=source.file_name,
            upload_timestamp=source.upload_timestamp,
            validation_status=source.validation_status,
            row_count=source.row_count,
            schema_info=source.schema_info,
        )
