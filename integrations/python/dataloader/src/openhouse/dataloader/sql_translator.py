from __future__ import annotations

import sqlglot
from sqlglot.dialects.dialect import Dialect

# Ensure the DataFusion dialect is registered by importing it.
import openhouse.dataloader.datafusion_dialect  # noqa: F401

SUPPORTED_SOURCE_DIALECTS = sorted(Dialect.classes)


class DataFusionSQLTranslator:
    def __init__(self, source_dialect: str) -> None:
        if source_dialect not in Dialect.classes:
            raise ValueError(
                f"Unsupported source dialect '{source_dialect}'. "
                f"Supported dialects: {', '.join(SUPPORTED_SOURCE_DIALECTS)}"
            )
        self.source_dialect = source_dialect

    def translate(self, sql: str) -> str:
        statements = sqlglot.transpile(sql, read=self.source_dialect, write="datafusion")
        if len(statements) != 1:
            raise ValueError(f"Expected exactly one SQL statement, got {len(statements)}")
        return statements[0]
