from __future__ import annotations

import sqlglot

# Ensure the DataFusion dialect is registered by importing it.
import openhouse.dataloader.datafusion_dialect  # noqa: F401


class SparkToDataFusionSQLTranslator:
    def translate(self, sql: str) -> str:
        statements = sqlglot.transpile(sql, read="spark", write="datafusion")
        if len(statements) != 1:
            raise ValueError(f"Expected exactly one SQL statement, got {len(statements)}")
        return statements[0]
