from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy, rename_func
from sqlglot.generator import Generator as _Generator
from sqlglot.parser import Parser as _Parser
from sqlglot.tokens import Tokenizer as _Tokenizer
from sqlglot.tokens import TokenType

from openhouse.dataloader.table_identifier import TableIdentifier


class DataFusion(Dialect):
    DIALECT = "datafusion"
    NORMALIZE_FUNCTIONS: bool | str = "lower"
    NORMALIZATION_STRATEGY = NormalizationStrategy.LOWERCASE
    NULL_ORDERING = "nulls_are_last"
    INDEX_OFFSET = 0
    TYPED_DIVISION = True
    SUPPORTS_USER_DEFINED_TYPES = False
    LOG_BASE_FIRST = True
    SUPPORTS_ORDER_BY_ALL = True

    class Tokenizer(_Tokenizer):
        IDENTIFIERS = ['"']
        KEYWORDS = {
            **_Tokenizer.KEYWORDS,
            "UTF8": TokenType.TEXT,
        }

    class Parser(_Parser):
        FUNCTIONS = {
            **_Parser.FUNCTIONS,
            "MAKE_ARRAY": exp.Array.from_arg_list,
            "CARDINALITY": exp.ArraySize.from_arg_list,
            "ARRAY_SORT": exp.SortArray.from_arg_list,
            "ARRAY_HAS": exp.ArrayContains.from_arg_list,
            "BOOL_AND": exp.LogicalAnd.from_arg_list,
            "BOOL_OR": exp.LogicalOr.from_arg_list,
            "BIT_AND": exp.BitwiseAndAgg.from_arg_list,
            "BIT_OR": exp.BitwiseOrAgg.from_arg_list,
            "BIT_XOR": exp.BitwiseXorAgg.from_arg_list,
            "VAR_POP": exp.VariancePop.from_arg_list,
            "VAR_SAMPLE": exp.Variance.from_arg_list,
            "STDDEV_POP": exp.StddevPop.from_arg_list,
            "COVAR_POP": exp.CovarPop.from_arg_list,
            "COVAR_SAMP": exp.CovarSamp.from_arg_list,
            "APPROX_DISTINCT": exp.ApproxDistinct.from_arg_list,
            "APPROX_PERCENTILE_CONT": exp.ApproxQuantile.from_arg_list,
            "STRING_AGG": exp.GroupConcat.from_arg_list,
            "NOW": exp.CurrentTimestamp.from_arg_list,
        }

    class Generator(_Generator):
        JOIN_HINTS = False
        TABLE_HINTS = False
        QUERY_HINTS = False
        NVL2_SUPPORTED = False
        SUPPORTS_CREATE_TABLE_LIKE = False

        TYPE_MAPPING = {
            **_Generator.TYPE_MAPPING,
            exp.DataType.Type.CHAR: "VARCHAR",
            exp.DataType.Type.NCHAR: "VARCHAR",
            exp.DataType.Type.NVARCHAR: "VARCHAR",
            exp.DataType.Type.TEXT: "VARCHAR",
            exp.DataType.Type.BINARY: "BYTEA",
            exp.DataType.Type.VARBINARY: "BYTEA",
            exp.DataType.Type.DATETIME: "TIMESTAMP",
            exp.DataType.Type.TIMESTAMPLTZ: "TIMESTAMPTZ",
            exp.DataType.Type.TIMESTAMPNTZ: "TIMESTAMP",
        }

        TRANSFORMS = {
            **_Generator.TRANSFORMS,
            # Array
            exp.Array: rename_func("make_array"),
            exp.ArraySize: rename_func("cardinality"),
            exp.SortArray: rename_func("array_sort"),
            exp.ArrayContains: rename_func("array_has"),
            # Aggregate
            exp.LogicalAnd: rename_func("bool_and"),
            exp.LogicalOr: rename_func("bool_or"),
            exp.BitwiseAndAgg: rename_func("bit_and"),
            exp.BitwiseOrAgg: rename_func("bit_or"),
            exp.BitwiseXorAgg: rename_func("bit_xor"),
            exp.VariancePop: rename_func("var_pop"),
            exp.Variance: rename_func("var_sample"),
            exp.StddevPop: rename_func("stddev_pop"),
            exp.CovarPop: rename_func("covar_pop"),
            exp.CovarSamp: rename_func("covar_samp"),
            exp.ApproxDistinct: rename_func("approx_distinct"),
            exp.ApproxQuantile: rename_func("approx_percentile_cont"),
            exp.GroupConcat: rename_func("string_agg"),
            # Datetime
            exp.CurrentTimestamp: lambda *_: "now()",
        }


def to_datafusion_sql(
    sql: str,
    source_dialect: str,
    *,
    table: TableIdentifier | None = None,
    filter_sql: str | None = None,
) -> str:
    """Transpile a SQL statement to the DataFusion dialect.

    When *table* is provided the statement is validated to reference exactly
    that table.  When *filter_sql* is also provided the table reference is
    wrapped in a filtered subquery so the predicate appears at the table scan
    level for Iceberg pushdown::

        -- input (spark dialect), table=db.tbl, filter_sql = '"a" > 10'
        SELECT foo(a), b FROM db.tbl WHERE x > 1
        -- output
        SELECT foo(a), b FROM (SELECT * FROM "db"."tbl" WHERE "a" > 10) AS tbl WHERE x > 1

    Args:
        sql: SQL statement in the source dialect.
        source_dialect: sqlglot dialect name (e.g. "spark", "postgres").
        table: Expected table the SQL must reference.  When set the function
            verifies the SQL contains exactly one table matching this
            identifier.
        filter_sql: Optional DataFusion-dialect WHERE expression to inject at
            the table scan level.  Ignored when *table* is ``None``.

    Raises:
        ValueError: If the dialect is unsupported, the SQL is invalid, the
            input contains more than one statement, or (when *table* is set)
            the table reference does not match.
    """
    if source_dialect not in Dialect.classes:
        raise ValueError(
            f"Unsupported source dialect '{source_dialect}'. Supported dialects: {', '.join(sorted(Dialect.classes))}"
        )
    if source_dialect == DataFusion.DIALECT and table is None:
        return sql

    try:
        statements = sqlglot.parse(sql, dialect=source_dialect)
    except sqlglot.errors.SqlglotError as e:
        raise ValueError(f"Failed to transpile SQL from '{source_dialect}' to DataFusion: {e}") from e
    if len(statements) != 1 or statements[0] is None:
        raise ValueError(f"Expected exactly one SQL statement, got {len(statements)}: {statements}")
    ast = statements[0]

    if table is not None:
        table_node = _find_and_validate_table(ast, table, sql)
        if filter_sql is not None:
            filter_ast = sqlglot.parse_one(filter_sql, dialect=DataFusion.DIALECT)
            inner_select = exp.Select().select(exp.Star()).from_(table_node.copy()).where(filter_ast)
            subquery = exp.Subquery(
                this=inner_select,
                alias=exp.TableAlias(this=exp.to_identifier(table_node.alias_or_name)),
            )
            table_node.replace(subquery)

    return ast.sql(dialect=DataFusion.DIALECT)


def _find_and_validate_table(ast: exp.Expression, table: TableIdentifier, original_sql: str) -> exp.Table:
    """Find the single table reference in *ast* and verify it matches *table*.

    Raises ``ValueError`` if there is not exactly one table reference or the
    referenced table name does not match.
    """
    tables = list(ast.find_all(exp.Table))
    if len(tables) != 1:
        raise ValueError(f"Transformer SQL must reference exactly 1 table, found {len(tables)} in: {original_sql}")
    table_node = tables[0]
    actual_db = table_node.db
    actual_name = table_node.name
    # TODO add OpenHouse branch validation
    if actual_db != table.database or actual_name != table.table:
        raise ValueError(
            f"Transformer SQL references {actual_db}.{actual_name}, expected {table.database}.{table.table}"
        )
    return table_node
