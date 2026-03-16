from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect, NormalizationStrategy, rename_func
from sqlglot.generator import Generator as _Generator
from sqlglot.parser import Parser as _Parser
from sqlglot.tokens import Tokenizer as _Tokenizer
from sqlglot.tokens import TokenType


class DataFusion(Dialect):
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
            "APPROX_MEDIAN": exp.Median.from_arg_list,
            "APPROX_PERCENTILE_CONT": exp.PercentileCont.from_arg_list,
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
            exp.Median: rename_func("approx_median"),
            exp.PercentileCont: rename_func("approx_percentile_cont"),
            exp.GroupConcat: rename_func("string_agg"),
            # Datetime
            exp.CurrentTimestamp: lambda *_: "now()",
        }


def to_datafusion_sql(sql: str, source_dialect: str) -> str:
    if source_dialect not in Dialect.classes:
        raise ValueError(
            f"Unsupported source dialect '{source_dialect}'. Supported dialects: {', '.join(sorted(Dialect.classes))}"
        )
    if source_dialect == "datafusion":
        return sql
    try:
        statements = sqlglot.transpile(sql, read=source_dialect, write="datafusion")
    except sqlglot.errors.SqlglotError as e:
        raise ValueError(
            f"Failed to transpile SQL from '{source_dialect}' to DataFusion: {e}"
        ) from e
    if len(statements) != 1:
        raise ValueError(f"Expected exactly one SQL statement, got {len(statements)}: {statements}")
    return statements[0]
