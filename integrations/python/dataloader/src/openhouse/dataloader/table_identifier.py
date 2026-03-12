from dataclasses import dataclass


def _quote_identifier(name: str) -> str:
    """Escape a SQL identifier by doubling embedded double quotes and wrapping in double quotes."""
    return '"' + name.replace('"', '""') + '"'


@dataclass
class TableIdentifier:
    """Identifier for a table in OpenHouse

    Args:
        database: Database name
        table: Table name
        branch: Optional branch name
    """

    database: str
    table: str
    branch: str | None = None

    @property
    def sql_name(self) -> str:
        """Return the quoted DataFusion SQL identifier, e.g. ``"db"."tbl"``."""
        return f"{_quote_identifier(self.database)}.{_quote_identifier(self.table)}"

    def __str__(self) -> str:
        """Return the fully qualified table name."""
        base = f"{self.database}.{self.table}"
        return f"{base}.{self.branch}" if self.branch else base
