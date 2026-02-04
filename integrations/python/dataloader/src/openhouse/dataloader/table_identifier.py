from dataclasses import dataclass


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

    def __str__(self) -> str:
        """Return the fully qualified table name."""
        base = f"{self.database}.{self.table}"
        return f"{base}.{self.branch}" if self.branch else base
