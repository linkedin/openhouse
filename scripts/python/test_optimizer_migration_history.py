"""Exercise the migration-history CLI against real Git trees and local edits."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check_optimizer_migration_history.py")
MIGRATIONS = Path("libs/optimizer/optimizer-common/src/main/resources/db/migration")


class MigrationHistoryTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.repository = Path(self.temporary.name)
        self.git("init", "--quiet")
        self.git("config", "user.name", "Migration history test")
        self.git("config", "user.email", "migration-test@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "core.hooksPath", "/dev/null")
        self.old = "V9__initial.sql"
        self.write(self.old)
        self.commit()
        self.base = self.git("rev-parse", "HEAD").strip()

    def git(self, *args):
        return subprocess.check_output(
            ["git", "-C", str(self.repository), *args], text=True, stderr=subprocess.PIPE
        )

    def write(self, name, sql="CREATE TABLE t (id INT);\n"):
        file = self.repository / MIGRATIONS / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(sql)
        return file

    def commit(self):
        self.git("add", ".")
        self.git("commit", "--quiet", "-m", "Test migration state")

    def run_guard(self, base=None):
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--base", base or self.base],
            cwd=self.repository,
            capture_output=True,
            text=True,
        )

    def assert_rejected(self, reason, base=None):
        result = self.run_guard(base)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn(reason, result.stderr)

    def test_numeric_new_version_and_mutable_checksum_manifest(self):
        self.write("V10__add_column.sql", "ALTER TABLE t ADD COLUMN value INT;\n")
        self.write("atlas.sum", "updated checksums\n")
        result = self.run_guard()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("1 immutable migrations, 1 additions", result.stdout)

    def test_merged_migration_cannot_be_edited_even_when_manifest_changes(self):
        self.write(self.old, "DROP TABLE t;\n")
        self.write("atlas.sum", "rehashed changed SQL\n")
        self.assert_rejected("Merged migration was modified")

    def test_merged_migration_cannot_be_deleted_or_renamed(self):
        original = self.repository / MIGRATIONS / self.old
        original.unlink()
        self.assert_rejected("Merged migration was deleted or renamed")
        self.write("V10__renamed_initial.sql")
        self.assert_rejected("Merged migration was deleted or renamed")

    def test_older_and_equivalent_versions_are_rejected(self):
        for name in ("V8__late.sql", "V09_0__same_version.sql"):
            with self.subTest(name=name):
                file = self.write(name)
                self.assert_rejected("New migration must sort after")
                file.unlink()

    def test_equivalent_new_versions_collide(self):
        self.write("V10__first.sql")
        self.write("V010.0__duplicate.sql")
        self.assert_rejected("Duplicate migration version")

    def test_compare_to_current_base_not_branch_point(self):
        self.write("V11__merged_on_base.sql")
        self.commit()
        newer_base = self.git("rev-parse", "HEAD").strip()
        self.write("V10__late_branch.sql")
        self.assert_rejected("New migration must sort after", newer_base)

    def test_first_migration_can_be_added_when_base_has_none(self):
        self.git("rm", "--quiet", str(MIGRATIONS / self.old))
        self.commit()
        empty_base = self.git("rev-parse", "HEAD").strip()
        self.write("V1__first.sql")
        result = self.run_guard(empty_base)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("0 immutable migrations, 1 additions", result.stdout)

    def test_missing_base_fails_instead_of_skipping(self):
        self.assert_rejected("Base commit is unavailable", "refs/heads/not-fetched")

    def test_invalid_untracked_filename_fails(self):
        self.write("Vbad__invalid.sql")
        self.assert_rejected("Invalid versioned migration name")


if __name__ == "__main__":
    unittest.main()
