"""Shiv preamble: runs integration tests then exits.

This runs after shiv extracts and configures sys.path with all
bundled dependencies, so all imports (pyarrow, datafusion, etc.)
are available.
"""

import runpy
import sys

runpy.run_path("/app/tests/integration_tests.py", run_name="__main__")
sys.exit(0)
