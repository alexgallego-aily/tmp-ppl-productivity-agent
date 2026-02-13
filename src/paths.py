"""Path constants for the PPL analytics package."""

from pathlib import Path

# Package root directory (src/)
PACKAGE_ROOT = Path(__file__).resolve().parent

# Project root directory (one level up from src/)
PROJECT_ROOT = PACKAGE_ROOT.parent

# SQL queries directory
SQL_DIR = PROJECT_ROOT / "sql"
