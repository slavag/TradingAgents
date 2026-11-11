"""The Docker build context must exclude every local credential .env file."""

from pathlib import Path


def test_dockerignore_excludes_root_and_nested_env_variants():
    ignore = (Path(__file__).resolve().parents[1] / ".dockerignore").read_text(
        encoding="utf-8"
    ).splitlines()
    rules = {line.strip() for line in ignore if line.strip() and not line.startswith("#")}
    assert ".env*" in rules
    assert "**/.env*" in rules
