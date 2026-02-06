import pytest
import yaml
from pathlib import Path

def test_yaml_syntax():
    """Validates that all YAML files in the repository have valid syntax."""
    repo_root = Path(__file__).resolve().parents[1]
    yaml_files = list(repo_root.rglob("*.yml")) + list(repo_root.rglob("*.yaml"))
    
    for yaml_file in yaml_files:
        # Skip hidden directories like .git, .mypy_cache, etc.
        if any(part.startswith(".") and part != ".github" for part in yaml_file.parts):
            continue
            
        with open(yaml_file, "r", encoding="utf-8") as f:
            try:
                yaml.safe_load(f)
            except yaml.YAMLError as exc:
                pytest.fail(f"YAML syntax error in {yaml_file.relative_to(repo_root)}:\n{exc}")
