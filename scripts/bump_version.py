#!/usr/bin/env python3
import re
import subprocess
import sys
from pathlib import Path

if len(sys.argv) != 2 or sys.argv[1] not in {"patch", "minor", "major"}:
    print("Usage: bump_version.py [patch|minor|major]")
    sys.exit(1)

# Check for uncommitted changes
status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
if status.stdout.strip():
    print("Error: You have uncommitted changes. Please commit or stash them before bumping the version.")
    sys.exit(1)

bump_type = sys.argv[1]
pyproject = Path(__file__).parent.parent / "pyproject.toml"

content = pyproject.read_text()

match = re.search(r'^version = "(\d+)\.(\d+)\.(\d+)"', content, re.MULTILINE)
if not match:
    print("Could not find version in pyproject.toml")
    sys.exit(1)

major, minor, patch = map(int, match.groups())

if bump_type == "patch":
    patch += 1
elif bump_type == "minor":
    minor += 1
    patch = 0
elif bump_type == "major":
    major += 1
    minor = 0
    patch = 0

new_version = f"{major}.{minor}.{patch}"


def replace_version(m):
    return f"{m.group(1)}{new_version}{m.group(2)}"


new_content = re.sub(r'^(version = ")\d+\.\d+\.\d+(".*)$', replace_version, content, flags=re.MULTILINE)
pyproject.write_text(new_content)

# Update README.md version strings
readme = Path(__file__).parent.parent / "README.md"
readme_content = readme.read_text()

# Pattern to match the version in the README.md install strings
readme_pattern = r"(immaterialdb @ git\+https://github.com/dysomni/immaterialdb.git@)v\d+\.\d+\.\d+"
readme_new_content = re.sub(readme_pattern, f"\\1v{new_version}", readme_content)
readme.write_text(readme_new_content)

# Commit the changes
commit_msg = f"Bump {bump_type} version to {new_version}"
subprocess.run(["git", "add", str(pyproject), str(readme)], check=True)
subprocess.run(["git", "commit", "-m", commit_msg], check=True)

print(f"Bumped {bump_type} version to {new_version} and committed changes.")
