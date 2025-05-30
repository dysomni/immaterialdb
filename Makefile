repl:
	uv run ipython

tag:
	git tag v$(shell grep '^version =' pyproject.toml | sed -E 's/version = "([^"]+)"/\1/')