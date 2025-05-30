repl:
	uv run ipython

tag:
	git tag v$(shell grep '^version =' pyproject.toml | sed -E 's/version = "([^"]+)"/\1/')
	git push --tags

bump/patch:
	python3 scripts/bump_version.py patch

bump/minor:
	python3 scripts/bump_version.py minor

bump/major:
	python3 scripts/bump_version.py major


