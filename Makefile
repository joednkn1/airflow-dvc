lint:
	poetry run black .
	poetry run isort .
	poetry run flakehell lint

install:
	poetry install

update:
	poetry update

test:
	poetry run pytest -n 4

publish:
	poetry run publish

documentation:
	rm -rf pydoc-markdown.yml > /dev/null 2> /dev/null
	rm -rf build/docs > /dev/null 2> /dev/null
	poetry run pydoc-markdown --bootstrap hugo
	poetry run pydoc-markdown
