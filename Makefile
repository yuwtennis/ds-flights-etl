
GIT_COMMIT_HASH ?= $$(git rev-parse HEAD)

build:
	docker build -t dsflightsetl:$(GIT_COMMIT_HASH) .

test:
	poetry run pylint __main__.py
	poetry run pylint dsflightsetl/
	poetry run mypy dsflightsetl/