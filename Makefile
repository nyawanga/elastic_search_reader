# all: test lint
COVERAGE_THRESHOLD ?= 90

install-requirements:
	pip install --no-cache -r requirements.txt

test:
	pip install --no-cache pytest mock pytest-mock coverage ;
	coverage run -m pytest ;
	coverage report --fail-under=${COVERAGE_THRESHOLD}


lint:
# try out the ruff linting package
	pip install --no-cache ruff
	find . -type f -name "*.py" | xargs ruff
	# --ignored-modules=test --extension-pkg-whitelist=test;
