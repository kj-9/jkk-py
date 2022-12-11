SHELL=/bin/bash

.PHONY:
	run
	ga-commit
	pip-upgrade
	install
	install-dev

pip-upgrade:
	python -m pip install --upgrade pip

pip-install: pip-upgrade
	pip install -r requirements.txt

pip-install-dev: install
	pip install -r requirements-dev.txt

playwright-install:
	python -m playwright install --with-deps chromium

run:
	python main.py true true

# for github actions workflow
ga-commit:
ifdef MAKE_ENV
	ifeq ($(MAKE_ENV),GITHUB_ACTIONS)
		git config --local user.email "action@github.com"
		git config --local user.name "GitHub Action"
		git commit -m "Automatic data update" -a
	else
		echo MAKE_ENV is: $(MAKE_ENV). not run.
	endif
else
	echo "MAKE_ENV is not set. not run."
endif