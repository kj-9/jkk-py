SHELL=/bin/bash
PREFECT_LOGIN_OPTS=


.PHONY:
	pip-upgrade
	pip-install
	pip-install-dev
	run
	ga-commit

pip-upgrade:
	python -m pip install --upgrade pip

pip-install: pip-upgrade
	pip install -r flow/notify/requirements.txt

pip-install-dev: pip-install
	pip install -r requirements-dev.txt

prefect-login:
	prefect cloud login $(PREFECT_LOGIN_OPTS)

run:
	python flow/notify/main.py '{"send_line": true}'

run-silent:
	python flow/notify/main.py '{"send_line": false}'

# for github actions workflow
ga-commit:
ifeq ($(MAKE_ENV),GITHUB_ACTIONS)
	git config --local user.email "action@github.com"
	git config --local user.name "GitHub Action"
	git commit -m "Automatic data update" -a
else
	echo MAKE_ENV is: $(MAKE_ENV). not run.
endif

pre-commit:
	pre-commit run --all-files --show-diff-on-failure
