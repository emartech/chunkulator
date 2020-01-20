DOCKER = docker
DOCKER_COMPOSE = docker-compose
IMAGE = dev
CONTAINER = dev
RABBIT_CONTAINER = rabbit

ifndef TESTMETHOD
FILTERARGS=
else
FILTERARGS=--filter $(TESTMETHOD)
endif

.PHONY: all test destroy update libtest

help:
	@echo "Targets:"
	@fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//' | sed -e 's/\(.*\):.*##[ \t]*/    \1 ## /' | column -t -s '##'
	@echo

all: stop destroy run packages ## Build all

destroy: ## Destroy containers
	-$(DOCKER_COMPOSE) stop
	-$(DOCKER_COMPOSE) rm -f

run: ## Run containers
	$(DOCKER_COMPOSE) up -d

stop: ## Stop containers
	$(DOCKER_COMPOSE) stop

restart: stop run ## Restart containers

packages: ## Install packages
	$(DOCKER_COMPOSE) run -e http_proxy=${http_proxy} -e https_proxy=${https_proxy} $(CONTAINER) /bin/bash -l -c "composer install"

pu: packages-update
packages-update: ## Update packages
	$(DOCKER_COMPOSE) run -e http_proxy=${http_proxy} -e https_proxy=${https_proxy} $(CONTAINER) /bin/bash -l -c "composer update"

ssh: sh

sh: ## Open sh in the container
	$(DOCKER_COMPOSE) exec $(CONTAINER) /bin/bash

logs: ## Show logs
	$(DOCKER_COMPOSE) logs --tail 10 --follow $(CONTAINER) $(RABBIT_CONTAINER) 2>&1

test: phpunit ## Run all test

phpunit: unit integration acceptance ## Test with phpunit

unit:
	$(DOCKER_COMPOSE) exec $(CONTAINER) bash -l -c "vendor/bin/phpunit --enforce-time-limit -c test/phpunit.xml --testsuite unit $(FILTERARGS) $(TESTFILE)"

integration:
	$(DOCKER_COMPOSE) exec $(CONTAINER) bash -l -c "vendor/bin/phpunit --enforce-time-limit -c test/phpunit.xml --testsuite integration $(FILTERARGS) $(TESTFILE)"

acceptance:
	$(DOCKER_COMPOSE) exec $(CONTAINER) bash -l -c "vendor/bin/phpunit --enforce-time-limit -c test/phpunit.xml --testsuite acceptance $(FILTERARGS) $(TESTFILE)"

