.PHONY: quality quality-fix test db image up down clean

quality: ## Audit dictionary consistency (read-only)
	python3 script/check_data_quality.py

quality-fix: ## Apply deterministic fixes except UUID changes
	python3 script/check_data_quality.py --fix

test: ## Run data quality rule tests
	python3 -m unittest discover -s tests -v

db: quality ## Build genji.db from JSON data
	python3 script/json_to_sqlite.py

image: ## Build Docker image
	docker compose build

up: ## Start services
	docker compose up -d --build

down: ## Stop services
	docker compose down

clean: ## Remove genji.db
	rm -f genji.db

help: ## Show this help
	@grep -E '^[a-z]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'
