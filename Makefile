.PHONY: install dev run format lint test test-provider-integration clean help \
	kubernetes-aws-plan kubernetes-aws-deploy kubernetes-aws-port-forward \
	kubernetes-aws-test kubernetes-aws-destroy

PYTHON_VERSION := 3.12

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

venv_check:
	@if [ ! -f .venv/bin/activate ]; then \
		echo "Virtualenv not found! Run \`make install\` first."; \
		exit 1; \
	fi

install: ## Install dependencies
	uv venv --python $(PYTHON_VERSION)
	uv sync --dev

dev: venv_check  ## Start the development server
	uv run fastapi dev main.py --port 0

lint: ## CHeck style with ruff
	uv run ruff check .

format: ## Format code with ruff
	uv run ruff check --fix .

typecheck: ## Type check code with basedpyright
	uv run basedpyright .

test: ## Run tests
	uv run pytest --ignore=tests/integration

test-provider-integration:
	uv run pytest tests/integration

docker-build: ## Build Docker image
	docker build -t benchmark-service:latest .

docker-run: ## Run Docker container
	docker run -p 8001:8001 benchmark-service:latest

kubernetes-aws-plan kubernetes-aws-deploy kubernetes-aws-port-forward kubernetes-aws-test kubernetes-aws-destroy:
	$(MAKE) -C infra/kubernetes/aws $(@:kubernetes-aws-%=%)
