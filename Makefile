PYTHON := uv run python
DATA_DIR := data
RAW_DIR := $(DATA_DIR)/raw
PROCESSED_DIR := $(DATA_DIR)/processed
PARQUET := $(PROCESSED_DIR)/flights.parquet

all: dashboard

# environ setup
setup:
	@echo "Setting up environment..."
	@uv sync

# data download
download:
	@echo "Downloading data..."
	@$(PYTHON) src/download.py

# combine CSVs to single parquet, and cleanup files
transform: $(PARQUET)

$(PARQUET):
	@echo "Building parquet..."
	@mkdir -p $(PROCESSED_DIR)
	@$(PYTHON) src/transform.py

# render dashboard
dashboard: transform
	@echo "Launching dashboard..."
	@PYTHONPATH=. uv run streamlit run src/report.py

# local dev
clean:
	rm -rf $(DATA_DIR)
	rm -rf __pycache__
	rm -rf .pytest_cache

.PHONY: all setup download transform dashboard clean
