# Flight Delay Dashboard

An interactive dashboard for exploring U.S. domestic flight delay patterns from 2018–2025, built with DuckDB, Plotly, and Streamlit.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)
- Make is pre-installed on macOS/Linux. However, if it's missing on macOS run `xcode-select --install`. See [GNU Make](https://www.gnu.org/software/make/) for other platforms.

## Dev workflow

```bash
make setup      # install dependencies
make download   # download raw flight data from BTS (~85 months of CSVs); link sourced from https://rdrr.io/cran/skynet/src/R/download_ontime.R
make dashboard  # build parquet (once) and launch the dashboard
```

Note: To reset your environment run `rm -rf .venv && uv sync`
