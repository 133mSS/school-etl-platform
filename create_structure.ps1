$dirs = @(
    "airflow",
    "dags",
    "src/config",
    "src/models", 
    "src/etl",
    "src/validation",
    "src/utils",
    "sql/source",
    "sql/warehouse",
    "great_expectations/expectations",
    "great_expectations/checkpoints",
    "tests",
    "grafana",
    "monitoring",
    "scripts",
    "docs/diagrams",
    "data/raw",
    "data/staging",
    "data/processed",
    "data/sample"
)

foreach ($dir in $dirs) {
    New-Item -ItemType Directory -Force -Path $dir
}

$init_files = @(
    "src/__init__.py",
    "src/config/__init__.py",
    "src/models/__init__.py",
    "src/etl/__init__.py",
    "src/validation/__init__.py",
    "src/utils/__init__.py",
    "tests/__init__.py",
    "dags/__init__.py"
)

foreach ($file in $init_files) {
    New-Item -ItemType File -Force -Path $file
}

Write-Host "✅ Project structure created!"