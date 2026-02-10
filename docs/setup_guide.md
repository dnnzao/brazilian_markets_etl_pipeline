# How to Run the Brazilian Market ETL Pipeline

This guide provides detailed step-by-step instructions to deploy, run, and test the Brazilian Financial Markets ETL Pipeline. Each step includes comprehensive explanations of what commands do, why specific technologies were chosen, and the reasoning behind architectural decisions.

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Initial Project Setup](#2-initial-project-setup)
3. [Starting the Database](#3-starting-the-database)
4. [Verifying Database Connection](#4-verifying-database-connection)
5. [Setting Up Python Environment](#5-setting-up-python-environment)
6. [Running the Data Backfill](#6-running-the-data-backfill)
7. [Running dbt Transformations](#7-running-dbt-transformations)
8. [Starting Airflow (Orchestration)](#8-starting-airflow-orchestration)
9. [Starting the Dashboard](#9-starting-the-dashboard)
10. [Running Tests](#10-running-tests)
11. [Daily Operations](#11-daily-operations)
12. [Stopping Services](#12-stopping-services)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. Prerequisites

Before starting, ensure you have the following installed on your system. This section explains not just what to install, but why each tool is necessary and how it fits into the overall architecture.

### 1.1 Required Software

#### Docker and Docker Compose

Docker is a containerization platform that packages applications and their dependencies into isolated containers. We use Docker for several critical reasons in this project.

First, Docker provides environment consistency. The PostgreSQL database runs identically on any machine regardless of the host operating system. This eliminates the classic "works on my machine" problem that plagues development teams. When you run `docker compose up`, you get the exact same PostgreSQL 15 Alpine image that was tested during development.

Second, Docker simplifies dependency management. Instead of manually installing PostgreSQL, configuring users, creating databases, and setting up schemas, Docker handles all of this automatically through initialization scripts. The database comes pre-configured with the exact settings needed for this project.

Third, Docker provides isolation. The PostgreSQL container runs in its own network namespace with its own filesystem. This means it won't conflict with any local PostgreSQL installation you might have, and you can run multiple projects with different database requirements simultaneously.

We chose Docker Compose over plain Docker commands because Compose provides declarative infrastructure definition. The `docker-compose.yml` file describes all services, networks, and volumes in a single YAML file that can be version-controlled. This is the "Infrastructure as Code" approach that modern DevOps practices recommend.

Check if Docker is installed:
```bash
docker --version
# Expected output: Docker version 20.10.x or higher

docker compose version
# Expected output: Docker Compose version v2.x.x
```

If not installed, on Arch Linux:
```bash
sudo pacman -S docker docker-compose
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
# Log out and log back in for group changes to take effect
```

The `sudo systemctl enable --now docker` command does two things simultaneously: `enable` configures Docker to start automatically on system boot, while `--now` also starts it immediately. The `usermod -aG docker $USER` command adds your user to the `docker` group, allowing you to run Docker commands without `sudo`. This is a security convenience trade-off: it's more convenient but means any process running as your user can control Docker containers.

**Why Docker Engine over Docker Desktop on Linux?**

On Linux systems, you have two options: Docker Engine (the native daemon) and Docker Desktop (a GUI application that uses QEMU virtualization). We recommend Docker Engine for several reasons. Docker Engine runs containers natively on the Linux kernel without virtualization overhead, resulting in better performance. Docker Desktop on Linux uses QEMU to provide a consistent experience across operating systems, but this adds complexity and can cause stability issues (like the QEMU crash errors you might encounter). Docker Engine is also lighter weight, using fewer system resources since there's no GUI or virtualization layer.

#### Python 3.11+

Python is the primary programming language for this project's extraction and loading scripts. We require Python 3.11 or higher for several reasons.

Python 3.11 introduced significant performance improvements (10-60% faster than 3.10 for many workloads) through the Faster CPython project. It also added better error messages with precise line-level tracebacks, making debugging easier. The `tomllib` module for parsing TOML files was added to the standard library, which some of our dependencies use.

Python 3.12 and 3.13 are also fully supported and bring additional performance improvements and features.

Python 3.14 (currently in development/beta) is supported but may require newer package versions. The `requirements.txt` uses flexible version specifiers (`>=`) to accommodate this. If you encounter compilation errors with Python 3.14, it's usually because a package hasn't yet released a version compatible with Python 3.14's updated C API.

Check Python version:
```bash
python --version
# Expected output: Python 3.11.x, 3.12.x, 3.13.x, or 3.14.x
```

If not installed:
```bash
sudo pacman -S python python-pip
```

**Why not Python 3.10 or earlier?**

While the code would likely run on Python 3.10, we specify 3.11+ because the data engineering ecosystem has moved forward. Many libraries we depend on (pandas 2.x, dbt-core 1.7+) are optimized for newer Python versions. Additionally, Python 3.10 reaches end-of-life in October 2026, so starting a new project on 3.11+ ensures longer support.

#### Git

Git is the version control system used to track changes to the codebase. Even if you're not planning to contribute changes back, Git is useful for tracking your own modifications and reverting if something breaks.

```bash
git --version
# Expected output: git version 2.x.x
```

### 1.2 Optional but Recommended

#### DBeaver or psql

DBeaver is a graphical database management tool that provides a user-friendly interface for exploring database schemas, writing queries, and visualizing data. It's particularly helpful for understanding the data model and debugging transformation issues.

`psql` is PostgreSQL's command-line interface. It's lightweight and fast, ideal for quick queries and scripting. We use `psql` in this guide for verification commands because it's easily scriptable and produces consistent output.

```bash
# psql client only (no server needed - we use Docker for the server)
sudo pacman -S postgresql-libs

# Or install DBeaver for GUI access
sudo pacman -S dbeaver
```

We install `postgresql-libs` rather than the full `postgresql` package because we only need the client tools. The full package would install the PostgreSQL server, which would conflict with our Docker-based approach and consume unnecessary disk space.

---

## 2. Initial Project Setup

This section prepares the project directory with the necessary configuration files and folder structure.

### 2.1 Navigate to the Project Directory

```bash
cd ~/Desktop/programming/projects/brazilian_markets_etl_pipeline
```

All subsequent commands assume you're in this directory. The project follows a standard Python project layout with separate directories for different concerns: `extract/` for data extraction logic, `load/` for database loading, `dbt_project/` for transformations, `dashboard/` for visualization, and `scripts/` for utility commands.

### 2.2 Create Environment File

Environment files store configuration that varies between deployments (development, staging, production) and sensitive information like passwords. We use a `.env` file following the twelve-factor app methodology.

```bash
cp .env.example .env
```

The `cp` command copies the example file to create your actual configuration. We don't commit the `.env` file to Git (it's in `.gitignore`) because it may contain secrets. The `.env.example` file serves as documentation of required variables and provides safe default values for local development.

Review the default values in `.env`:
```bash
cat .env
```

The defaults are:
```
POSTGRES_USER=dataeng
POSTGRES_PASSWORD=dataeng123
POSTGRES_DB=brazilian_market
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

**Understanding each variable:**

`POSTGRES_USER=dataeng` defines the database username. We use `dataeng` (short for "data engineering") as a descriptive name that indicates this is a data engineering project. In production, you'd use a more secure, randomly generated username.

`POSTGRES_PASSWORD=dataeng123` is the database password. This simple password is acceptable for local development but would never be used in production. Production passwords should be long, random strings stored in a secrets manager.

`POSTGRES_DB=brazilian_market` is the database name. PostgreSQL can host multiple databases in a single instance; this creates a dedicated database for our project, keeping it isolated from other applications.

`POSTGRES_HOST=localhost` tells the Python scripts where to find the database. When running scripts locally (outside Docker), the database is accessible at `localhost` because Docker maps the container's port to the host machine.

`POSTGRES_PORT=5432` is PostgreSQL's default port. We keep the default for simplicity, but you could change this if port 5432 is already in use by another application.

### 2.3 Create Required Directories

```bash
mkdir -p logs
mkdir -p airflow/logs
mkdir -p airflow/plugins
```

The `mkdir -p` command creates directories and any necessary parent directories. The `-p` flag prevents errors if the directory already exists.

The `logs/` directory stores application logs from the extraction scripts. Centralized logging makes debugging easier and allows you to review what happened during past runs.

The `airflow/logs/` directory is where Apache Airflow writes its execution logs. Airflow generates extensive logs for each task execution, which is crucial for debugging failed pipeline runs.

The `airflow/plugins/` directory is for custom Airflow plugins. While we don't use custom plugins in this project, Airflow expects this directory to exist.

### 2.4 Make Scripts Executable

```bash
chmod +x scripts/*.sh scripts/*.py
```

The `chmod +x` command adds execute permission to files. On Unix-like systems, a file needs the execute bit set to be run directly (e.g., `./script.py` instead of `python script.py`). The `*` wildcard matches all files with the specified extension.

While Python scripts can always be run with `python script.py`, making them executable allows them to be run directly using the shebang (`#!/usr/bin/env python3`) at the top of each file. This is a Unix convention that makes scripts more portable and self-documenting about their interpreter.

---

## 3. Starting the Database

This section launches the PostgreSQL database in a Docker container. The database is the foundation of the entire pipeline, storing raw extracted data, intermediate transformations, and final analytical tables.

### 3.1 Start PostgreSQL Container

```bash
docker compose up -d postgres
```

This command does several things. The `docker compose up` command reads the `docker-compose.yml` file and starts the specified services. The `-d` flag runs containers in "detached" mode (in the background), returning control to your terminal immediately. The `postgres` argument specifies that we only want to start the PostgreSQL service, not all services defined in the compose file.

**Why start only PostgreSQL first?**

We start PostgreSQL in isolation to ensure it's healthy before starting dependent services. The database needs time to initialize (create users, run initialization scripts, etc.), and starting everything simultaneously could cause race conditions where other services try to connect before the database is ready.

When the container starts for the first time, Docker automatically runs any SQL scripts in the `/docker-entrypoint-initdb.d/` directory (mapped from our `database/init/` folder). This creates our three-schema architecture:

- `raw` schema: Landing zone for extracted data, stored exactly as received from source systems
- `staging` schema: Cleaned and validated data, ready for transformation
- `analytics` schema: Final dimensional model (star schema) optimized for queries

Expected output:
```
[+] Running 2/2
 ✔ Network brazilian_market_network  Created
 ✔ Container brazilian_market_db     Started
```

The output shows that Docker created a network and started the container. The network (`brazilian_market_network`) is a Docker bridge network that allows containers in this compose project to communicate with each other using service names as hostnames.

### 3.2 Verify Container is Running

```bash
docker compose ps
```

The `ps` command (short for "process status") lists all containers managed by this compose project along with their current state.

Expected output:
```
NAME                   IMAGE                  STATUS                   PORTS
brazilian_market_db    postgres:15-alpine     Up X seconds (healthy)   0.0.0.0:5432->5432/tcp
```

**Understanding the output:**

`IMAGE: postgres:15-alpine` shows we're using PostgreSQL version 15 with the Alpine Linux base image. Alpine is a minimal Linux distribution that results in smaller container images (about 80MB vs 400MB for the standard Debian-based image). Smaller images download faster and use less disk space.

`STATUS: Up X seconds (healthy)` indicates the container is running and passing health checks. The health check (defined in docker-compose.yml) periodically runs `pg_isready` to verify PostgreSQL is accepting connections. This takes 10-30 seconds after startup.

`PORTS: 0.0.0.0:5432->5432/tcp` shows the port mapping. The container's internal port 5432 is mapped to the host's port 5432 on all network interfaces (0.0.0.0). This is what allows your local Python scripts to connect to `localhost:5432`.

### 3.3 Check Container Logs

```bash
docker compose logs postgres
```

The `logs` command displays output from the container's standard output and standard error streams. This is invaluable for debugging startup issues.

You should see messages about database initialization:
```
/usr/local/bin/docker-entrypoint.sh: running /docker-entrypoint-initdb.d/01_create_schemas.sql
/usr/local/bin/docker-entrypoint.sh: running /docker-entrypoint-initdb.d/02_create_raw_tables.sql
/usr/local/bin/docker-entrypoint.sh: running /docker-entrypoint-initdb.d/03_create_analytics_tables.sql
```

The initialization scripts run in alphabetical order (hence the numeric prefixes). This ensures schemas are created before tables, and raw tables exist before analytics tables that might reference them.

**Why PostgreSQL over other databases?**

We chose PostgreSQL for several reasons specific to data engineering workloads.

PostgreSQL has excellent analytical query support with advanced window functions, CTEs (Common Table Expressions), and lateral joins. These features are essential for calculating metrics like rolling averages, cumulative returns, and time-series analysis.

PostgreSQL's JSONB data type allows storing semi-structured data alongside relational data, useful if source APIs change their response format.

PostgreSQL is the most commonly used database in the modern data stack, with first-class support from tools like dbt, Airflow, and most BI platforms. This means better documentation, more Stack Overflow answers, and smoother integrations.

PostgreSQL is free and open-source with no licensing costs, making it ideal for both learning projects and production deployments.

**Why not MySQL, SQLite, or cloud databases?**

MySQL lacks some advanced analytical features (though MariaDB has improved this). Its default configuration also has some quirks around case sensitivity and strict mode that can cause subtle bugs.

SQLite is excellent for embedded applications but lacks concurrent write support and network access, making it unsuitable for multi-user or multi-process data pipelines.

Cloud databases (AWS RDS, Google Cloud SQL, Azure Database) are excellent for production but add cost and complexity for a local development/portfolio project. The Docker approach lets you develop locally with zero cloud costs, then deploy the same schemas to cloud databases later.

---

## 4. Verifying Database Connection

This section confirms that the database is accessible and properly initialized. Verification is a critical step before proceeding, as many subsequent steps depend on a working database connection.

### 4.1 Test Connection with psql

```bash
psql -h localhost -p 5432 -U dataeng -d brazilian_market -c "\dt raw.*"
```

When prompted for password, enter: `dataeng123`

**Understanding the command:**

`psql` is PostgreSQL's interactive terminal. It connects to a database and allows you to run SQL commands.

`-h localhost` specifies the host. Even though PostgreSQL is running in a container, Docker's port mapping makes it accessible at localhost.

`-p 5432` specifies the port number. This matches the port mapping in docker-compose.yml.

`-U dataeng` specifies the username to connect as. This is the user created by the `POSTGRES_USER` environment variable.

`-d brazilian_market` specifies which database to connect to. PostgreSQL instances can contain multiple databases; we connect directly to ours.

`-c "\dt raw.*"` executes a command and exits. The `\dt` meta-command lists tables, and `raw.*` is a pattern matching all tables in the `raw` schema.

Expected output:
```
           List of relations
 Schema |    Name    | Type  |  Owner
--------+------------+-------+---------
 raw    | indicators | table | dataeng
 raw    | stocks     | table | dataeng
(2 rows)
```

This confirms that the initialization scripts successfully created both tables in the raw schema.

### 4.2 Verify All Schemas Exist

```bash
psql -h localhost -p 5432 -U dataeng -d brazilian_market -c "\dn"
```

The `\dn` meta-command lists all schemas in the database.

Expected output:
```
      List of schemas
   Name    |  Owner
-----------+----------
 analytics | dataeng
 public    | pg_database_owner
 raw       | dataeng
 staging   | dataeng
(4 rows)
```

**Understanding the schemas:**

`raw` is the Bronze layer in the medallion architecture. Data lands here exactly as extracted from source systems, with minimal processing. This preserves the original data for auditing and reprocessing.

`staging` is the Silver layer. Data is cleaned, validated, and standardized here. This is where we fix data quality issues without modifying the raw data.

`analytics` is the Gold layer. This contains the final dimensional model (star schema) with fact and dimension tables optimized for analytical queries and dashboard performance.

`public` is PostgreSQL's default schema, created automatically. We don't use it but it exists in every PostgreSQL database.

### 4.3 Test Connection with DBeaver (Optional)

If you prefer a graphical interface, DBeaver provides a more visual way to explore the database. It's particularly useful for writing complex queries, visualizing table relationships, and exporting data.

1. Open DBeaver
2. Click **Database** → **New Database Connection**
3. Select **PostgreSQL** → Click **Next**
4. Fill in the connection details:
   - Host: `localhost`
   - Port: `5432`
   - Database: `brazilian_market`
   - Username: `dataeng`
   - Password: `dataeng123`
5. Click **Test Connection** - Should show "Connected"
6. Click **Finish**

DBeaver stores these connection settings, so you only need to configure this once. The connection will be available in the Database Navigator panel on the left side of the window.

---

## 5. Setting Up Python Environment

This section creates an isolated Python environment and installs all required packages. Proper environment management is crucial for reproducible builds and avoiding dependency conflicts.

### 5.1 Create Virtual Environment

```bash
python -m venv venv
```

**Understanding virtual environments:**

A virtual environment is an isolated Python installation with its own packages, separate from the system Python. This solves several problems.

Dependency isolation prevents conflicts between projects. Project A might need pandas 1.5 while Project B needs pandas 2.0. With virtual environments, each project has its own pandas version.

Reproducibility ensures that your project uses the exact package versions you tested with, regardless of what's installed system-wide.

Clean uninstallation is simple: just delete the `venv` directory. No need to track down and remove globally installed packages.

The `python -m venv venv` command runs Python's built-in `venv` module to create a virtual environment in a directory called `venv`. The `-m` flag tells Python to run a module as a script.

**Why venv over conda, virtualenv, or poetry?**

`venv` is included in Python's standard library since Python 3.3, requiring no additional installation. It's simple, lightweight, and sufficient for most projects.

`conda` is more powerful (manages non-Python dependencies too) but heavier and slower. It's excellent for data science projects with complex binary dependencies (like CUDA for GPU computing) but overkill for this project.

`virtualenv` was the standard before venv was added to the standard library. It offers slightly more features but requires separate installation.

`poetry` provides dependency resolution and lock files but adds complexity. It's better for libraries you plan to publish than for application projects.

### 5.2 Activate Virtual Environment

```bash
source venv/bin/activate
```

The `source` command executes a script in the current shell, rather than a subprocess. This is necessary because activation modifies environment variables (like `PATH`) that need to persist in your current session.

After activation, your prompt changes to show `(venv)` at the beginning, indicating which environment is active. The `PATH` environment variable is modified to prioritize the virtual environment's `bin` directory, so running `python` or `pip` uses the virtual environment's versions.

**Why not just run `venv/bin/activate`?**

Running a script normally (without `source`) executes it in a subprocess. Any environment variable changes would only affect that subprocess and disappear when it exits. Using `source` (or its synonym `.`) runs the script in the current shell, making changes permanent for your session.

### 5.3 Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

**Understanding the commands:**

`pip install --upgrade pip` ensures you have the latest pip version. Newer pip versions have better dependency resolution, faster downloads, and security fixes. This avoids warnings about outdated pip during subsequent installs.

`pip install -r requirements.txt` reads package specifications from the requirements file and installs them. The `-r` flag means "read requirements from file."

**Understanding requirements.txt:**

The requirements file lists all direct dependencies with version constraints. Let's examine the key packages:

`yfinance>=0.2.40` is a Python wrapper for Yahoo Finance's API. It provides historical stock prices, company information, and financial statements. We use it because it's free, requires no API key, and provides comprehensive Brazilian stock data (tickers ending in `.SA`).

`requests>=2.31.0` is Python's de facto standard HTTP library. We use it for calling the Brazilian Central Bank (BCB) API, which provides economic indicators like SELIC rate, inflation, and exchange rates.

`pandas>=2.2.0` is the core data manipulation library for Python. It provides DataFrame objects for tabular data, essential for transforming and cleaning extracted data. Version 2.2+ is required for Python 3.14 compatibility.

`sqlalchemy>=2.0.25` is Python's most popular database toolkit. It provides a consistent interface across different databases, connection pooling, and protection against SQL injection. We use it instead of raw `psycopg2` for these safety and portability benefits.

`psycopg2-binary>=2.9.9` is the PostgreSQL adapter for Python. The `-binary` variant includes pre-compiled binaries, avoiding the need to compile against PostgreSQL headers. This simplifies installation significantly.

`dbt-core>=1.7.4` and `dbt-postgres>=1.7.4` provide the data build tool (dbt), which transforms raw data into analytical models using SQL. dbt brings software engineering practices (version control, testing, documentation) to SQL transformations.

`streamlit>=1.30.0` is a Python framework for building data applications and dashboards. It turns Python scripts into interactive web apps without requiring frontend development knowledge.

`plotly>=5.18.0` creates interactive visualizations. Unlike matplotlib (static images), Plotly charts support zooming, panning, and hovering for data exploration.

`loguru>=0.7.2` provides better logging than Python's standard library. It offers colorful output, automatic formatting, and simpler configuration.

Installation takes 2-5 minutes depending on your internet speed and whether packages need to be compiled from source.

### 5.4 Verify Installation

```bash
python -c "import yfinance; import pandas; import sqlalchemy; print('All packages installed successfully!')"
```

The `-c` flag tells Python to execute a string as code. This quick test imports the core packages to verify they're installed correctly. Import errors at this stage are much easier to debug than failures during pipeline execution.

---

## 6. Running the Data Backfill

This step extracts historical data from external APIs and loads it into the database. The backfill creates the foundation dataset that subsequent transformations will use.

### 6.1 Run the Backfill Script

```bash
python scripts/backfill_data.py
```

**Understanding the backfill process:**

The backfill script performs a historical data load, extracting approximately 10 years of data from two sources.

**Yahoo Finance** provides daily stock price data (open, high, low, close, volume, adjusted close) for 20 major Brazilian stocks traded on B3 (the Brazilian stock exchange). These include blue-chip companies like Petrobras (PETR4), Vale (VALE3), and major banks.

**Brazilian Central Bank (BCB) API** provides economic indicators including SELIC (the base interest rate), IPCA (official inflation index), CDI (interbank rate), and USD/BRL exchange rate. These indicators are essential for analyzing macro-economic impacts on stock performance.

**Why a separate backfill script?**

The backfill is separated from regular incremental loads for several reasons. Historical loads are one-time operations with different requirements (longer timeouts, more data volume, different error handling). They can take 5-10 minutes, unlike incremental loads that take seconds. Running them separately allows monitoring and restarting without affecting the daily pipeline.

You will see output like:
```
2026-02-09 10:00:00 | INFO     | ============================================================
2026-02-09 10:00:00 | INFO     | BRAZILIAN MARKET ETL - HISTORICAL BACKFILL
2026-02-09 10:00:00 | INFO     | ============================================================
2026-02-09 10:00:00 | INFO     | Start date: 2015-01-01
2026-02-09 10:00:00 | INFO     | End date: 2026-02-09
2026-02-09 10:00:01 | INFO     | [1/20] Extracting PETR4.SA
2026-02-09 10:00:02 | INFO     |   Retrieved 2763 rows for PETR4.SA
...
```

**Important notes about BCB API:**

The BCB API has a 10-year limit for daily data series. If you request more than 10 years, you'll get a 406 error. The extraction code automatically adjusts the date range for daily series to stay within this limit.

Daily series (SELIC, CDI, USD/BRL) contain about 2,500 rows each (approximately 252 trading days per year times 10 years).

Monthly series (IPCA, IGP-M, Unemployment) contain about 120-130 rows each (12 months per year times 10 years).

The extraction may take 2-3 minutes per daily indicator because the BCB API is slow for large data requests.

### 6.2 Monitor Progress

The script extracts:
- 20 stocks from Yahoo Finance (~54,000 rows total, ~2,700 per stock)
- 7 economic indicators from BCB API (~9,000 rows total)

Progress is logged in real-time. Stock extraction completes quickly (about 1 second per ticker). BCB indicators take longer, especially daily series which may take 1-2 minutes each.

### 6.3 Verify Data Was Loaded

After the script completes, verify the data:

```bash
psql -h localhost -p 5432 -U dataeng -d brazilian_market -c "SELECT COUNT(*) as total_rows, COUNT(DISTINCT ticker) as tickers, MIN(date) as earliest, MAX(date) as latest FROM raw.stocks;"
```

Expected output (numbers will vary based on extraction date):
```
 total_rows | tickers |  earliest  |   latest
------------+---------+------------+------------
      54370 |      20 | 2015-01-02 | 2026-02-06
```

```bash
psql -h localhost -p 5432 -U dataeng -d brazilian_market -c "SELECT indicator_code, indicator_name, COUNT(*) as rows FROM raw.indicators GROUP BY indicator_code, indicator_name ORDER BY rows DESC;"
```

Expected output:
```
 indicator_code | indicator_name | rows
----------------+----------------+------
 432            | SELIC          | 3621
 1              | USD_BRL        | 2488
 4389           | CDI            | 2487
 189            | IGP_M          |  133
 433            | IPCA           |  132
 24363          | Unemployment   |  131
 4380           | SELIC_Target   |  118
```

**Understanding the row counts:**

Daily indicators (SELIC, USD_BRL, CDI) have ~2,500 rows because they're limited to 10 years of trading days.

Monthly indicators (IGP_M, IPCA, Unemployment) have ~130 rows representing monthly data points over 11 years.

SELIC has slightly more rows than USD_BRL because the SELIC rate is published on some days when currency markets are closed.

---

## 7. Running dbt Transformations

Now we transform the raw data into the dimensional model using dbt (data build tool). dbt brings software engineering practices to SQL transformations, including version control, testing, and documentation.

### 7.1 Why We Run dbt in Docker

Due to compatibility issues between Python 3.14 and some dbt dependencies (specifically the `mashumaro` library), we run dbt inside a Docker container that uses Python 3.11. This ensures consistent behavior regardless of your local Python version.

The dbt Docker container is pre-configured with:
- Python 3.11 (stable, widely tested)
- dbt-postgres 1.7.4
- Connection to the PostgreSQL container via Docker networking

A helper script (`scripts/run_dbt.sh`) simplifies running dbt commands inside the container.

### 7.2 Start the dbt Container and Install Dependencies

```bash
sudo docker compose --profile dbt up -d dbt && ./scripts/run_dbt.sh deps
```

This command does two things. First, `docker compose --profile dbt up -d dbt` starts the dbt container (which is defined with a profile, meaning it doesn't start by default). Second, `./scripts/run_dbt.sh deps` runs `dbt deps` inside that container.

The `dbt deps` command installs packages specified in `packages.yml`. These are reusable dbt packages from the community, similar to pip packages for Python.

We use `dbt-labs/dbt_utils`, which provides utility macros for common SQL patterns like generating surrogate keys, pivoting data, and handling null values. Using community packages avoids reinventing the wheel and ensures we follow best practices.

Expected output:
```
Running: dbt deps
============================================
Installing dbt-labs/dbt_utils
  Installed from version 1.1.1
```

### 7.3 Test Database Connection

```bash
./scripts/run_dbt.sh debug
```

The `dbt debug` command verifies that dbt can connect to the database and that your project configuration is valid. It checks the dbt version, Python environment, project structure, and database connectivity.

Look for these lines at the end:
```
  Connection test: [OK connection ok]
All checks passed!
```

If connection fails, check that the Docker network is properly configured and that the PostgreSQL container is running.

**Why dbt over plain SQL scripts?**

dbt provides several advantages over managing SQL files manually.

Dependency management automatically determines the correct order to run models based on `ref()` functions. If `fact_daily_market` depends on `stg_stocks`, dbt runs `stg_stocks` first.

Incremental models only process new or changed data instead of rebuilding entire tables, dramatically improving performance for large datasets.

Built-in testing allows you to assert data quality expectations (no nulls, unique keys, referential integrity) that run automatically after each transformation.

Documentation generation creates a navigable website showing your data models, their relationships, and column descriptions.

Version control integration treats SQL as code that can be reviewed, tested, and deployed through standard CI/CD pipelines.

### 7.4 Load Seed Data

```bash
./scripts/run_dbt.sh seed
```

Seeds are CSV files that dbt loads into the database as tables. We use seeds for static reference data that doesn't come from external APIs, like stock sector classifications or calendar tables.

Expected output:
```
Running: dbt seed
============================================
Running with dbt=1.7.4
...
Completed successfully
Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1
```

**Why seeds over manual INSERT statements?**

Seeds are version-controlled alongside the dbt project, so changes are tracked and reviewable. They're also idempotent: running `dbt seed` multiple times produces the same result, unlike INSERT statements that might create duplicates.

### 7.5 Run All Models

```bash
./scripts/run_dbt.sh run
```

This command executes all SQL models in dependency order, creating views and tables in the database.

**Understanding the model layers:**

Staging models (`stg_*`) clean and standardize raw data. They're typically views (not tables) that rename columns, cast data types, and filter invalid records. Changes to raw data immediately flow through to staging without needing to rebuild.

Intermediate models (`int_*`) apply business logic like calculating daily returns, rolling volatility, and joining datasets. These handle the complex transformations that are too complicated for staging but shouldn't clutter the final analytical models.

Mart models include dimension tables (`dim_*`) containing descriptive attributes (stock names, sectors, dates) and fact tables (`fact_*`) containing measurable events (daily prices, returns, volumes). This is the star schema pattern optimized for analytical queries.

Expected output:
```
Running with dbt=1.7.4
Found 9 models, 1 seed...

Concurrency: 4 threads

1 of 9 START sql view model staging.stg_stocks ............................. [RUN]
2 of 9 START sql view model staging.stg_indicators ......................... [RUN]
...
9 of 9 START sql incremental model analytics.fact_daily_market ............. [RUN]
9 of 9 OK created sql incremental model analytics.fact_daily_market ........ [INSERT in 5.23s]

Completed successfully
Done. PASS=9 WARN=0 ERROR=0 SKIP=0 TOTAL=9
```

The concurrency setting (4 threads) means dbt runs up to 4 models in parallel when their dependencies allow. This significantly speeds up execution for larger projects.

### 7.6 Run Data Quality Tests

```bash
./scripts/run_dbt.sh test
```

dbt tests verify that your data meets expectations. Tests are SQL queries that should return zero rows if the assertion passes.

**Types of tests:**

Schema tests (defined in YAML files) check column properties: `not_null` ensures no null values, `unique` ensures no duplicates, `accepted_values` checks for valid categories, and `relationships` verifies foreign keys.

Custom tests (SQL files in `tests/`) check complex business rules like "no stock should have returns greater than 100% in a single day" or "all dates in the fact table should exist in the date dimension."

Expected output:
```
Running: dbt test
============================================
Running with dbt=1.7.4
Found 9 models, 15 tests...

Concurrency: 4 threads

1 of 15 START test not_null_stg_stocks_ticker .............................. [RUN]
...
15 of 15 OK test assert_valid_returns ...................................... [PASS in 0.45s]

Completed successfully
Done. PASS=15 WARN=0 ERROR=0 SKIP=0 TOTAL=15
```

**Why are data quality tests important?**

Data quality issues compound as they flow through the pipeline. A null value in raw data becomes a null return calculation, which becomes a misleading average in a dashboard. Catching issues early (through tests) is far cheaper than debugging incorrect reports.

### 7.7 Shortcut: Run Everything at Once

Instead of running each command separately, you can run the full dbt pipeline with a single command:

```bash
./scripts/run_dbt.sh all
```

This executes `dbt deps`, `dbt run`, and `dbt test` in sequence. Use this after initial setup or when you want to rebuild everything.

### 7.8 Generate Documentation (Optional)

```bash
./scripts/run_dbt.sh docs
```

This generates a static website with your project documentation, including model descriptions, column definitions, lineage graphs showing data flow, and test results.

The lineage graph is particularly valuable: it visualizes how data flows from sources through staging to final models, helping you understand impact analysis ("if I change this table, what else is affected?").

Note: To serve the documentation and view it in a browser, you would need to run `dbt docs serve` inside the container or copy the generated files to your host machine.

---

## 8. Starting Airflow (Orchestration)

Apache Airflow manages scheduled pipeline runs, ensuring data is extracted and transformed daily without manual intervention. This step is optional for initial testing but essential for automated operations.

### 8.1 Start Airflow Services

```bash
docker compose up -d airflow-webserver airflow-scheduler
```

This starts two Airflow components.

The **scheduler** monitors DAG (Directed Acyclic Graph) definitions and triggers task execution based on schedules and dependencies. It's the "brain" of Airflow that decides when to run what.

The **webserver** provides the web UI for monitoring DAGs, viewing logs, and manually triggering runs. It doesn't execute tasks; it's purely for visibility and control.

**Why Airflow over cron, Prefect, or Dagster?**

Cron is simpler but lacks visibility, retry logic, and dependency management. You can't easily see if yesterday's job failed or restart from a specific point.

Airflow is the most widely used orchestration tool in data engineering, meaning better job prospects, more community resources, and easier hiring of people who know it. Its web UI provides excellent visibility into pipeline health.

Prefect and Dagster are newer alternatives with some advantages (better Python API, easier testing), but Airflow's market dominance makes it a better portfolio project choice for job-seekers.

First startup takes 2-3 minutes as Airflow initializes its metadata database (which tracks DAG runs, task states, and history).

### 8.2 Check Airflow Status

```bash
docker compose ps
```

Wait until all services show `Up` and `(healthy)`:
```
NAME                   STATUS                   PORTS
airflow_scheduler      Up 2 minutes
airflow_webserver      Up 2 minutes (healthy)   0.0.0.0:8080->8080/tcp
brazilian_market_db    Up 15 minutes (healthy)  0.0.0.0:5432->5432/tcp
```

The webserver health check verifies the web UI is responding. The scheduler doesn't have a health endpoint but should show "Up."

### 8.3 Access Airflow Web UI

Open your browser and go to: http://localhost:8080

Login credentials:
- Username: `admin`
- Password: `admin`

These default credentials are set in docker-compose.yml. In production, you'd use a secrets manager and more secure passwords.

### 8.4 View Available DAGs

You should see two DAGs:
- `backfill_historical` - One-time historical load (what we ran manually in step 6)
- `daily_market_etl` - Daily incremental load that runs automatically

DAGs may appear paused (gray toggle) by default. Airflow requires explicit enabling to prevent accidental runs of newly deployed DAGs.

### 8.5 Enable the Daily DAG

1. Find `daily_market_etl` in the list
2. Toggle the switch on the left to enable it (turns blue)
3. The DAG will run automatically at 6 AM Brazil time (9 AM UTC)

**Why 6 AM Brazil time?**

The Brazilian stock market (B3) opens at 10 AM and closes at 5 PM Brazil time. Running at 6 AM ensures we capture the previous day's complete data (which becomes available overnight) before the market opens.

### 8.6 Manually Trigger a DAG (Optional)

To test the DAG without waiting for the schedule:
1. Click on `daily_market_etl` to open the DAG detail view
2. Click the "Play" button (▶) in the top right corner
3. Click "Trigger DAG" in the confirmation dialog
4. Watch the progress in the Graph view

The Graph view shows each task as a box. Colors indicate status: green (success), red (failed), yellow (running), white (pending). Click any task to view its logs.

---

## 9. Starting the Dashboard

The Streamlit dashboard provides interactive visualizations for exploring the data. It queries the analytics schema directly, showing the transformed data from our dimensional model.

### 9.1 Option A: Run Locally (Recommended for Development)

```bash
streamlit run dashboard/app.py
```

Running locally is recommended during development because you can see code changes immediately (Streamlit auto-reloads), errors are easier to debug with full tracebacks, and there's no Docker overhead.

The dashboard opens automatically at http://localhost:8501

**Why Streamlit over Dash, Flask, or React?**

Streamlit requires minimal code to create interactive dashboards. A complete page with charts, filters, and data tables can be built in under 100 lines of Python.

Dash (by Plotly) is more flexible but requires more boilerplate code and understanding of callbacks. It's better for complex, production applications.

Flask would require writing HTML templates and JavaScript, significantly more work for a portfolio project.

React would require learning JavaScript/TypeScript and maintaining a separate frontend codebase. It's the right choice for large teams with dedicated frontend developers.

### 9.2 Option B: Run in Docker

```bash
docker compose --profile dashboard up -d dashboard
```

The `--profile dashboard` flag starts services tagged with the "dashboard" profile. Profiles allow optional services that aren't started by default with `docker compose up`.

Running in Docker is useful when you want consistent behavior across different machines or when deploying to a server.

Access at http://localhost:8501

### 9.3 Explore the Dashboard

The dashboard has multiple pages accessible from the sidebar:

**Home** provides a welcome page with key metrics: total stocks tracked, date range covered, and quick statistics.

**Market Overview** shows stock price trends, top gainers and losers, and volume analysis. Interactive charts let you zoom into specific time periods.

**Sector Analysis** compares performance across sectors (finance, commodities, retail, etc.). This helps identify which parts of the economy are outperforming.

**Macro Correlation** analyzes relationships between economic indicators (SELIC rate, USD/BRL exchange rate) and stock performance. This is the core value proposition of combining stock and macro data.

**Stock Screener** lets you filter stocks by various criteria (sector, price range, return threshold) and view detailed information.

---

## 10. Running Tests

Testing ensures code quality and catches bugs before they reach production. This project includes unit tests for Python code and data quality tests through dbt.

### 10.1 Run Python Unit Tests

```bash
pytest tests/ -v
```

The `pytest` command discovers and runs all test files matching the pattern `test_*.py` in the specified directory.

The `-v` flag enables verbose output, showing each test name and its result. Without it, pytest only shows a summary.

Expected output:
```
========================= test session starts ==========================
collected 15 items

tests/test_stock_extractor.py::TestStockExtractor::test_initialization_with_default_tickers PASSED
tests/test_stock_extractor.py::TestStockExtractor::test_initialization_with_custom_tickers PASSED
...
========================= 15 passed in 2.34s ===========================
```

**What do the tests cover?**

Extractor initialization tests verify that classes can be instantiated with both default and custom configurations.

Extraction tests use mock API responses to verify data is correctly parsed and transformed into DataFrames.

Database loading tests verify that data is correctly written to the database with proper handling of duplicates.

### 10.2 Run Tests with Coverage Report

```bash
pytest tests/ -v --cov=extract --cov=load --cov-report=term-missing
```

The `--cov` flag measures code coverage, showing which lines of code were executed during tests.

The `--cov-report=term-missing` option shows uncovered lines directly in the terminal output.

**Why measure coverage?**

Coverage helps identify untested code paths. While 100% coverage doesn't guarantee bug-free code, low coverage areas are more likely to contain hidden bugs.

### 10.3 Run Data Validation Script

```bash
python scripts/validate_data.py
```

This script runs comprehensive data quality checks beyond what dbt tests cover. It checks for orphaned records, statistical outliers, and temporal consistency.

Expected output:
```
============================================================
DATA VALIDATION REPORT
Generated: 2026-02-09 10:30:00
============================================================

VALIDATION RESULTS
------------------------------------------------------------

raw.stocks:
  total_rows: 54370
  unique_tickers: 20
  date_range: 2015-01-02 to 2026-02-06
  null_prices: 0
  invalid_prices: 0
  Status: ✓ PASS

...

============================================================
ALL VALIDATIONS PASSED ✓
```

---

## 11. Daily Operations

Once everything is set up, here are common daily operations for maintaining the pipeline.

### 11.1 Start All Services

```bash
docker compose up -d
```

Without specifying services, this starts all non-profile services defined in docker-compose.yml.

The `-d` detached mode is important for daily use; you don't want terminal windows tied up by container output.

### 11.2 Check Service Health

```bash
docker compose ps
```

Run this periodically to verify all services are healthy. Unhealthy services might indicate resource issues, crashes, or configuration problems.

### 11.3 View Logs

```bash
# All services - useful for initial debugging
docker compose logs -f

# Specific service - useful for targeted investigation
docker compose logs -f postgres
docker compose logs -f airflow-scheduler
```

The `-f` flag follows logs in real-time (like `tail -f`). Press `Ctrl+C` to stop following.

Logs are your primary debugging tool. When something fails, check logs for error messages, stack traces, and context about what happened before the failure.

### 11.4 Run Incremental Load Manually

If you need to run the daily load outside of Airflow (perhaps Airflow is down or you want to test changes):

```bash
python -c "
from extract.stock_extractor import StockExtractor
from extract.bcb_extractor import BCBExtractor
import os

db_url = 'postgresql://dataeng:dataeng123@localhost:5432/brazilian_market'

# Extract stocks
stock_ext = StockExtractor(db_url)
df = stock_ext.extract_incremental(lookback_days=5)
stock_ext.load_to_database(df)

# Extract indicators
bcb_ext = BCBExtractor(db_url)
df = bcb_ext.extract_incremental(lookback_days=5)
bcb_ext.load_to_database(df)

print('Incremental load complete!')
"
```

Then run dbt to transform the new data:
```bash
cd dbt_project && dbt run && cd ..
```

**Why lookback_days=5?**

The 5-day lookback window handles cases where data arrives late. Some data sources update historical values (like adjusted prices after stock splits). Looking back 5 days ensures we capture any retroactive updates.

---

## 12. Stopping Services

Proper shutdown preserves data integrity and frees system resources.

### 12.1 Stop All Services (Keep Data)

```bash
docker compose down
```

This stops and removes containers but preserves volumes (where PostgreSQL data is stored). Your data remains intact for the next time you start the services.

**Why "down" instead of "stop"?**

`docker compose stop` stops containers but doesn't remove them. They remain in a stopped state, consuming some resources. `docker compose down` removes stopped containers, cleaning up completely. Both preserve volumes.

### 12.2 Stop and Remove Everything (Including Data)

```bash
docker compose down -v
```

**Warning**: The `-v` flag deletes volumes, permanently destroying all database data. Only use this when you want a completely fresh start.

Use cases for `-v`:
- Starting over after a corrupted state
- Testing initialization scripts
- Freeing disk space when no longer using the project

### 12.3 Stop Individual Services

```bash
docker compose stop postgres
docker compose stop airflow-webserver airflow-scheduler
```

Stopping individual services is useful when debugging or when you only need certain components. For example, you might stop Airflow while keeping the database running for manual queries.

### 12.4 Deactivate Python Virtual Environment

```bash
deactivate
```

This reverses the `source venv/bin/activate` command, restoring your PATH to use the system Python. It's good practice to deactivate when you're done working on the project, preventing confusion about which Python environment you're using.

---

## 13. Troubleshooting

This section addresses common issues and their solutions.

### Problem: Docker permission denied

**Symptoms:**
```
permission denied while trying to connect to the docker API at unix:///var/run/docker.sock
```

**Cause:** Your user isn't in the docker group, or group membership hasn't taken effect.

**Solution:**
```bash
sudo usermod -aG docker $USER
# Then log out and log back in completely
# Or use: newgrp docker (creates a new shell with the group)
```

### Problem: Docker context pointing to Docker Desktop

**Symptoms:**
```
failed to connect to the docker API at unix:///home/user/.docker/desktop/docker.sock
```

**Cause:** Docker CLI is configured to use Docker Desktop, which isn't running or installed.

**Solution:**
```bash
docker context use default
```

### Problem: pip install fails with compilation errors (Python 3.14)

**Symptoms:**
```
error: too few arguments to function '_PyLong_AsByteArray'
```

**Cause:** Package doesn't support Python 3.14's updated C API.

**Solution 1:** The requirements.txt uses flexible versions (`>=`) that should work. Try:
```bash
pip cache purge
pip install -r requirements.txt
```

**Solution 2:** Install build dependencies:
```bash
sudo pacman -S base-devel
```

**Solution 3:** Use Python 3.11 or 3.12:
```bash
python3.12 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Problem: Port 5432 already in use

**Symptoms:**
```
Error starting userland proxy: listen tcp4 0.0.0.0:5432: bind: address already in use
```

**Cause:** Another process (likely a local PostgreSQL) is using port 5432.

**Solution 1:** Stop the local PostgreSQL:
```bash
sudo systemctl stop postgresql
```

**Solution 2:** Change the port in `.env`:
```
POSTGRES_PORT=5433
```
Then rebuild: `docker compose down && docker compose up -d`

### Problem: BCB API returns 406 error

**Symptoms:**
```
406 Client Error: Not Acceptable for url: https://api.bcb.gov.br/dados/serie/...
```

**Cause:** The BCB API limits daily data series to a 10-year query window.

**Solution:** The extraction code automatically adjusts date ranges. If you still see this error, verify you're using the latest code that includes this fix.

### Problem: Database connection refused

**Symptoms:**
```
connection to server at "localhost" (127.0.0.1), port 5432 failed: Connection refused
```

**Cause:** PostgreSQL container isn't running or hasn't finished starting.

**Solution:**
```bash
# Check container status
docker compose ps

# Wait for healthy status
docker compose ps | grep postgres
# Should show (healthy)

# Check logs for errors
docker compose logs postgres
```

### Problem: dbt can't connect to database

**Symptoms:**
```
Could not connect to the database
```

**Cause:** Environment variables not set or profiles.yml misconfigured.

**Solution:**
```bash
# Set environment variables
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5432
export POSTGRES_USER=dataeng
export POSTGRES_PASSWORD=dataeng123
export POSTGRES_DB=brazilian_market

# Or source from .env file
export $(cat .env | grep -v '^#' | xargs)

# Verify with dbt debug
cd dbt_project && dbt debug
```

### Problem: Yahoo Finance returns no data

**Symptoms:**
```
No data returned for TICKER.SA
```

**Cause:** Ticker might be delisted, renamed, or API is rate-limiting.

**Solution:**
- Wait a few minutes and retry (rate limiting)
- Verify ticker symbols are correct (should end with `.SA` for Brazilian stocks)
- Check if the stock was delisted or renamed (search B3 website)

---

## Quick Reference Commands

| Action | Command |
|--------|---------|
| Start database | `sudo docker compose up -d postgres` |
| Start all services | `sudo docker compose up -d` |
| Check status | `sudo docker compose ps` |
| View logs | `sudo docker compose logs -f` |
| Run backfill | `python scripts/backfill_data.py` |
| Start dbt container | `sudo docker compose --profile dbt up -d dbt` |
| Run dbt (all steps) | `./scripts/run_dbt.sh all` |
| Run dbt models only | `./scripts/run_dbt.sh run` |
| Run dbt tests only | `./scripts/run_dbt.sh test` |
| Run Python tests | `pytest tests/ -v` |
| Start dashboard | `streamlit run dashboard/app.py` |
| Stop services | `sudo docker compose down` |
| Reset everything | `sudo docker compose down -v` |

---

## Success Checklist

After completing this guide, verify you have:

- [ ] PostgreSQL running in Docker with schemas created (raw, staging, analytics)
- [ ] Raw data loaded (~54,000 stock rows, ~9,000 indicator rows)
- [ ] dbt models executed successfully (9 models)
- [ ] dbt tests passing (15+ tests)
- [ ] Dashboard accessible at localhost:8501
- [ ] Airflow accessible at localhost:8080 (optional)

Congratulations! Your Brazilian Market ETL Pipeline is now fully operational.
