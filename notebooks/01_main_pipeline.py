# Databricks notebook source

# 1. Setup
from src.pipeline.main import run_full_pipeline
from src.logger_config import setup_logger
import config

logger = setup_logger(__name__)

# 2. Widgets for Parameterization
dbutils.widgets.text("run_date", "YYYY-MM-DD", "Run Date for Pipeline")
run_date = dbutils.widgets.get("run_date")

# 3. Execution
logger.info(f"Starting full pipeline for date: {run_date}")
try:
    # O objeto 'spark' já está disponível no Databricks
    run_full_pipeline(spark=spark, run_date=run_date)
    logger.info("Pipeline completed successfully.")
except Exception as e:
    logger.critical(f"Pipeline failed: {e}", exc_info=True)
    raise 