"""Data Extraction"""

import json
import os
from datetime import timedelta
import pandas as pd

from jinja2 import Template
import structlog

from bigmodule import I  # noqa: N812

# metadata
# Module author
author = "AFE"
# Module category
category = "Data"
# Module display name
friendly_name = "Data Extraction (DAI)"
# Documentation URL, optional
doc_url = "wiki/doc/aistudio-HVwrgP4J1A#h-数据抽取dai5"
# Whether to automatically cache results
cacheable = True

logger = structlog.get_logger()


def run(
    sql: I.port("Data SQL, the data to be extracted", specific_type_name="DataSource"),
    start_date: I.str("Start date/start_date, used to replace the start date in the SQL if applicable. Example: 2023-01-01") = "2020-01-01",
    start_date_bound_to_trading_date: I.bool("Bind start date to trading date, in simulation and live trading modes, the start date is replaced by the trading date") = False,
    end_date: I.str("End date/end_date, example: 2023-01-01") = "2020-12-31",
    end_date_bound_to_trading_date: I.bool("Bind end date to trading date, in simulation and live trading modes, the end date is replaced by the trading date") = False,
    before_start_days: I.int("Number of days to look back for historical data, the actual start date will be reduced by this number of days, used for calculating factors that require historical data, e.g., m_lag(close, 10), which requires looking back 10 days") = 90,
    keep_before: I.bool("Keep the data from the look-back period (note: ensure 'Remove Nulls' is unchecked in the preceding 'Input Features' module)") = False,
    debug: I.bool("Debug mode, show debug logs") = False,
) -> [I.port("Data", "data")]:
    """DAI Data Extraction Module. Extracts data based on the provided DAI SQL."""
    import dai

    sql = sql.read()
    if isinstance(sql, dict):
        sql = sql["sql"]

    trading_date = os.environ.get("TRADING_DATE")
    if trading_date:
        if start_date_bound_to_trading_date:
            start_date = trading_date
        if end_date_bound_to_trading_date:
            end_date = trading_date

    # TODO: remove this in future
    # For old versions upgraded, provide a warning prompt
    if "{" in sql and "{{" not in sql:
        import re

        # Define a regex pattern to detect {start_date} and {end_date} (including possible spaces)
        pattern = r"\{\s*start_date\s*\}|\{\s*end_date\s*\}"
        # Search the string using the regex pattern
        if re.search(pattern, sql):
            logger.warning("Detected old-style { start_date } { end_date }, please use {{ start_date }} {{ end_date }} in the new version")

    sql = Template(sql).render(start_date=start_date, end_date=end_date)
    if debug:
        logger.debug(sql)

    if before_start_days >= 0:
        query_start_date = (pd.to_datetime(start_date) - timedelta(days=before_start_days)).strftime("%Y-%m-%d %H:%M:%S")
    else:
        query_start_date = start_date

    try:
        if int(os.getenv("CPU_LIMIT", 1)) < 4:
            # DAI supports parallel computing; more resources save time
            logger.warning(f'{start_date=}, {end_date=}, {query_start_date=} (support acceleration [url="command:switch-quota"]upgrade resources[/url]) ..')
    except:
        pass
    data = dai.query(sql, filters={"date": [query_start_date, end_date]}).df()
    if keep_before:
        start_date = query_start_date
    if "date" in data.columns:
        data = data[(data["date"] >= start_date) & (data["date"] <= end_date)]
    logger.info(f"data extracted: {data.shape}")
    if len(data) == 0:
        logger.warning("data extracted: 0 rows")
    if debug:
        logger.debug(f"data head: {data.head()}")
        logger.debug(f"data tail: {data.tail()}")

    outputs = I.Outputs(
        data=dai.DataSource.write_bdb(data, extra=json.dumps({"start_date": start_date, "end_date": end_date, "before_start_days": before_start_days})),
    )
    return outputs


def post_run(outputs):
    """Post-run function"""
    return outputs


def cache_key(kwargs):
    """Cache key"""
    if os.environ.get("TRADING_DATE"):
        if kwargs.get("start_date_bound_to_trading_date") or kwargs.get("end_date_bound_to_trading_date"):
            # If trading date can be obtained, it means paper/live trading is in progress; disable cache
            logger.warning("cache disabled for paper/live trading for bound date")
            return None
        if kwargs.get("end_date", None) == os.environ.get("TRADING_DATE", None):
            logger.warning("cache disabled for paper/live trading for trading date")
            return None

    return kwargs