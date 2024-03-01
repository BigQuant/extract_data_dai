"""数据抽取"""
import json
import os
from datetime import datetime, timedelta
import structlog

from bigmodule import I  # noqa: N812

# metadata
# 模块作者
author = "BigQuant"
# 模块分类
category = "特征抽取"
# 模块显示名
friendly_name = "数据抽取(DAI)"
# 文档地址, optional
doc_url = "https://bigquant.com/wiki/doc/aistudio-aiide-NzAjgKapzW#h-数据抽取"
# 是否自动缓存结果
cacheable = True

logger = structlog.get_logger()


def run(
    sql: I.port("数据SQL，需要抽取的数据", specific_type_name="DataSource"),
    start_date: I.str("开始日期/start_date，用于替换sql中的开始日期，如果有的话。示例 2023-01-01") = "2020-01-01",
    start_date_bound_to_trading_date: I.bool("开始日期绑定交易日，在模拟和实盘交易模式下，开始日期替换为交易日") = False,
    end_date: I.str("结束日期/end_date，示例 2023-01-01") = "2020-12-31",
    end_date_bound_to_trading_date: I.bool("结束日期绑定交易日，在模拟和实盘交易模式下，结束日期替换为交易日") = False,
    before_start_days: I.int("历史数据向前取的天数，世纪开始日期会减去此天数，用于计算需要向前历史数据的因子，比如 m_lag(close, 10)，需要向前去10天数据") = 90,
    debug: I.bool("调试模式，显示调试日志") = False,
) -> [I.port("数据", "data")]:
    """DAI 数据抽取模块。根据给定的DAI SQL，抽取数据。"""
    import dai

    sql = sql.read_text()

    trading_date = os.environ.get("TRADING_DATE")
    if trading_date:
        if start_date_bound_to_trading_date:
            start_date = trading_date
        if end_date_bound_to_trading_date:
            end_date = trading_date

    sql = sql.format(start_date=start_date, end_date=end_date)
    if debug:
        logger.debug(sql)

    date_format = "%Y-%m-%d"
    if before_start_days >= 0:
        query_start_date = (datetime.strptime(start_date, date_format) - timedelta(days=before_start_days)).strftime(date_format)
    else:
        query_start_date = start_date

    logger.info(f'{start_date=}, {end_date=}, {query_start_date=} (支持加速 [url="command:switch-quota"]升级资源[/url]) ..')
    data = dai.query(sql, filters={"date": [query_start_date, end_date]}).df()
    if "date" in data.columns:
        data = data[(data["date"] >= start_date) & (data["date"] <= end_date)]
    logger.info(f"data extracted: {data.shape}")
    if debug:
        logger.debug(f"data head: {data.head()}")
        logger.debug(f"data tail: {data.tail()}")

    outputs = I.Outputs(
        data=dai.DataSource.write_bdb(data, extra=json.dumps({"start_date": start_date, "end_date": end_date})),
    )
    return outputs


def post_run(outputs):
    """后置运行函数"""
    return outputs


def cache_key(kwargs):
    """缓存 key"""
    if os.environ.get("TRADING_DATE"):
        if kwargs.get("start_date_bound_to_trading_date") or kwargs.get("end_date_bound_to_trading_date"):
            # 如果能获取到交易日则证明在进行模拟交易或实盘交易，返回None禁用缓存
            logger.warning("cache disabled for paper/live trading for bound date")
            return None
        if kwargs.get("end_date", None) == os.environ.get("TRADING_DATE", None):
            logger.warning("cache disabled for paper/live trading for trading date")
            return None

    return kwargs
