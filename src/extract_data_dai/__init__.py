"""数据抽取"""

import json
import os
from datetime import datetime, timedelta

import structlog
from bigmodule import I  # noqa: N812
from jinja2 import Template

# metadata
# 模块作者
author = "BigQuant"
# 模块分类
category = "数据"
# 模块显示名
friendly_name = "数据抽取(DAI)"
# 文档地址, optional
doc_url = "https://bigquant.com/wiki/doc/aistudio-HVwrgP4J1A#h-数据抽取dai5"
# 是否自动缓存结果
cacheable = True

logger = structlog.get_logger()


def run(
    sql: I.port("数据SQL，需要抽取的数据", specific_type_name="DataSource"),
    start_date: I.str("开始日期/start_date，用于替换sql中的开始日期，如果有的话。示例 2023-01-01") = "2020-01-01",
    start_date_bound_to_trading_date: I.bool("开始日期绑定交易日，在模拟和实盘交易模式下，开始日期替换为交易日") = False,
    end_date: I.str("结束日期/end_date，示例 2023-01-01") = "2020-12-31",
    end_date_bound_to_trading_date: I.bool("结束日期绑定交易日，在模拟和实盘交易模式下，结束日期替换为交易日") = False,
    before_start_days: I.int("历史数据向前取的天数，实际开始日期会减去此天数，用于计算需要向前历史数据的因子，比如 m_lag(close, 10)，需要向前去10天数据") = 90,
    debug: I.bool("调试模式，显示调试日志") = False,
) -> [I.port("数据", "data")]:
    """DAI 数据抽取模块。根据给定的DAI SQL，抽取数据。"""
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
    # 对于旧版升级上来的, 给出 warning 提示
    if "{" in sql and "{{" not in sql:
        import re

        # 定义一个用于检测 {start_date} 和 {end_date}（包括其中可能的空格）的正则表达式
        pattern = r"\{\s*start_date\s*\}|\{\s*end_date\s*\}"
        # 使用正则表达式搜索字符串
        if re.search(pattern, sql):
            logger.warning("检测到旧版的 { start_date } { end_date }, 在新版请使用 {{ start_date }} {{ end_date }}")

    sql = Template(sql).render(start_date=start_date, end_date=end_date)
    if debug:
        logger.debug(sql)

    # 自动解析日期时间的两种格式
    def parse_date_fmt(date):
        formats = ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]
        for fmt in formats:
            try:
                datetime.strptime(date, fmt)
                return fmt
            except ValueError:
                continue
        raise ValueError(f'时间格式不正确: {date}, 应为 "YYYY-MM-DD" 或 "YYYY-MM-DD HH:MM:SS"')

    date_format = parse_date_fmt(start_date)
    if before_start_days >= 0:
        query_start_date = (datetime.strptime(start_date, date_format) - timedelta(days=before_start_days)).strftime(date_format)
    else:
        query_start_date = start_date

    try:
        if int(os.getenv("CPU_LIMIT", 1)) < 4:
            # DAI 支持并行计算，更多计算资源，更省时间
            logger.warning(f'{start_date=}, {end_date=}, {query_start_date=} (支持加速 [url="command:switch-quota"]升级资源[/url]) ..')
    except:
        pass
    data = dai.query(sql, filters={"date": [query_start_date, end_date]}).df()
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
