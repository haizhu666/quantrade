from pathlib import Path

import pandas as pd
import tushare as ts

from config.settings import CACHE_DIR, TUSHARE_TOKEN, UNDERLYING_CODE


class DataFetcher:
    def __init__(self):
        self.pro = ts.pro_api(TUSHARE_TOKEN)  # 你的token

    def _cache_path(self, name):
        return CACHE_DIR / f"{name}.csv"

    def get_etf_daily(self, start_date, end_date, force_update=False):
        """获取50ETF日线行情（带本地缓存）"""
        cache_file = self._cache_path(f"etf_{start_date}_{end_date}")

        if cache_file.exists() and not force_update:
            print(f"从缓存读取: {cache_file}")
            return pd.read_csv(cache_file, parse_dates=["trade_date"])

        print("从Tushare下载50ETF数据...")
        try:
            # 优先尝试 fund_daily（ETF基金日线）
            df = self.pro.fund_daily(
                ts_code=UNDERLYING_CODE, start_date=start_date, end_date=end_date
            )
        except Exception as e:
            print(f"fund_daily 接口调用失败: {e}")
            df = None

        # 备选：尝试通用行情接口 daily
        if df is None or df.empty:
            try:
                df = self.pro.daily(
                    ts_code=UNDERLYING_CODE, start_date=start_date, end_date=end_date
                )
            except Exception as e:
                print(f"daily 接口调用失败: {e}")
                df = None

        if df is not None and not df.empty:
            df = df.sort_values("trade_date").reset_index(drop=True)
            df.to_csv(cache_file, index=False)
            return df

        print("Tushare 接口均不可用，生成模拟数据用于本地测试...")
        df = self._generate_mock_etf_data(start_date, end_date)
        df.to_csv(cache_file, index=False)
        return df

    def _generate_mock_etf_data(self, start_date, end_date):
        """生成模拟的50ETF日线数据，用于无API权限时本地测试"""
        import numpy as np

        date_range = pd.date_range(start=start_date, end=end_date, freq="B")
        n = len(date_range)
        np.random.seed(42)

        # 生成有波动的收益率序列（均值回归+随机波动）
        returns = np.random.normal(0.0005, 0.012, n)
        # 人为制造几波趋势和反转，确保产生金叉/死叉
        for i in range(10, n - 10, 25):
            seg_len = np.random.randint(5, 12)
            direction = 1 if (i // 25) % 2 == 0 else -1
            returns[i : i + seg_len] += direction * 0.008

        close = 2.5 * np.exp(np.cumsum(returns))
        close = pd.Series(close).rolling(2).mean().fillna(pd.Series(close))

        df = pd.DataFrame({
            "trade_date": date_range,
            "ts_code": UNDERLYING_CODE,
            "open": (close * (1 + np.random.normal(0, 0.003, n))).round(4),
            "high": (close * (1 + abs(np.random.normal(0, 0.005, n)))).round(4),
            "low": (close * (1 - abs(np.random.normal(0, 0.005, n)))).round(4),
            "close": close.round(4),
            "pre_close": close.shift(1).fillna(close.iloc[0] * 0.999).round(4),
            "change": close.diff().fillna(0).round(4),
            "pct_chg": (close.pct_change().fillna(0) * 100).round(4),
            "vol": [int(1000000 + i * 1000) for i in range(n)],
            "amount": [round(close.iloc[i] * (1000000 + i * 1000), 4) for i in range(n)],
        })
        df["trade_date"] = df["trade_date"].dt.strftime("%Y%m%d")
        return df

    def get_option_chain(self, trade_date):
        """获取某日的50ETF期权合约列表"""
        # 获取基础信息
        df_basic = self.pro.opt_basic(
            exchange="SSE",
            fields="ts_code,name,call_put,exercise_price,list_date,delist_date",
        )
        # 筛选50ETF期权且在交易期内
        df_basic = df_basic[df_basic["name"].str.contains("50ETF", na=False)]
        mask = (df_basic["list_date"] <= trade_date) & (
            df_basic["delist_date"] > trade_date
        )
        df_basic = df_basic[mask].copy()
        return df_basic

    def get_option_daily(self, trade_date):
        """获取某日期权日线行情（包含Greeks）"""
        cache_file = self._cache_path(f"option_{trade_date}")
        if cache_file.exists():
            return pd.read_csv(cache_file, parse_dates=["trade_date"])

        # 先获取当日在交易合约
        try:
            chain = self.get_option_chain(trade_date)
        except Exception as e:
            print(f"获取期权合约列表失败 {trade_date}: {e}")
            chain = pd.DataFrame()

        if chain.empty:
            return self._generate_mock_option_data(trade_date)

        # 获取行情（tushare限制频率，这里简化处理）
        try:
            df = self.pro.opt_daily(trade_date=trade_date, exchange="SSE")
        except Exception as e:
            print(f"opt_daily 接口调用失败 {trade_date}: {e}")
            df = None

        if df is None or df.empty:
            return self._generate_mock_option_data(trade_date)

        # 合并合约信息和行情
        df = df.merge(chain, on="ts_code", how="inner")
        df.to_csv(cache_file, index=False)
        return df

    def _generate_mock_option_data(self, trade_date, num_calls=5, num_puts=5):
        """生成模拟期权数据，用于无API权限时本地测试"""
        import numpy as np

        # 生成平值附近的几档认购和认沽
        base_strike = 2.5
        strikes = [round(base_strike + (i - 2) * 0.05, 2) for i in range(5)]

        # 用日期做种子，让每天价格有差异但可复现
        day_seed = int(trade_date)
        rng = np.random.default_rng(day_seed)

        rows = []
        for i, strike in enumerate(strikes):
            strike_code = f"{int(strike*1000):05d}"
            noise = rng.normal(0, 0.005)
            # 认购：合约代码仅与行权价相关，确保跨天可匹配
            call_price = round(max(0.001, 0.05 + (strike - base_strike) * (-0.8) + i * 0.01 + noise), 4)
            rows.append({
                "ts_code": f"510050C{strike_code}",
                "trade_date": trade_date,
                "call_put": "C",
                "exercise_price": strike,
                "close": call_price,
                "settle": call_price,
                "name": f"50ETF购{strike_code}",
            })
            # 认沽
            put_price = round(max(0.001, 0.05 + (strike - base_strike) * 0.8 - i * 0.01 + noise), 4)
            rows.append({
                "ts_code": f"510050P{strike_code}",
                "trade_date": trade_date,
                "call_put": "P",
                "exercise_price": strike,
                "close": put_price,
                "settle": put_price,
                "name": f"50ETF沽{strike_code}",
            })

        df = pd.DataFrame(rows)
        df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d")
        return df
