import pandas as pd

from strategies.base import BaseStrategy


class TrendFollowStrategy(BaseStrategy):
    """
    最简单的50ETF趋势策略：
    - 价格上穿5日均线 → 买入近月平值认购（赌继续上涨）
    - 价格下穿5日均线 → 平仓
    """

    def __init__(self, ma_window=5):
        super().__init__("TrendFollow")
        self.ma_window = ma_window

    def generate_signals(self, etf_data, option_data):
        if etf_data is None or len(etf_data) < self.ma_window:
            return pd.DataFrame()

        # 计算均线
        etf_data = etf_data.copy()
        etf_data["ma"] = etf_data["close"].rolling(self.ma_window).mean()

        # 生成信号：上穿买入，下穿卖出
        etf_data["prev_close"] = etf_data["close"].shift(1)
        etf_data["prev_ma"] = etf_data["ma"].shift(1)

        # 买入信号：昨天收盘价低于均线，今天高于均线
        buy_signal = (etf_data["prev_close"] < etf_data["prev_ma"]) & (
            etf_data["close"] > etf_data["ma"]
        )

        # 卖出信号：昨天收盘价高于均线，今天低于均线
        sell_signal = (etf_data["prev_close"] > etf_data["prev_ma"]) & (
            etf_data["close"] < etf_data["ma"]
        )

        etf_data["signal"] = 0
        etf_data.loc[buy_signal, "signal"] = 1
        etf_data.loc[sell_signal, "signal"] = -1

        # 选择有信号的日期
        signals = etf_data[etf_data["signal"] != 0][
            ["trade_date", "signal", "close"]
        ].copy()
        return signals
