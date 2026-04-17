# strategies/base.py
from abc import ABC, abstractmethod
from typing import Union

import pandas as pd


class BaseStrategy(ABC):
    def __init__(self, name: str = "BaseStrategy"):
        self.name = name

    @abstractmethod
    def generate_signals(
        self, etf_data: pd.DataFrame, option_data: pd.DataFrame
    ) -> Union[pd.DataFrame, pd.Series]:
        """
        根据当日数据生成交易信号
        返回 DataFrame，columns至少包含: [trade_date, signal, option_code, action]
        signal: 1做多, -1做空, 0空仓
        action: "buy" / "sell"
        """
        pass

    def on_data(self, context):
        """每天调用一次，context包含当日所有数据"""
        pass
