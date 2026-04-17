import pandas as pd


class VectorizedBacktest:
    """
    简化版向量化回测：
    假设每天收盘按收盘价成交，不考虑滑点和手续费（Phase 2再加入）
    """

    def __init__(self, init_cash=100000):
        self.init_cash = init_cash
        self.cash = init_cash
        self.positions = {}  # 当前持仓 {option_code: {"qty": 1, "cost": 0.05}}
        self.history = []  # 每日净值记录

    def run(
        self, etf_data: pd.DataFrame, signals: pd.DataFrame, option_data_dict: dict
    ):
        """
        etf_data: 50ETF日线
        signals: 策略信号
        option_data_dict: {trade_date: option_df} 每日期权数据
        """
        etf_data = etf_data.copy()
        etf_data["trade_date"] = pd.to_datetime(etf_data["trade_date"])

        signals = signals.copy()
        signals["trade_date"] = pd.to_datetime(signals["trade_date"])

        portfolio_values = []

        for _, row in etf_data.iterrows():
            date = pd.to_datetime(row["trade_date"])
            date_str = date.strftime("%Y%m%d")

            # 检查当日是否有信号
            day_signal = signals[signals["trade_date"] == date]
            # 防止空 signals 导致后续报错
            if day_signal.empty:
                day_signal = pd.DataFrame()

            if not day_signal.empty:
                signal = day_signal.iloc[0]["signal"]

                if signal == 1 and not self.positions:  # 买入信号且空仓
                    # 选近月平值认购（简化：选当天最接近收盘价的认购）
                    opts = option_data_dict.get(date_str)
                    if opts is not None and not opts.empty:
                        calls = opts[opts["call_put"] == "C"].copy()
                        if not calls.empty:
                            # 找到最接近平值的
                            calls["dist"] = abs(calls["exercise_price"] - row["close"])
                            target = calls.sort_values("dist").iloc[0]
                            # 买入1张（假设1张=10000份，简化处理）
                            cost = (
                                float(target["close"])
                                if not pd.isna(target["close"])
                                else float(target["settle"])
                            )
                            # 支付权利金
                            self.cash -= cost * 10000
                            self.positions = {
                                "code": target["ts_code"],
                                "qty": 1,
                                "cost": cost,
                                "entry_date": date,
                            }
                            print(f"{date_str}: 买入 {target['ts_code']} @ {cost:.4f}")

                elif signal == -1 and self.positions:  # 卖出信号且持仓
                    # 平仓（按当天收盘价估算）
                    opts = option_data_dict.get(date_str)
                    if opts is not None and not opts.empty:
                        hold_code = self.positions["code"]
                        current = opts[opts["ts_code"] == hold_code]
                        if not current.empty:
                            sell_price = float(
                                current.iloc[0]["close"]
                                if not pd.isna(current.iloc[0]["close"])
                                else current.iloc[0]["settle"]
                            )
                            # 收回权利金
                            self.cash += sell_price * 10000
                            profit = (sell_price - self.positions["cost"]) * 10000
                            print(
                                f"{date_str}: 平仓 {hold_code} @ {sell_price:.4f}, 盈亏: {profit:.2f}"
                            )
                            self.positions = {}
                        else:
                            # 合约不在当日行情中，按成本价强制平仓（避免无限持仓）
                            print(f"{date_str}: 警告 持仓合约 {hold_code} 无行情，按成本价强制平仓")
                            self.cash += self.positions["cost"] * 10000
                            self.positions = {}
                    else:
                        # 无当日期权数据，按成本价强制平仓（避免无限持仓）
                        print(f"{date_str}: 警告 无当日期权数据，持仓按成本价强制平仓")
                        self.cash += self.positions["cost"] * 10000
                        self.positions = {}

            # 计算当日净值
            nav = self.cash
            if self.positions:
                # 尝试按当日期权市场价计算市值，无数据则按成本价
                opts = option_data_dict.get(date_str)
                hold_code = self.positions["code"]
                market_price = self.positions["cost"]
                if opts is not None and not opts.empty:
                    current = opts[opts["ts_code"] == hold_code]
                    if not current.empty:
                        market_price = float(
                            current.iloc[0]["close"]
                            if not pd.isna(current.iloc[0]["close"])
                            else current.iloc[0]["settle"]
                        )
                nav += market_price * 10000

            portfolio_values.append(
                {
                    "date": date,
                    "nav": nav,
                    "cash": self.cash,
                    "holding": 1 if self.positions else 0,
                }
            )

        return pd.DataFrame(portfolio_values)
