import inspect

import pandas as pd

from backtest.vectorized import VectorizedBacktest
from config.settings import END_DATE, INIT_CASH, START_DATE
from data.fetcher import DataFetcher
from strategies.trend_follow import TrendFollowStrategy


def main():
    # 1. 获取数据
    fetcher = DataFetcher()
    etf_data = fetcher.get_etf_daily(START_DATE, END_DATE)

    if etf_data is None or etf_data.empty:
        print("错误: 无法获取ETF数据，回测终止。")
        return

    # 2. 生成信号
    strategy = TrendFollowStrategy(ma_window=5)
    signals = strategy.generate_signals(etf_data, pd.DataFrame())

    if signals.empty:
        print("警告: 未生成任何交易信号。")

    # 获取期权数据（优先获取有信号的日期，确保回测能正常执行）
    trade_dates = etf_data["trade_date"].tolist()
    # 先收集所有可能有信号的日期（信号日 + 每5天采样）
    signal_dates = set()
    if not signals.empty:
        signal_dates = set(
            pd.to_datetime(signals["trade_date"]).dt.strftime("%Y%m%d").tolist()
        )
    sample_dates = set(
        (date.replace("-", "") if isinstance(date, str) else date.strftime("%Y%m%d"))
        for date in trade_dates[::5]
    )
    dates_to_fetch = sorted(signal_dates | sample_dates)

    option_data_dict = {}
    for date_str in dates_to_fetch:
        try:
            opt_df = fetcher.get_option_daily(date_str)
        except Exception as e:
            print(f"获取期权数据失败 {date_str}: {e}")
            opt_df = pd.DataFrame()
        if not opt_df.empty:
            option_data_dict[date_str] = opt_df

    # 3. 回测
    engine = VectorizedBacktest(init_cash=INIT_CASH)
    result = engine.run(etf_data, signals, option_data_dict)

    # 4. 简单绩效
    if result is not None and not result.empty:
        final_nav = result["nav"].iloc[-1]
        total_return = (final_nav - INIT_CASH) / INIT_CASH * 100
        print(f"\n回测完成:")
        print(f"初始资金: {INIT_CASH}")
        print(f"最终资金: {final_nav:.2f}")
        print(f"总收益率: {total_return:.2f}%")

        # 保存结果
        result.to_csv("backtest_result.csv", index=False)
        print("结果已保存到 backtest_result.csv")


if __name__ == "__main__":
    main()
