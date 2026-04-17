"""所有配置集中在这里，修改时不用到处找代码"""

from pathlib import Path

# 项目根目录
BASE_DIR = Path(__file__).parent.parent

# Tushare Token（去官网注册获取）
TUSHARE_TOKEN = "你的token"

# 标的
UNDERLYING_CODE = "510050.SH"  # 上证50ETF

# 回测设置
START_DATE = "20230101"
END_DATE = "20231231"
INIT_CASH = 100000  # 10万初始资金

# 数据缓存路径
CACHE_DIR = BASE_DIR / "cached_data"
CACHE_DIR.mkdir(exist_ok=True)
