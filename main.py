"""Windows 侧执行服务的根目录启动入口。

在导入 miniqmt_follower 之前先做 Python 版本闸门，把低版本环境的报错前移，
再委托给 app.main() 拉起跟单主循环。
"""

import sys
from pathlib import Path

# 版本闸门必须在 import miniqmt_follower 之前跑完，否则先炸出来的是从包深处
# 冒上来的 ImportError("cannot import name 'StrEnum' from 'enum'") —— 在交易机
# 上排查那条报错要绕一大圈。下限 3.11 来自 models.py 的 enum.StrEnum。
if sys.version_info < (3, 11):
    raise SystemExit(
        "❌ 需要 Python 3.11 或更高版本，当前是 {}.{}。\n"
        "   miniqmt_follower 用了 enum.StrEnum（3.11 才有），低版本整个包都 import 不进去。\n"
        "   依赖与版本要求见 requirements.txt。".format(*sys.version_info[:2])
    )

from miniqmt_follower import app  # noqa: E402 —— 必须在版本闸门之后

# 默认配置路径锚定在 main.py 同级目录: 把 config.yaml / config.strategy.yaml
# 放在 main.py 旁边, 无论从哪个工作目录执行 ``python main.py`` 都能找到。
_DEFAULT_CONFIG = str(Path(__file__).resolve().parent / "config.yaml")


def main():
    """项目根目录快捷入口: python main.py (配置默认取本文件同级的 config.yaml)。"""
    app.main(default_config=_DEFAULT_CONFIG)


if __name__ == "__main__":
    main()
