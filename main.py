import sys

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


def main():
    """项目根目录快捷入口: python main.py。"""
    app.main()


if __name__ == "__main__":
    main()
