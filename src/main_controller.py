# main_controller.py
import asyncio
import os
import signal
import sys
from pathlib import Path
from config.database import DatabaseManager
from distributed_controller import DistributedController

db_manager = DatabaseManager()


class MainController:
    def __init__(self):
        self.is_running = False
        self.controller = None

    def signal_handler(self, signum, frame):
        """信号处理器"""
        print(f"\n⏹️ 收到信号 {signum}，正在停止...")
        self.is_running = False
        if self.controller:
            self.controller.is_running = False

    async def run(self):
        """运行主控制器"""
        print("=== BookVoice 分布式自动化处理系统 ===")

        # 设置信号处理器
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # 创建分布式控制器
        self.controller = DistributedController(check_interval=30)

        # 注册机器
        if not self.controller.register_machine():
            print("❌ 无法注册机器")
            return

        self.is_running = True

        try:
            # 运行分布式控制器
            await self.controller.run()
        except Exception as e:
            print(f"❌ 主控制器运行时出错: {e}")
        finally:
            self.is_running = False
            print("🔚 主控制器已停止")


async def main():
    """主函数"""
    controller = MainController()
    await controller.run()


if __name__ == "__main__":
    asyncio.run(main())
