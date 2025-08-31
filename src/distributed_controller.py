# distributed_controller.py
import asyncio
import os
import socket
import uuid
import platform
import psutil
import time
from pathlib import Path
from config.database import DatabaseManager
from wattpad_downloader import download_single_story, YOUR_WATTPAD_COOKIES, OUTPUT_DIR

db_manager = DatabaseManager()


# 生成机器ID
def get_machine_id():
    """生成机器唯一标识符"""
    # 尝试从环境变量获取
    machine_id = os.getenv('MACHINE_ID')
    if machine_id:
        return machine_id

    # 否则生成基于主机信息的ID
    hostname = socket.gethostname()
    mac = ':'.join(['{:02x}'.format((uuid.getnode() >> elements) & 0xff)
                    for elements in range(0, 2 * 6, 2)][::-1])
    return f"{hostname}_{mac}"


# 获取机器信息
def get_machine_info():
    """获取机器硬件信息"""
    try:
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        cpu_count = psutil.cpu_count()
        memory_gb = round(psutil.virtual_memory().total / (1024 ** 3), 2)

        # 获取GPU信息（如果有的话）
        gpu_info = None
        try:
            # 如果安装了nvidia-ml-py
            import pynvml
            pynvml.nvmlInit()
            device_count = pynvml.nvmlDeviceGetCount()
            if device_count > 0:
                gpu_info = f"{device_count} GPU(s)"
        except ImportError:
            gpu_info = "No GPU info (pynvml not installed)"
        except Exception:
            gpu_info = "GPU detection failed"

        return {
            'id': get_machine_id(),
            'hostname': hostname,
            'ip_address': ip_address,
            'cpu_count': cpu_count,
            'memory_gb': memory_gb,
            'gpu_info': gpu_info
        }
    except Exception as e:
        print(f"获取机器信息时出错: {e}")
        return {
            'id': get_machine_id(),
            'hostname': 'unknown',
            'ip_address': 'unknown',
            'cpu_count': 0,
            'memory_gb': 0,
            'gpu_info': None
        }


class DistributedController:
    def __init__(self, check_interval: int = 30):
        self.machine_info = get_machine_info()
        self.check_interval = check_interval
        self.is_running = False

    def register_machine(self):
        """注册当前机器到数据库"""
        try:
            db_manager.register_machine(
                machine_id=self.machine_info['id'],
                hostname=self.machine_info['hostname'],
                ip_address=self.machine_info['ip_address'],
                cpu_count=self.machine_info['cpu_count'],
                memory_gb=self.machine_info['memory_gb'],
                gpu_info=self.machine_info['gpu_info']
            )
            print(f"✅ 机器已注册: {self.machine_info['id']}")
            return True
        except Exception as e:
            print(f"❌ 机器注册失败: {e}")
            return False

    def update_heartbeat(self):
        """更新机器心跳"""
        try:
            db_manager.update_machine_heartbeat(self.machine_info['id'])
            return True
        except Exception as e:
            print(f"❌ 心跳更新失败: {e}")
            return False

    async def check_and_assign_stories(self):
        """检查并分配未处理的故事"""
        try:
            # 获取未分配的故事
            unassigned_stories = db_manager.get_unassigned_stories()

            if not unassigned_stories:
                print("🔍 暂无未分配的故事")
                return

            print(f"🔍 发现 {len(unassigned_stories)} 个未分配的故事")

            # 尝试分配一个故事给当前机器
            for story in unassigned_stories:
                if db_manager.assign_story_to_machine(story['title'], self.machine_info['id']):
                    print(f"✅ 成功分配故事 '{story['title']}' 给机器 {self.machine_info['id']}")

                    # 处理分配到的故事
                    await self.process_assigned_story(story)
                    break
                else:
                    print(f"⚠️ 未能分配故事 '{story['title']}'")

        except Exception as e:
            print(f"❌ 检查和分配故事时出错: {e}")

    # 在 distributed_controller.py 中更新 process_assigned_story 函数

    async def process_assigned_story(self, story_info):
        """处理分配给当前机器的故事"""
        try:
            story_title = story_info["title"]
            print(f"🔊 开始处理分配的故事: {story_title}")

            # 下载故事（传入机器ID）
            result = await download_single_story(story_info, YOUR_WATTPAD_COOKIES, OUTPUT_DIR, self.machine_info['id'])

            if result:
                print(f"✅ 故事 '{story_title}' 处理完成")

                # 处理音频生成
                story_output_dir = os.path.join(OUTPUT_DIR, story_title)
                print(f"🎵 开始生成音频: {story_title}")
                try:
                    from batch_audiobook_generator import generate_audiobooks_in_directory
                    generate_audiobooks_in_directory(story_output_dir, "config.yaml", force_rebuild=False)
                    print(f"✅ 音频生成完成: {story_title}")
                except Exception as e:
                    print(f"❌ 音频生成失败: {e}")

                # 处理RSS更新
                print(f"📡 开始更新RSS: {story_title}")
                try:
                    from generate_and_deploy_rss import run_rss_update_process
                    run_rss_update_process(story_output_dir)
                    print(f"✅ RSS更新完成: {story_title}")
                except Exception as e:
                    print(f"❌ RSS更新失败: {e}")
            else:
                print(f"❌ 故事 '{story_title}' 处理失败")

        except Exception as e:
            print(f"❌ 处理分配的故事时出错: {e}")
        finally:
            # 释放故事
            db_manager.release_story_from_machine(story_info['title'], self.machine_info['id'])

    async def run(self):
        """运行分布式控制器"""
        print(f"🚀 启动分布式控制器 (机器ID: {self.machine_info['id']})")

        # 注册机器
        if not self.register_machine():
            print("❌ 无法注册机器，控制器将退出")
            return

        self.is_running = True

        try:
            while self.is_running:
                # 更新心跳
                self.update_heartbeat()

                # 检查并分配故事
                await self.check_and_assign_stories()

                # 等待下次检查
                print(f"⏳ 等待 {self.check_interval} 秒后进行下次检查...")
                await asyncio.sleep(self.check_interval)

        except KeyboardInterrupt:
            print("\n⏹️ 收到停止信号，正在关闭控制器...")
        except Exception as e:
            print(f"❌ 控制器运行时出错: {e}")
        finally:
            self.is_running = False
            print("🔚 分布式控制器已停止")


# 主函数
async def main():
    """主函数"""
    # 创建控制器实例
    controller = DistributedController(check_interval=30)  # 每30秒检查一次

    # 运行控制器
    await controller.run()


if __name__ == "__main__":
    asyncio.run(main())
