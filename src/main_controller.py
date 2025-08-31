# main_controller.py
import asyncio
import os
from pathlib import Path
from config.database import DatabaseManager
from wattpad_downloader import download_single_story, YOUR_WATTPAD_COOKIES, OUTPUT_DIR

db_manager = DatabaseManager()


def get_stories_from_database():
    """从数据库获取需要处理的故事列表"""
    try:
        # 这里可以根据需要添加查询条件，例如只获取状态为pending或processing的故事
        # 目前我们先获取所有故事
        with db_manager.get_connection() as conn:
            cursor = conn.cursor(dictionary=True)
            cursor.execute('''
                SELECT title, url FROM stories 
                WHERE status IN ('pending', 'downloading', 'partial') 
                OR status IS NULL 
                ORDER BY created_at
            ''')
            stories = cursor.fetchall()

            # 转换为与原来STORIES_TO_DOWNLOAD相同的格式
            stories_to_download = [
                {"title": story["title"], "url": story["url"]}
                for story in stories if story["url"]
            ]

            return stories_to_download
    except Exception as e:
        print(f"从数据库获取故事列表时出错: {e}")
        return []


async def process_story_with_resume(story_info):
    """处理单个故事，支持断点续传"""
    story_title = story_info["title"].strip()
    story_output_dir = os.path.join(OUTPUT_DIR, story_title)

    print(f"\n=== 处理故事: {story_title} ===")

    # 检查数据库中的故事状态
    story_record = db_manager.get_story_by_title(story_title)

    if story_record and story_record['status'] == 'completed':
        print(f"故事 '{story_title}' 已完成，跳过下载阶段")
    else:
        # 下载故事
        await download_single_story(story_info, YOUR_WATTPAD_COOKIES, OUTPUT_DIR)

    # 处理音频生成
    print(f"检查 '{story_title}' 的音频生成状态...")
    unprocessed_audio = db_manager.get_unprocessed_audio_chapters(story_title)
    if unprocessed_audio:
        print(f"发现 {len(unprocessed_audio)} 个章节需要生成音频")
        # 调用音频生成 (使用已有的 batch_audiobook_generator 方法)
        from batch_audiobook_generator import generate_audiobooks_in_directory
        generate_audiobooks_in_directory(story_output_dir, "config.yaml", force_rebuild=False)
    else:
        print(f"'{story_title}' 的音频已全部生成")

    # 处理RSS更新
    print(f"检查 '{story_title}' 的RSS更新状态...")
    unprocessed_rss = db_manager.get_unprocessed_rss_chapters(story_title)
    if unprocessed_rss:
        print(f"发现 {len(unprocessed_rss)} 个章节需要更新RSS")
        # 调用RSS更新 (使用已有的 generate_and_deploy_rss 方法)
        from generate_and_deploy_rss import run_rss_update_process
        run_rss_update_process(story_output_dir)
    else:
        print(f"'{story_title}' 的RSS已全部更新")


async def main():
    """主控制函数"""
    print("=== BookVoice 自动化处理系统 ===")

    # 确保输出目录存在
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 从数据库获取需要处理的故事列表
    stories_to_download = get_stories_from_database()

    if not stories_to_download:
        print("数据库中没有找到需要处理的故事")
        return

    # 处理所有故事
    for i, story in enumerate(stories_to_download, 1):
        print(f"\n--- 处理第 {i}/{len(stories_to_download)} 个故事 ---")
        try:
            await process_story_with_resume(story)
        except Exception as e:
            print(f"处理故事 '{story['title']}' 时出错: {e}")
            import traceback
            traceback.print_exc()

    print("\n=== 所有故事处理完成 ===")


if __name__ == "__main__":
    asyncio.run(main())
