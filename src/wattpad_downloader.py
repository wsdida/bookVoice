# wattpad_downloader.py (修复导入卡住问题)
import asyncio
import os
import glob
import json
from datetime import datetime
from crawl4ai import AsyncWebCrawler
from crawl4ai.extraction_strategy import NoExtractionStrategy
from bs4 import BeautifulSoup
import time
import re
from config.database import DatabaseManager
# 在文件顶部添加
db_manager = DatabaseManager()
# --- 配置 ---
YOUR_WATTPAD_COOKIES = "wp_id=d3622c8c-f8bf-4725-9b3b-58c6a9bb6040; locale=en_US; lang=1; _gcl_au=1.1.229635524.1753744777; _fbp=fb.1.1753744777766.777872414890237537; _gid=GA1.2.120974872.1753744778; _col_uuid=298a2882-0726-406d-ac2a-637369060a41-3t3k; fs__exp=1; ff=1; dpr=1; tz=-8; X-Time-Zone=Asia%2FShanghai; token=523540601%3A2%3A1753747628%3Aocwa-PRhjN9qSUuepUlBUwz7hhPnL78ZtAHXLdx3q59U3JMl5Qde68qX1-H0WwJQ; te_session_id=1753796025284; isStaff=1; AMP_TOKEN=%24NOT_FOUND; signupFrom=story_reading; TRINITY_USER_ID=702f883b-89be-4578-8c95-d939ccc884f5; TRINITY_USER_DATA=eyJ1c2VySWRUUyI6MTc1Mzc5NjExODM0OCwiZmlyc3RDbGlja1RTIjoxNzUzNzk2MTM0OTcwfQ==; _pubcid=b5931962-36d4-4a22-85ec-aef93fdc80c7; _pubcid_cst=VyxHLMwsHQ%3D%3D; __qca=I0-428836967-1753796333975; cto_bundle=HNW7j18wRUc4SmlKM1d5bENGQmp5RTUlMkZZazlFbTNUSjU4UmNXNTRNakF6ZW1BeW1pUWJVZEo0Nk5iVlFubGVkOTR0TVNlbXAlMkZmYzNlT3BTUjZDSyUyQmVWUk54d00xTEslMkJia1Z6WUZtdEptUzFrVEhyam9yZGJFdEFxcWtYejdZOEUwdU9H; cto_bidid=0NEFNV9DVkF5bzFxeUFsak44eVhTNU1uSGpQWk9qTUR5aTdYYWhxcjhyciUyQmVWUk54d00xTEslMkJia1Z6WUZtdEptUzFrVEhyam9yZGJFdEFxcWtYejdZOEUwdU9H; _ga=GA1.1.408120238.1753744776; _ga_FNDTZ0MZDQ=GS2.1.s1753796037$o4$g1$t1753797711$j22$l0$h0; _dd_s=logs=1&id=8b383015-fee2-4d25-8b5a-b8b5c3034a5f&created=1753796025287&expire=1753798728025; RT=nu=https%3A%2F%2Fwww.wattpad.com%2F1420810072-the-escaped-con%2527s-hostage-three-buckle-up&cl=1753797726766&r=https%3A%2F%2Fwww.wattpad.com%2F1420515771-the-escaped-con%2527s-hostage-two-don%2527t-scream&ul=1753797835435"  # 替换为你的真实 Cookie

STORIES_TO_DOWNLOAD = [
    {
        "url": "https://www.wattpad.com/story/50979962-moonrise",
        "title": "Moonrise"
    },
    {
        "url": "http://wattpad.com/story/258988576-see-me",
        "title": "See Me"
    }, {
        "url": "https://www.wattpad.com/story/9304697-alpha%27s-girl-wolf-interracial",
        "title": "AlphaGirl"
    }, {
        "url": "https://www.wattpad.com/story/220048804-the-godfather",
        "title": "The Godfather"
    }, {
        "url": "https://www.wattpad.com/story/223762317-criminal-desire-%E2%9C%93",
        "title": "Criminal Desire"
    }, {
        "url": "https://www.wattpad.com/story/387202915-rocco",
        "title": "Rocco"
    }, {
        "url": "https://www.wattpad.com/story/300421715-the-general",
        "title": "The General"
    }
]

OUTPUT_DIR = "./downloaded_stories"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- 状态管理 ---
STATUS_FILE = ".status.json"


def load_status(output_dir):
    """加载下载状态"""
    status_path = os.path.join(output_dir, STATUS_FILE)
    if os.path.exists(status_path):
        try:
            with open(status_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return {
        "completed_chapters": [],
        "failed_chapters": [],
        "total_chapters": 0,
        "completed": False,
        "audiobook_generated": False,
        "rss_updated": False,
        "last_updated": None
    }


def save_status(output_dir, status):
    """保存下载状态"""
    status["last_updated"] = datetime.now().isoformat()
    status_path = os.path.join(output_dir, STATUS_FILE)
    with open(status_path, 'w', encoding='utf-8') as f:
        json.dump(status, f, ensure_ascii=False, indent=2)


# --- 爬取逻辑 ---
async def get_chapter_links(story_url: str, cookies_str: str):
    """获取章节链接"""
    print(f"正在获取故事主页: {story_url}")
    headers = {"Cookie": cookies_str}
    try:
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(
                url=story_url,
                headers=headers,
                timeout=120000,
                extraction_strategy=NoExtractionStrategy()
            )
            if result.success:
                soup = BeautifulSoup(result.html, 'html.parser')
                toc_container = soup.find('ul', {'aria-label': 'story-parts'})
                chapter_links = []
                if toc_container:
                    for a in toc_container.find_all('a', href=True):
                        href = a['href'].strip()
                        if href.startswith('/'):
                            href = "https://www.wattpad.com" + href
                        base_url = href.split('#')[0]
                        if base_url not in chapter_links:
                            chapter_links.append(base_url)
                print(f"提取到 {len(chapter_links)} 个章节链接。")
                return chapter_links
            else:
                print(f"主页获取失败: {result.error_message}")
                return []
    except Exception as e:
        print(f"获取章节链接失败: {e}")
        return []


async def download_single_page(page_url: str, headers: dict, page_index: int) -> str:
    """下载单页内容"""
    try:
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(
                url=page_url,
                headers=headers,
                timeout=60000,
                extraction_strategy=NoExtractionStrategy()
            )
            if not result.success:
                return ""
            soup = BeautifulSoup(result.html, 'html.parser')
            pre = soup.find('pre')
            content = ""
            if pre:
                ps = pre.find_all('p', attrs={'data-p-id': True})
                content = '\n'.join(p.get_text(strip=True) for p in ps if p.get_text(strip=True))
            else:
                container = (soup.find('pre', id='storytext') or
                             soup.find('div', {'data-testid': 'content'}) or
                             soup.find('div', class_='panel-reading'))
                if container:
                    content = container.get_text(separator='\n', strip=True)
            return content.strip()
    except:
        return ""


async def download_chapter_content(chapter_url: str, chapter_index: int, output_dir: str, cookies_str: str,
                                   status: dict, story_title: str):
    """下载单个章节（支持断点续传）"""
    filename = f"Chapter_{chapter_index:04d}.txt"
    filepath = os.path.join(output_dir, filename)

    # 检查数据库中的状态
    db_manager.create_or_update_chapter(story_title, chapter_index, file_path=filepath)

    # 检查是否已成功下载
    if chapter_index in status["completed_chapters"]:
        print(f"({chapter_index}) 章节已标记为完成，跳过: {filepath}")
        db_manager.update_chapter_download_status(story_title, chapter_index, 'completed')
        return True

    # 检查文件是否存在且有效
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read().strip()
            if content and not content.startswith(("[警告]", "[错误]", "[异常]")) and len(content) > 20:
                print(f"({chapter_index}) 文件已存在且有效，跳过: {filepath}")
                if chapter_index not in status["completed_chapters"]:
                    status["completed_chapters"].append(chapter_index)
                    status["completed_chapters"] = sorted(list(set(status["completed_chapters"])))
                    save_status(output_dir, status)
                db_manager.update_chapter_download_status(story_title, chapter_index, 'completed', len(content))
                return True
        except Exception as e:
            print(f"({chapter_index}) 检查现有文件时出错: {e}")

    print(f"({chapter_index}) 正在下载章节...")
    headers = {"Cookie": cookies_str}
    chapter_title = "未知章节"
    all_content = []

    # 获取第一页
    async with AsyncWebCrawler() as crawler:
        result = await crawler.arun(url=chapter_url, headers=headers, timeout=60000,
                                    extraction_strategy=NoExtractionStrategy())
        if not result.success:
            print(f"({chapter_index}) 第一页加载失败")
            db_manager.update_chapter_download_status(story_title, chapter_index, 'failed')
            return False
        soup = BeautifulSoup(result.html, 'html.parser')
        title_tag = soup.find('h1', class_='h2')
        chapter_title = title_tag.get_text(strip=True) if title_tag else f"第 {chapter_index} 章"
        pre = soup.find('pre')
        if pre:
            ps = pre.find_all('p', attrs={'data-p-id': True})
            text = '\n'.join(p.get_text(strip=True) for p in ps if p.get_text(strip=True))
            if text:
                all_content.append(text)

    # 后续页面
    for page in range(2, 21):
        page_url = f"{chapter_url}/page/{page}"
        content = await download_single_page(page_url, headers, page)
        if content:
            all_content.append(content)
        else:
            break
        await asyncio.sleep(0.5)

    final_content = "\n\n".join(all_content).strip()
    if not final_content:
        final_content = f"[警告] 无法提取章节内容。\nURL: {chapter_url}"

    # 添加标题
    output_text = f"{chapter_title}\n\n{final_content}"

    # 保存
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(output_text)
    print(f"({chapter_index}) 已保存: {filepath}")

    # 更新状态
    if chapter_index not in status["completed_chapters"]:
        status["completed_chapters"].append(chapter_index)
    status["completed_chapters"] = sorted(list(set(status["completed_chapters"])))
    save_status(output_dir, status)

    # 更新数据库
    db_manager.update_chapter_download_status(story_title, chapter_index, 'completed', len(output_text))

    await asyncio.sleep(1)
    return True

async def retry_failed_chapters(output_dir, chapter_urls, cookies_str, status):
    """重试失败章节"""
    error_files = glob.glob(os.path.join(output_dir, "*_ERROR.txt")) + glob.glob(
        os.path.join(output_dir, "*_EXCEPTION.txt"))
    if not error_files:
        return

    print(f"发现 {len(error_files)} 个失败章节，开始重试...")
    for file in error_files:
        match = re.match(r"Chapter_(\d+)_", os.path.basename(file))
        if match:
            idx = int(match.group(1))
            if 1 <= idx <= len(chapter_urls):
                url = chapter_urls[idx - 1]
                print(f"重试章节 {idx}")
                await download_chapter_content(url, idx, output_dir, cookies_str, status)
                try:
                    os.remove(file)
                except:
                    pass


# --- 主下载函数 ---
# 在 wattpad_downloader.py 中更新 download_single_story 函数

async def download_single_story(story_info: dict, cookies_str: str, base_output_dir: str, machine_id: str = None):
    story_url = story_info["url"].strip()
    story_title = story_info["title"].strip()
    story_output_dir = os.path.join(base_output_dir, story_title)
    os.makedirs(story_output_dir, exist_ok=True)

    print(f"\n=== 开始下载故事: {story_title} ===")
    start_time = time.time()

    # 检查是否已完成
    status = load_status(story_output_dir)
    chapter_urls = await get_chapter_links(story_url, cookies_str)
    if not chapter_urls:
        print("未找到章节链接。")
        return False

    status["total_chapters"] = len(chapter_urls)

    # 更新数据库中的故事信息
    db_manager.create_or_update_story(story_title, story_url, len(chapter_urls))

    # 如果提供了机器ID，更新分配信息
    if machine_id:
        db_manager.assign_story_to_machine(story_title, machine_id)

    # 即使故事已完成，也要继续执行后续流程
    if status.get("completed", False) and len(status["completed_chapters"]) >= len(chapter_urls):
        print(f"故事 '{story_title}' 已完成，继续执行后续流程...")
    else:
        print(f"共 {len(chapter_urls)} 章节，开始下载...")

        # 获取数据库中未下载的章节
        undownloaded_chapters = db_manager.get_undownloaded_chapters(story_title)
        if undownloaded_chapters:
            print(f"发现 {len(undownloaded_chapters)} 个未下载章节，继续下载...")
            for chapter_num in undownloaded_chapters:
                if 1 <= chapter_num <= len(chapter_urls):
                    url = chapter_urls[chapter_num - 1]
                    await download_chapter_content(url, chapter_num, story_output_dir, cookies_str, status, story_title)
        else:
            # 全量下载
            for i, url in enumerate(chapter_urls, 1):
                if i in status["completed_chapters"]:
                    continue
                await download_chapter_content(url, i, story_output_dir, cookies_str, status, story_title)

        # 重试失败
        await retry_failed_chapters(story_output_dir, chapter_urls, cookies_str, status)

        # 检查是否全部完成
        completed = len(status["completed_chapters"]) >= len(chapter_urls)
        status["completed"] = completed
        save_status(story_output_dir, status)

        # 更新数据库故事状态
        db_manager.update_story_status(story_title, 'completed' if completed else 'partial',
                                       len(status["completed_chapters"]))
        print(f"故事下载阶段完成。")

    end_time = time.time()
    print(f"\n故事 '{story_title}' 处理完成。耗时: {end_time - start_time:.2f} 秒")

    # 继续执行有声书生成和RSS更新流程，无论之前是否已完成
    try:
        # 延迟导入，避免在模块加载时就导入大型依赖
        print("开始执行有声书生成...")
        from batch_audiobook_generator import generate_audiobooks_in_directory
        config_path = "config.yaml"
        # force_rebuild=False 表示启用断点续传
        generate_audiobooks_in_directory(story_output_dir, config_path, force_rebuild=False)
        status["audiobook_generated"] = True
        print("有声书生成调用完成。")

        # 更新RSS
        print("开始执行RSS更新..." + story_output_dir)
        from generate_and_deploy_rss import run_rss_update_process
        run_rss_update_process(story_output_dir)
        status["rss_updated"] = True
        print("RSS更新完成。")

        # 保存最终状态
        save_status(story_output_dir, status)

    except Exception as e:
        print(f"执行后续流程时出错: {e}")
        import traceback
        traceback.print_exc()
        # 保存当前状态
        save_status(story_output_dir, status)
        return False

    return True


# --- 主函数 ---
async def main():
    if not YOUR_WATTPAD_COOKIES or "REPLACE" in YOUR_WATTPAD_COOKIES:
        print("请设置有效的 Cookies")
        return

    print("=== 批量下载启动 ===")
    success = 0
    for i, story in enumerate(STORIES_TO_DOWNLOAD, 1):
        print(f"\n--- 处理第 {i}/{len(STORIES_TO_DOWNLOAD)}: {story['title']} ---")
        try:
            if await download_single_story(story, YOUR_WATTPAD_COOKIES, OUTPUT_DIR):
                success += 1
        except Exception as e:
            print(f"下载失败: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n=== 完成: {success}/{len(STORIES_TO_DOWNLOAD)} 成功 ===")


if __name__ == "__main__":
    asyncio.run(main())
