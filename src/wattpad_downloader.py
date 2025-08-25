# wattpad_downloader.py
import asyncio
import os
import glob # 用于文件名模式匹配
from crawl4ai import AsyncWebCrawler
from crawl4ai.extraction_strategy import NoExtractionStrategy
from bs4 import BeautifulSoup
import time
import re

# --- 配置 ---
# 替换为您的实际 Wattpad 会话 cookies
YOUR_WATTPAD_COOKIES = "wp_id=d3622c8c-f8bf-4725-9b3b-58c6a9bb6040; locale=en_US; lang=1; _gcl_au=1.1.229635524.1753744777; _fbp=fb.1.1753744777766.777872414890237537; _gid=GA1.2.120974872.1753744778; _col_uuid=298a2882-0726-406d-ac2a-637369060a41-3t3k; fs__exp=1; ff=1; dpr=1; tz=-8; X-Time-Zone=Asia%2FShanghai; token=523540601%3A2%3A1753747628%3Aocwa-PRhjN9qSUuepUlBUwz7hhPnL78ZtAHXLdx3q59U3JMl5Qde68qX1-H0WwJQ; te_session_id=1753796025284; isStaff=1; AMP_TOKEN=%24NOT_FOUND; signupFrom=story_reading; TRINITY_USER_ID=702f883b-89be-4578-8c95-d939ccc884f5; TRINITY_USER_DATA=eyJ1c2VySWRUUyI6MTc1Mzc5NjExODM0OCwiZmlyc3RDbGlja1RTIjoxNzUzNzk2MTM0OTcwfQ==; _pubcid=b5931962-36d4-4a22-85ec-aef93fdc80c7; _pubcid_cst=VyxHLMwsHQ%3D%3D; __qca=I0-428836967-1753796333975; cto_bundle=HNW7j18wRUc4SmlKM1d5bENGQmp5RTUlMkZZazlFbTNUSjU4UmNXNTRNakF6ZW1BeW1pUWJVZEo0Nk5iVlFubGVkOTR0TVNlbXAlMkZmYzNlT3BTUjZDSyUyQmVWUk54d00xTEslMkJia1Z6WUZtdEptUzFrVEhyam9yZGJFdEFxcWtYejdZOEUwdU9H; cto_bidid=0NEFNV9DVkF5bzFxeUFsak44eVhTNU1uSGpQWk9qTUR5aTdYYWhxcjhyciUyQlA5MSUyRmhZaFBqZ3JSSlp6ZCUyQmUlMkZqRXNWTzN3QjRBZ2ElMkJPYVRkaWcwTUZ3RlB6eEElM0QlM0Q; _ga=GA1.1.408120238.1753744776; _ga_FNDTZ0MZDQ=GS2.1.s1753796037$o4$g1$t1753797711$j22$l0$h0; _dd_s=logs=1&id=8b383015-fee2-4d25-8b5a-b8b5c3034a5f&created=1753796025287&expire=1753798728025; RT=nu=https%3A%2F%2Fwww.wattpad.com%2F1420810072-the-escaped-con%2527s-hostage-three-buckle-up&cl=1753797726766&r=https%3A%2F%2Fwww.wattpad.com%2F1420515771-the-escaped-con%2527s-hostage-two-don%2527t-scream&ul=1753797835435" # <-- 请填入您的真实 Cookie
# --- 批量下载列表 ---
STORIES_TO_DOWNLOAD = [
    {
        "url": "https://www.wattpad.com/story/50979962-moonrise",
        "title": "Moonrise"
    },{
        "url":"http://wattpad.com/story/258988576-see-me",
         "title":"See Me"
    }
    # 可以在这里添加更多小说
    # {
    #     "url": "https://www.wattpad.com/story/ANOTHER_STORY_ID",
    #     "title": "Another Story Title"
    # },
]

OUTPUT_DIR = "./downloaded_stories"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# --- 辅助函数 ---
def parse_cookies(cookie_string: str) -> dict:
    """将 Cookie 字符串解析为字典"""
    cookies = {}
    for item in cookie_string.split(';'):
        item = item.strip()
        if '=' in item:
            key, value = item.split('=', 1)
            cookies[key] = value
    return cookies

# --- 爬取逻辑 (保持不变) ---
# get_chapter_links, download_single_page, download_chapter_content 函数
# 请将您原始文件中的这三个函数复制到这里，保持不变。
# 为了简洁，这里省略了这三个函数的完整代码，因为它们没有变化。
# --- 从 Pasted_Text_1753878075773.txt 复制以下函数 ---
async def get_chapter_links(story_url: str, cookies_str: str):
    """使用 crawl4ai 0.7.0 获取故事主页并提取所有章节链接。"""
    print(f"正在获取故事主页: {story_url}")
    headers = {"Cookie": cookies_str}
    try:
        async with AsyncWebCrawler() as crawler:
            print("启动爬虫实例...")
            result = await crawler.arun(
                url=story_url,
                headers=headers,
                timeout=120000,
                extraction_strategy=NoExtractionStrategy()
            )
            if result.success:
                html_content = result.html
                print("故事主页获取成功。")
                soup = BeautifulSoup(html_content, 'html.parser')
                chapter_links = []
                print("开始解析章节链接...")
                toc_container = soup.find('ul', {'aria-label': 'story-parts'})
                if toc_container:
                    print("找到章节列表容器 (ul[aria-label='story-parts'])")
                    chapter_link_tags = toc_container.find_all('a', href=True)
                    print(f"在容器中找到 {len(chapter_link_tags)} 个 <a> 标签")
                    for tag in chapter_link_tags:
                        href = tag.get('href', '').strip()
                        if href:
                            if href.startswith('/'):
                                full_link = "https://www.wattpad.com" + href
                            else:
                                full_link = href
                            base_link = full_link.split('#')[0]
                            if base_link not in chapter_links:
                                chapter_links.append(base_link)
                                print(f"  找到章节链接: {base_link}")
                else:
                    print("错误：未找到章节列表容器 ul[aria-label='story-parts']。")
                print(f"总共提取到 {len(chapter_links)} 个章节链接。")
                return chapter_links
            else:
                error_msg = getattr(result, 'error_message', 'Unknown error from crawler')
                print(f"获取故事主页失败: {error_msg}")
                return []
    except Exception as e:
        print(f"在 get_chapter_links 中发生异常: {e}")
        import traceback
        traceback.print_exc()
        return []

async def download_single_page(page_url: str, headers: dict, page_index: int) -> str:
    """下载并提取单个页面的内容"""
    try:
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(
                url=page_url,
                headers=headers,
                timeout=60000,
                extraction_strategy=NoExtractionStrategy()
            )
            if result.success:
                html_content = result.html
                soup = BeautifulSoup(html_content, 'html.parser')
                title_tag = soup.find('h1', class_='h2')
                pre_container = soup.find('pre')
                if not title_tag and not pre_container:
                    page_title = soup.title.string if soup.title else ""
                    if page_title and ("404" in page_title or "Not Found" in page_title or "Error" in page_title):
                        print(f"  检测到第 {page_index} 页可能是错误页面 (404/错误)。")
                        return ""
                content_text = ""
                if pre_container:
                    paragraph_tags = pre_container.find_all('p', attrs={'data-p-id': True})
                    if paragraph_tags:
                        paragraph_texts = []
                        for p_tag in paragraph_tags:
                            p_text = p_tag.get_text(separator=' ', strip=True)
                            if p_text:
                                paragraph_texts.append(p_text)
                        content_text = '\n'.join(paragraph_texts)
                        content_text = content_text.strip()
                    else:
                        content_text = pre_container.get_text(separator='\n', strip=True)
                else:
                    content_container = soup.find('pre', id='storytext')
                    if not content_container:
                        content_container = soup.find('div', {'data-testid': 'content'})
                    if not content_container:
                        content_container = soup.find('div', class_='panel-reading')
                    if content_container:
                        content_text = content_container.get_text(separator='\n', strip=True)
                    else:
                        if page_index > 1:
                            print(f"  第 {page_index} 页未找到任何内容容器，可能是无效页面。")
                            return ""
                        else:
                            print(f"  警告：第 {page_index} 页未能定位到任何章节内容容器。")
                if content_text:
                    pass
                else:
                    if page_index > 1:
                        print(f"  第 {page_index} 页内容为空，可能是已到达章节末尾或页面不存在。")
                    else:
                        print(f"  警告：第 {page_index} 页提取到的内容为空。")
                return content_text
            else:
                error_msg = getattr(result, 'error_message', 'Unknown error from crawler')
                print(f"  下载页面 {page_url} 失败: {error_msg}")
                return ""
    except Exception as e:
        print(f"  下载页面 {page_url} 时发生错误: {e}")
        return ""

async def download_chapter_content(chapter_url: str, chapter_index: int, output_dir: str, cookies_str: str):
    """使用 crawl4ai 0.7.0 获取单个章节的所有分页内容并保存。"""
    try:
        url_parts = chapter_url.split('/')
        url_part = url_parts[-1].split('-')[0] if '-' in url_parts[-1] else url_parts[-1]
        filename = f"Chapter_{chapter_index:03d}_{url_part}.txt"
        filepath = os.path.join(output_dir, filename)
        print(f"({chapter_index}) 正在下载章节: {chapter_url}")
        headers = {"Cookie": cookies_str}

        # --- 检查是否已存在且无错误 ---
        # 检查是否有对应的错误文件
        error_files = glob.glob(os.path.join(output_dir, f"Chapter_{chapter_index:03d}_*.txt"))
        has_error_file = any(kw in f for kw in ['ERROR', 'EXCEPTION'] for f in error_files)

        if os.path.exists(filepath) and not has_error_file:
             print(f"({chapter_index}) 章节文件已存在且无错误记录，跳过下载: {filepath}")
             return # 跳过下载
        else:
             # 如果文件存在但有错误，或者文件不存在，则继续下载（覆盖旧文件或创建新文件）
             if os.path.exists(filepath):
                 print(f"({chapter_index}) 章节文件存在但有错误记录或需要重新下载，将覆盖: {filepath}")
             # 删除可能存在的旧错误文件
             for ef in error_files:
                 try:
                     os.remove(ef)
                     print(f"({chapter_index}) 删除旧错误文件: {ef}")
                 except OSError as e:
                     print(f"({chapter_index}) 删除旧错误文件失败 {ef}: {e}")


        # 1. 获取第一页内容和章节标题
        print(f"({chapter_index}) 正在获取第一页内容和章节标题...")
        first_page_content = ""
        chapter_title = "未知章节标题"
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(
                url=chapter_url,
                headers=headers,
                timeout=60000,
                extraction_strategy=NoExtractionStrategy()
            )
            if result.success:
                first_page_html = result.html
                soup = BeautifulSoup(first_page_html, 'html.parser')
                title_tag = soup.find('h1', class_='h2')
                if title_tag:
                    chapter_title = title_tag.get_text(strip=True)
                    chapter_title = re.sub(r'\s+', ' ', chapter_title).strip()
                    print(f"({chapter_index}) 找到章节标题: {chapter_title}")
                else:
                    print(f"({chapter_index}) 未找到章节标题 h1.h2，使用默认标题。")
                    page_title = soup.title.string if soup.title else ""
                    if page_title and " - " in page_title:
                        chapter_title = page_title.split(" - ")[0].strip()
                        print(f"({chapter_index}) 从页面标题提取章节标题: {chapter_title}")
                pre_container = soup.find('pre')
                if pre_container:
                    paragraph_tags = pre_container.find_all('p', attrs={'data-p-id': True})
                    if paragraph_tags:
                        paragraph_texts = []
                        for p_tag in paragraph_tags:
                            p_text = p_tag.get_text(separator=' ', strip=True)
                            if p_text:
                                paragraph_texts.append(p_text)
                        first_page_content = '\n'.join(paragraph_texts)
                        first_page_content = first_page_content.strip()
                    else:
                        first_page_content = pre_container.get_text(separator='\n', strip=True)
                else:
                    content_container = soup.find('pre', id='storytext')
                    if not content_container:
                        content_container = soup.find('div', {'data-testid': 'content'})
                    if not content_container:
                        content_container = soup.find('div', class_='panel-reading')
                    if content_container:
                        first_page_content = content_container.get_text(separator='\n', strip=True)
            else:
                error_msg = getattr(result, 'error_message', 'Unknown error from crawler')
                print(f"({chapter_index}) 获取第一页失败: {error_msg}")
                filename = f"Chapter_{chapter_index:03d}_ERROR.txt"
                filepath = os.path.join(output_dir, filename)
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(f"[错误] 获取章节第一页失败: {chapter_url}\n错误信息: {error_msg}\n")
                return

        all_pages_content = [first_page_content] if first_page_content else []
        print(f"({chapter_index}) 第一页内容获取完成。")

        # 3. 循环下载后续页面
        print(f"({chapter_index}) 开始循环下载后续页面 (最多20页)...")
        for page_num in range(2, 21):
            page_url = f"{chapter_url}/page/{page_num}"
            print(f"({chapter_index}) 正在下载第 {page_num} 页: {page_url}")
            page_content = await download_single_page(page_url, headers, page_num)
            if page_content:
                all_pages_content.append(page_content)
                print(f"({chapter_index}) 第 {page_num} 页下载成功。")
            else:
                print(f"({chapter_index}) 第 {page_num} 页获取失败或内容为空，停止下载后续页面。")
                break
            await asyncio.sleep(0.5)

        # 4. 合并所有页面内容
        if all_pages_content:
            final_chapter_content = "\n \n".join(all_pages_content)
            print(f"({chapter_index}) 所有页面内容合并完成，总长度: {len(final_chapter_content)} 字符")
        else:
            final_chapter_content = "[警告] 无法从此章节提取任何内容。"
            print(f"({chapter_index}) 警告：未能提取到任何页面的内容。")

        # 5. 组合标题和正文并保存
        final_content = ""
        if chapter_title and chapter_title != "未知章节标题":
            final_content += f"章节标题: {chapter_title}\n\n"
        final_content += final_chapter_content

        # 保存到文件 (覆盖或新建)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(final_content)
        print(f"({chapter_index}) 已保存章节 (标题+所有分页内容) 到: {filepath}")
        print(f"({chapter_index}) 等待 1 秒...")
        await asyncio.sleep(1)
    except Exception as e:
        print(f"({chapter_index}) 下载章节 {chapter_url} 时发生错误: {e}")
        import traceback
        traceback.print_exc()
        try:
            filename = f"Chapter_{chapter_index:03d}_EXCEPTION.txt"
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(f"[异常] 下载章节时发生异常: {chapter_url}\n异常信息: {e}\n")
        except:
            pass

# --- 新增：检查并重试错误章节 ---
async def retry_failed_chapters(story_output_dir, chapter_urls, cookies_str, max_retries=3):
    """检查输出目录中的错误文件，并重试下载对应的章节"""
    retry_count = 0
    while retry_count < max_retries:
        print(f"\n--- 检查错误章节 (第 {retry_count + 1}/{max_retries} 轮) ---")
        # 查找所有错误文件
        error_files = []
        error_files.extend(glob.glob(os.path.join(story_output_dir, "*_ERROR.txt")))
        error_files.extend(glob.glob(os.path.join(story_output_dir, "*_EXCEPTION.txt")))

        if not error_files:
            print("未发现错误章节文件。")
            break # 没有错误文件，退出循环

        print(f"发现 {len(error_files)} 个错误章节文件，开始重试...")
        tasks = []
        for error_file in error_files:
            # 从错误文件名推断章节索引
            filename = os.path.basename(error_file)
            # 匹配 Chapter_001_... 或 Chapter_1_... 等模式
            match = re.match(r'Chapter_(\d+)_', filename)
            if match:
                chapter_index = int(match.group(1))
                if 1 <= chapter_index <= len(chapter_urls):
                    chapter_url = chapter_urls[chapter_index - 1]
                    print(f"准备重试章节 {chapter_index}: {chapter_url}")
                    # 创建重试任务
                    task = download_chapter_content(chapter_url, chapter_index, story_output_dir, cookies_str)
                    tasks.append(task)
                    # 删除旧的错误文件
                    try:
                        os.remove(error_file)
                        print(f"已删除错误文件: {error_file}")
                    except OSError as e:
                        print(f"删除错误文件失败 {error_file}: {e}")
                else:
                    print(f"警告：从文件名 {filename} 推断的章节索引 {chapter_index} 超出范围 [1, {len(chapter_urls)}]，跳过。")
            else:
                 print(f"警告：无法从错误文件名 {filename} 推断章节索引，跳过。")

        if tasks:
            # 并发执行重试任务
            await asyncio.gather(*tasks)
            print(f"第 {retry_count + 1} 轮重试完成。")
        else:
            print("没有找到有效的错误章节进行重试。")
            break # 没有任务可执行，退出循环

        retry_count += 1
        await asyncio.sleep(2) # 重试轮次之间稍作等待

    if retry_count == max_retries:
        print(f"\n已达到最大重试轮次 ({max_retries})。")
        final_error_files = []
        final_error_files.extend(glob.glob(os.path.join(story_output_dir, "*_ERROR.txt")))
        final_error_files.extend(glob.glob(os.path.join(story_output_dir, "*_EXCEPTION.txt")))
        if final_error_files:
            print("最终仍存在的错误章节文件:")
            for ef in final_error_files:
                print(f"  - {ef}")
        else:
            print("所有错误章节均已成功重试。")


# --- 修改后的主下载函数，支持单个故事 ---
async def download_single_story(story_info: dict, cookies_str: str, base_output_dir: str):
    """下载单个 Wattpad 故事"""
    story_url = story_info["url"]
    story_title = story_info["title"]
    story_output_dir = os.path.join(base_output_dir, story_title)
    os.makedirs(story_output_dir, exist_ok=True)

    print(f"\n=== 开始下载故事: {story_title} ===")
    start_time = time.time()

    chapter_urls = await get_chapter_links(story_url, cookies_str)
    if not chapter_urls:
        print("\n未找到任何章节链接。")
        print("可能的原因：")
        print("1. Cookies 无效或已过期。请重新获取。")
        print("2. Wattpad 页面结构已更改。")
        print("3. 网络问题或 crawl4ai 未能正确加载页面。")
        print("4. 故事主页需要特殊权限才能查看章节列表。")
        return False

    print(f"\n开始下载 {len(chapter_urls)} 个章节...")
    # 按顺序下载章节 (已包含跳过逻辑)
    download_tasks = []
    for i, url in enumerate(chapter_urls):
        # download_chapter_content 内部已处理跳过逻辑
        task = download_chapter_content(url, i + 1, story_output_dir, cookies_str)
        download_tasks.append(task)

    # 并发执行下载任务 (可选，根据网站限制调整)
    # await asyncio.gather(*download_tasks)
    # 或者按顺序执行 (更稳定)
    for task in download_tasks:
         await task

    # --- 下载完成后，检查并重试错误章节 ---
    await retry_failed_chapters(story_output_dir, chapter_urls, cookies_str, max_retries=3)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n故事 '{story_title}' 下载完成。")
    print(f"总共下载 {len(chapter_urls)} 个章节。")
    print(f"耗时: {elapsed_time:.2f} 秒")
    print(f"内容保存在: {story_output_dir}")

    # --- 下载完成后，调用有声书生成 (可选) ---
    # ... (此处可插入调用 audiobook_generator 的代码，参考上一个回答) ...
    print(f"准备调用有声书生成脚本处理目录: {story_output_dir}")
    try:
        # --- 方式一：直接导入并调用函数 (推荐) ---
        # 确保 audiobook_generator.py 在 Python 路径中，或者在相同目录下
        # from audiobook_generator import generate_audiobook # 如果 generate_audiobook 是处理单个文件的函数
        # 但我们想处理整个目录，所以需要一个批量处理函数
        from batch_audiobook_generator import generate_audiobooks_in_directory  # 假设您已经添加了这个函数
        config_path = "config.yaml"  # 根据您的实际路径调整
        # 调用批量处理函数处理当前故事的输出目录
        # 这里传递的是故事的输出目录，里面包含所有章节的 txt 文件
        generate_audiobooks_in_directory(story_output_dir, config_path)
        print(f"有声书生成调用完成: {story_output_dir}")

    except Exception as gen_error:
        print(f"调用有声书生成脚本时出错: {gen_error}")
        import traceback
        traceback.print_exc()
        # 可以选择是否因为生成失败而标记整个故事下载为失败
        # return False

    print(f"总共下载 {len(chapter_urls)} 个章节。")
    print(f"耗时: {elapsed_time:.2f} 秒")
    print(f"内容保存在: {story_output_dir}")
    return True

# --- 主函数：批量下载 ---
async def main():
    if not YOUR_WATTPAD_COOKIES or "REPLACE_WITH_YOUR_COOKIE" in YOUR_WATTPAD_COOKIES or "..." in YOUR_WATTPAD_COOKIES:
        print("错误：请在代码中设置有效的 Wattpad Cookies (YOUR_WATTPAD_COOKIES)。")
        print("请登录 Wattpad，使用浏览器开发者工具获取 Cookie，并替换代码中的占位符。")
        return

    print("=== 开始批量下载 Wattpad 小说 ===")
    successful_downloads = 0
    total_stories = len(STORIES_TO_DOWNLOAD)

    for i, story_info in enumerate(STORIES_TO_DOWNLOAD):
        print(f"\n--- 开始处理第 {i+1}/{total_stories} 个故事: {story_info['title']} ---")
        try:
            success = await download_single_story(story_info, YOUR_WATTPAD_COOKIES, OUTPUT_DIR)
            if success:
                successful_downloads += 1
                print(f"--- 故事 {story_info['title']} 处理完成 ---")
            else:
                print(f"--- 故事 {story_info['title']} 处理失败 ---")
        except Exception as e:
            print(f"处理故事 {story_info['title']} 时发生未捕获的异常: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n=== 批量处理完成 ===")
    print(f"成功处理 {successful_downloads}/{total_stories} 个故事。")


# --- 运行脚本 ---
if __name__ == "__main__":
    asyncio.run(main())