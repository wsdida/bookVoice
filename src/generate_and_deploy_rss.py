# rss_generator_with_sftp.py
import os
import yaml
import glob
import re
import time
import hashlib  # 用于计算 MD5
from datetime import datetime, timezone, timedelta
from pathlib import Path
import urllib.parse
# 尝试导入 feedgen，如果失败则提示安装
try:
    from feedgen.feed import FeedGenerator
    # --- 新增：导入用于解析现有 RSS 的模块 ---
    import feedparser # 需要安装: pip install feedparser
    # ---
except ImportError as e:
    raise ImportError(f"请先安装所需库: pip install feedgen feedparser") from e

# 尝试导入 ollama，如果失败则提示安装
try:
    from ollama import Client
except ImportError:
    raise ImportError("请先安装 ollama 库: pip install ollama")

# --- 新增：尝试导入 paramiko，如果失败则提示安装 ---
try:
    import paramiko
    import stat  # 用于检查文件/目录属性
except ImportError:
    raise ImportError("请先安装 paramiko 库: pip install paramiko")
# --- 导入结束 ---

# --- 在脚本顶部定义常量 ---
# 用于查找章节目录的模式
CHAPTER_DIR_PATTERN = "Chapter_*_audiobook_output"
# 用于查找最终 MP3 文件的模式 (在章节目录内)
MP3_FILE_PATTERN = "chapter_*_final.mp3"
# 用于查找章节文本文件的模式 (在章节目录内)
TXT_FILE_PATTERN = "chapter_*.txt"
# 章节内容子目录 (如果存在)
CHAPTERS_SUBDIR = "chapters"
ollama_clent = Client() # 注意：原文件此处拼写为 ollama_clent，保持一致
# --- 常量定义结束 ---
def remove_special_chars(url):
    """Remove invalid characters from a URL path and return a clean path.""";
    """
    将字符串中的空格和换行符替换为下划线
    :param input_str: 输入字符串
    :return: 处理后的字符串
    """

    return re.sub(r'[\s\n]+', '_', url.strip())


def discover_chapters_by_audio(config):
    """
    根据音频文件的存在状态发现章节
    """
    print("开始根据音频文件发现章节...")
    chapters_info = []
    paths_config = config['paths']
    rss_config = config['rss']
    ollama_config = config.get('ollama', {})
    novels_root = Path(paths_config['novels_root_dir'])
    novel_folder_name = paths_config['novel_folder_name']
    novel_dir = novels_root / novel_folder_name

    if not novel_dir.exists():
        raise FileNotFoundError(f"小说目录不存在: {novel_dir}")

    # 查找章节目录
    chapter_pattern = CHAPTER_DIR_PATTERN
    full_pattern = str(novel_dir / chapter_pattern)
    print(f"搜索章节目录: {full_pattern}")
    chapter_dirs = glob.glob(full_pattern)
    print(f"找到 {len(chapter_dirs)} 个章节目录候选。")

    for chapter_dir_path in chapter_dirs:
        chapter_dir_path = Path(chapter_dir_path)
        chapter_subdir_name = chapter_dir_path.name

        # 检查音频文件是否存在
        audio_exists, mp3_file_path = check_chapter_audio_exists(novel_dir, chapter_subdir_name)
        if not audio_exists:
            print(f"  -> 跳过章节 {chapter_subdir_name} (音频文件不存在或为空)")
            continue

        # 提取章节编号
        chapter_num_match = re.search(r'Chapter[_\s]*([0-9]+)', chapter_subdir_name, re.IGNORECASE)
        if not chapter_num_match:
            print(f"  -> 警告: 无法从目录名 '{chapter_subdir_name}' 提取章节编号，跳过。")
            continue
        chapter_number_str = chapter_num_match.group(1)
        try:
            chapter_number = int(chapter_number_str)
            chapter_number_padded = f"{chapter_number:02d}"
        except ValueError:
            print(f"  -> 警告: 章节编号 '{chapter_number_str}' 无效，跳过。")
            continue

        # 查找 TXT 文件用于提取标题
        txt_search_paths = [
            os.path.join(chapter_dir_path, TXT_FILE_PATTERN),
            os.path.join(chapter_dir_path, CHAPTERS_SUBDIR, TXT_FILE_PATTERN)
        ]
        txt_file_path = None
        txt_files_found = []
        for pattern in txt_search_paths:
            txt_files_found.extend(glob.glob(pattern))
        if len(txt_files_found) > 0:
            txt_file_path = Path(txt_files_found[0])

        # 获取文件大小
        try:
            file_size = Path(mp3_file_path).stat().st_size
        except Exception as e:
            print(f"  -> 警告: 无法获取文件大小 {mp3_file_path}: {e}, 使用 0。")
            file_size = 0

        # 确定章节标题和描述
        chapter_title = f"Chapter {chapter_number_padded}"
        chapter_description = rss_config['default_chapter_description']
        if txt_file_path and txt_file_path.exists():
            ollama_title, ollama_desc = extract_chapter_info_with_ollama(
                str(txt_file_path), ollama_config, chapter_title, chapter_description
            )
            chapter_title = ollama_title
            chapter_description = ollama_desc

        # 构造公网音频 URL
        audio_base_url_template = paths_config['audio_base_url']
        audio_url = audio_base_url_template.format(
            novel_name=remove_special_chars(novel_folder_name),
            chapter_subdir=remove_special_chars(chapter_subdir_name)
        )

        # 获取实际的文件名
        mp3_filename = Path(mp3_file_path).name
        audio_url = f"{audio_url.rstrip('/')}/{mp3_filename}"

        # 确定发布日期
        pub_date = datetime.now(timezone.utc) + timedelta(days=rss_config.get('publish_date_offset_days', 0))

        chapters_info.append({
            'id': f"{novel_folder_name}_{chapter_subdir_name}",
            'number': chapter_number,
            'number_padded': chapter_number_padded,
            'subdir_name': chapter_subdir_name,
            'title': chapter_title,
            'description': chapter_description,
            'mp3_local_path': str(mp3_file_path),
            'mp3_url': audio_url,
            'file_size': file_size,
            'pub_date': pub_date
        })
        print(f"  -> 发现已完成章节: {chapter_subdir_name} (标题: {chapter_title})")

    # 按章节号排序
    chapters_info.sort(key=lambda x: x['number'])
    print(f"共发现 {len(chapters_info)} 个已完成的章节。")
    return chapters_info


# 修复 generate_and_deploy_rss.py 中的 check_chapter_audio_exists 函数
def check_chapter_audio_exists(novel_dir, chapter_subdir_name):
    """
    检查章节的音频文件是否存在
    """
    # 正确的路径应该是 novel_dir / chapter_subdir_name / CHAPTERS_SUBDIR
    chapter_dir_path = Path(novel_dir) / chapter_subdir_name / CHAPTERS_SUBDIR

    # 查找音频文件
    mp3_search_paths = [
        os.path.join(chapter_dir_path, MP3_FILE_PATTERN)
    ]

    for pattern in mp3_search_paths:
        mp3_files_found = glob.glob(pattern)
        if len(mp3_files_found) > 0:
            mp3_file_path = Path(mp3_files_found[0])
            if mp3_file_path.exists() and mp3_file_path.stat().st_size > 0:
                return True, str(mp3_file_path)

    return False, None


# 在 generate_and_deploy_rss.py 文件末尾添加以下函数

def check_and_synthesize_missing_audio(input_directory, config_path='config.yaml'):
    """
    检查并合成缺失的音频文件
    """
    try:
        # 遍历所有章节输出目录
        chapter_dirs = list(Path(input_directory).glob("Chapter_*_audiobook_output"))

        for chapter_dir in chapter_dirs:
            print(f"检查章节目录: {chapter_dir.name}")

            # 检查最终MP3文件是否存在
            txt_file = list(chapter_dir.parent.glob(f"{chapter_dir.name.replace('_audiobook_output', '')}.txt"))
            if txt_file:
                txt_filename = txt_file[0].stem
                final_mp3 = chapter_dir / "chapters" / f"{txt_filename}_final.mp3"

                if not final_mp3.exists():
                    print(f"  -> 缺失最终MP3文件: {final_mp3}")

                    # 检查日志文件
                    log_file = chapter_dir / "logs" / "audiobook.log"
                    if log_file.exists():
                        # 检查日志中是否显示生成完成
                        with open(log_file, 'r', encoding='utf-8') as f:
                            log_content = f.read()

                        if "✅ === 有声书生成完成" in log_content:
                            print(f"  -> 日志显示已完成但缺少MP3文件，重新合成: {chapter_dir.name}")
                            # 重新生成该章节
                            try:
                                from audiobook_generator import generate_audiobook
                                generate_audiobook(str(chapter_dir.parent), str(txt_file[0]), config_path,
                                                   force_rebuild=True)
                                print(f"  -> 重新合成完成: {chapter_dir.name}")
                            except Exception as e:
                                print(f"  -> 重新合成失败: {e}")
                        else:
                            print(f"  -> 日志显示未完成生成: {chapter_dir.name}")
                    else:
                        print(f"  -> 缺少日志文件: {log_file}")
                else:
                    print(f"  -> 最终MP3文件已存在: {final_mp3}")

    except Exception as e:
        print(f"检查和合成过程中出错: {e}")


def verify_audio_files_integrity(input_directory):
    """
    验证音频文件的完整性
    """
    try:
        chapter_dirs = list(Path(input_directory).glob("Chapter_*_audiobook_output"))

        for chapter_dir in chapter_dirs:
            print(f"验证章节目录: {chapter_dir.name}")

            # 检查日志文件
            log_file = chapter_dir / "logs" / "audiobook.log"
            if not log_file.exists():
                print(f"  -> 缺少日志文件: {log_file}")
                continue

            # 读取日志内容
            with open(log_file, 'r', encoding='utf-8') as f:
                log_lines = f.readlines()

            # 查找混音完成记录
            mix_completed = any("✅ 混音完成" in line for line in log_lines)
            generation_completed = any("✅ === 有声书生成完成" in line for line in log_lines)

            print(f"  -> 混音完成: {mix_completed}")
            print(f"  -> 生成完成: {generation_completed}")

            # 检查最终MP3文件
            txt_file = list(chapter_dir.parent.glob(f"{chapter_dir.name.replace('_audiobook_output', '')}.txt"))
            if txt_file:
                txt_filename = txt_file[0].stem
                final_mp3 = chapter_dir / "chapters" / f"{txt_filename}_final.mp3"
                if final_mp3.exists():
                    file_size = final_mp3.stat().st_size
                    print(f"  -> 最终MP3文件大小: {file_size} 字节")
                    if file_size == 0:
                        print(f"  -> 最终MP3文件为空，需要重新生成")
                else:
                    print(f"  -> 最终MP3文件不存在")

    except Exception as e:
        print(f"验证音频文件完整性时出错: {e}")


def check_rss_consistency(input_directory, config_path='rss_config.yaml'):
    """
    检查RSS文件与实际音频文件的一致性
    """
    try:
        # 加载RSS配置
        import yaml
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

        # RSS文件路径
        rss_file = Path(input_directory) / config['paths']['local_rss_output']

        # 查找所有章节目录
        chapter_dirs = list(Path(input_directory).glob("Chapter_*_audiobook_output"))
        completed_chapters = []

        # 检查每个章节目录中是否有完成的音频文件
        for chapter_dir in chapter_dirs:
            txt_file = list(chapter_dir.parent.glob(f"{chapter_dir.name.replace('_audiobook_output', '')}.txt"))
            if txt_file:
                txt_filename = txt_file[0].stem
                final_mp3 = chapter_dir / "chapters" / f"{txt_filename}_final.mp3"
                if final_mp3.exists() and final_mp3.stat().st_size > 0:
                    # 从目录名提取章节编号
                    match = re.search(r'Chapter[_\s]*([0-9]+)', chapter_dir.name, re.IGNORECASE)
                    if match:
                        chapter_num = int(match.group(1))
                        completed_chapters.append(chapter_num)

        print(f"已完成的章节: {sorted(completed_chapters)}")
        print(f"总章节数: {len(chapter_dirs)}")
        print(f"已完成章节数: {len(completed_chapters)}")

        # 如果有RSS文件，检查其中的条目数
        if rss_file.exists():
            try:
                import feedparser
                feed = feedparser.parse(str(rss_file))
                rss_entries = len(feed.entries)
                print(f"RSS条目数: {rss_entries}")

                if rss_entries != len(completed_chapters):
                    print(f"  -> RSS条目数与完成章节数不一致，需要更新RSS")
                    return True  # 需要更新RSS
            except Exception as e:
                print(f"  -> 解析RSS文件出错: {e}")
                return True  # 需要重新生成RSS
        elif len(completed_chapters) > 0:
            print(f"  -> 缺少RSS文件但有完成的章节，需要生成RSS")
            return True  # 需要生成RSS

        return False  # 不需要更新RSS

    except Exception as e:
        print(f"检查RSS一致性时出错: {e}")
        return True  # 出错时默认需要更新


def comprehensive_check_and_update(input_directory, config_path='config.yaml', rss_config_path='rss_config.yaml'):
    """
    综合检查并更新音频文件和RSS
    """
    print("=== 开始综合检查 ===")

    # 1. 检查并合成缺失的音频文件
    print("\n1. 检查并合成缺失的音频文件...")
    check_and_synthesize_missing_audio(input_directory, config_path)

    # 2. 验证音频文件完整性
    print("\n2. 验证音频文件完整性...")
    verify_audio_files_integrity(input_directory)

    # 3. 检查RSS一致性
    print("\n3. 检查RSS一致性...")
    need_rss_update = check_rss_consistency(input_directory, rss_config_path)

    # 4. 如果需要，更新RSS
    if need_rss_update:
        print("\n4. 更新RSS文件...")
        try:
            run_rss_update_process(input_directory)
            print("✅ RSS更新完成")
        except Exception as e:
            print(f"❌ RSS更新失败: {e}")
    else:
        print("\n4. RSS文件已是最新，无需更新")

    print("\n=== 综合检查完成 ===")


def load_config(config_path='rss_config.yaml'):
    """加载 RSS 配置文件"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        print(f"成功加载配置文件: {config_path}")
        return config
    except Exception as e:
        print(f"加载配置文件失败 {config_path}: {e}")
        raise

def load_processed_chapters(log_file_path):
    """从日志文件加载已处理的章节标识符"""
    processed = set()
    if os.path.exists(log_file_path):
        try:
            with open(log_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    chapter_id = line.strip()
                    if chapter_id:
                        processed.add(chapter_id)
            print(f"已加载 {len(processed)} 个已处理的章节记录。")
        except Exception as e:
            print(f"读取已处理章节日志失败 {log_file_path}: {e}")
    else:
        print(f"已处理章节日志文件不存在，将创建新文件: {log_file_path}")
    return processed

def save_processed_chapter(log_file_path, chapter_id):
    """将新处理的章节标识符追加到日志文件"""
    try:
        with open(log_file_path, 'a', encoding='utf-8') as f:
            f.write(f"{chapter_id}\n")
        print(f"已记录章节处理状态: {chapter_id}")
    except Exception as e:
        print(f"记录章节处理状态失败 {chapter_id}: {e}")

def extract_chapter_title_from_file(text_file_path, fallback_title):
    """尝试从章节文本文件的第一行提取标题"""
    try:
        with open(text_file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
        if first_line:
            # --- 修改后的正则表达式 ---
            # 使用 re.IGNORECASE 标志来忽略大小写
            # 匹配中文前缀 (例如: 第一章, 序言, 第1回)
            # 或英文前缀 (例如: Chapter 1, CHAPTER I, Prologue, Epilogue, Introduction)
            # ^: 行首
            # (?: ... | ... ): 非捕获组，用于组合不同的模式
            # 第?[0-9零一二三四五六七八九十百千万亿]+[章节回幕集部篇卷册]? : 中文数字和章节类型
            # (?:Chapter|CHAPTER)\s+[0-9IVXLCDM]+ : 英文 "Chapter"/"CHAPTER" 后跟阿拉伯数字或罗马数字
            # (?:Prologue|Epilogue|Introduction|Preface|Foreword|Conclusion|Appendix|序章|引子|尾声|后记|前言|序言|附录) : 常见的非数字章节标题 (可根据需要扩展)
            # \s*: 匹配前缀后的可选空白字符
            cleaned_title = re.sub(
                r'^(?:第?[0-9零一二三四五六七八九十百千万亿]+[章节回幕集部篇卷册]?|(?:Chapter|CHAPTER)\s+[0-9IVXLCDM]+|(?:Prologue|Epilogue|Introduction|Preface|Foreword|Conclusion|Appendix|序章|引子|尾声|后记|前言|序言|附录))\s*',
                '',
                first_line,
                flags=re.IGNORECASE
            )
            # --- 修改结束 ---
            if cleaned_title:  # 如果清理后还有内容，则认为是标题
                return cleaned_title
            else:  # 如果清理后没有内容了，可能整行都是前缀，返回原行或回退标题
                # 可以选择返回原始行（可能整行就是 "Chapter 1"）
                # return first_line
                # 或者返回回退标题
                return fallback_title
    except Exception as e:
        print(f"  -> 警告: 从文件提取章节标题失败 {text_file_path}: {e}")
    return fallback_title


def extract_chapter_info_with_ollama(text_file_path, ollama_config, fallback_title, fallback_description):
    """
    使用 Ollama 大模型从章节文本中提取英文标题和简介。
    返回 (title, description) 元组。
    """
    if not ollama_config.get('enabled', False):
        print(f"  -> Ollama 未启用，跳过内容分析。")
        return fallback_title, fallback_description

    model_name = ollama_config.get('model', 'qwen2:7b')
    timeout = ollama_config.get('timeout', 120)
    retries = ollama_config.get('retries', 2)

    try:
        # 读取文本内容
        with open(text_file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        if not content.strip():
            print(f"  -> 警告: 章节文件内容为空 {text_file_path}。")
            return fallback_title, fallback_description

        # 构造英文提示词
        prompt = f"""You are a novel content analysis assistant. Please carefully read the following novel chapter text content and provide a concise and clear title and summary in English.

Requirements:
1. Title: Provide a short title that summarizes the core content of this chapter, no more than 20 words.
2. Summary: Summarize the main plot, key events, or important turning points of this chapter in one paragraph (50-100 words).
3. Return the result strictly in the following JSON format without any other content:
{{
  "title": "The extracted title in English",
  "summary": "The extracted summary in English"
}}

The text content is as follows:
{content[:4000]} # Limit the text sent to the model to avoid timeout or exceeding context window
"""

        messages = [
            {"role": "user", "content": prompt}
        ]

        print(f"  -> 调用 Ollama 模型 '{model_name}' 分析章节内容...")
        for attempt in range(retries + 1):
            try:
                response = ollama_clent.chat(
                    model=model_name,
                    messages=messages,
                )
                response_text = response['message']['content'].strip()

                # 尝试解析 JSON
                import json
                json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                    try:
                        data = json.loads(json_str)
                        title = data.get('title', fallback_title).strip()
                        summary = data.get('summary', fallback_description).strip()
                        if title and summary:
                            print(f"  -> Ollama 分析成功: 标题='{title}', 简介='{summary[:30]}...'")
                            return title, summary
                        else:
                            print(f"  -> 警告: Ollama 返回的 JSON 缺少 title 或 summary 字段。")
                    except json.JSONDecodeError as je:
                        print(f"  -> 警告: Ollama 返回的文本无法解析为 JSON ({je})。响应内容: {response_text[:100]}...")
                else:
                    print(f"  -> 警告: Ollama 返回的文本中未找到 JSON 格式。响应内容: {response_text[:100]}...")

            except Exception as e:
                print(f"  -> 警告: Ollama 调用失败 (尝试 {attempt + 1}/{retries + 1}): {e}")
                if attempt < retries:
                    print(f"      等待 {2 ** attempt} 秒后重试...")
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    print(f"  -> 错误: Ollama 调用最终失败，使用回退值。")

    except Exception as e:
        print(f"  -> 错误: 调用 Ollama 分析章节内容时发生未知错误 {text_file_path}: {e}")

    return fallback_title, fallback_description


def discover_and_filter_chapters(config, processed_chapters):
    """
    根据配置发现章节，并过滤掉已处理的章节
    返回章节信息列表
    """
    print("开始发现新章节...")
    chapters_info = []
    paths_config = config['paths']
    rss_config = config['rss']
    ollama_config = config.get('ollama', {})  # 获取 Ollama 配置
    novels_root = Path(paths_config['novels_root_dir'])
    novel_folder_name = paths_config['novel_folder_name']  # 从配置读取
    novel_dir = novels_root / novel_folder_name
    if not novel_dir.exists():
        raise FileNotFoundError(f"小说目录不存在: {novel_dir}")
    # --- 使用常量定义的模式查找章节目录 ---
    chapter_pattern = CHAPTER_DIR_PATTERN
    full_pattern = str(novel_dir / chapter_pattern)
    print(f"搜索章节目录: {full_pattern}")
    chapter_dirs = glob.glob(full_pattern)
    print(f"找到 {len(chapter_dirs)} 个章节目录候选。")
    for chapter_dir_path in chapter_dirs:
        chapter_dir_path = Path(chapter_dir_path)
        chapter_subdir_name = chapter_dir_path.name
        # 生成章节唯一标识符 (用于断点续传)
        chapter_id = f"{novel_folder_name}_{chapter_subdir_name}"
        if chapter_id in processed_chapters:
            print(f"  -> 跳过已处理章节: {chapter_id}")
            continue
        # --- 提取章节编号 ---
        chapter_num_match = re.search(r'Chapter[_\s]*([0-9]+)', chapter_subdir_name, re.IGNORECASE)
        if not chapter_num_match:
            print(f"  -> 警告: 无法从目录名 '{chapter_subdir_name}' 提取章节编号，跳过。")
            continue
        chapter_number_str = chapter_num_match.group(1)
        try:
            chapter_number = int(chapter_number_str)
            chapter_number_padded = f"{chapter_number:02d}"
        except ValueError:
            print(f"  -> 警告: 章节编号 '{chapter_number_str}' 无效，跳过。")
            continue
        # --- 动态查找文件 ---
        # 构造可能的文件路径 (考虑 chapters 子目录)
        # 修正文件路径查找逻辑
        mp3_search_paths = [
            os.path.join(chapter_dir_path, MP3_FILE_PATTERN),
            os.path.join(chapter_dir_path, CHAPTERS_SUBDIR, MP3_FILE_PATTERN)
        ]
        txt_search_paths = [
            os.path.join(chapter_dir_path, TXT_FILE_PATTERN),
            os.path.join(chapter_dir_path, CHAPTERS_SUBDIR, TXT_FILE_PATTERN)
        ]
        mp3_file_path = None
        txt_file_path = None
        # 1. 查找 MP3 文件
        mp3_files_found = []
        for pattern in mp3_search_paths:
            mp3_files_found.extend(glob.glob(pattern))
        if len(mp3_files_found) > 0:
            mp3_file_path = Path(mp3_files_found[0])
            mp3_filename = mp3_file_path.name  # 获取文件名，用于构造 URL
        elif len(mp3_files_found) == 0:
            print(
                f"  -> 警告: 在 {chapter_dir_path} 及其 {CHAPTERS_SUBDIR} 子目录中未找到匹配 '{MP3_FILE_PATTERN}' 的 MP3 文件，跳过章节 {chapter_id}。")
            continue
        else:
            print(
                f"  -> 警告: 在 {chapter_dir_path} 及其 {CHAPTERS_SUBDIR} 子目录中找到多个匹配 '{MP3_FILE_PATTERN}' 的 MP3 文件 {mp3_files_found}，无法确定，跳过章节 {chapter_id}。")
            continue
        # 2. 查找 TXT 文件
        txt_files_found = []
        for pattern in txt_search_paths:
            txt_files_found.extend(glob.glob(pattern))
        if len(txt_files_found) > 0:
            txt_file_path = Path(txt_files_found[0])
        elif len(txt_files_found) == 0:
            print(
                f"  -> 警告: 在 {chapter_dir_path} 及其 {CHAPTERS_SUBDIR} 子目录中未找到匹配 '{TXT_FILE_PATTERN}' 的 TXT 文件。将无法从文件提取标题和简介。")
            # 不跳过，因为标题和简介可能有默认值或其他来源
        else:
            print(
                f"  -> 警告: 在 {chapter_dir_path} 及其 {CHAPTERS_SUBDIR} 子目录中找到多个匹配 '{TXT_FILE_PATTERN}' 的 TXT 文件 {txt_files_found}。将使用第一个用于标题和简介提取。")
            txt_file_path = Path(txt_files_found[0])  # 使用第一个
        # --- 获取文件大小 ---
        try:
            file_size = mp3_file_path.stat().st_size
        except Exception as e:
            print(f"  -> 警告: 无法获取文件大小 {mp3_file_path}: {e}, 使用 0。")
            file_size = 0
        # --- 确定章节标题和描述 ---
        chapter_title = f"第 {chapter_number_padded} 章"
        chapter_description = rss_config['default_chapter_description']
        ollama_title, ollama_desc = extract_chapter_info_with_ollama(
            str(txt_file_path), ollama_config, chapter_title, chapter_description
        )
        chapter_title = ollama_title
        chapter_description = ollama_desc
        # --- 构造公网音频 URL ---
        audio_base_url_template = paths_config['audio_base_url']
        audio_url = audio_base_url_template.format(
            novel_name=remove_special_chars(novel_folder_name),
            chapter_subdir=remove_special_chars(chapter_subdir_name)
        )
        # 确保 URL 格式正确，并拼接实际找到的文件名
        audio_url = f"{audio_url.rstrip('/')}/{mp3_filename}"
        # --- 确定发布日期 ---
        pub_date = datetime.now(timezone.utc) + timedelta(days=rss_config.get('publish_date_offset_days', 0))
        chapters_info.append({
            'id': chapter_id,
            'number': chapter_number,
            'number_padded': chapter_number_padded,
            'subdir_name': chapter_subdir_name,
            'title': chapter_title,
            'description': chapter_description,
            'mp3_local_path': str(mp3_file_path),
            'mp3_url': audio_url,
            'file_size': file_size,
            'pub_date': pub_date
        })
        print(f"  -> 发现新章节: {chapter_id} (标题: {chapter_title}, 音频: {mp3_filename})")
    # 按章节号排序
    chapters_info.sort(key=lambda x: x['number'])
    print(f"共发现 {len(chapters_info)} 个新章节待处理。")
    return chapters_info

# --- 修改：load_or_create_feed 函数，实现断点续传 ---
def load_or_create_feed(config, rss_output_path):
    """
    根据配置创建一个新的 FeedGenerator 对象，并加载 podcast 扩展。
    如果 RSS 文件已存在，则加载旧文件中的条目以实现真正的断点续传。
    """
    print("创建或加载 FeedGenerator 对象 (使用 podcast 扩展)...")
    podcast_config = config['podcast']
    paths_config = config['paths']
    novel_name = paths_config['novel_folder_name']

    # 确定播客标题
    podcast_title = podcast_config['title']
    if config['rss'].get('use_novel_name_as_title', False):
        podcast_title = novel_name

    # 创建 FeedGenerator 对象
    fg = FeedGenerator()

    # --- 设置基本 RSS 信息 ---
    fg.title(podcast_title)
    fg.description(podcast_config['description'])
    fg.link(href=podcast_config['link'], rel='alternate')
    fg.image(url=podcast_config['image_url'])
    fg.language(podcast_config['language'])
    fg.copyright(podcast_config['copyright'])

    # --- 加载并设置 Podcast 扩展信息 ---
    try:
        fg.load_extension('podcast')
        print("  -> 成功加载 feedgen.podcast 扩展。")
    except Exception as e:
        print(f"  -> 警告: 加载 feedgen.podcast 扩展时出错: {e}")
        print("      将尝试生成基本 RSS，可能缺少部分播客特定标签。")

    try:
        fg.podcast.itunes_author(podcast_config['author'])
        fg.podcast.itunes_summary(podcast_config['description'])
        fg.podcast.itunes_owner(name=podcast_config['author'], email=podcast_config['email'])
        fg.podcast.itunes_image(podcast_config['image_url'])
        category_name = podcast_config.get('category', 'Arts')
        fg.podcast.itunes_category([{'cat': category_name}])
        explicit_value = podcast_config['explicit'].lower()
        if explicit_value in ('yes', 'true', '1'):
            fg.podcast.itunes_explicit('yes')
        elif explicit_value in ('no', 'false', '0'):
            fg.podcast.itunes_explicit('no')
        else:
            fg.podcast.itunes_explicit('clean')
        print("  -> Podcast 扩展元数据已设置。")
    except Exception as e:
        print(f"  -> 警告: 设置 Podcast 扩展元数据时发生未知错误: {e}")
        print("      将尝试生成基本 RSS，可能缺少部分播客特定标签。")

    # --- 新增：加载已存在的 RSS 条目 ---
    existing_entries = {} # 使用字典存储，键为 entry.id()，值为 entry 对象
    if os.path.exists(rss_output_path):
        print(f"  -> 尝试加载已存在的 RSS 文件: {rss_output_path}")
        try:
            # 使用 feedparser 解析旧 RSS 文件
            parsed_feed = feedparser.parse(rss_output_path)
            if parsed_feed.bozo: # bozo 为 True 表示解析时有警告或错误
                 print(f"  -> 警告: 解析旧 RSS 文件时遇到问题: {parsed_feed.bozo_exception}")

            # 遍历解析出的条目，转换为 feedgen.entry.FeedEntry 并添加到 fg
            for parsed_entry in parsed_feed.entries:
                # 创建一个新的 FeedEntry 对象
                entry = fg.add_entry(order='append') # 使用 append 确保顺序
                # 将解析出的数据复制到新 entry
                # feedparser 和 feedgen 的字段名可能略有不同
                entry.id(parsed_entry.get('id', ''))
                entry.title(parsed_entry.get('title', ''))
                entry.description(parsed_entry.get('description', ''))
                entry.link(href=parsed_entry.get('link', ''))
                if 'published_parsed' in parsed_entry:
                    # feedparser 将时间解析为 struct_time，需要转换为 datetime
                    pub_date = datetime(*parsed_entry.published_parsed[:6], tzinfo=timezone.utc)
                    entry.published(pub_date)
                elif 'updated_parsed' in parsed_entry:
                     pub_date = datetime(*parsed_entry.updated_parsed[:6], tzinfo=timezone.utc)
                     entry.published(pub_date)

                # 处理 enclosure (音频文件)
                if parsed_entry.enclosures:
                    enc = parsed_entry.enclosures[0] # 通常只有一个 enclosure
                    entry.enclosure(url=enc.href, length=str(enc.get('length', 0)), type=enc.get('type', 'audio/mpeg'))

                # 将 entry 存入字典以便后续去重
                entry_id = entry.id()
                if entry_id:
                    existing_entries[entry_id] = entry

            print(f"  -> 成功从旧 RSS 文件加载了 {len(existing_entries)} 个条目。")
        except Exception as e:
            print(f"  -> 警告: 加载或解析旧 RSS 文件失败: {e}。将创建全新的 RSS。")
    else:
        print(f"  -> 旧 RSS 文件不存在 ({rss_output_path})，将创建全新的 RSS。")

    print(f"  -> FeedGenerator 对象已创建/加载 (标题: {podcast_title})")
    # 返回 fg 对象和已存在的条目字典
    return fg, existing_entries

# --- 修改：add_chapters_to_feed 函数，避免重复添加 ---
def add_chapters_to_feed(fg, chapters_info_to_add, existing_entries):
    """将新的章节信息添加到 FeedGenerator 对象中，避免重复添加"""
    print(f"向 Feed 添加 {len(chapters_info_to_add)} 个新章节...")
    added_count = 0
    for chapter_info in chapters_info_to_add:
        # --- 检查是否已存在 ---
        entry_id = chapter_info['mp3_url'] # 使用音频 URL 作为唯一 ID
        if entry_id in existing_entries:
            print(f"  -> 跳过已存在的章节: {chapter_info['title']} (ID: {entry_id})")
            continue # 跳过已存在的条目

        # 创建一个 Feed Entry (对应 RSS 的 item)
        entry = fg.add_entry(order='append') # 添加到末尾

        # --- 设置 Entry 的基本 RSS 信息 ---
        entry.id(entry_id)
        entry.title(chapter_info['title'])
        entry.description(chapter_info['description'])
        entry.link(href=chapter_info['mp3_url'])
        entry.published(chapter_info['pub_date'])

        # --- 设置 Enclosure (音频文件) ---
        entry.enclosure(
            url=chapter_info['mp3_url'],
            length=str(chapter_info['file_size']),
            type='audio/mpeg'
        )

        # --- 设置 Podcast 扩展信息 ---
        try:
            if hasattr(entry, 'podcast'):
                if hasattr(entry.podcast, 'itunes_subtitle'):
                    entry.podcast.itunes_subtitle(chapter_info['description'])
                if hasattr(entry.podcast, 'itunes_summary'):
                    entry.podcast.itunes_summary(chapter_info['description'])
            print(f"    -> 已为单集 '{chapter_info['title']}' 设置 Podcast 扩展信息。")
        except Exception as e:
            print(f"    -> 警告: 为单集 '{chapter_info['title']}' 设置 Podcast 扩展信息时出错: {e}")

        print(f"  -> 已添加新单集: {chapter_info['title']} (ID: {entry_id})")
        added_count += 1
        # 将新添加的条目也加入 existing_entries 字典，以防后续逻辑有依赖
        existing_entries[entry_id] = entry

    print(f"章节添加完成。新增了 {added_count} 个章节。")

def save_feed(fg, output_path):
    """将 FeedGenerator 对象保存到文件"""
    print(f"保存 Podcast RSS 到: {output_path}")
    try:
        # 确保输出目录存在
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        # 保存 RSS Feed (atom_file 用于 Atom, rss_file 用于 RSS)
        fg.rss_file(filename=output_path, pretty=True)  # pretty=True 保留格式化
        print("Podcast RSS 保存成功。")
    except Exception as e:
        print(f"保存 Podcast RSS 失败 {output_path}: {e}")
        raise

# --- 新增：SFTP 上传相关函数 ---
def _calculate_local_md5(file_path, chunk_size=8192):
    """计算本地文件的 MD5 哈希值"""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(chunk_size), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"    -> 计算本地文件 MD5 失败 {file_path}: {e}")
        return None

def _calculate_remote_md5(sftp_client, remote_file_path, ssh_client):
    """通过 SSH 在远程服务器上计算文件的 MD5 哈希值"""
    try:
        # 执行远程 md5sum 命令
        # 使用引号包围路径，以防路径中包含空格
        stdin, stdout, stderr = ssh_client.exec_command(f'md5sum "{remote_file_path}"')
        output = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        if error:
            print(f"    -> 远程执行 md5sum 出错: {error}")
            return None
        # md5sum 输出格式通常是 "hash  filename"
        # 我们只需要哈希部分
        if output:
            remote_md5 = output.split()[0]
            return remote_md5
        else:
            print(f"    -> 远程 md5sum 命令未返回输出。")
            return None
    except Exception as e:
        print(f"    -> 计算远程文件 MD5 失败 {remote_file_path}: {e}")
        return None

def _should_upload_file(sftp_client, ssh_client, local_file_path, remote_file_path):
    """
    比较本地文件和远程文件的 MD5 值，决定是否需要上传。
    返回 True 表示需要上传，False 表示不需要。
    """
    print(f"    -> 比较文件 MD5: {local_file_path} <-> {remote_file_path}")
    # 1. 检查远程文件是否存在
    try:
        sftp_client.stat(remote_file_path)
    except FileNotFoundError:
        print(f"    -> 远程文件不存在，需要上传。")
        return True
    except Exception as e:
        print(f"    -> 检查远程文件是否存在时出错: {e}。决定重新上传。")
        return True  # 出错时默认重新上传
    # 2. 计算本地文件 MD5
    local_md5 = _calculate_local_md5(local_file_path)
    if local_md5 is None:
        print(f"    -> 无法计算本地文件 MD5，决定重新上传。")
        return True
    # 3. 计算远程文件 MD5
    remote_md5 = _calculate_remote_md5(sftp_client, remote_file_path, ssh_client)
    if remote_md5 is None:
        print(f"    -> 无法计算远程文件 MD5，决定重新上传。")
        return True
    # 4. 比较 MD5
    print(f"    -> 本地 MD5: {local_md5}")
    print(f"    -> 远程 MD5: {remote_md5}")
    if local_md5 == remote_md5:
        print(f"    -> MD5 值一致，跳过上传。")
        return False
    else:
        print(f"    -> MD5 值不一致，需要上传。")
        return True

def _ensure_remote_dir_exists(sftp_client, remote_directory):
    """
    确保远程服务器上的目录存在，如果不存在则递归创建。
    """
    if not remote_directory or remote_directory == '/' or remote_directory == '.':
        return
    try:
        sftp_client.stat(remote_directory)  # 检查目录是否存在
        # print(f"  -> 远程目录已存在: {remote_directory}")
    except FileNotFoundError:
        # 目录不存在，需要创建
        # 递归确保父目录存在
        parent_dir = os.path.dirname(remote_directory)
        _ensure_remote_dir_exists(sftp_client, parent_dir)
        # 创建当前目录
        print(f"  -> 正在创建远程目录: {remote_directory}")
        sftp_client.mkdir(remote_directory)
    except Exception as e:
        # 其他错误，可能是权限问题等
        print(f"  -> 检查/创建远程目录 {remote_directory} 时出错: {e}")
        raise

def upload_files_via_sftp(config, rss_local_path, chapters_info_to_add):
    """
    使用 SFTP 将 RSS 文件和 MP3 文件上传到服务器。
    包含文件 MD5 比对逻辑：如果远程文件不存在或 MD5 不一致，则上传。
    RSS 文件将上传到以小说名称命名的远程目录下。
    """
    sftp_config = config.get('sftp', {})
    if not sftp_config.get('enabled', False):
        print("SFTP 上传未启用，跳过上传步骤。")
        return
    print("\n=== 开始 SFTP 文件上传 (含 MD5 比对) ===")
    # 1. 创建 SFTP 和 SSH 客户端
    transport = None
    sftp = None
    ssh = None  # 新增 SSH 客户端用于执行命令
    try:
        transport = paramiko.Transport((sftp_config['host'], sftp_config['port']))
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 自动添加主机密钥（生产环境应更谨慎）
        # 2. 连接到服务器 (使用密码或密钥)
        auth_method = "密码"
        if 'private_key_path' in sftp_config and sftp_config['private_key_path']:
            # 使用私钥认证
            auth_method = "私钥"
            private_key_path = sftp_config['private_key_path']
            passphrase = sftp_config.get('private_key_passphrase')
            # 尝试不同的密钥类型
            private_key = None
            key_classes = [paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey, paramiko.DSSKey]
            for key_class in key_classes:
                try:
                    if passphrase:
                        private_key = key_class.from_private_key_file(private_key_path, password=passphrase)
                    else:
                        private_key = key_class.from_private_key_file(private_key_path)
                    break  # 成功加载则跳出循环
                except paramiko.PasswordRequiredException:
                    print(f"  -> 私钥 {private_key_path} 需要密码。")
                    break  # 跳出，让下面的错误处理捕获
                except Exception:
                    continue  # 尝试下一种密钥类型
            if private_key is None:
                 raise paramiko.AuthenticationException(f"无法使用提供的任何支持的密钥类型加载私钥 {private_key_path}")
            transport.connect(username=sftp_config['username'], pkey=private_key)
            ssh.connect(sftp_config['host'], port=sftp_config['port'], username=sftp_config['username'], pkey=private_key)
            print(f"  -> 已使用私钥 {private_key_path} 连接到 SFTP/SSH 服务器 {sftp_config['host']}:{sftp_config['port']}")
        else:
            # 使用密码认证
            transport.connect(username=sftp_config['username'], password=sftp_config['password'])
            ssh.connect(sftp_config['host'], port=sftp_config['port'], username=sftp_config['username'], password=sftp_config['password'])
            print(f"  -> 已使用{auth_method}连接到 SFTP/SSH 服务器 {sftp_config['host']}:{sftp_config['port']}")
        sftp = paramiko.SFTPClient.from_transport(transport)
        # 3. 上传 RSS 文件 (先比对 MD5)
        # 从配置中获取 RSS 路径模板
        remote_rss_path_template = sftp_config.get('remote_rss_path_template')
        if not remote_rss_path_template:
             # 如果模板不存在，尝试使用旧的固定路径配置作为后备
             remote_rss_path_template = sftp_config.get('remote_rss_path', '')
             if not remote_rss_path_template:
                 raise ValueError("SFTP 配置中缺少 remote_rss_path_template 或 remote_rss_path")
        # 获取小说名称
        novel_name = remove_special_chars(config['paths']['novel_folder_name'])
        # 使用小说名称替换模板中的占位符，生成实际的远程路径
        remote_rss_path = remote_rss_path_template.format(novel_name=novel_name)
        print(f"\n  -> 检查/上传 RSS 文件: {rss_local_path} -> {remote_rss_path}")
        if _should_upload_file(sftp, ssh, rss_local_path, remote_rss_path):
            # 确保远程 RSS 文件的目录存在
            remote_rss_dir = os.path.dirname(remote_rss_path)
            _ensure_remote_dir_exists(sftp, remote_rss_dir)
            print(f"    -> 正在上传 RSS 文件: {rss_local_path} -> {remote_rss_path}")
            sftp.put(rss_local_path, remote_rss_path)
            print(f"    -> RSS 文件上传成功。")
        else:
             print(f"    -> RSS 文件无需上传。")
        # 4. 上传 MP3 文件 (先比对 MD5)
        # 获取配置
        remote_mp3_base_dir = sftp_config['remote_mp3_base_dir'].rstrip('/').rstrip('\\') # 兼容 Windows
        novels_root = config['paths']['novels_root_dir'].rstrip('/').rstrip('\\') # 兼容 Windows
        print(f"\n  -> 开始检查/上传 {len(chapters_info_to_add)} 个章节的 MP3 文件...")
        for chapter_info in chapters_info_to_add:
            local_mp3_full_path = chapter_info['mp3_local_path']
            # --- 计算相对路径以确保一致性 ---
            try:
                local_mp3_path_obj = Path(local_mp3_full_path)
                novels_root_path_obj = Path(novels_root)
                # 计算相对路径 (使用 pathlib 处理跨平台路径)
                local_mp3_rel_path_obj = local_mp3_path_obj.relative_to(novels_root_path_obj)
                # 转换为 POSIX 风格路径字符串 (使用 / 分隔符)，便于拼接远程路径
                local_mp3_rel_path_posix =remove_special_chars(local_mp3_rel_path_obj.as_posix())
            except ValueError as ve:
                print(f"    -> 错误: 本地 MP3 路径 {local_mp3_full_path} 不在小说根目录 {novels_root} 下。跳过上传此文件。错误: {ve}")
                continue
            # 2. 构造远程路径
            if remote_mp3_base_dir:
                 remote_mp3_path = f"{remote_mp3_base_dir}/{local_mp3_rel_path_posix}"
            else:
                 remote_mp3_path = local_mp3_rel_path_posix # 如果没有基础目录，则直接使用相对路径
            print(f"\n    -> 检查/上传 MP3: {local_mp3_full_path}")
            if _should_upload_file(sftp, ssh, local_mp3_full_path, remote_mp3_path):
                # 确保远程 MP3 文件的目录存在
                remote_mp3_dir = os.path.dirname(remote_mp3_path)
                _ensure_remote_dir_exists(sftp, remote_mp3_dir)
                print(f"      -> 正在上传 MP3: {local_mp3_full_path} -> {remote_mp3_path}")
                sftp.put(local_mp3_full_path, remote_mp3_path)
                print(f"      -> MP3 文件上传成功。")
            else:
                 print(f"      -> MP3 文件无需上传。")
        print("\n=== 所有文件检查/上传完成 (MD5 比对) ===")
    except Exception as e:
        print(f"SFTP 上传过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        raise # 重新抛出异常，让主函数知道上传失败
    finally:
        # 5. 关闭连接
        if ssh:
            ssh.close()
            print("  -> SSH 连接已关闭。")
        if sftp:
            sftp.close()
            print("  -> SFTP 连接已关闭 (SFTP Client)。")
        if transport:
            transport.close()
            print("  -> SFTP 连接已关闭 (Transport)。")

# --- 修改：run_rss_update_process 函数，整合断点续传和 SFTP 上传 ---
def load_existing_rss_entries(rss_output_path):
    """
    加载已存在的RSS条目，返回章节编号集合
    """
    existing_chapter_numbers = set()
    if os.path.exists(rss_output_path):
        try:
            # 使用 feedparser 解析 RSS 文件
            parsed_feed = feedparser.parse(rss_output_path)
            if parsed_feed.bozo:  # bozo 为 True 表示解析时有警告或错误
                print(f"  -> 警告: 解析 RSS 文件时遇到问题: {parsed_feed.bozo_exception}")

            # 遍历解析出的条目，提取章节编号
            for entry in parsed_feed.entries:
                # 从条目链接或标题中提取章节编号
                # 假设链接格式为 .../chapter_01_final.mp3 或类似格式
                if hasattr(entry, 'link'):
                    # 使用正则表达式从链接中提取章节编号
                    match = re.search(r'chapter[_\s]*([0-9]+)', entry.link, re.IGNORECASE)
                    if match:
                        chapter_number = int(match.group(1))
                        existing_chapter_numbers.add(chapter_number)
                        continue

                # 如果链接中没有找到，尝试从标题中提取
                if hasattr(entry, 'title'):
                    match = re.search(r'[第]?\s*([0-9]+)\s*[章节回]', entry.title)
                    if match:
                        chapter_number = int(match.group(1))
                        existing_chapter_numbers.add(chapter_number)

            print(f"  -> 成功从 RSS 文件加载了 {len(existing_chapter_numbers)} 个已存在的章节条目。")
        except Exception as e:
            print(f"  -> 警告: 加载或解析 RSS 文件失败: {e}。")
    else:
        print(f"  -> RSS 文件不存在 ({rss_output_path})。")

    return existing_chapter_numbers


def get_generated_chapters_info(novel_dir):
    """
    获取文件夹下已生成的音频章节信息
    """
    generated_chapters = {}

    # 查找章节目录
    chapter_pattern = CHAPTER_DIR_PATTERN
    full_pattern = str(Path(novel_dir) / chapter_pattern)
    chapter_dirs = glob.glob(full_pattern)

    for chapter_dir_path in chapter_dirs:
        chapter_dir_path = Path(chapter_dir_path)
        chapter_subdir_name = chapter_dir_path.name

        # 检查音频文件是否存在
        audio_exists, mp3_file_path = check_chapter_audio_exists(novel_dir, chapter_subdir_name)
        if not audio_exists:
            continue

        # 提取章节编号
        chapter_num_match = re.search(r'Chapter[_\s]*([0-9]+)', chapter_subdir_name, re.IGNORECASE)
        if not chapter_num_match:
            continue

        try:
            chapter_number = int(chapter_num_match.group(1))
            generated_chapters[chapter_number] = {
                'dir_name': chapter_subdir_name,
                'mp3_path': mp3_file_path
            }
        except ValueError:
            continue

    return generated_chapters


def compare_rss_and_generated_chapters(config, rss_output_path):
    """
    比较RSS中的章节条目与文件夹下已生成的音频章节是否一致
    返回需要添加到RSS的章节信息列表
    """
    paths_config = config['paths']
    novels_root = Path(paths_config['novels_root_dir'])
    novel_folder_name = paths_config['novel_folder_name']
    novel_dir = novels_root / novel_folder_name

    # 获取已存在的RSS条目
    existing_rss_chapters = load_existing_rss_entries(rss_output_path)
    print(f"RSS中已存在的章节数: {len(existing_rss_chapters)}")

    # 获取文件夹下已生成的音频章节
    generated_chapters = get_generated_chapters_info(novel_dir)
    print(f"文件夹下已生成的音频章节数: {len(generated_chapters)}")

    # 找出需要添加到RSS中的章节（已生成但RSS中不存在的章节）
    chapters_to_add = []
    for chapter_number in sorted(generated_chapters.keys()):
        if chapter_number not in existing_rss_chapters:
            chapter_info = generated_chapters[chapter_number]
            chapters_to_add.append({
                'number': chapter_number,
                'dir_name': chapter_info['dir_name'],
                'mp3_path': chapter_info['mp3_path']
            })

    print(f"需要添加到RSS的章节数: {len(chapters_to_add)}")
    return chapters_to_add


def discover_chapters_by_audio_for_rss(config, chapters_to_add):
    """
    根据需要添加的章节列表生成完整的章节信息用于RSS
    """
    if not chapters_to_add:
        return []

    print("根据需要添加的章节生成RSS信息...")
    chapters_info = []
    paths_config = config['paths']
    rss_config = config['rss']
    ollama_config = config.get('ollama', {})
    novels_root = Path(paths_config['novels_root_dir'])
    novel_folder_name = paths_config['novel_folder_name']

    for chapter in chapters_to_add:
        chapter_number = chapter['number']
        chapter_subdir_name = chapter['dir_name']
        mp3_file_path = chapter['mp3_path']

        chapter_dir_path = Path(novels_root) / novel_folder_name / chapter_subdir_name
        chapter_number_padded = f"{chapter_number:02d}"

        # 查找 TXT 文件用于提取标题
        txt_search_paths = [
            os.path.join(chapter_dir_path, TXT_FILE_PATTERN),
            os.path.join(chapter_dir_path, CHAPTERS_SUBDIR, TXT_FILE_PATTERN)
        ]
        txt_file_path = None
        txt_files_found = []
        for pattern in txt_search_paths:
            txt_files_found.extend(glob.glob(pattern))
        if len(txt_files_found) > 0:
            txt_file_path = Path(txt_files_found[0])

        # 获取文件大小
        try:
            file_size = Path(mp3_file_path).stat().st_size
        except Exception as e:
            print(f"  -> 警告: 无法获取文件大小 {mp3_file_path}: {e}, 使用 0。")
            file_size = 0

        # 确定章节标题和描述
        chapter_title = f"第 {chapter_number_padded} 章"
        chapter_description = rss_config['default_chapter_description']
        if txt_file_path and txt_file_path.exists():
            ollama_title, ollama_desc = extract_chapter_info_with_ollama(
                str(txt_file_path), ollama_config, chapter_title, chapter_description
            )
            chapter_title = ollama_title
            chapter_description = ollama_desc

        # 构造公网音频 URL
        audio_base_url_template = paths_config['audio_base_url']
        audio_url = audio_base_url_template.format(
            novel_name=remove_special_chars(novel_folder_name),
            chapter_subdir=remove_special_chars(chapter_subdir_name)
        )

        # 获取实际的文件名
        mp3_filename = Path(mp3_file_path).name
        audio_url = f"{audio_url.rstrip('/')}/{mp3_filename}"

        # 确定发布日期
        pub_date = datetime.now(timezone.utc) + timedelta(days=rss_config.get('publish_date_offset_days', 0))

        chapters_info.append({
            'id': f"{novel_folder_name}_{chapter_subdir_name}",
            'number': chapter_number,
            'number_padded': chapter_number_padded,
            'subdir_name': chapter_subdir_name,
            'title': chapter_title,
            'description': chapter_description,
            'mp3_local_path': str(mp3_file_path),
            'mp3_url': audio_url,
            'file_size': file_size,
            'pub_date': pub_date
        })
        print(f"  -> 准备添加章节: {chapter_subdir_name} (标题: {chapter_title})")

    # 按章节号排序
    chapters_info.sort(key=lambda x: x['number'])
    print(f"共准备添加 {len(chapters_info)} 个章节到RSS。")
    return chapters_info


def run_rss_update_process(input_directory):
    """封装 RSS 更新和上传的主逻辑，供其他模块调用"""
    try:
        config = load_config('rss_config.yaml')
        input_directory_rsplit = input_directory.rsplit('/', 1)
        config['paths']['novels_root_dir'] = input_directory_rsplit[0]
        config['paths']['novel_folder_name'] = input_directory_rsplit[1]

        # 修改这里：比较RSS内容与文件夹下章节是否一致
        rss_output_path = input_directory + '/' + config['paths']['local_rss_output']

        # 比较RSS中的章节条目与文件夹下已生成的音频章节
        chapters_to_add = compare_rss_and_generated_chapters(config, rss_output_path)

        # 根据需要添加的章节生成完整的章节信息
        chapters_info = discover_chapters_by_audio_for_rss(config, chapters_to_add)

        # 加载或创建feed
        fg, existing_entries = load_or_create_feed(config, rss_output_path)

        # 添加章节到feed
        add_chapters_to_feed(fg, chapters_info, existing_entries)

        # 保存RSS文件
        save_feed(fg, rss_output_path)

        try:
            upload_files_via_sftp(config, rss_output_path, chapters_info)
            print("=== 文件检查/上传成功 (MD5 比对) ===")
        except Exception as upload_error:
            print(f"=== 文件检查/上传失败 (MD5 比对) ===")
            raise

        print("\n=== Podcast RSS 基于音频文件生成完成 ===")
        print(f"处理了 {len(chapters_info)} 个已完成的章节。")
        print(f"RSS 文件路径 (本地): {rss_output_path}")
        print(f"请确保音频文件和 RSS 文件可通过对应的公网 URL 访问。")
        return True
    except Exception as e:
        print(f"RSS 更新流程执行失败: {e}")
        import traceback
        traceback.print_exc()
        return False


# --- 主函数入口 ---
def main():
    """主函数入口，用于独立运行脚本"""
    success = run_rss_update_process('./downloaded_stories/Moonrise') # 修改为你的实际路径
    if not success:
        import sys
        sys.exit(1)

if __name__ == "__main__":
    main()
