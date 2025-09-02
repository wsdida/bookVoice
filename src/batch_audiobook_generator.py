# batch_audiobook_generator.py (添加校验功能)
import os
import re
from pathlib import Path
import glob
import sys
from audiobook_generator import generate_audiobook
from config.database import DatabaseManager
# 在文件顶部添加
db_manager = DatabaseManager()


def check_and_rebuild_if_needed(input_directory, txt_file_path, config_path='config.yaml'):
    """
    检查并重新构建（如果需要）
    """
    try:
        txt_filename = txt_file_path.stem
        output_dir_name = f"{txt_filename}_audiobook_output"
        output_dir = Path(input_directory) / output_dir_name

        # 检查日志文件
        log_file = output_dir / "logs" / "audiobook.log"
        if not log_file.exists():
            print(f"  -> 日志文件不存在: {log_file}")
            return False

        # 检查日志中是否包含完成标记
        with open(log_file, 'r', encoding='utf-8') as f:
            log_content = f.read()

        if "✅ === 有声书生成完成" not in log_content:
            print(f"  -> 日志中未找到完成标记")
            return False

        # 检查最终MP3文件
        final_mp3 = output_dir / "chapters" / f"{txt_filename}_final.mp3"
        if not final_mp3.exists() or final_mp3.stat().st_size == 0:
            print(f"  -> 最终MP3文件缺失或为空，重新合成: {final_mp3}")

            # 重新导入并调用混音函数
            try:
                # 延迟导入避免循环依赖
                from audiobook_generator import mix_audio, load_config

                # 加载配置
                config = load_config(config_path)
                config['input_file'] = str(txt_file_path)
                config['output_dir'] = str(output_dir)

                # 尝试重新混音
                # 需要读取annotations文件来重新混音
                annotations_dir = output_dir / "annotations"
                annotations = {}

                # 读取所有章节的注解文件
                for anno_file in annotations_dir.glob("chapter_*.json"):
                    chapter_num = anno_file.stem
                    try:
                        import json
                        with open(anno_file, 'r', encoding='utf-8') as f:
                            annotations[chapter_num] = json.load(f)
                    except Exception as e:
                        print(f"  -> 读取注解文件失败 {anno_file}: {e}")
                        continue

                if annotations:
                    # 重新混音
                    from audiobook_generator import mix_audio
                    mix_audio(annotations, str(output_dir), config.get('effect_dir', 'effects'), force_rebuild=True)
                    print(f"  -> 重新混音完成")
                    return True
                else:
                    print(f"  -> 未找到注解文件，无法重新混音")
                    return False

            except Exception as e:
                print(f"  -> 重新混音失败: {e}")
                return False
        else:
            print(f"  -> 最终MP3文件已存在且有效: {final_mp3}")
            return True

    except Exception as e:
        print(f"  -> 检查和重建过程中出错: {e}")
        return False

def verify_audiobook_generation(input_directory, txt_file_path, story_title):
    """
    验证有声书生成结果
    1. 先查看logs的audiobook.log文件是否生成完成（即有"有声书生成完成"标志）
    2. 如果生成完成，查看是否存在chapter_*_final.mp3，不存在则对已有音频进行合成，如果存在则校验通过
    """
    try:
        # 从txt文件名推断输出目录名
        txt_filename = txt_file_path.stem
        output_dir_name = f"{txt_filename}_audiobook_output"
        output_dir = Path(input_directory) / output_dir_name

        # 检查日志文件
        log_file = output_dir / "logs" / "audiobook.log"
        if not log_file.exists():
            return False, "日志文件不存在"

        # 检查日志中是否包含完成标记
        with open(log_file, 'r', encoding='utf-8') as f:
            log_content = f.read()

        if "✅ === 有声书生成完成" not in log_content:
            return False, "日志中未找到完成标记"

        # 检查最终MP3文件
        final_mp3 = output_dir / "chapters" / f"{txt_filename}_final.mp3"
        if not final_mp3.exists():
            # 如果日志显示已完成但缺少最终MP3文件，则尝试重新合成
            return False, "最终MP3文件不存在，需要重新合成"
        elif final_mp3.stat().st_size == 0:
            return False, "最终MP3文件为空"

        # 更新数据库状态
        # 从文件名提取章节号
        chapter_match = re.search(r'Chapter_(\d+)', txt_filename)
        if chapter_match:
            chapter_num = int(chapter_match.group(1))
            db_manager.update_chapter_audio_status(story_title, chapter_num, 'completed')

        return True, "验证通过"
    except Exception as e:
        return False, f"验证过程中出错: {e}"


def generate_audiobooks_in_directory(directory_path, config_path='config.yaml', force_rebuild=False):
    """
    在指定目录中批量生成有声书，支持断点续传和数据库状态检查
    """
    directory_path = Path(directory_path)
    story_title = directory_path.name

    if not directory_path.exists():
        print(f"目录不存在: {directory_path}")
        return

    # 获取所有txt文件（章节文件）
    txt_files = list(directory_path.glob("Chapter_*.txt"))
    txt_files.sort(key=lambda x: int(re.search(r'Chapter_(\d+)', x.name).group(1))
    if re.search(r'Chapter_(\d+)', x.name) else 0)

    if not txt_files:
        print(f"在目录 {directory_path} 中未找到章节文件")
        return

    print(f"找到 {len(txt_files)} 个章节文件")

    # 获取数据库中未处理的音频章节
    if not force_rebuild:
        unprocessed_chapters = db_manager.get_unprocessed_audio_chapters(story_title)
        if not unprocessed_chapters:
            print("数据库中所有章节均已处理完成")
            return
        print(f"数据库中有 {len(unprocessed_chapters)} 个章节需要处理: {unprocessed_chapters}")

    # 为每个需要处理的章节生成有声书
    for txt_file in txt_files:
        match = re.search(r'Chapter_(\d+)', txt_file.name)
        if not match:
            continue

        chapter_number = int(match.group(1))

        # 检查是否需要处理该章节
        if not force_rebuild:
            if chapter_number not in unprocessed_chapters:
                print(f"章节 {chapter_number} 已在数据库中标记为完成，跳过")
                continue

        print(f"开始处理章节: {txt_file.name}")
        try:
            generate_audiobook(
                str(directory_path),
                str(txt_file),
                config_path,
                force_rebuild=force_rebuild,
                auto_update_rss=False  # 批量处理时不自动更新RSS
            )
            # 更新数据库状态
            db_manager.update_chapter_audio_status(story_title, chapter_number, 'completed')
            print(f"✅ 章节 {chapter_number} 处理完成")
        except Exception as e:
            print(f"❌ 章节 {chapter_number} 处理失败: {e}")
            # 更新数据库状态为失败
            db_manager.update_chapter_audio_status(story_title, chapter_number, 'failed')
            continue

    print("所有章节处理完成")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("📌 用法: python batch_audiobook_generator.py <input_directory> [config_path] [force_rebuild]")
        print("   示例: python batch_audiobook_generator.py ./downloaded_stories/Moonrise config.yaml false")
        sys.exit(1)

    input_path = sys.argv[1]
    config_path = sys.argv[2] if len(sys.argv) > 2 else 'config.yaml'
    force_rebuild_str = sys.argv[3].lower() if len(sys.argv) > 3 else 'false'
    force_rebuild = force_rebuild_str in ('true', '1', 'yes', 'on')

    generate_audiobooks_in_directory(input_path, config_path, force_rebuild)
