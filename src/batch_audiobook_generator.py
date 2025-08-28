# batch_audiobook_generator.py (添加校验功能)
import os
from pathlib import Path
import glob
import sys
from audiobook_generator import generate_audiobook


def verify_audiobook_generation(input_directory, txt_file_path):
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

        return True, "验证通过"
    except Exception as e:
        return False, f"验证过程中出错: {e}"


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


def generate_audiobooks_in_directory(input_directory: str, config_path: str = 'config.yaml',
                                     force_rebuild: bool = False):
    """
    批量处理目录中的所有 .txt 文件，为每个文件生成对应的有声书（.mp3）

    Args:
        input_directory (str): 包含 .txt 文件的输入目录
        config_path (str): 配置文件路径
        force_rebuild (bool): 是否强制重新生成所有文件
    """
    input_dir = Path(input_directory)

    if not input_dir.exists():
        print(f"❌ 错误: 目录不存在: {input_directory}")
        return

    if not input_dir.is_dir():
        print(f"❌ 错误: 路径不是目录: {input_directory}")
        return

    if not os.path.exists(config_path):
        print(f"❌ 错误: 配置文件不存在: {config_path}")
        return

    txt_files = list(input_dir.glob("*.txt"))
    if not txt_files:
        print(f"🟡 警告: 在目录 '{input_directory}' 中未找到任何 .txt 文件。")
        return

    print(f"📁 在目录 '{input_directory}' 中找到 {len(txt_files)} 个 .txt 文件。\n")

    processed_count = 0
    failed_files = []

    txt_files.sort(key=lambda x: x.name)

    for i, txt_file_path in enumerate(txt_files, 1):
        # 检查对应的输出目录和最终MP3文件是否存在
        txt_filename = txt_file_path.stem
        output_dir_name = f"{txt_filename}_audiobook_output"
        output_dir = Path(input_directory) / output_dir_name
        final_mp3 = output_dir / "chapters" / f"{txt_filename}_final.mp3"

        if final_mp3.exists() and not force_rebuild:
            print(f"✅ ({i}/{len(txt_files)}) 跳过，音频已存在: {final_mp3.name}")
            processed_count += 1
            continue

        # 如果不是强制重建，检查是否已完成但需要重新合成
        if not force_rebuild:
            is_valid, message = verify_audiobook_generation(str(input_directory), txt_file_path)
            if is_valid:
                print(f"✅ ({i}/{len(txt_files)}) 校验通过: {final_mp3.name}")
                processed_count += 1
                continue
            elif "需要重新合成" in message:
                print(f"🔄 ({i}/{len(txt_files)}) 检测到需要重新合成: {txt_file_path.name}")
                print(f"   信息: {message}")
                # 尝试重新合成
                if check_and_rebuild_if_needed(str(input_directory), txt_file_path, config_path):
                    print(f"✅ ({i}/{len(txt_files)}) 重新合成成功: {final_mp3.name}")
                    processed_count += 1
                    continue
                else:
                    print(f"❌ ({i}/{len(txt_files)}) 重新合成失败: {txt_file_path.name}")

        print(f"🔊 ({i}/{len(txt_files)}) 正在处理: {txt_file_path.name}")
        try:
            generate_audiobook(str(input_directory), str(txt_file_path), config_path, force_rebuild=force_rebuild)
            # 验证生成结果
            is_valid, message = verify_audiobook_generation(str(input_directory), txt_file_path)
            if is_valid:
                print(f"✅ ({i}/{len(txt_files)}) 成功生成: {final_mp3.name}")
                processed_count += 1
            else:
                print(f"❌ ({i}/{len(txt_files)}) 生成验证失败: {txt_file_path.name}")
                print(f"   错误: {message}")
                failed_files.append(txt_file_path.name)
        except Exception as e:
            print(f"❌ ({i}/{len(txt_files)}) 处理失败: {txt_file_path.name}")
            print(f"   错误: {e}")
            failed_files.append(txt_file_path.name)

    print(f"\n" + "=" * 60)
    print(f"✅ 批量处理完成: {input_directory}")
    print(f"📊 总文件数: {len(txt_files)}")
    print(f"🟢 成功: {processed_count}")
    print(f"🔴 失败: {len(failed_files)}")
    if failed_files:
        print("📋 失败文件列表:")
        for fname in failed_files:
            print(f"  - {fname}")
    print("=" * 60)


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
