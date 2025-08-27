# batch_audiobook_generator.py
import os
from pathlib import Path
import glob
import sys
from audiobook_generator import generate_audiobook


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
        mp3_file = txt_file_path.with_suffix('.mp3')

        if mp3_file.exists() and not force_rebuild:
            print(f"✅ ({i}/{len(txt_files)}) 跳过，音频已存在: {mp3_file.name}")
            processed_count += 1
            continue

        print(f"🔊 ({i}/{len(txt_files)}) 正在处理: {txt_file_path.name}")
        try:
            generate_audiobook(str(input_directory), str(txt_file_path), config_path, force_rebuild=force_rebuild)
            print(f"✅ ({i}/{len(txt_files)}) 成功生成: {mp3_file.name}")
            processed_count += 1
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