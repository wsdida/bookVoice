# audiobook_generator.py (修改后的主入口部分)
# ... (文件前面的代码保持不变) ...

# --- 将 main 函数封装为可调用函数 ---
# ... (generate_audiobook 函数保持不变) ...
import os
# --- 新增：批量处理目录中的所有 txt 文件 ---
from  audiobook_generator import generate_audiobook
def generate_audiobooks_in_directory(input_directory, config_path='config.yaml'):
    """批量处理目录下的所有 .txt 文件"""
    import glob
    txt_files = glob.glob(os.path.join(input_directory, "*.txt"))

    if not txt_files:
        print(f"在目录 '{input_directory}' 中未找到任何 .txt 文件。")
        return

    print(f"在目录 '{input_directory}' 中找到 {len(txt_files)} 个 .txt 文件。")

    for i, txt_file_path in enumerate(txt_files):
        try:
            print(f"\n--- 开始处理文件 ({i + 1}/{len(txt_files)}): {os.path.basename(txt_file_path)} ---")
            generate_audiobook(input_directory,txt_file_path, config_path)
            print(f"--- 文件处理完成: {os.path.basename(txt_file_path)} ---")
        except Exception as e:
            print(f"处理文件 '{txt_file_path}' 时出错: {e}")
            import traceback
            traceback.print_exc()

    print(f"\n=== 目录 '{input_directory}' 批量处理完成 ===")


if __name__ == "__main__":
    import sys

    input_path = "./downloaded_stories/The Player Next Door"
    config_path = "C://software//workspace//bookVoice//src//config.yaml"

    if not os.path.isdir(input_path):
        print(f"错误: 路径 '{input_path}' 不是一个有效的目录。")
        sys.exit(1)

    generate_audiobooks_in_directory(input_path, config_path)

    # 原有的单文件处理逻辑
      #  input_file_path = sys.argv[1]
       ##generate_audiobook(input_file_path, config_path)
