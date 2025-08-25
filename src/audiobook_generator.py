# audiobook_generator.py
import os
import logging
import yaml
import torch
import torch_directml
import whisper
import json
import re
import jiwer
import jieba
from ollama import Client
from TTS.api import TTS
from pydub import AudioSegment
import subprocess  # 用于启动子进程
import sys  # 用于获取当前 Python 解释器路径

# 设置环境变量
os.environ["COQUI_TOS_AGREED"] = "1"
# 全局客户端初始化
ollama_client = Client()


# --- 日志配置函数 ---
def setup_logger(output_dir):
    """根据输出目录设置日志"""
    log_dir = os.path.join(output_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'audiobook.log')
    # 清除之前的 handlers (如果有的话)
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    # 配置新的 logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info(f"日志已配置到: {log_file}")
    return logger


def load_config(config_path='config.yaml'):
    """加载配置文件"""
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        # logger.info("配置文件加载成功") # logger 可能在调用时才初始化
        return config
    except Exception as e:
        print(f"配置文件加载失败: {str(e)}")  # 使用 print 以防 logger 未初始化
        raise


def normalize_text(text):
    """规范化文本，去除标点并转换为小写"""
    if not isinstance(text, str):
        return ""
    text = re.sub(r'\W+', ' ', text).lower()
    return text.strip()


def chinese_tokenizer(text):
    """中文分词"""
    return list(jieba.cut(text))


def extract_chapters(input_file, output_dir):
    """提取中英文小说章节"""
    try:
        # 确保输出目录存在
        chapters_dir = os.path.join(output_dir, 'chapters')
        os.makedirs(chapters_dir, exist_ok=True)

        # --- 修改后的文件读取部分 ---
        content = None
        encodings_to_try = ['utf-8', 'gbk', 'gb18030', 'latin1', 'cp1252']  # 尝试多种编码
        for enc in encodings_to_try:
            try:
                with open(input_file, 'r', encoding=enc) as f:
                    content = f.read()
                print(f"成功使用编码 '{enc}' 读取文件: {input_file}")
                break  # 成功读取则跳出循环
            except UnicodeDecodeError as ue:
                print(f"使用编码 '{enc}' 读取文件失败: {ue}")
                continue  # 尝试下一种编码
            except Exception as e:
                print(f"使用编码 '{enc}' 读取文件时发生其他错误: {e}")
                continue

        # 如果所有编码都失败了，使用 errors='replace' 强制读取
        if content is None:
            print(f"警告：无法使用标准编码读取文件 {input_file}。将使用 'utf-8' 编码并替换错误字符。")
            with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
            # 或者使用 latin1 (iso-8859-1)，它能解码任何字节流，但可能不是原文
            # print(f"警告：无法使用标准编码读取文件 {input_file}。将使用 'latin1' 编码读取。")
            # with open(input_file, 'r', encoding='latin1') as f:
            #     content = f.read()
        # --- 文件读取部分结束 ---

        # 中英文通用章节匹配规则
        chapter_pattern = (
            r'(?P<title>(?:Chapter\s+\d+|CHAPTER\s+[IVXLC]+|'
            r'第[\s\S]{1,9}?章|序章|引子|尾声|后记))'
        )
        matches = list(re.finditer(chapter_pattern, content, flags=re.IGNORECASE))
        chapter_files = []
        toc = []
        if not matches:
            # 如果没有找到章节标题，将整个文件作为一个章节
            print(f"警告: 在 {input_file} 中未检测到章节标题，将整个文件作为一章处理")
            chapter_file = os.path.join(chapters_dir, 'chapter_01.txt')
            with open(chapter_file, 'w', encoding='utf-8') as f:  # 写出时仍使用 UTF-8
                f.write(content)
            toc.append({
                "chapter": 1,
                "title": "全文",
                "file": chapter_file
            })
            chapter_files.append(chapter_file)
        else:
            for i in range(len(matches)):
                start = matches[i].start()
                end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
                chapter_title = matches[i].group('title').strip()
                chapter_text = content[start:end].strip()
                chapter_file = os.path.join(chapters_dir, f'chapter_{i + 1:02d}.txt')
                with open(chapter_file, 'w', encoding='utf-8') as f:
                    f.write(chapter_text)
                toc.append({
                    "chapter": i + 1,
                    "title": chapter_title,
                    "file": chapter_file
                })
                chapter_files.append(chapter_file)
        # 保存章节目录
        with open(os.path.join(output_dir, 'toc.json'), 'w', encoding='utf-8') as f:
            json.dump(toc, f, ensure_ascii=False, indent=2)
        print(f"章节分割完成，共 {len(chapter_files)} 个章节")
        return chapter_files
    except Exception as e:
        print(f"章节分割失败: {str(e)}")
        raise

def clean_ollama_response(response_text):
    """
    清理 Ollama 返回的文本，去除 <THINK> 标签和其他不必要的内容
    """
    if not isinstance(response_text, str):
        return response_text
    # 去除 <THINK>...</THINK> 标签及其内容
    cleaned_text = re.sub(r'<THINK>.*?</THINK>', '', response_text, flags=re.DOTALL | re.IGNORECASE)
    # 去除可能的多余空白行
    cleaned_text = re.sub(r'\n\s*\n', '\n', cleaned_text)
    # 去除开头和结尾的空白
    cleaned_text = cleaned_text.strip()
    return cleaned_text

def analyze_chapter(text):
    """
    使用 Ollama 对整章文本进行分析，在原文上直接标注角色和情感信息
    不拆分JSON，而是返回带有标注的完整文本
    """
    try:
        prompt = f"""You are a novel analysis assistant. Please carefully read the following novel text and annotate it based on the original text:
Requirements:
1.  Preserve all content and formatting of the original text.
2.  Add a marker before dialogue: [Character Name|Emotion], for example: [Zhang San|joy]"Hello!"
3.  Add a marker before narrative paragraphs: [Narration|Emotion], for example: [Narration|neutral]Night fell.
4.  Emotion categories are limited to: joy, anger, fear, sadness, surprise, neutral.
5.  Do not change the original text content, only add markers.
6.  Every paragraph or sentence must be annotated.
7.  Maintain the original line breaks and paragraph structure.
8.  Return only the annotated text, do not add any explanations or notes.
9.  Do not use <THINK> tags or any other thought process indicators.
The text is as follows:
{text}
Please return the fully annotated text.
"""
        messages = [
            {"role": "system", "content": "You are a novel text annotation expert. Please strictly add markers to the original text according to the format, and do not use <THINK> tags."},
            {"role": "user", "content": prompt}
        ]
        response = ollama_client.chat(model="mistral:7b", messages=messages)
        annotated_text = response["message"]["content"]
        # 清理返回的文本，去除 <THINK> 标签
        annotated_text = clean_ollama_response(annotated_text)
        # print(f"DEBUG: 章节分析完成，返回长度: {len(annotated_text)}")
        return annotated_text
    except Exception as e:
        print(f"章节分析失败: {str(e)}")
        # 返回原文作为 fallback
        return f"[叙述|neutral]{text}"

def parse_annotated_text(annotated_text):
    """
    解析带有标注的文本，转换为结构化数据
    """
    try:
        annotations = []
        lines = annotated_text.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # 匹配标注格式 [角色|情绪]内容
            pattern = r'\[([^|\]]+)\|([^\]]+)\](.*)'
            match = re.match(pattern, line)
            if match:
                speaker_or_type = match.group(1)
                emotion = match.group(2)
                content = match.group(3).strip()
                # 判断是对话还是叙述
                if speaker_or_type == "叙述":
                    anno_type = "narration"
                    speaker = "Narrator"
                else:
                    anno_type = "dialogue"
                    speaker = speaker_or_type
                annotations.append({
                    "type": anno_type,
                    "speaker": speaker,
                    "text": content,
                    "emotion": emotion.lower()
                })
            else:
                # 如果没有标注格式，作为叙述处理
                if line.strip():
                    annotations.append({
                        "type": "narration",
                        "speaker": "Narrator",
                        "text": line,
                        "emotion": "neutral"
                    })
        return annotations
    except Exception as e:
        print(f"解析标注文本失败: {str(e)}")
        return []

def annotate_text(chapters, output_dir):
    """整章分析：角色提取 + 情感分析，一次调用"""
    try:
        # 确保注释目录存在
        annotations_dir = os.path.join(output_dir, 'annotations')
        os.makedirs(annotations_dir, exist_ok=True)
        annotations = {}
        for chapter_file in chapters:
            with open(chapter_file, 'r', encoding='utf-8') as f:
                text = f.read()
            chapter_num = os.path.basename(chapter_file).split('.')[0]
            print(f"分析章节：{chapter_num}")
            # 对整章进行标注
            annotated_text = analyze_chapter(text)
            # 保存标注后的文本
            annotated_file = os.path.join(annotations_dir, f'{chapter_num}_annotated.txt')
            with open(annotated_file, 'w', encoding='utf-8') as f:
                f.write(annotated_text)
            # 解析为结构化数据
            result = parse_annotated_text(annotated_text)
            # 保存结构化数据
            anno_file = os.path.join(annotations_dir, f'{chapter_num}.json')
            with open(anno_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            annotations[chapter_num] = result
        print("文本标注完成")
        return annotations
    except Exception as e:
        print(f"文本标注失败: {str(e)}")
        raise

def check_transcription(audio_file, original_text, whisper_model, threshold=0.1):
    """使用 Whisper 校验音频与原文一致性"""
    try:
        if not os.path.exists(audio_file):
            print(f"音频文件不存在: {audio_file}")
            return False, float('inf')
        if os.path.getsize(audio_file) == 0:
            print(f"音频文件为空: {audio_file}")
            return False, float('inf')
        result = whisper_model.transcribe(audio_file, language="en")
        transcribed_text = result["text"]
        original_clean = normalize_text(original_text)
        transcribed_clean = normalize_text(transcribed_text)
        if not original_clean or not transcribed_clean:
            print(f"警告: 文本为空，跳过 WER 计算: {audio_file}")
            return True, 0.0
        # 计算 WER
        wer = jiwer.wer(original_clean, transcribed_clean)
        # print(f"DEBUG: 音频 {audio_file} WER: {wer}")
        return wer <= threshold, wer
    except Exception as e:
        print(f"转录失败 {audio_file}: {str(e)}")
        return False, float('inf')

def create_speaker_mapper(tts, role_to_speaker):
    """创建智能 speaker 映射器"""
    available_speakers = list(tts.synthesizer.tts_model.speaker_manager.speakers.keys())
    print(f"可用的 speakers: {available_speakers}")
    speaker_mapping_cache = {}
    def get_valid_speaker(role_name):
        if role_name in speaker_mapping_cache:
            return speaker_mapping_cache[role_name]
        requested_speaker = role_to_speaker.get(role_name, role_to_speaker.get("Narrator", available_speakers[0]))
        if requested_speaker in available_speakers:
            speaker_mapping_cache[role_name] = requested_speaker
            return requested_speaker
        fallback_speaker = available_speakers[0] if available_speakers else "default"
        print(f"警告: 角色 '{requested_speaker}' 不存在，映射到 '{fallback_speaker}'")
        speaker_mapping_cache[role_name] = fallback_speaker
        return fallback_speaker
    return get_valid_speaker

def synthesize_tts(chapter_file, annotations, role_to_speaker, output_dir, tts, whisper_model, threshold=0.1):
    """TTS 合成与 Whisper 校验"""
    try:
        get_valid_speaker = create_speaker_mapper(tts, role_to_speaker)
        chapter_num = os.path.basename(chapter_file).split('.')[0]
        print(f"开始合成章节: {chapter_num}")
        for i, anno in enumerate(annotations):
            text = anno.get('text', '')
            role = anno.get('speaker', 'Narrator')
            if not text.strip():
                print(f"警告: 章节 {chapter_num} 第 {i} 段文本为空，跳过")
                continue
            speaker = get_valid_speaker(role)
            output_file = os.path.join(output_dir, 'chapters', f'{chapter_num}_{role}_{i:03d}.wav')
            # TTS 合成
            try:
                tts.tts_to_file(
                    text=text,
                    speaker=speaker,
                    language="en",
                    file_path=output_file
                )
                # print(f"DEBUG: 合成完成: {output_file}")
            except Exception as e:
                print(f"TTS 合成失败 {output_file}: {str(e)}")
                continue
            # 反向转写校验
            is_ok, wer = check_transcription(output_file, text, whisper_model, threshold)
            if not is_ok:
                print(f"警告: 转录不匹配 {output_file}, WER: {wer}")
            else:
                # print(f"DEBUG: 校验通过 {output_file}, WER: {wer}")
                pass
        print(f"章节 {chapter_num} TTS 合成完成")
    except Exception as e:
        print(f"TTS 合成过程出错: {str(e)}")
        raise

def mix_audio(annotations, output_dir, effect_dir):
    """音效混音"""
    try:
        print("开始音效混音")
        for chapter, anno_list in annotations.items():
            print(f"混音章节: {chapter}")
            # 初始化章节音频
            chapter_audio = AudioSegment.silent(duration=0)
            # 拼接语音片段
            for i, anno in enumerate(anno_list):
                # 注意：这里假设角色名没有特殊字符，否则需要处理文件名
                safe_speaker_name = re.sub(r'[\\/:*?"<>|]', '_', anno["speaker"])
                audio_file = os.path.join(output_dir, 'chapters', f'{chapter}_{safe_speaker_name}_{i:03d}.wav')
                if not os.path.exists(audio_file):
                    print(f"警告: 音频文件不存在，跳过: {audio_file}")
                    continue
                try:
                    audio = AudioSegment.from_wav(audio_file)
                    # 添加短暂停顿使过渡更自然
                    chapter_audio += audio + AudioSegment.silent(duration=200)  # 200ms 停顿
                except Exception as e:
                    print(f"加载音频失败 {audio_file}: {str(e)}")
                    continue
            # 添加背景音效
            effect_file = os.path.join(effect_dir, 'background.wav')
            if not os.path.exists(effect_file):
                # 尝试其他常见音效文件
                for fallback in ['forest.wav', 'ambient.wav', 'music.wav']:
                    fallback_file = os.path.join(effect_dir, fallback)
                    if os.path.exists(fallback_file):
                        effect_file = fallback_file
                        break
            if os.path.exists(effect_file):
                try:
                    bg_audio = AudioSegment.from_wav(effect_file) - 15  # 降低背景音量
                    chapter_audio = chapter_audio.overlay(bg_audio, loop=True)
                    # print(f"DEBUG: 添加背景音效: {effect_file}")
                except Exception as e:
                    print(f"加载背景音效失败 {effect_file}: {str(e)}")
            else:
                print("警告: 未找到背景音效文件")
            # 导出最终音频
            output_file = os.path.join(output_dir, 'chapters', f'{chapter}_final.mp3')
            try:
                chapter_audio.export(output_file, format='mp3', bitrate='192k')
                print(f"混音完成: {output_file}")
            except Exception as e:
                print(f"导出音频失败 {output_file}: {str(e)}")
        print("音效混音完成")
    except Exception as e:
        print(f"音效混音过程出错: {str(e)}")
        raise

# --- 将 main 函数封装为可调用函数 ---
def generate_audiobook(input_directory,input_file_path, config_path='config.yaml'):
    """主函数：执行有声书生成全流程"""
    try:
        # 为每个小说创建独立的输出目录
        base_output_dir = os.path.dirname(input_file_path)
        story_title = os.path.splitext(os.path.basename(input_file_path))[0]
        output_dir = os.path.join(base_output_dir, f"{story_title}_audiobook_output")
        os.makedirs(output_dir, exist_ok=True)

        # 设置日志到输出目录
        logger = setup_logger(output_dir)

        print(f"=== 开始生成有声书: {story_title} ===")
        logger.info(f"=== 开始生成有声书: {story_title} ===")

        # 加载配置
        config = load_config(config_path)
        # 覆盖配置中的 input_file 和 output_dir 为当前处理的小说
        config['input_file'] = input_file_path
        config['output_dir'] = output_dir

        # 初始化模型
        print("初始化 TTS 和 Whisper 模型...")
        logger.info("初始化 TTS 和 Whisper 模型...")
        device = "cuda" if torch.cuda.is_available() else "cpu"
        tts = TTS("tts_models/multilingual/multi-dataset/xtts_v2").to(device)
        whisper_model = whisper.load_model(config.get('whisper_model', 'base'))
        available_speakers = list(tts.synthesizer.tts_model.speaker_manager.speakers.keys())
        print(f"可用 XTTS 语音：{available_speakers}")
        logger.info(f"可用 XTTS 语音：{available_speakers}")

        # 1. 小说导入与章节分割
        print("开始小说导入与章节分割")
        logger.info("开始小说导入与章节分割")
        chapters = extract_chapters(config['input_file'], config['output_dir'])

        # 2. 角色提取、情感分析、文本标注
        print("开始文本标注")
        logger.info("开始文本标注")
        annotations = annotate_text(chapters, config['output_dir'])

        # 3. 收集唯一角色并分配语音
        print("收集角色并分配语音")
        logger.info("收集角色并分配语音")
        all_speakers = set()
        for anno_list in annotations.values():
            for anno in anno_list:
                if anno.get('type') == 'dialogue' and anno.get('speaker') not in [None, "Unknown", ""]:
                    all_speakers.add(anno['speaker'])
        all_speakers.add("Narrator")
        role_to_speaker = {
           # "Narrator": config.get('narrator_speaker', available_speakers[0]),
            "Narrator": config.get('narrator_speaker','Mary'),
            "Unknown": config.get('narrator_speaker', available_speakers[0])
        }
        speaker_index = 0
        for speaker in all_speakers:
            if speaker not in role_to_speaker:
                role_to_speaker[speaker] = available_speakers[speaker_index % len(available_speakers)]
                speaker_index += 1
        print(f"分配语音完成，角色数：{len(role_to_speaker)}")
        logger.info(f"分配语音完成，角色数：{len(role_to_speaker)}")

        # 4. TTS 合成与 Whisper 校验
        print("开始TTS合成与校验")
        logger.info("开始TTS合成与校验")
        for chapter_file, anno_list in zip(chapters, annotations.values()):
            synthesize_tts(chapter_file, anno_list, role_to_speaker, config['output_dir'], tts, whisper_model,
                           config.get('whisper_threshold', 0.1))

        # 5. 音效混音
        print("开始音效混音")
        logger.info("开始音效混音")
        mix_audio(annotations, config['output_dir'], config.get('effect_dir', 'effects'))

        # 6. 生成元数据
        print("生成元数据")
        logger.info("生成元数据")
        manifest = {
            "chapters": [
                {"chapter": i + 1, "file": f"chapter_{i + 1:02d}_final.mp3",
                 "speakers": list(set(anno.get('speaker', 'Narrator') for anno in anno_list))}
                for i, anno_list in enumerate(annotations.values())
            ]
        }
        with open(os.path.join(config['output_dir'], 'manifest.json'), 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print("元数据生成完成")
        logger.info("元数据生成完成")

        print(f"=== 有声书生成完成: {story_title} ===")
        logger.info(f"=== 有声书生成完成: {story_title} ===")
        # ... (在 generate_audiobook 函数末尾，元数据生成之后)

        print("元数据生成完成")
        logger.info("元数据生成完成")

        # --- 新增：通知 RSS 系统进行更新 ---
        print("准备调用 RSS 更新脚本...")
        logger.info("准备调用 RSS 更新脚本...")
        try:
            # --- 方式一：直接导入并调用函数 (推荐) ---
            # 确保 rss_and_upload.py 在 Python 路径中，或者在相同目录下
            #假设 rss_and_upload.py 有一个名为 run_rss_update 的函数
            from generate_and_deploy_rss import run_rss_update_process # 需要创建这个函数
            run_rss_update_process(input_directory) # 调用 RSS 更新

            # --- 方式二：作为子进程运行 ---
            # import subprocess
            # import sys
            # rss_script_path = "rss_and_upload.py"  # 根据您的实际文件名和路径调整
            # # 调用 RSS 脚本，让它自己发现并处理新章节
            # result = subprocess.run(
            #     [sys.executable, rss_script_path],  # 不需要传递参数，RSS 脚本自己读配置
            #     cwd=os.path.dirname(os.path.abspath(__file__)),  # 设置工作目录
            #     capture_output=True, text=True
            # )
            # if result.returncode == 0:
            #     print("RSS 更新子进程成功完成。")
            #     logger.info("RSS 更新子进程成功完成。")
            #     print("--- RSS 脚本输出 ---")
            #     print(result.stdout)
            #     print("--------------------")
            #     logger.info(f"RSS 脚本输出: {result.stdout}")
            # else:
            #     print("RSS 更新子进程失败。")
            #     logger.error("RSS 更新子进程失败。")
            #     print("--- RSS 脚本错误 ---")
            #     print(result.stderr)
            #     print("--------------------")
            #     logger.error(f"RSS 脚本错误: {result.stderr}")
                # 可以选择 raise 或者记录日志

        except Exception as rss_error:
            print(f"调用 RSS 更新脚本时出错: {rss_error}")
            logger.error(f"调用 RSS 更新脚本时出错: {rss_error}")
            import traceback
            traceback.print_exc()
            logger.exception(rss_error)

        print(f"=== 有声书生成完成: {story_title} ===")
        logger.info(f"=== 有声书生成完成: {story_title} ===")
        # --- 调用 RSS 更新结束 ---
    except Exception as e:
        print(f"运行出错: {str(e)}")
        # logger.exception(e) # 如果 logger 未正确初始化，这会失败
        import traceback
        traceback.print_exc()
        raise

# if __name__ == "__main__":
#     # 可以保留一个主入口，用于直接调用
#     import sys
#     if len(sys.argv) != 3:
#         print("用法: python audiobook_generator.py <input_file_path> <config_path>")
#         sys.exit(1)
#     input_file_path = sys.argv[1]
#     config_path = sys.argv[2]
#     generate_audiobook(input_file_path, config_path)
