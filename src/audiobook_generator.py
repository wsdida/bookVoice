# audiobook_generator.py (添加检查现有wav文件功能)
import os
import logging
from pathlib import Path

import yaml
import torch
import whisper
import json
import re
import jiwer
import jieba
from ollama import Client
from TTS.api import TTS
from pydub import AudioSegment
import sys
import glob
from config.database import DatabaseManager

db_manager = DatabaseManager()
# 设置环境变量
os.environ["COQUI_TOS_AGREED"] = "1"
# 全局客户端初始化
ollama_client = Client()


# --- 日志配置函数 ---
def setup_logger(output_dir):
    log_dir = os.path.join(output_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'audiobook.log')
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
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
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        print(f"配置文件加载失败: {str(e)}")
        raise


def normalize_text(text):
    if not isinstance(text, str):
        return ""
    text = re.sub(r'\W+', ' ', text).lower()
    return text.strip()


def chinese_tokenizer(text):
    return list(jieba.cut(text))


def extract_chapters(input_file, output_dir):
    chapters_dir = os.path.join(output_dir, 'chapters')
    os.makedirs(chapters_dir, exist_ok=True)

    content = None
    encodings_to_try = ['utf-8', 'gbk', 'gb18030', 'latin1', 'cp1252']
    for enc in encodings_to_try:
        try:
            with open(input_file, 'r', encoding=enc) as f:
                content = f.read()
            print(f"成功使用编码 '{enc}' 读取文件: {input_file}")
            break
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"使用编码 '{enc}' 读取文件时发生其他错误: {e}")
            continue

    if content is None:
        print(f"警告：无法使用标准编码读取文件 {input_file}。将使用 'utf-8' 编码并替换错误字符。")
        with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()

    chapter_pattern = r'(?P<title>(?:Chapter\s+\d+|CHAPTER\s+[IVXLC]+|第[\s\S]{1,9}?章|序章|引子|尾声|后记))'
    matches = list(re.finditer(chapter_pattern, content, flags=re.IGNORECASE))
    chapter_files = []
    toc = []
    if not matches:
        print(f"警告: 在 {input_file} 中未检测到章节标题，将整个文件作为一章处理")
        chapter_file = os.path.join(chapters_dir, 'chapter_01.txt')
        with open(chapter_file, 'w', encoding='utf-8') as f:
            f.write(content)
        toc.append({"chapter": 1, "title": "全文", "file": chapter_file})
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
            toc.append({"chapter": i + 1, "title": chapter_title, "file": chapter_file})
            chapter_files.append(chapter_file)
    with open(os.path.join(output_dir, 'toc.json'), 'w', encoding='utf-8') as f:
        json.dump(toc, f, ensure_ascii=False, indent=2)
    print(f"章节分割完成，共 {len(chapter_files)} 个章节")
    return chapter_files


def clean_ollama_response(response_text):
    if not isinstance(response_text, str):
        return response_text
    cleaned_text = re.sub(r'<THINK>.*?</THINK>', '', response_text, flags=re.DOTALL | re.IGNORECASE)
    cleaned_text = re.sub(r'\n\s*\n', '\n', cleaned_text)
    cleaned_text = cleaned_text.strip()
    return cleaned_text


def analyze_chapter(text):
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
            {"role": "system",
             "content": "You are a novel text annotation expert. Please strictly add markers to the original text according to the format, and do not use <THINK> tags."},
            {"role": "user", "content": prompt}
        ]
        response = ollama_client.chat(model="mistral:7b", messages=messages)
        annotated_text = response["message"]["content"]
        annotated_text = clean_ollama_response(annotated_text)
        return annotated_text
    except Exception as e:
        print(f"章节分析失败: {str(e)}")
        return f"[叙述|neutral]{text}"


def parse_annotated_text(annotated_text):
    try:
        annotations = []
        lines = annotated_text.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            pattern = r'\[([^|\]]+)\|([^\]]+)\](.*)'
            match = re.match(pattern, line)
            if match:
                speaker_or_type = match.group(1)
                emotion = match.group(2)
                content = match.group(3).strip()
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
    try:
        annotations_dir = os.path.join(output_dir, 'annotations')
        os.makedirs(annotations_dir, exist_ok=True)
        annotations = {}
        for chapter_file in chapters:
            with open(chapter_file, 'r', encoding='utf-8') as f:
                text = f.read()
            chapter_num = os.path.basename(chapter_file).split('.')[0]
            print(f"分析章节：{chapter_num}")
            annotated_text = analyze_chapter(text)
            annotated_file = os.path.join(annotations_dir, f'{chapter_num}_annotated.txt')
            with open(annotated_file, 'w', encoding='utf-8') as f:
                f.write(annotated_text)
            result = parse_annotated_text(annotated_text)
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
    try:
        if not os.path.exists(audio_file) or os.path.getsize(audio_file) == 0:
            return False, float('inf')
        result = whisper_model.transcribe(audio_file, language="en")
        transcribed_text = result["text"]
        original_clean = normalize_text(original_text)
        transcribed_clean = normalize_text(transcribed_text)
        if not original_clean or not transcribed_clean:
            return True, 0.0
        wer = jiwer.wer(original_clean, transcribed_clean)
        return wer <= threshold, wer
    except Exception as e:
        print(f"转录失败 {audio_file}: {str(e)}")
        return False, float('inf')


def create_speaker_mapper(tts, role_to_speaker):
    available_speakers = list(tts.synthesizer.tts_model.speaker_manager.speakers.keys())
    print(f"可用的 speakers: {available_speakers}")
    speaker_mapping_cache = {}

    def get_valid_speaker(role_name):
        if role_name in speaker_mapping_cache:
            return speaker_mapping_cache[role_name]
        requested_speaker = role_to_speaker.get(role_name, role_to_speaker.get("Narrator", available_speakers[
            0] if available_speakers else "default"))
        if requested_speaker in available_speakers:
            speaker_mapping_cache[role_name] = requested_speaker
            return requested_speaker
        fallback_speaker = available_speakers[0] if available_speakers else "default"
        print(f"警告: 角色 '{requested_speaker}' 不存在，映射到 '{fallback_speaker}'")
        speaker_mapping_cache[role_name] = fallback_speaker
        return fallback_speaker

    return get_valid_speaker


def synthesize_tts(chapter_file, annotations, role_to_speaker, output_dir, tts, whisper_model, threshold=0.1,
                   force_rebuild=False):
    try:
        get_valid_speaker = create_speaker_mapper(tts, role_to_speaker)
        chapter_num = os.path.basename(chapter_file).split('.')[0]
        print(f"开始合成章节: {chapter_num}")

        chapter_audio_dir = os.path.join(output_dir, 'chapters')
        os.makedirs(chapter_audio_dir, exist_ok=True)

        for i, anno in enumerate(annotations):
            text = anno.get('text', '')
            role = anno.get('speaker', 'Narrator')
            if not text.strip():
                print(f"警告: 章节 {chapter_num} 第 {i} 段文本为空，跳过")
                continue

            speaker = get_valid_speaker(role)
            # 保持现有格式：使用实际的speaker名称作为角色名
            safe_speaker_name = re.sub(r'[\\/:*?"<>|]', '_', speaker)
            output_file = os.path.join(chapter_audio_dir, f'{chapter_num}_{safe_speaker_name}_{i:03d}.wav')

            if os.path.exists(output_file) and not force_rebuild:
                if os.path.getsize(output_file) > 0:
                    print(f"✅ 跳过已存在的语音文件: {output_file}")
                    continue

            if force_rebuild and os.path.exists(output_file):
                try:
                    os.remove(output_file)
                    print(f"🗑️ 已删除旧语音文件: {output_file}")
                except OSError as e:
                    print(f"⚠️ 删除文件失败 {output_file}: {e}")

            try:
                tts.tts_to_file(text=text, speaker=speaker, language="en", file_path=output_file)
                print(f"🔊 合成完成: {output_file}")
            except Exception as e:
                print(f"TTS 合成失败 {output_file}: {str(e)}")
                continue

            is_ok, wer = check_transcription(output_file, text, whisper_model, threshold)
            if not is_ok:
                print(f"⚠️ 转录不匹配 {output_file}, WER: {wer:.3f}")
            else:
                print(f"✅ 校验通过 {output_file}, WER: {wer:.3f}")

        print(f"✅ 章节 {chapter_num} TTS 合成完成")
    except Exception as e:
        print(f"TTS 合成过程出错: {str(e)}")
        raise


def find_existing_wav_files(chapter_audio_dir, chapter_formatted):
    """
    查找指定章节的所有wav文件
    返回按序号排序的文件列表
    """
    # 查找该章节的所有wav文件
    pattern = os.path.join(chapter_audio_dir, f'{chapter_formatted}_*_[0-9][0-9][0-9].wav')
    wav_files = glob.glob(pattern)

    # 按序号排序
    wav_files.sort(
        key=lambda x: int(re.search(r'_([0-9]{3})\.wav$', x).group(1)) if re.search(r'_([0-9]{3})\.wav$', x) else 0)

    return wav_files


def mix_audio(annotations, output_dir, effect_dir, role_to_speaker=None, force_rebuild=False):
    try:
        print("开始音效混音")
        chapter_audio_dir = os.path.join(output_dir, 'chapters')
        os.makedirs(chapter_audio_dir, exist_ok=True)

        # 如果role_to_speaker未提供，创建默认映射
        if role_to_speaker is None:
            role_to_speaker = {"Narrator": "default"}

        for chapter_key, anno_list in annotations.items():
            # 确保使用正确的章节编号格式
            # 如果chapter_key是"chapter_01"这样的格式，直接使用
            # 否则需要提取数字并格式化
            if chapter_key.startswith('chapter_'):
                chapter_formatted = chapter_key
            else:
                # 尝试从chapter_key中提取数字
                match = re.search(r'(\d+)', chapter_key)
                if match:
                    chapter_number = int(match.group(1))
                    chapter_formatted = f'chapter_{chapter_number:02d}'
                else:
                    chapter_formatted = chapter_key

            final_output_file = os.path.join(chapter_audio_dir, f'{chapter_formatted}_final.mp3')

            if os.path.exists(final_output_file) and not force_rebuild:
                if os.path.getsize(final_output_file) > 0:
                    print(f"✅ 跳过已存在的最终音频: {final_output_file}")
                    continue

            if force_rebuild and os.path.exists(final_output_file):
                try:
                    os.remove(final_output_file)
                    print(f"🗑️ 已删除旧最终音频: {final_output_file}")
                except OSError as e:
                    print(f"⚠️ 删除最终音频失败 {final_output_file}: {e}")

            print(f"混音章节: {chapter_formatted}")

            # 首先检查是否已存在wav文件，如果存在则直接使用
            existing_wav_files = find_existing_wav_files(chapter_audio_dir, chapter_formatted)
            if existing_wav_files :
                print(f"  -> 发现 {len(existing_wav_files)} 个已存在的wav文件，直接使用")
                chapter_audio = AudioSegment.silent(duration=0)

                for wav_file in existing_wav_files:
                    if os.path.exists(wav_file) and os.path.getsize(wav_file) > 0:
                        try:
                            audio = AudioSegment.from_wav(wav_file)
                            chapter_audio += audio + AudioSegment.silent(duration=200)
                            print(f"  -> 已添加: {os.path.basename(wav_file)}")
                        except Exception as e:
                            print(f"  -> 加载音频失败 {wav_file}: {str(e)}")
                            continue
                    else:
                        print(f"  -> 跳过无效文件: {wav_file}")
            else:
                # 如果没有现有wav文件，则按原有逻辑生成
                print(f"  -> 未发现现有wav文件，按原有逻辑生成")
                chapter_audio = AudioSegment.silent(duration=0)

                for i, anno in enumerate(anno_list):
                    # 保持现有格式：使用实际的speaker名称作为角色名
                    role = anno.get('speaker', 'Narrator')
                    speaker = role_to_speaker.get(role, role_to_speaker.get("Narrator", "default"))
                    safe_speaker_name = re.sub(r'[\\/:*?"<>|]', '_', speaker)
                    audio_file = os.path.join(chapter_audio_dir, f'{chapter_formatted}_{safe_speaker_name}_{i:03d}.wav')
                    if not os.path.exists(audio_file):
                        print(f"❌ 音频文件不存在，跳过: {audio_file}")
                        continue
                    try:
                        audio = AudioSegment.from_wav(audio_file)
                        chapter_audio += audio + AudioSegment.silent(duration=200)
                    except Exception as e:
                        print(f"加载音频失败 {audio_file}: {str(e)}")
                        continue

            if len(chapter_audio) == 0:
                print(f"⚠️ 章节 {chapter_formatted} 无有效音频数据，跳过导出")
                continue

            bg_added = False
            effect_file = os.path.join(effect_dir, 'background.wav')
            if not os.path.exists(effect_file):
                for fallback in ['forest.wav', 'ambient.wav', 'music.wav']:
                    fallback_file = os.path.join(effect_dir, fallback)
                    if os.path.exists(fallback_file):
                        effect_file = fallback_file
                        break
            if os.path.exists(effect_file):
                try:
                    bg_audio = AudioSegment.from_wav(effect_file) - 15
                    chapter_audio = chapter_audio.overlay(bg_audio, loop=True)
                    bg_added = True
                except Exception as e:
                    print(f"加载背景音效失败 {effect_file}: {str(e)}")

            if not bg_added:
                print("🟡 未添加背景音效")

            try:
                chapter_audio.export(final_output_file, format='mp3', bitrate='192k')
                print(f"✅ 混音完成: {final_output_file}")
            except Exception as e:
                print(f"❌ 导出音频失败 {final_output_file}: {str(e)}")

        print("✅ 音效混音完成")
    except Exception as e:
        print(f"❌ 音效混音过程出错: {str(e)}")
        raise


def get_chapter_status(output_dir, chapter_num):
    """检查章节是否已经完成生成"""
    chapters_dir = os.path.join(output_dir, 'chapters')
    # 确保chapter_num格式正确
    if not chapter_num.startswith('chapter_'):
        # 如果传入的是数字，格式化为正确的格式
        if isinstance(chapter_num, int) or chapter_num.isdigit():
            chapter_num = f'chapter_{int(chapter_num):02d}'
        else:
            # 如果chapter_num是类似"01"的字符串，添加前缀
            match = re.match(r'^(\d+)', chapter_num)
            if match:
                chapter_num = f'chapter_{int(match.group(1)):02d}'

    final_file = os.path.join(chapters_dir, f'{chapter_num}_final.mp3')
    return os.path.exists(final_file) and os.path.getsize(final_file) > 0


def find_last_completed_chapter(output_dir, total_chapters):
    """查找最后一个完成的章节"""
    for i in range(total_chapters, 0, -1):
        chapter_num = f'chapter_{i:02d}'
        if get_chapter_status(output_dir, chapter_num):
            return i
    return 0

def generate_audiobook(input_directory, input_file_path, config_path='config.yaml', force_rebuild=False, auto_update_rss=True):
    try:
        base_output_dir = os.path.dirname(input_file_path)
        story_title = os.path.splitext(os.path.basename(input_file_path))[0]
        output_dir = os.path.join(base_output_dir, f"{story_title}_audiobook_output")
        os.makedirs(output_dir, exist_ok=True)

        logger = setup_logger(output_dir)
        print(f"=== 开始生成有声书: {story_title} ===")
        logger.info(f"=== 开始生成有声书: {story_title} ===")

        config = load_config(config_path)
        config['input_file'] = input_file_path
        config['output_dir'] = output_dir

        device = "cuda" if torch.cuda.is_available() else "cpu"
        tts = TTS("tts_models/multilingual/multi-dataset/xtts_v2").to(device)
        whisper_model = whisper.load_model(config.get('whisper_model', 'base'))

        chapters = extract_chapters(config['input_file'], config['output_dir'])

        # 断点续传逻辑 - 基于数据库状态
        start_index = 0
        if not force_rebuild:
            # 从数据库获取未处理的音频章节
            unprocessed_chapters = db_manager.get_unprocessed_audio_chapters(story_title)
            if unprocessed_chapters:
                # 找到最小的未处理章节编号
                start_index = min(unprocessed_chapters) - 1  # 转换为0索引
                print(f"根据数据库状态，从第 {start_index + 1} 个章节开始生成")
            else:
                # 检查本地文件状态作为后备
                last_completed = find_last_completed_chapter(output_dir, len(chapters))
                if last_completed > 0:
                    print(f"检测到前 {last_completed} 个章节已生成，从第 {last_completed + 1} 个章节开始生成")
                    start_index = last_completed
                else:
                    print("从第一个章节开始生成")

        # 如果不是强制重建且所有章节都已完成，则跳过
        if not force_rebuild and start_index >= len(chapters):
            print("所有章节均已生成完成，无需重复生成")
            return

        # 只对需要生成的章节进行处理
        chapters_to_process = chapters[start_index:]

        # 如果需要重新处理注释（因为可能需要全部章节的注释）
        annotations = annotate_text(chapters, config['output_dir'])

        all_speakers = {"Narrator"}
        for anno_list in annotations.values():
            for anno in anno_list:
                if anno.get('type') == 'dialogue' and anno.get('speaker'):
                    all_speakers.add(anno['speaker'])
        available_speakers = list(tts.synthesizer.tts_model.speaker_manager.speakers.keys())
        role_to_speaker = {
            "Narrator": config.get('narrator_speaker', available_speakers[0] if available_speakers else "default"),
            "Unknown": config.get('narrator_speaker', available_speakers[0] if available_speakers else "default")
        }
        speaker_index = 0
        for speaker in all_speakers:
            if speaker not in role_to_speaker:
                role_to_speaker[speaker] = available_speakers[speaker_index % len(available_speakers)]
                speaker_index += 1

        # 只处理需要生成的章节
        for i, (chapter_file, anno_list) in enumerate(
                zip(chapters_to_process, list(annotations.values())[start_index:])):
            actual_index = start_index + i
            chapter_num = f'chapter_{actual_index + 1:02d}'
            print(f"开始处理章节 {actual_index + 1}/{len(chapters)}: {chapter_num}")

            # 检查数据库中的章节状态
            if not force_rebuild:
                db_chapter_status = db_manager.get_unprocessed_audio_chapters(story_title)
                if (actual_index + 1) not in db_chapter_status and get_chapter_status(output_dir, chapter_num):
                    print(f"章节 {chapter_num} 已在数据库中标记为完成且文件存在，跳过")
                    continue

            synthesize_tts(chapter_file, anno_list, role_to_speaker, config['output_dir'], tts, whisper_model,
                           config.get('whisper_threshold', 0.1), force_rebuild=force_rebuild)

            # 更新数据库状态
            db_manager.update_chapter_audio_status(story_title, actual_index + 1, 'completed')

        # 混音处理也可以添加断点逻辑
        annotations_subset = dict(list(annotations.items())[start_index:])
        mix_audio(annotations_subset, config['output_dir'], config.get('effect_dir', 'effects'),
                  role_to_speaker, force_rebuild=force_rebuild)

        manifest = {
            "chapters": [
                {
                    "chapter": i + 1,
                    "file": f"chapter_{i + 1:02d}_final.mp3",
                    "speakers": list(set(anno.get('speaker', 'Narrator') for anno in anno_list))
                }
                for i, anno_list in enumerate(annotations.values())
            ]
        }
        with open(os.path.join(config['output_dir'], 'manifest.json'), 'w', encoding='utf-8') as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print("✅ 元数据生成完成")
        logger.info("✅ 元数据生成完成"+input_directory)

        # 将原来的 RSS 更新代码替换为:
        if auto_update_rss:
            try:
                from generate_and_deploy_rss import run_rss_update_process
                run_rss_update_process(input_directory)
                print("✅ RSS 更新完成")
            except Exception as rss_error:
                print(f"❌ 调用 RSS 更新脚本时出错: {rss_error}")
                logger.error(f"❌ 调用 RSS 更新脚本时出错: {rss_error}")
        else:
            print("⏭️ 跳过自动 RSS 更新，由主控制器处理")

        print(f"✅ === 有声书生成完成: {story_title} ===")
        logger.info(f"✅ === 有声书生成完成: {story_title} ===")

    except Exception as e:
        print(f"❌ 运行出错: {str(e)}")
        import traceback
        traceback.print_exc()
        raise
