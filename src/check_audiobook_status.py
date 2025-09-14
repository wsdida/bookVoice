
from config.database import DatabaseManager
import os
from pathlib import Path
import glob
from generate_and_deploy_rss import  check_rss_consistency

local_rss_file_name="/generate_podcast_rss.xml"
remote_rss_file_name="podcast.xml"

class CheckAudioBookStatus:
    def __init__(self, input_directory,story_title):
        self.input_directory = input_directory
        self.story_title = story_title
        self.db = DatabaseManager
        self.chapters = DatabaseManager
    def get_chapters_info(self):
        try:
            with self.db.get_connection() as  conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute('''
                    SELECT s.title as story_title, c.chapter_number, c.*
                    FROM chapters c
                    JOIN stories s ON c.story_id = s.id
                    WHERE s.title = %s
                ''', (self.story_title,))
                return cursor.fetchall()
        except Exception as e:
            print(f"❌ 获取章节信息时出错: {e}")


    def chess_audio_update(self):
        chapters_error = []
        if not self.chapters:
            print(f"❌ 故事 {self.story_title} 没有任何章节")
        else:
            for chapter in self.chapters:
                print(f"✅ 找到故事 {self.story_title} 的章节 {chapter['chapter_number']}")
                file_path=chapter['file_path'].as_posix()
                text_file_name=file_path.stem
                output_dir_name=f"{text_file_name}_audiobook_output"
                output_dir=Path(file_path).parent/output_dir_name
                file_mp3_path=glob.glob(output_dir/"chapters"/"*_final.mp3")
                if file_mp3_path:
                    for mp3_path in file_mp3_path:
                        if os.path.exists(mp3_path):
                            # 检查MP3文件是否可播放
                            try:
                                from pydub import AudioSegment
                                audio = AudioSegment.from_mp3(str(mp3_path))
                            except Exception as e:
                                chapters_error.append(chapter)
                                print( f"最终MP3文件无法播放: {e}")
                            print(f"✅ 文件 {mp3_path} 存在")
                        else:
                            print(f"❌ 文件 {mp3_path} 不存在")
                            chapters_error.append(chapter)
                        print(f"✅ 文件 {mp3_path} 存在")
                    return True
                else:
                    print(f"❌ 文件 {file_mp3_path} 不存在")
                    chapters_error.append(chapter)

        return chapters_error
    def check_file_exists(self):
        self.chapters=self.get_chapters_info(self.story_title)
        if not self.chapters:
            print(f"❌ 故事 {self.story_title} 没有任何章节")
            return False
        else:
            for chapter in self.chapters:
                print(f"✅ 找到故事 {self.story_title} 的章节 {chapter['chapter_number']}")
                file_path=chapter['file_path'].as_prosix()
                download_status=chapter['download_status']
                if not os.path.exists(file_path) or download_status != "completed":
                    print(f"❌ 文件 {file_path} 不存在", "或下载状态不是 completed")
                    return False
        return  True

