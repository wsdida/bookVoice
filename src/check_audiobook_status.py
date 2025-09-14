
from config.database import DatabaseManager
import os
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



    def chess_rss_status(input_directory):
        podcast_rss_status=input_directory+local_rss_file_name


    def chess_audio_update(input_directory):
        return False
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

