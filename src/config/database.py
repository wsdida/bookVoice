# database.py
import mysql.connector
from mysql.connector import Error
from typing import Optional, Dict, Any
import os
from contextlib import contextmanager


class DatabaseManager:
    def __init__(self, host: str = None, database: str = None,
                 user: str = None, password: str = None, port: int = 3306):
        self.host = host or os.getenv('MYSQL_HOST', '59.110.17.240')
        self.database = database or os.getenv('MYSQL_DATABASE', 'bookvoice')
        self.user = user or os.getenv('MYSQL_USER', 'admin')
        self.password = password or os.getenv('MYSQL_PASSWORD', 'Qwer!@#456')
        self.port = port or int(os.getenv('MYSQL_PORT', 3306))
        self.init_database()

    @contextmanager
    def get_connection(self):
        """获取数据库连接的上下文管理器"""
        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port,
                charset='utf8mb4',
                collation='utf8mb4_unicode_ci'
            )
            yield connection
        except Error as e:
            if connection and connection.is_connected():
                connection.rollback()
            raise e
        finally:
            if connection and connection.is_connected():
                connection.close()

    def init_database(self):
        """初始化数据库表"""
        try:
            # 首先确保数据库存在
            self._create_database_if_not_exists()

            # 创建表
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 创建故事表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stories (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        title VARCHAR(255) UNIQUE NOT NULL,
                        url TEXT,
                        total_chapters INT DEFAULT 0,
                        downloaded_chapters INT DEFAULT 0,
                        status VARCHAR(50) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                ''')

                # 创建章节表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS chapters (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        story_id INT,
                        chapter_number INT,
                        title VARCHAR(255),
                        file_path TEXT,
                        download_status VARCHAR(50) DEFAULT 'pending',
                        audio_generation_status VARCHAR(50) DEFAULT 'pending',
                        rss_status VARCHAR(50) DEFAULT 'pending',
                        word_count INT DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        UNIQUE KEY unique_chapter (story_id, chapter_number),
                        FOREIGN KEY (story_id) REFERENCES stories(id) ON DELETE CASCADE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                ''')

                # 创建音频文件表
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS audio_files (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        chapter_id INT,
                        file_path TEXT,
                        file_size INT,
                        duration INT,
                        status VARCHAR(50) DEFAULT 'pending',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                        FOREIGN KEY (chapter_id) REFERENCES chapters(id) ON DELETE CASCADE
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                ''')

                conn.commit()
                print("数据库表初始化完成")

        except Error as e:
            print(f"初始化数据库时出错: {e}")
            raise

    def _create_database_if_not_exists(self):
        """创建数据库（如果不存在）"""
        try:
            # 连接到 MySQL 服务器（不指定数据库）
            connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port
            )
            cursor = connection.cursor()

            # 创建数据库
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database} "
                           f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
            connection.commit()

        except Error as e:
            print(f"创建数据库时出错: {e}")
            raise
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()

    def get_story_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """根据标题获取故事信息"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute('SELECT * FROM stories WHERE title = %s', (title,))
                row = cursor.fetchone()
                return row
        except Error as e:
            print(f"查询故事信息时出错: {e}")
            return None

    def create_or_update_story(self, title: str, url: str = None, total_chapters: int = 0):
        """创建或更新故事信息"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO stories 
                    (title, url, total_chapters, updated_at) 
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON DUPLICATE KEY UPDATE
                    url = VALUES(url),
                    total_chapters = VALUES(total_chapters),
                    updated_at = CURRENT_TIMESTAMP
                ''', (title, url, total_chapters))
                conn.commit()
        except Error as e:
            print(f"创建或更新故事时出错: {e}")
            raise

    def update_story_status(self, title: str, status: str, downloaded_chapters: int = None):
        """更新故事状态"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                if downloaded_chapters is not None:
                    cursor.execute('''
                        UPDATE stories 
                        SET status = %s, downloaded_chapters = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE title = %s
                    ''', (status, downloaded_chapters, title))
                else:
                    cursor.execute('''
                        UPDATE stories 
                        SET status = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE title = %s
                    ''', (status, title))

                conn.commit()
        except Error as e:
            print(f"更新故事状态时出错: {e}")
            raise

    def create_or_update_chapter(self, story_title: str, chapter_number: int,
                                 title: str = None, file_path: str = None):
        """创建或更新章节信息"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    return None

                story_id = story_row[0]

                # 插入或更新章节
                cursor.execute('''
                    INSERT INTO chapters 
                    (story_id, chapter_number, title, file_path, updated_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    file_path = VALUES(file_path),
                    updated_at = CURRENT_TIMESTAMP
                ''', (story_id, chapter_number, title, file_path))

                conn.commit()
        except Error as e:
            print(f"创建或更新章节时出错: {e}")
            raise

    def update_chapter_download_status(self, story_title: str, chapter_number: int,
                                       status: str, word_count: int = None):
        """更新章节下载状态"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    return

                story_id = story_row[0]

                if word_count is not None:
                    cursor.execute('''
                        UPDATE chapters
                        SET download_status = %s, word_count = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE story_id = %s AND chapter_number = %s
                    ''', (status, word_count, story_id, chapter_number))
                else:
                    cursor.execute('''
                        UPDATE chapters
                        SET download_status = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE story_id = %s AND chapter_number = %s
                    ''', (status, story_id, chapter_number))

                conn.commit()
        except Error as e:
            print(f"更新章节下载状态时出错: {e}")
            raise

    def update_chapter_audio_status(self, story_title: str, chapter_number: int, status: str):
        """更新章节音频生成状态"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    return

                story_id = story_row[0]

                cursor.execute('''
                    UPDATE chapters
                    SET audio_generation_status = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE story_id = %s AND chapter_number = %s
                ''', (status, story_id, chapter_number))

                conn.commit()
        except Error as e:
            print(f"更新章节音频状态时出错: {e}")
            raise

    def update_chapter_rss_status(self, story_title: str, chapter_number: int, status: str):
        """更新章节RSS状态"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    return

                story_id = story_row[0]

                cursor.execute('''
                    UPDATE chapters
                    SET rss_status = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE story_id = %s AND chapter_number = %s
                ''', (status, story_id, chapter_number))

                conn.commit()
        except Error as e:
            print(f"更新章节RSS状态时出错: {e}")
            raise

    def get_undownloaded_chapters(self, story_title: str) -> list:
        """获取未下载的章节列表"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    return []

                story_id = story_row[0]

                cursor.execute('''
                    SELECT chapter_number FROM chapters 
                    WHERE story_id = %s AND download_status != %s
                    ORDER BY chapter_number
                ''', (story_id, 'completed'))

                chapters = [row[0] for row in cursor.fetchall()]
                return chapters
        except Error as e:
            print(f"查询未下载章节时出错: {e}")
            return []

    def get_unprocessed_audio_chapters(self, story_title: str) -> list:
        """获取未处理音频的章节列表"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    return []

                story_id = story_row[0]

                cursor.execute('''
                    SELECT chapter_number FROM chapters 
                    WHERE story_id = %s AND download_status = %s 
                    AND audio_generation_status != %s
                    ORDER BY chapter_number
                ''', (story_id, 'completed', 'completed'))

                chapters = [row[0] for row in cursor.fetchall()]
                return chapters
        except Error as e:
            print(f"查询未处理音频章节时出错: {e}")
            return []

    def get_unprocessed_rss_chapters(self, story_title: str) -> list:
        """获取未处理RSS的章节列表"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    return []

                story_id = story_row[0]

                cursor.execute('''
                    SELECT chapter_number FROM chapters 
                    WHERE story_id = %s AND audio_generation_status = %s 
                    AND rss_status != %s
                    ORDER BY chapter_number
                ''', (story_id, 'completed', 'completed'))

                chapters = [row[0] for row in cursor.fetchall()]
                return chapters
        except Error as e:
            print(f"查询未处理RSS章节时出错: {e}")
            return []


    # 在 DatabaseManager 类中添加以下方法

    def assign_story_to_machine(self, story_title: str, machine_id: str):
        """将故事分配给特定机器"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE stories 
                    SET machine_id = %s, assigned_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE title = %s AND (machine_id IS NULL OR machine_id = %s)
                ''', (machine_id, story_title, machine_id))
                conn.commit()
                return cursor.rowcount > 0
        except Error as e:
            print(f"分配故事给机器时出错: {e}")
            return False


    def assign_chapter_to_machine(self, story_title: str, chapter_number: int, machine_id: str):
        """将章节分配给特定机器"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    return False

                story_id = story_row[0]

                cursor.execute('''
                    UPDATE chapters
                    SET machine_id = %s, assigned_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE story_id = %s AND chapter_number = %s AND (machine_id IS NULL OR machine_id = %s)
                ''', (machine_id, story_id, chapter_number, machine_id))
                conn.commit()
                return cursor.rowcount > 0
        except Error as e:
            print(f"分配章节给机器时出错: {e}")
            return False


    def get_unassigned_stories(self):
        """获取未分配给任何机器的故事"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute('''
                    SELECT title, url FROM stories 
                    WHERE status IN ('pending', 'partial') 
                    AND machine_id IS NULL
                    ORDER BY created_at
                ''')
                stories = cursor.fetchall()
                return stories
        except Error as e:
            print(f"查询未分配故事时出错: {e}")
            return []


    def get_unassigned_audio_chapters(self):
        """获取未分配给任何机器且有待处理音频的章节"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute('''
                    SELECT s.title as story_title, c.chapter_number, c.title
                    FROM chapters c
                    JOIN stories s ON c.story_id = s.id
                    WHERE c.audio_generation_status = 'pending'
                    AND c.machine_id IS NULL
                    ORDER BY s.created_at, c.chapter_number
                ''')
                chapters = cursor.fetchall()
                return chapters
        except Error as e:
            print(f"查询未分配音频章节时出错: {e}")
            return []


    def register_machine(self, machine_id: str, hostname: str = None, ip_address: str = None,
                         cpu_count: int = None, memory_gb: float = None, gpu_info: str = None):
        """注册或更新机器信息"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO machines 
                    (id, hostname, ip_address, cpu_count, memory_gb, gpu_info, last_heartbeat)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON DUPLICATE KEY UPDATE
                    hostname = VALUES(hostname),
                    ip_address = VALUES(ip_address),
                    cpu_count = VALUES(cpu_count),
                    memory_gb = VALUES(memory_gb),
                    gpu_info = VALUES(gpu_info),
                    last_heartbeat = CURRENT_TIMESTAMP
                ''', (machine_id, hostname, ip_address, cpu_count, memory_gb, gpu_info))
                conn.commit()
        except Error as e:
            print(f"注册机器时出错: {e}")
            raise


    def update_machine_heartbeat(self, machine_id: str):
        """更新机器心跳"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE machines 
                    SET last_heartbeat = CURRENT_TIMESTAMP
                    WHERE id = %s
                ''', (machine_id,))
                conn.commit()
        except Error as e:
            print(f"更新机器心跳时出错: {e}")
            raise


    def get_active_machines(self, minutes: int = 5):
        """获取活跃机器列表（最近几分钟内有心跳）"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute('''
                    SELECT * FROM machines 
                    WHERE status = 'active' 
                    AND last_heartbeat > DATE_SUB(NOW(), INTERVAL %s MINUTE)
                ''', (minutes,))
                machines = cursor.fetchall()
                return machines
        except Error as e:
            print(f"查询活跃机器时出错: {e}")
            return []


    def release_story_from_machine(self, story_title: str, machine_id: str):
        """从机器释放故事（当处理完成或失败时）"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE stories 
                    SET machine_id = NULL, assigned_at = NULL
                    WHERE title = %s AND machine_id = %s
                ''', (story_title, machine_id))
                conn.commit()
                return cursor.rowcount > 0
        except Error as e:
            print(f"从机器释放故事时出错: {e}")
            return False


    def release_chapter_from_machine(self, story_title: str, chapter_number: int, machine_id: str):
        """从机器释放章节（当处理完成或失败时）"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    return False

                story_id = story_row[0]

                cursor.execute('''
                    UPDATE chapters
                    SET machine_id = NULL, assigned_at = NULL
                    WHERE story_id = %s AND chapter_number = %s AND machine_id = %s
                ''', (story_id, chapter_number, machine_id))
                conn.commit()
                return cursor.rowcount > 0
        except Error as e:
            print(f"从机器释放章节时出错: {e}")
            return False


    # 在 database.py 的 DatabaseManager 类中添加以下方法

    def register_machine(self, machine_id: str, hostname: str = None, ip_address: str = None,
                         cpu_count: int = None, memory_gb: float = None, gpu_info: str = None):
        """注册或更新机器信息"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT INTO machines 
                    (id, hostname, ip_address, cpu_count, memory_gb, gpu_info, last_heartbeat)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON DUPLICATE KEY UPDATE
                    hostname = VALUES(hostname),
                    ip_address = VALUES(ip_address),
                    cpu_count = VALUES(cpu_count),
                    memory_gb = VALUES(memory_gb),
                    gpu_info = VALUES(gpu_info),
                    last_heartbeat = CURRENT_TIMESTAMP
                ''', (machine_id, hostname, ip_address, cpu_count, memory_gb, gpu_info))
                conn.commit()
                print(f"✅ 机器 {machine_id} 注册/更新成功")
        except Error as e:
            print(f"❌ 注册机器 {machine_id} 时出错: {e}")
            raise


    def update_machine_heartbeat(self, machine_id: str):
        """更新机器心跳"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE machines 
                    SET last_heartbeat = CURRENT_TIMESTAMP
                    WHERE id = %s
                ''', (machine_id,))
                conn.commit()
                if cursor.rowcount == 0:
                    print(f"⚠️  未找到机器 {machine_id}，可能需要重新注册")
        except Error as e:
            print(f"❌ 更新机器 {machine_id} 心跳时出错: {e}")
            raise


    def get_active_machines(self, minutes: int = 5):
        """获取活跃机器列表（最近几分钟内有心跳）"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute('''
                    SELECT * FROM machines 
                    WHERE status = 'active' 
                    AND last_heartbeat > DATE_SUB(NOW(), INTERVAL %s MINUTE)
                    ORDER BY last_heartbeat DESC
                ''', (minutes,))
                machines = cursor.fetchall()
                print(f"🔍 找到 {len(machines)} 个活跃机器")
                return machines
        except Error as e:
            print(f"❌ 查询活跃机器时出错: {e}")
            return []


    def assign_story_to_machine(self, story_title: str, machine_id: str):
        """将故事分配给特定机器"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # 只有未分配的故事才能被分配
                cursor.execute('''
                    UPDATE stories 
                    SET machine_id = %s, assigned_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE title = %s AND (machine_id IS NULL OR machine_id = %s)
                ''', (machine_id, story_title, machine_id))
                conn.commit()
                if cursor.rowcount > 0:
                    print(f"✅ 故事 '{story_title}' 已分配给机器 {machine_id}")
                    return True
                else:
                    print(f"⚠️  故事 '{story_title}' 无法分配给机器 {machine_id}（可能已被其他机器占用）")
                    return False
        except Error as e:
            print(f"❌ 分配故事 '{story_title}' 给机器 {machine_id} 时出错: {e}")
            return False


    def assign_chapter_to_machine(self, story_title: str, chapter_number: int, machine_id: str):
        """将章节分配给特定机器"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    print(f"⚠️  未找到故事 '{story_title}'")
                    return False

                story_id = story_row[0]

                # 只有未分配的章节才能被分配
                cursor.execute('''
                    UPDATE chapters
                    SET machine_id = %s, assigned_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
                    WHERE story_id = %s AND chapter_number = %s AND (machine_id IS NULL OR machine_id = %s)
                ''', (machine_id, story_id, chapter_number, machine_id))
                conn.commit()
                if cursor.rowcount > 0:
                    print(f"✅ 章节 {chapter_number} of '{story_title}' 已分配给机器 {machine_id}")
                    return True
                else:
                    print(f"⚠️  章节 {chapter_number} of '{story_title}' 无法分配给机器 {machine_id}（可能已被其他机器占用）")
                    return False
        except Error as e:
            print(f"❌ 分配章节 {chapter_number} of '{story_title}' 给机器 {machine_id} 时出错: {e}")
            return False


    def get_unassigned_stories(self):
        """获取未分配给任何机器的故事"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute('''
                    SELECT title, url, total_chapters FROM stories 
                    WHERE status IN ('pending', 'partial') 
                    AND machine_id IS NULL
                    ORDER BY created_at
                ''')
                stories = cursor.fetchall()
                print(f"🔍 找到 {len(stories)} 个未分配的故事")
                return stories
        except Error as e:
            print(f"❌ 查询未分配故事时出错: {e}")
            return []


    def get_unassigned_audio_chapters(self):
        """获取未分配给任何机器且有待处理音频的章节"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute('''
                    SELECT s.title as story_title, c.chapter_number, c.title, s.total_chapters
                    FROM chapters c
                    JOIN stories s ON c.story_id = s.id
                    WHERE c.audio_generation_status = 'pending'
                    AND c.machine_id IS NULL
                    AND s.status IN ('pending', 'partial', 'downloading')
                    ORDER BY s.created_at, c.chapter_number
                ''')
                chapters = cursor.fetchall()
                print(f"🔍 找到 {len(chapters)} 个未分配的待处理音频章节")
                return chapters
        except Exception as e:
            print(f"❌ 查询未分配音频章节时出错: {e}")
            return []


    def release_story_from_machine(self, story_title: str, machine_id: str):
        """从机器释放故事（当处理完成或失败时）"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    UPDATE stories 
                    SET machine_id = NULL, assigned_at = NULL
                    WHERE title = %s AND machine_id = %s
                ''', (story_title, machine_id))
                conn.commit()
                if cursor.rowcount > 0:
                    print(f"✅ 故事 '{story_title}' 已从机器 {machine_id} 释放")
                    return True
                else:
                    print(f"⚠️  故事 '{story_title}' 未从机器 {machine_id} 释放（可能未被该机器占用）")
                    return False
        except Error as e:
            print(f"❌ 从机器 {machine_id} 释放故事 '{story_title}' 时出错: {e}")
            return False


    def release_chapter_from_machine(self, story_title: str, chapter_number: int, machine_id: str):
        """从机器释放章节（当处理完成或失败时）"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    print(f"⚠️  未找到故事 '{story_title}'")
                    return False

                story_id = story_row[0]

                cursor.execute('''
                    UPDATE chapters
                    SET machine_id = NULL, assigned_at = NULL
                    WHERE story_id = %s AND chapter_number = %s AND machine_id = %s
                ''', (story_id, chapter_number, machine_id))
                conn.commit()
                if cursor.rowcount > 0:
                    print(f"✅ 章节 {chapter_number} of '{story_title}' 已从机器 {machine_id} 释放")
                    return True
                else:
                    print(f"⚠️  章节 {chapter_number} of '{story_title}' 未从机器 {machine_id} 释放（可能未被该机器占用）")
                    return False
        except Error as e:
            print(f"❌ 从机器 {machine_id} 释放章节 {chapter_number} of '{story_title}' 时出错: {e}")
            return False


    def get_machine_workload(self, machine_id: str):
        """获取机器当前工作负载"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)

                # 获取分配给该机器的故事数量
                cursor.execute('''
                    SELECT COUNT(*) as story_count FROM stories 
                    WHERE machine_id = %s AND status IN ('pending', 'partial', 'downloading')
                ''', (machine_id,))
                story_count = cursor.fetchone()['story_count']

                # 获取分配给该机器的章节数量
                cursor.execute('''
                    SELECT COUNT(*) as chapter_count FROM chapters 
                    WHERE machine_id = %s AND audio_generation_status = 'pending'
                ''', (machine_id,))
                chapter_count = cursor.fetchone()['chapter_count']

                return {
                    'stories': story_count,
                    'chapters': chapter_count
                }
        except Error as e:
            print(f"❌ 获取机器 {machine_id} 工作负载时出错: {e}")
            return {'stories': 0, 'chapters': 0}
    def get_chapter_audio_status(self, story_title: str, chapter_number: int) -> str:
        """获取特定章节的音频生成状态"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()

                # 获取故事ID
                cursor.execute('SELECT id FROM stories WHERE title = %s', (story_title,))
                story_row = cursor.fetchone()
                if not story_row:
                    return 'pending'

                story_id = story_row[0]

                cursor.execute('''
                    SELECT audio_generation_status FROM chapters 
                    WHERE story_id = %s AND chapter_number = %s
                ''', (story_id, chapter_number))

                row = cursor.fetchone()
                return row[0] if row else 'pending'
        except Error as e:
            print(f"查询章节音频状态时出错: {e}")
            return 'pending'
