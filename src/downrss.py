import feedparser
import mysql.connector
from datetime import datetime, timedelta
import logging
import uuid

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RssSyncService:
    def __init__(self, db_config):
        self.db_config = db_config

    def get_db_connection(self):
        """获取数据库连接"""
        return mysql.connector.connect(**self.db_config)

    def sync_podcast_by_rss(self, rss_url):
        """根据RSS URL同步播客内容"""
        logger.info(f"开始同步播客: rss_url={rss_url}")

        try:
            # 获取播客信息
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)

            # 查询播客信息（根据rss_url）
            cursor.execute("SELECT * FROM podcasts WHERE rss_url = %s", (rss_url,))
            podcast = cursor.fetchone()

            # 获取RSS内容
            feed = self.parse_rss(rss_url)

            # 如果播客不存在，则创建新的播客
            if not podcast:
                logger.info(f"播客不存在，创建新播客: rss_url={rss_url}")
                podcast = self.create_podcast_from_rss(rss_url, feed, cursor)

            podcast_id = podcast['id']

            # 解析单集信息
            new_episodes = self.extract_episodes(feed, podcast_id)

            # 检查新单集
            new_episode_count = 0
            updated_episode_count = 0

            for episode in new_episodes:
                # 检查是否已存在
                cursor.execute("SELECT * FROM episodes WHERE guid = %s", (episode['guid'],))
                existing_episode = cursor.fetchone()

                if not existing_episode:
                    # 插入新单集
                    insert_query = """
                    INSERT INTO episodes (
                        podcast_id, title, description, audio_url, audio_length, 
                        audio_type, duration, guid, link_url, is_explicit, 
                        publish_date, created_at, updated_at
                    ) VALUES (
                        %(podcast_id)s, %(title)s, %(description)s, %(audio_url)s, 
                        %(audio_length)s, %(audio_type)s, %(duration)s, %(guid)s, 
                        %(link_url)s, %(is_explicit)s, %(publish_date)s, 
                        %(created_at)s, %(updated_at)s
                    )
                    """
                    cursor.execute(insert_query, episode)
                    new_episode_count += 1
                else:
                    # 更新现有单集信息（如果需要）
                    if (existing_episode['title'] != episode['title'] or
                            existing_episode['description'] != episode['description']):
                        update_query = """
                        UPDATE episodes SET 
                            title = %(title)s, 
                            description = %(description)s, 
                            publish_date = %(publish_date)s,
                            updated_at = %(updated_at)s
                        WHERE guid = %(guid)s
                        """
                        update_data = {
                            'title': episode['title'],
                            'description': episode['description'],
                            'publish_date': episode['publish_date'],
                            'guid': episode['guid'],
                            'updated_at': datetime.now()
                        }
                        cursor.execute(update_query, update_data)
                        updated_episode_count += 1

            # 更新播客信息
            update_podcast_query = """
            UPDATE podcasts SET 
                title = %(title)s,
                description = %(description)s,
                image_url = %(image_url)s,
                last_sync = %(last_sync)s,
                next_sync = %(next_sync)s
            WHERE id = %(id)s
            """

            now = datetime.now()
            next_sync = now + timedelta(hours=1)

            podcast_update_data = {
                'id': podcast_id,
                'title': feed.feed.get('title', '未知播客'),
                'description': getattr(feed.feed, 'description', ''),
                'image_url': self.extract_image_url(feed),
                'last_sync': now,
                'next_sync': next_sync
            }

            cursor.execute(update_podcast_query, podcast_update_data)

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(
                f"播客同步完成: id={podcast_id}, 新增单集={new_episode_count}, 更新单集={updated_episode_count}")

            return {
                'success': True,
                'newEpisodes': new_episode_count,
                'updatedEpisodes': updated_episode_count,
                'message': '同步成功',
                'podcastId': podcast_id,
                'podcastTitle': feed.feed.get('title', '未知播客')
            }

        except Exception as e:
            logger.error(f"播客同步失败: rss_url={rss_url}, error={str(e)}")
            return {
                'success': False,
                'message': f'同步失败: {str(e)}'
            }

    def create_podcast_from_rss(self, rss_url, feed, cursor):
        """根据RSS信息创建新播客"""
        logger.info(f"创建新播客: rss_url={rss_url}")

        insert_query = """
        INSERT INTO podcasts (
            rss_url, title, description, image_url, 
            last_sync, next_sync, created_at, updated_at
        ) VALUES (
            %(rss_url)s, %(title)s, %(description)s, %(image_url)s,
            %(last_sync)s, %(next_sync)s, %(created_at)s, %(updated_at)s
        )
        """

        now = datetime.now()
        next_sync = now + timedelta(hours=1)

        podcast_data = {
            'rss_url': rss_url,
            'title': feed.feed.get('title', '未知播客'),
            'description': getattr(feed.feed, 'description', ''),
            'image_url': self.extract_image_url(feed),
            'last_sync': now,
            'next_sync': next_sync,
            'created_at': now,
            'updated_at': now
        }

        cursor.execute(insert_query, podcast_data)
        podcast_id = cursor.lastrowid

        # 返回新创建的播客信息
        return {
            'id': podcast_id,
            'rss_url': rss_url,
            'title': feed.feed.get('title', '未知播客'),
            'description': getattr(feed.feed, 'description', ''),
            'image_url': self.extract_image_url(feed)
        }

    def sync_podcast(self, podcast_id):
        """同步播客内容（保持原有方法）"""
        logger.info(f"开始同步播客: id={podcast_id}")

        try:
            # 获取播客信息
            conn = self.get_db_connection()
            cursor = conn.cursor(dictionary=True)

            # 查询播客信息
            cursor.execute("SELECT * FROM podcasts WHERE id = %s", (podcast_id,))
            podcast = cursor.fetchone()

            if not podcast:
                raise Exception("播客不存在")

            # 获取RSS内容
            feed = self.parse_rss(podcast['rss_url'])

            # 解析单集信息
            new_episodes = self.extract_episodes(feed, podcast_id)

            # 检查新单集
            new_episode_count = 0
            updated_episode_count = 0

            for episode in new_episodes:
                # 检查是否已存在
                cursor.execute("SELECT * FROM episodes WHERE guid = %s", (episode['guid'],))
                existing_episode = cursor.fetchone()

                if not existing_episode:
                    # 插入新单集
                    insert_query = """
                    INSERT INTO episodes (
                        podcast_id, title, description, audio_url, audio_length, 
                        audio_type, duration, guid, link_url, is_explicit, 
                        publish_date, created_at, updated_at
                    ) VALUES (
                        %(podcast_id)s, %(title)s, %(description)s, %(audio_url)s, 
                        %(audio_length)s, %(audio_type)s, %(duration)s, %(guid)s, 
                        %(link_url)s, %(is_explicit)s, %(publish_date)s, 
                        %(created_at)s, %(updated_at)s
                    )
                    """
                    cursor.execute(insert_query, episode)
                    new_episode_count += 1
                else:
                    # 更新现有单集信息（如果需要）
                    if (existing_episode['title'] != episode['title'] or
                            existing_episode['description'] != episode['description']):
                        update_query = """
                        UPDATE episodes SET 
                            title = %(title)s, 
                            description = %(description)s, 
                            publish_date = %(publish_date)s,
                            updated_at = %(updated_at)s
                        WHERE guid = %(guid)s
                        """
                        update_data = {
                            'title': episode['title'],
                            'description': episode['description'],
                            'publish_date': episode['publish_date'],
                            'guid': episode['guid'],
                            'updated_at': datetime.now()
                        }
                        cursor.execute(update_query, update_data)
                        updated_episode_count += 1

            # 更新播客信息
            update_podcast_query = """
            UPDATE podcasts SET 
                title = %(title)s,
                description = %(description)s,
                image_url = %(image_url)s,
                last_sync = %(last_sync)s,
                next_sync = %(next_sync)s
            WHERE id = %(id)s
            """

            now = datetime.now()
            next_sync = now + timedelta(hours=1)

            podcast_update_data = {
                'id': podcast_id,
                'title': feed.feed.get('title', '未知播客'),
                'description': getattr(feed.feed, 'description', ''),
                'image_url': self.extract_image_url(feed),
                'last_sync': now,
                'next_sync': next_sync
            }

            cursor.execute(update_podcast_query, podcast_update_data)

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(
                f"播客同步完成: id={podcast_id}, 新增单集={new_episode_count}, 更新单集={updated_episode_count}")

            return {
                'success': True,
                'newEpisodes': new_episode_count,
                'updatedEpisodes': updated_episode_count,
                'message': '同步成功',
                'podcastId': podcast_id,
                'podcastTitle': feed.feed.get('title', '未知播客')
            }

        except Exception as e:
            logger.error(f"播客同步失败: id={podcast_id}, error={str(e)}")
            return {
                'success': False,
                'message': f'同步失败: {str(e)}'
            }

    def parse_rss(self, rss_url):
        """解析RSS"""
        try:
            # 使用feedparser解析RSS
            feed = feedparser.parse(rss_url)

            if feed.bozo:
                logger.warning(f"RSS解析警告: {rss_url}, warning={feed.bozo_exception}")

            return feed
        except Exception as e:
            logger.error(f"解析RSS失败: url={rss_url}, error={str(e)}")
            raise Exception(f"解析RSS失败: {str(e)}")

    def extract_episodes(self, feed, podcast_id):
        """从feed中提取单集信息"""
        episodes = []

        for entry in feed.entries:
            try:
                episode = self.extract_episode_info(entry, podcast_id)
                if episode:
                    episodes.append(episode)
            except Exception as e:
                logger.warning(f"解析单集失败: title={getattr(entry, 'title', '未知标题')}, error={str(e)}")

        return episodes

    def extract_episode_info(self, entry, podcast_id):
        """提取单集信息"""
        # 必需字段检查
        if not getattr(entry, 'title', '').strip():
            logger.warning("单集标题为空，跳过")
            return None

        # 提取音频信息
        audio_url = None
        audio_type = None
        audio_length = None

        if hasattr(entry, 'enclosures') and entry.enclosures:
            for enclosure in entry.enclosures:
                if getattr(enclosure, 'type', '').startswith('audio/') or \
                        getattr(enclosure, 'href', '').endswith(('.mp3', '.m4a', '.wav', '.aac')):
                    audio_url = getattr(enclosure, 'href', None)
                    audio_type = getattr(enclosure, 'type', None)
                    audio_length = getattr(enclosure, 'length', None)
                    break

        # 如果没有找到音频文件，跳过这个单集
        if not audio_url:
            logger.warning(f"单集 {entry.title} 没有音频文件，跳过")
            return None

        # 提取发布日期
        publish_date = datetime.now()
        if getattr(entry, 'published_parsed', None):
            publish_date = datetime(*entry.published_parsed[:6])
        elif getattr(entry, 'updated_parsed', None):
            publish_date = datetime(*entry.updated_parsed[:6])

        episode = {
            'podcast_id': podcast_id,
            'title': entry.title,
            'description': getattr(entry, 'summary', ''),
            'audio_url': audio_url,
            'audio_length': int(audio_length) if audio_length else None,
            'audio_type': audio_type,
            'duration': None,  # feedparser不直接提供
            'guid': getattr(entry, 'id', str(uuid.uuid4())),
            'link_url': getattr(entry, 'link', None),
            'is_explicit': False,  # feedparser可能需要额外处理
            'publish_date': publish_date,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }

        return episode

    def extract_image_url(self, feed):
        """提取图片URL"""
        # 尝试从feed中提取图片
        if hasattr(feed.feed, 'image') and hasattr(feed.feed.image, 'href'):
            return feed.feed.image.href

        # 尝试从iTunes扩展中提取
        if hasattr(feed.feed, 'itunes_image'):
            return getattr(feed.feed.itunes_image, 'href', None)

        return None


# 使用示例
if __name__ == "__main__":
    # 数据库配置
    db_config = {
        'host': '59.110.17.240',
        'user': 'admin',
        'password': 'Qwer!@#456',
        'database': 'podcast',
        'charset': 'utf8mb4'
    }

    # 创建同步服务实例
    sync_service = RssSyncService(db_config)

    # 根据RSS URL同步播客
    result = sync_service.sync_podcast_by_rss("https://anchor.fm/s/108621f0c/podcast/rss")
    print(result)
