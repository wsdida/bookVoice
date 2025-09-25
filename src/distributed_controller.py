# 在 distributed_controller.py 中添加或修改相关方法
import asyncio
import os
import sys
from pathlib import Path
import glob
from config.database import DatabaseManager
from wattpad_downloader import OUTPUT_DIR, YOUR_WATTPAD_COOKIES
from generate_and_deploy_rss import run_rss_update_process


class DistributedController:
    def __init__(self, check_interval=30):
        self.check_interval = check_interval
        self.machine_id = self.get_machine_id()
        self.is_running = False
        self.db_manager = DatabaseManager()

    def get_machine_id(self):
        """获取机器唯一标识"""
        import socket
        hostname = socket.gethostname()
        return f"{hostname}"

    
    def get_assigned_stories(self):
        """
        获取分配给当前机器的故事（排除已完成的故事）
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute('''
                    SELECT title, url FROM stories 
                    WHERE machine_id = %s AND status IN ('pending','partial', 'downloading')
                ''', (self.machine_id,))
                return cursor.fetchall()
        except Exception as e:
            print(f"❌ 查询分配给机器的故事时出错: {e}")
            return []

    # 在 distributed_controller.py 中更新 get_assigned_chapters 方法

    def get_assigned_chapters(self):
        """
        获取分配给当前机器的章节（排除已完成的章节）
        """
        try:
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor(dictionary=True)
                cursor.execute('''
                    SELECT s.title as story_title, c.chapter_number, c.title
                    FROM chapters c
                    JOIN stories s ON c.story_id = s.id
                    WHERE c.machine_id = %s AND (
                        c.audio_generation_status IN ('pending', 'failed') OR
                        c.download_status IN ('pending', 'failed') OR
                        c.rss_status IN ('pending', 'failed')
                    )
                ''', (self.machine_id,))
                return cursor.fetchall()
        except Exception as e:
            print(f"❌ 查询分配给机器的章节时出错: {e}")
            return []

    def is_story_completed(self, story_title):
        """
        检查故事是否已完成所有处理步骤
        """
        try:
            story = self.db_manager.get_story_by_title(story_title)
            if not story:
                return False

            # 检查故事下载状态
            if story['status'] != 'completed':
                return False

            # 检查是否还有未处理的音频章节
            unprocessed_audio = self.db_manager.get_unprocessed_audio_chapters(story_title)
            if unprocessed_audio:
                return False

            # 检查是否还有未处理的RSS章节
            unprocessed_rss = self.db_manager.get_unprocessed_rss_chapters(story_title)
            if unprocessed_rss:
                return False

            return True
        except Exception as e:
            print(f"❌ 检查故事完成状态时出错: {e}")
            return False

    # 在 distributed_controller.py 中更新 process_story 方法

    def process_story(self, story):
        """
        处理分配给当前机器的故事
        """
        try:
            # 这里调用实际的故事处理逻辑
            # 例如：下载、生成音频、更新RSS等
            from wattpad_downloader import download_single_story, check_and_redownload_missing_chapters
            import asyncio

            # 创建异步事件循环并运行下载任务
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # 首先检查并重新下载缺失的章节
            loop.run_until_complete(
                check_and_redownload_missing_chapters(story, YOUR_WATTPAD_COOKIES, OUTPUT_DIR, self.machine_id)
            )

            # 然后执行完整的下载流程
            result = loop.run_until_complete(
                download_single_story(story, YOUR_WATTPAD_COOKIES, OUTPUT_DIR, self.machine_id)
            )
            loop.close()

            if result:
                print(f"✅ 故事 {story['title']} 处理完成")
                # 不再释放任务，保留机器分配信息
                # self.db_manager.release_story_from_machine(story['title'], self.machine_id)
            else:
                print(f"⚠️ 故事 {story['title']} 处理失败")

        except Exception as e:
            print(f"❌ 处理故事 {story['title']} 时出错: {e}")

    # 在 distributed_controller.py 的 process_chapter 方法中添加对MP3文件有效性的检查

    def process_chapter(self, story_title, chapter_number):
        """
        处理分配给当前机器的章节
        检查章节的下载、音频生成和RSS生成状态，并在未完成时继续处理
        """
        try:
            # 检查章节文件是否存在
            story_dir = os.path.join(OUTPUT_DIR, story_title)
            story_dir = Path(story_dir).as_posix()
            chapter_file = os.path.join(story_dir, f"Chapter_{chapter_number:04d}.txt")
            chapter_file = Path(chapter_file).as_posix()

            # 检查章节下载状态
            chapter_info = self.db_manager.get_chapter_info(story_title, chapter_number)
            if not chapter_info:
                print(f"⚠️ 章节信息不存在: {story_title} 第{chapter_number}章")
                return

            # 检查下载状态
            if chapter_info['download_status'] == 'failed' or not os.path.exists(chapter_file):
                print(f"⚠️ 章节文件不存在或下载失败: {chapter_file}")
                print(f"🔄 尝试重新下载章节 {chapter_number}...")

                # 同步调用重新下载方法
                result = self.redownload_missing_chapters(story_title)

                # 重新检查文件是否存在
                if result and os.path.exists(chapter_file):
                    print(f"✅ 章节 {chapter_number} 重新下载成功")
                    # 更新下载状态为完成
                    self.db_manager.update_chapter_download_status(story_title, chapter_number, 'completed')
                else:
                    print(f"❌ 章节 {chapter_number} 重新下载失败或文件仍不存在")
                    # 更新下载状态为失败
                    self.db_manager.update_chapter_download_status(story_title, chapter_number, 'failed')
                    return

            # 检查音频生成状态
            if chapter_info['audio_generation_status'] in ['pending', 'failed']:
                # 如果状态为failed，检查MP3文件是否存在以及是否有效
                if chapter_info['audio_generation_status'] == 'failed':
                    # 检查最终MP3文件
                    txt_filename = os.path.splitext(os.path.basename(chapter_file))[0]
                    output_dir_name = f"{txt_filename}_audiobook_output"
                    output_dir = os.path.join(story_dir, output_dir_name)
                    final_mp3 = os.path.join(output_dir, "chapters", f"{txt_filename}_final.mp3")

                    # 如果MP3文件存在，检查是否为有效的MP3文件
                    if os.path.exists(final_mp3):
                        try:
                            # 尝试加载MP3文件以验证其有效性
                            from pydub import AudioSegment
                            audio = AudioSegment.from_mp3(final_mp3)
                            print(f"✅ 检测到有效的MP3文件: {final_mp3}")
                        except Exception as e:
                            # MP3文件无效，删除它
                            print(f"❌ 检测到无效的MP3文件: {final_mp3}，错误: {e}")
                            try:
                                os.remove(final_mp3)
                                print(f"🗑️ 已删除无效的MP3文件: {final_mp3}")
                            except Exception as delete_error:
                                print(f"❌ 删除无效MP3文件失败: {delete_error}")

                print(
                    f"🔊 开始{'重新' if chapter_info['audio_generation_status'] == 'failed' else ''}生成音频: {story_title} 第{chapter_number}章")
                if os.path.exists(chapter_file):
                    from audiobook_generator import generate_audiobook
                    try:
                        generate_audiobook(
                            story_dir,
                            chapter_file,
                            'config.yaml',
                            force_rebuild=(chapter_info['audio_generation_status'] == 'failed'),  # 如果是失败状态则强制重建
                            auto_update_rss=False
                        )

                        # 更新数据库状态
                        self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'completed')
                        print(f"✅ 章节 {story_title} 第{chapter_number}章音频生成完成")
                    except Exception as e:
                        print(f"❌ 章节 {story_title} 第{chapter_number}章音频生成失败: {e}")
                        self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'failed')
                        return
                else:
                    print(f"❌ 章节文件不存在，无法生成音频: {chapter_file}")
                    self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'failed')
                    return
            else:
                # 音频状态为completed，但需要检查实际文件是否存在和有效
                print(f"✅ 章节 {story_title} 第{chapter_number}章音频已标记为生成完成，验证文件状态...")

                txt_filename = os.path.splitext(os.path.basename(chapter_file))[0]
                output_dir_name = f"{txt_filename}_audiobook_output"
                output_dir = os.path.join(story_dir, output_dir_name)
                final_mp3 = glob.glob(Path(output_dir+"/chapters"+"/*_final.mp3").as_posix())

                for mp3 in final_mp3:
                    # 检查最终MP3文件是否存在
                    if not os.path.exists(mp3):
                        print(f"❌ 检测到音频文件缺失: {final_mp3}，重新生成...")
                        # 文件不存在，重新生成
                        if os.path.exists(chapter_file):
                            from audiobook_generator import generate_audiobook
                            try:
                                # 强制重建，因为文件缺失
                                generate_audiobook(
                                    story_dir,
                                    chapter_file,
                                    'config.yaml',
                                    force_rebuild=True,
                                    auto_update_rss=False
                                )

                                # 更新数据库状态
                                self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'completed')
                                print(f"✅ 章节 {story_title} 第{chapter_number}章音频重新生成完成")
                            except Exception as e:
                                print(f"❌ 章节 {story_title} 第{chapter_number}章音频重新生成失败: {e}")
                                self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'failed')
                                return
                        else:
                            print(f"❌ 章节文件不存在，无法生成音频: {chapter_file}")
                            self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'failed')
                            return
                    else:
                        # 文件存在，检查是否为有效的MP3文件
                        try:
                            from pydub import AudioSegment
                            audio = AudioSegment.from_mp3(mp3)
                            print(f"✅ 检测到有效的MP3文件: {mp3}")
                        except Exception as e:
                            # MP3文件无效，删除它并重新生成
                            print(f"❌ 检测到无效的MP3文件: {mp3}，错误: {e}")
                            try:
                                os.remove(mp3)
                                print(f"🗑️ 已删除无效的MP3文件: {mp3}")
                            except Exception as delete_error:
                                print(f"❌ 删除无效MP3文件失败: {delete_error}")

                            # 重新生成音频
                            if os.path.exists(chapter_file):
                                from audiobook_generator import generate_audiobook
                                try:
                                    generate_audiobook(
                                        story_dir,
                                        chapter_file,
                                        'config.yaml',
                                        force_rebuild=True,  # 强制重建
                                        auto_update_rss=False
                                    )

                                    # 更新数据库状态
                                    self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'completed')
                                    print(f"✅ 章节 {story_title} 第{chapter_number}章音频重新生成完成")
                                except Exception as e:
                                    print(f"❌ 章节 {story_title} 第{chapter_number}章音频重新生成失败: {e}")
                                    self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'failed')
                                    return
                            else:
                                print(f"❌ 章节文件不存在，无法生成音频: {chapter_file}")
                                self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'failed')
                                return

            # 检查RSS生成状态
            if chapter_info['rss_status'] in ['pending', 'failed']:
                print(
                    f"📡 开始{'重新' if chapter_info['rss_status'] == 'failed' else ''}更新RSS: {story_title} 第{chapter_number}章")
                try:
                    story_dir = os.path.join(OUTPUT_DIR, story_title)
                    success = run_rss_update_process(story_dir)

                    if success:
                        # 更新数据库状态
                        self.db_manager.update_chapter_rss_status(story_title, chapter_number, 'completed')
                        print(f"✅ 章节 {story_title} 第{chapter_number}章RSS更新完成")
                    else:
                        print(f"❌ 章节 {story_title} 第{chapter_number}章RSS更新失败")
                        self.db_manager.update_chapter_rss_status(story_title, chapter_number, 'failed')
                        return
                except Exception as e:
                    print(f"❌ 章节 {story_title} 第{chapter_number}章RSS更新异常: {e}")
                    self.db_manager.update_chapter_rss_status(story_title, chapter_number, 'failed')
                    return
            else:
                print(f"✅ 章节 {story_title} 第{chapter_number}章RSS已更新")

            # 不再释放任务，保留机器分配信息
            # self.db_manager.release_chapter_from_machine(story_title, chapter_number, self.machine_id)
            print(f"✅ 章节 {story_title} 第{chapter_number}章处理完成")

        except Exception as e:
            print(f"❌ 处理章节 {story_title} 第{chapter_number}章时出错: {e}")
            # 更新数据库状态为失败
            try:
                self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'failed')
            except Exception as db_error:
                print(f"❌ 更新数据库状态时出错: {db_error}")

    def check_chapter_consistency(self, story_title, chapter_number):
        """
        检查数据库状态与实际文件的一致性
        1. 检查数据库章节与实际文件是否对应
        2. 检查语音生成与章节是否对应
        3. 检查RSS文件生成内容与语音生成章节是否对应
        """
        print(f"🔍 检查章节一致性: {story_title} 第{chapter_number}章")
        
        # 1. 检查数据库章节与实际文件是否对应
        story_dir = os.path.join(OUTPUT_DIR, story_title)
        chapter_file = os.path.join(story_dir, f"Chapter_{chapter_number:04d}.txt")
        
        # 获取数据库中的章节信息
        chapter_info = self.db_manager.get_chapter_info(story_title, chapter_number)
        if not chapter_info:
            print(f"❌ 数据库中不存在章节信息: {story_title} 第{chapter_number}章")
            return False
            
        # 检查下载状态与实际文件
        if chapter_info['download_status'] == 'completed':
            if not os.path.exists(chapter_file):
                print(f"❌ 数据库标记为已完成但文件不存在: {chapter_file}")
                # 更新数据库状态
                self.db_manager.update_chapter_download_status(story_title, chapter_number, 'failed')
                return False
            else:
                print(f"✅ 下载状态一致性检查通过")
        else:
            print(f"⚠️ 下载状态未完成: {chapter_info['download_status']}")
            
        # 2. 检查语音生成与章节是否对应
        if chapter_info['audio_generation_status'] == 'completed':
            txt_filename = os.path.splitext(os.path.basename(chapter_file))[0]
            output_dir_name = f"{txt_filename}_audiobook_output"
            output_dir = os.path.join(story_dir, output_dir_name)
            
            # 使用check_and_rebuild_if_needed函数检查并重建音频（如果需要）
            try:
                from batch_audiobook_generator import check_and_rebuild_if_needed
                rebuild_result = check_and_rebuild_if_needed(
                    story_dir, 
                    Path(chapter_file), 
                    story_title, 
                    chapter_number, 
                    'config.yaml'
                )
                
                if not rebuild_result:
                    print(f"❌ 音频文件检查或重建失败: {story_title} 第{chapter_number}章")
                    # 更新数据库状态
                    self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'failed')
                    return False
                else:
                    print(f"✅ 音频生成一致性检查通过")
            except Exception as e:
                print(f"❌ 调用check_and_rebuild_if_needed时出错: {e}")
                # 更新数据库状态
                self.db_manager.update_chapter_audio_status(story_title, chapter_number, 'failed')
                return False
        else:
            print(f"⚠️ 音频生成状态未完成: {chapter_info['audio_generation_status']}")

        # 3. 检查RSS文件生成内容与语音生成章节是否对应
        if chapter_info['rss_status'] == 'completed':
            # 检查RSS文件中是否包含该章节
            rss_file = os.path.join(story_dir, "generated_podcast_rss.xml")
            if os.path.exists(rss_file):
                try:
                    import xml.etree.ElementTree as ET
                    tree = ET.parse(rss_file)
                    root = tree.getroot()
                    
                    # 查找RSS中是否包含该章节
                    found = False
                    for item in root.findall(".//item"):
                        title_elem = item.find("title")
                        if title_elem is not None and f"Chapter {chapter_number:02d}" in title_elem.text:
                            found = True
                            break
                            
                    if found:
                        print(f"✅ RSS生成一致性检查通过")
                    else:
                        print(f"❌ RSS文件中未找到章节条目: Chapter {chapter_number:02d}")
                        # 这里不更新数据库状态，因为RSS是整体更新的
                        return False
                except Exception as e:
                    print(f"❌ 解析RSS文件时出错: {e}")
                    return False
            else:
                print(f"❌ RSS文件不存在: {rss_file}")
                return False
        else:
            print(f"⚠️ RSS生成状态未完成: {chapter_info['rss_status']}")
            
        return True

    def check_and_process_assigned_tasks(self):
        """
        检查分配给当前机器但未完成的任务并继续处理
        """
        print("🔍 检查分配给当前机器的未完成任务...")

        # 检查分配给当前机器但未完成的故事
        assigned_stories = self.get_assigned_stories()
        for story in assigned_stories:
            story_title = story['title']
            print(f"  -> 检查未完成故事: {story_title}")

            # 检查故事状态
            if self.is_story_completed(story_title):
                print(f"     故事 {story_title} 已完成，保留机器分配信息")
                # 不再释放任务，保留机器分配信息
                # self.db_manager.release_story_from_machine(story_title, self.machine_id)
                continue

            # 继续处理未完成的故事
            print(f"     继续处理未完成故事: {story_title}")
            self.process_story(story)

        # 检查分配给当前机器但未完成的章节（包括失败的章节）
        assigned_chapters = self.get_assigned_chapters()
        for chapter in assigned_chapters:
            story_title = chapter['story_title']
            chapter_number = chapter['chapter_number']

            # 首先检查一致性
            is_consistent = self.check_chapter_consistency(story_title, chapter_number)
            
            # 检查章节状态
            chapter_status = self.db_manager.get_chapter_info(story_title, chapter_number)
            # 检查是否已完成所有处理
            chapter_info = self.db_manager.get_chapter_info(story_title, chapter_number)
            is_completed = (chapter_info and 
                           chapter_info['download_status'] == 'completed' and
                           chapter_info['audio_generation_status'] == 'completed' and
                           chapter_info['rss_status'] == 'completed')
            
            if is_completed and is_consistent:
                print(f"     章节 {story_title} 第{chapter_number}章已完成且一致，保留机器分配信息")
                # 不再释放任务，保留机器分配信息
                # self.db_manager.release_chapter_from_machine(story_title, chapter_number, self.machine_id)
                continue

            # 继续处理未完成或失败的章节
            status_text = "失败" if chapter_status == 'failed' else "未完成"
            print(f"     继续处理{status_text}章节: {story_title} 第{chapter_number}章")
            # 同步处理章节
            self.process_chapter(story_title, chapter_number)

    def perform_comprehensive_check(self):
        """
        执行全面检查，确保数据库状态与实际文件一致
        """
        print("🔍 执行全面一致性检查...")
        
        # 获取所有分配给当前机器的故事
        assigned_stories = self.get_assigned_stories()
        for story in assigned_stories:
            story_title = story['title']
            print(f"  -> 检查故事: {story_title}")
            
            # 获取故事的所有章节信息
            all_chapters = self.db_manager.get_all_chapters_info(story_title)
            for chapter_info in all_chapters:
                chapter_number = chapter_info['chapter_number']
                self.check_chapter_consistency(story_title, chapter_number)
                
        print("✅ 全面一致性检查完成")

    def check_and_update_rss(self, story_title):
        """
        检查并更新RSS（当有新章节完成时）
        """
        try:
            # 检查是否有未处理的RSS章节
            unprocessed_rss = self.db_manager.get_unprocessed_rss_chapters(story_title)
            if not unprocessed_rss:
                # 所有章节都已完成，更新RSS
                story_dir = os.path.join(OUTPUT_DIR, story_title)
                from generate_and_deploy_rss import run_rss_update_process
                run_rss_update_process(story_dir)
                print(f"✅ RSS更新完成: {story_title}")

        except Exception as e:
            print(f"❌ 更新RSS时出错: {e}")

    def assign_new_tasks(self):
        """
        分配新任务给当前机器
        """
        try:
            # 获取机器当前工作负载
            workload = self.db_manager.get_machine_workload(self.machine_id)
            max_stories = 2  # 最大同时处理的故事数
            max_chapters = 10  # 最大同时处理的章节数

            # 如果当前负载较低，可以分配新任务
            if workload['stories'] < max_stories:
                # 分配新故事
                unassigned_stories = self.db_manager.get_unassigned_stories()
                for story in unassigned_stories[:max_stories - workload['stories']]:
                    if self.db_manager.assign_story_to_machine(story['title'], self.machine_id):
                        print(f"✅ 分配新故事: {story['title']}")
                        self.process_story(story)

            if workload['chapters'] < max_chapters:
                # 分配新章节
                unassigned_chapters = self.db_manager.get_unassigned_audio_chapters()
                for chapter in unassigned_chapters[:max_chapters - workload['chapters']]:
                    if self.db_manager.assign_chapter_to_machine(
                            chapter['story_title'],
                            chapter['chapter_number'],
                            self.machine_id
                    ):
                        print(f"✅ 分配新章节: {chapter['story_title']} 第{chapter['chapter_number']}章")
                        # 同步处理章节
                        self.process_chapter(chapter['story_title'], chapter['chapter_number'])

        except Exception as e:
            print(f"❌ 分配新任务时出错: {e}")



    def run(self):
        """
        运行分布式控制器主循环
        """
        print("🚀 启动分布式控制器...")
        self.is_running = True

        # 注册机器
        self.register_machine()
        
        # 添加计数器用于定期执行全面检查
        comprehensive_check_interval = 10  # 每10个周期执行一次全面检查
        cycle_count = 0

        while self.is_running:
            try:

                print(f"\n🔄 执行任务检查周期...")

                # 1. 首先检查并处理已分配但未完成的任务
                self.check_and_process_assigned_tasks()
                
                # 2. 然后分配新任务
                self.assign_new_tasks()
                
                # 3. 定期执行全面检查
                cycle_count += 1
                if cycle_count >= comprehensive_check_interval:
                    self.perform_comprehensive_check()
                    cycle_count = 0

                # 4. 更新机器心跳
                self.db_manager.update_machine_heartbeat(self.machine_id)

                print(f"⏳ 等待 {self.check_interval} 秒后进行下一次检查...")
                import time
                time.sleep(self.check_interval)

            except Exception as e:
                print(f"❌ 执行任务检查时出错: {e}")
                import time
                time.sleep(self.check_interval)

    def register_machine(self):
        """
        注册当前机器到数据库
        """
        try:
            import socket
            import psutil

            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
            cpu_count = psutil.cpu_count()
            memory_gb = psutil.virtual_memory().total / (1024 ** 3)

            # 简单的GPU信息检查
            gpu_info = "Unknown"
            try:
                import GPUtil
                gpus = GPUtil.getGPUs()
                if gpus:
                    gpu_info = f"{gpus[0].name} ({gpus[0].memoryTotal}MB)"
            except:
                pass

            # 注册机器到数据库
            self.db_manager.register_machine(
                self.machine_id,
                hostname,
                ip_address,
                cpu_count,
                memory_gb,
                gpu_info
            )
            
            # 确保机器心跳是最新的
            self.db_manager.update_machine_heartbeat(self.machine_id)
            
            print(f"✅ 机器注册成功: {self.machine_id}")
            return True
        except Exception as e:
            print(f"❌ 机器注册失败: {e}")
            return False

    def redownload_missing_chapters(self, story_title):
        """
        重新下载缺失的章节文件 (同步版本)
        """
        try:
            # 获取故事信息
            story = self.db_manager.get_story_by_title(story_title)
            if not story:
                print(f"❌ 未找到故事: {story_title}")
                return False

            story_dir = os.path.join(OUTPUT_DIR, story_title)
            if not os.path.exists(story_dir):
                print(f"❌ 故事目录不存在: {story_dir}")
                return False

            # 获取所有未下载完成的章节
            undownloaded_chapters = self.db_manager.get_undownloaded_chapters(story_title)
            if not undownloaded_chapters:
                print(f"✅ 故事 {story_title} 所有章节均已下载完成")
                return True

            print(f"🔍 发现 {len(undownloaded_chapters)} 个未下载章节: {undownloaded_chapters}")

            # 获取故事URL
            story_url = story.get('url')
            if not story_url:
                print(f"❌ 故事 URL 不存在: {story_title}")
                return False

            # 导入必要的函数
            sys.path.append(os.path.dirname(os.path.abspath(__file__)))
            from wattpad_downloader import get_chapter_links, download_chapter_content, load_status

            # 获取章节链接
            try:
                chapter_urls = get_chapter_links(story_url, YOUR_WATTPAD_COOKIES)
            except Exception as e:
                print(f"❌ 获取章节链接时出错: {e}")
                return False

            if not chapter_urls:
                print(f"❌ 无法获取章节链接: {story_url}")
                return False

            status = load_status(story_dir)

            # 下载缺失的章节
            success_count = 0
            for chapter_num in undownloaded_chapters:
                if 1 <= chapter_num <= len(chapter_urls):
                    chapter_url = chapter_urls[chapter_num - 1]
                    print(f"📥 重新下载章节 {chapter_num}...")

                    try:
                        success = download_chapter_content(
                                chapter_url,
                                chapter_num,
                                story_dir,
                                YOUR_WATTPAD_COOKIES,
                                status,
                                story_title
                            )

                        if success:
                            success_count += 1
                            print(f"✅ 章节 {chapter_num} 下载成功")
                        else:
                            print(f"❌ 章节 {chapter_num} 下载失败")
                    except Exception as e:
                        print(f"❌ 下载章节 {chapter_num} 时出错: {e}")
                        import traceback
                        traceback.print_exc()
                else:
                    print(f"⚠️ 章节编号 {chapter_num} 超出范围")

            print(f"📊 重新下载完成: {success_count}/{len(undownloaded_chapters)} 成功")

            # 更新数据库状态
            completed_chapters = len(chapter_urls) - len(undownloaded_chapters) + success_count
            self.db_manager.update_story_status(
                story_title,
                'completed' if completed_chapters == len(chapter_urls) else 'partial',
                completed_chapters
            )

            return success_count == len(undownloaded_chapters)

        except Exception as e:
            print(f"❌ 重新下载章节时出错: {e}")
            import traceback
            traceback.print_exc()
            return False



