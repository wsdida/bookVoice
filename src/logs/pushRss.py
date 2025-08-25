from playwright.async_api import async_playwright
import asyncio
import random
import time
import os

# 定义模拟浏览器的 Headers，确保所有值都是字符串
CUSTOM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://creators.spotify.com/",
    "DNT": str("1"),
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": str("1")
}

async def simulate_human_interaction(page, selector):
    """模拟真人鼠标移动和点击"""
    try:
        element = await page.query_selector(selector)
        if not element:
            raise Exception(f"未找到元素：{selector}")
        bounding_box = await element.bounding_box()
        if not bounding_box:
            raise Exception(f"无法获取元素 {selector} 的边界框")

        x = bounding_box["x"] + bounding_box["width"] / 2 + random.uniform(-10, 10)
        y = bounding_box["y"] + bounding_box["height"] / 2 + random.uniform(-5, 5)
        await page.mouse.move(x, y, steps=10)
        await asyncio.sleep(random.uniform(0.1, 0.5))
    except Exception as e:
        print(f"模拟鼠标交互失败：{e}")

async def submit_spotify_form(url, link_to_submit, profile_directory="Profile 1"):
    # 检查 Headers 值类型
    for key, value in CUSTOM_HEADERS.items():
        if not isinstance(value, str):
            print(f"警告：Headers 键 {key} 的值 {value} 不是字符串，已转换为字符串")
            CUSTOM_HEADERS[key] = str(value)

    async with async_playwright() as p:
        # 指定 Chrome 用户数据目录
        user_data_dir = r"C:\Users\ws\AppData\Local\Google\Chrome\User Data"  # 替换为实际路径
        browser = await p.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=False,  # 非 headless 便于观察
            viewport={"width": 1280, "height": 720},
            extra_http_headers=CUSTOM_HEADERS,
            channel="chrome",  # 使用已安装的 Chrome
            args=[f"--profile-directory={profile_directory}"]  # 指定用户配置文件
        )
        page = await browser.new_page()

        # 访问目标页面
        try:
            await page.goto(url, timeout=60000)
            print("已打开页面：", url)
        except Exception as e:
            print(f"页面加载失败：{e}")
            await browser.close()
            return

        # 等待页面加载完成
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
        except Exception as e:
            print(f"等待页面加载失败：{e}")
            await browser.close()
            return

        # 可选：处理登录（如果需要）
        """
        try:
            await page.goto("https://creators.spotify.com/login")
            await page.wait_for_load_state("networkidle")
            await page.type('input[name="username"]', "your_username", delay=100)
            await page.type('input[name="password"]', "your_password", delay=100)
            await simulate_human_interaction(page, 'button[type="submit"]')
            await page.click('button[type="submit"]')
            await page.wait_for_load_state("networkidle")
            await page.goto(url)
            await page.wait_for_load_state("networkidle")
            print("登录成功")
        except Exception as e:
            print(f"登录失败：{e}")
            await browser.close()
            return
        """

        # 查找输入框并模拟真人输入
        try:
            input_selector = 'input[type="text"], input[type="url"]'  # 需确认
            await page.wait_for_selector(input_selector, timeout=10000)
            await simulate_human_interaction(page, input_selector)
            await page.type(input_selector, link_to_submit, delay=random.uniform(50, 150))
            print(f"已将链接 '{link_to_submit}' 填入输入框")
            await asyncio.sleep(random.uniform(0.5, 1.5))
        except Exception as e:
            print(f"填充输入框失败：{e}")
            await browser.close()
            return

        # 查找并点击提交按钮
        try:
            submit_selector = 'button[type="submit"], button:contains("Submit")'  # 需确认
            await page.wait_for_selector(submit_selector, timeout=10000)
            await simulate_human_interaction(page, submit_selector)
            await page.click(submit_selector)
            print("已点击提交按钮")
        except Exception as e:
            print(f"提交失败：{e}")
            await browser.close()
            return

        # 等待提交响应
        try:
            await page.wait_for_load_state("networkidle", timeout=30000)
            print("提交操作完成，页面已加载")
            success_message = await page.query_selector("div.success-message, p.success")
            if success_message:
                print("提交成功：", await success_message.inner_text())
        except Exception as e:
            print(f"等待提交响应失败：{e}")

        # 保存截图以便调试
        await page.screenshot(path="spotify_submit_screenshot.png")
        print("已保存页面截图：spotify_submit_screenshot.png")

        # 关闭浏览器
        await browser.close()

# 示例运行
if __name__ == "__main__":
    target_url = "https://creators.spotify.com/dash/submit"
    generated_link = "https://example.com/rss-feed"  # 替换为实际链接
    profile_directory = "Profile 1"  # 替换为目标用户配置文件，例如 "Default" 或 "Profile 2"
    asyncio.run(submit_spotify_form(target_url, generated_link, profile_directory))

if __name__ == "__main__":
    target_url = "https://creators.spotify.com/dash/submit"
    generated_link = "http://59.110.17.240/podcasts/downloaded_stories/Last_Turn_Home/podcast.rss"  # 替换为实际生成的链接
    asyncio.run(submit_spotify_form(target_url, generated_link))