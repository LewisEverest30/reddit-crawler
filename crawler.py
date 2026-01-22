# coding=utf-8
import json
import logging
import random
import asyncio
import os
import shutil
from pathlib import Path
from playwright.async_api import async_playwright
import datetime
import re

def setup_logger(debug=False):
    level = logging.DEBUG if debug else logging.INFO
    format_str = '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    
    logging.basicConfig(
        level=level,
        format=format_str,
        handlers=[
            logging.FileHandler("reddit_crawler_playwright.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

class RedditCrawler:

    def __init__(self, subreddit_url, max_posts=100, headless=False, 
                 use_system_browser='chrome', sampling_ratios=None, delays=None):

        self.subreddit_url = subreddit_url
        self.max_posts = max_posts
        self.headless = headless
        self.use_system_browser = use_system_browser
        self.sampling_ratios = sampling_ratios or {
            'new': 0.65, 'top_year': 0.25, 'best': 0.1
        }
        self.delays = delays or {
            'page_min': 2000, 'page_max': 5000,
            'action_min': 500, 'action_max': 1500,
            'scroll_min': 3000, 'scroll_max': 6000,
            'api_min': 1000, 'api_max': 2000
        }
        
        # 提取subreddit名称并创建目录
        self.subreddit_name = self._extract_subreddit_name(subreddit_url)
        self.subreddit_dir = f".\\outputs\\{self.subreddit_name}"
        Path(self.subreddit_dir).mkdir(parents=True, exist_ok=True)
        
        # 结果文件路径与爬取状态记录路径
        self.output_file = os.path.join(self.subreddit_dir, f"{self.subreddit_name}_data.json")
        self.state_file = os.path.join(self.subreddit_dir, f"{self.subreddit_name}_crawler_state.json")
        
        # 浏览器数据目录
        self.user_data_dir = self._get_browser_data_dir()
        
        # 存储数据
        self.all_posts_data = []
        self.total_crawled_count = 0
        
        # Playwright对象
        self.playwright = None
        self.browser = None
        self.page = None

    def _extract_subreddit_name(self, url):
        """从Reddit URL中提取subreddit名称"""
        match = re.search(r'/r/([^/]+)', url)
        return match.group(1) if match else "unknown_subreddit"
    
    def _get_browser_data_dir(self):
        """获取浏览器数据目录"""
        if self.use_system_browser:
            data_dir = f"./browser_data_{self.use_system_browser}"
        else:
            data_dir = "./browser_data"
        Path(data_dir).mkdir(parents=True, exist_ok=True)
        return data_dir

    async def init_browser(self):
        """初始化浏览器"""
        self.playwright = await async_playwright().start()
        
        # 确定浏览器channel
        channel = None
        if self.use_system_browser:
            if self.use_system_browser.lower() == 'chrome':
                channel = 'chrome'
            elif self.use_system_browser.lower() in ['edge', 'msedge']:
                channel = 'msedge'
        
        self.browser = await self.playwright.chromium.launch_persistent_context(
            user_data_dir=self.user_data_dir,
            headless=self.headless,
            channel=channel,
            viewport={'width': 1920, 'height': 1080},
            args=[
                '--no-sandbox',
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-first-run',
                '--disable-notifications',
                '--disable-infobars',
            ]
        )
        
        # 反自动化检测
        await self.browser.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            delete navigator.__proto__.webdriver;
        """)
        
        self.page = await self.browser.new_page()

    async def _check_and_handle_captcha_or_login(self, current_url=None):
        """检测CAPTCHA或登录验证并等待手动处理"""
        try:
            # 检测常见的CAPTCHA元素
            captcha_selectors = [
                '[data-testid="captcha"]',  # Reddit specific
                '.g-recaptcha',  # Google reCAPTCHA
                '#recaptcha',
                '[class*="captcha"]',
                '[id*="captcha"]',
                'iframe[src*="recaptcha"]',
                'iframe[src*="captcha"]',
                '[aria-label*="captcha"]',
                '[aria-label*="verification"]'
            ]
            captcha_found = False
            for selector in captcha_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    if elements:
                        # 检查元素是否可见
                        for element in elements:
                            is_visible = await element.is_visible()
                            if is_visible:
                                captcha_found = True
                                break
                    if captcha_found:
                        break
                except Exception:
                    # 如果页面上下文失效，跳过这个检测
                    continue

            # 也检查页面文本中是否包含验证以及登录相关内容
            if not captcha_found:
                # 检查页面是否包含正常的Reddit数据结构，如果有就跳过验证检测
                json_element = await self.page.query_selector("pre")
                json_string = await json_element.text_content()
                if "title" in  json_string \
                    and "author" in json_string \
                    and "selftext" in json_string:
                    return
                try:
                    page_text = await self.page.locator('body').text_content()
                    if page_text:
                        captcha_keywords = ['captcha', 'verification', 'prove you are human', 'robot check', 'login', 'sign in', '登录']
                        page_text_lower = page_text.lower()
                        for keyword in captcha_keywords:
                            if keyword in page_text_lower:
                                captcha_found = True
                                break
                except Exception:
                    pass
            
            if captcha_found:
                logging.warning("检测到CAPTCHA或登录验证！程序已暂停，请手动完成验证或登录")
                logging.warning("完成后，请在此命令行输入 'c' 然后按回车键继续程序")
                # 等待用户输入
                while True:
                    try:
                        user_input = input("请输入 'c' 继续: ").strip().lower()
                        if user_input == 'c':
                            logging.info("继续执行程序...")
                            break
                        else:
                            print("请输入 'c' 来继续程序")
                    except KeyboardInterrupt:
                        logging.info("用户中断程序")
                        raise
                    except Exception as e:
                        logging.warning(f"输入处理错误: {e}")
                        continue
                
                # 用户处理完验证后，重新导航到当前页面以恢复上下文
                if current_url:
                    try:
                        logging.info("重新加载页面以恢复上下文...")
                        await self.page.goto(current_url, wait_until='domcontentloaded', timeout=30000)
                        await self.page.wait_for_timeout(random.randint(self.delays['page_min'], self.delays['page_max']))
                    except Exception as e:
                        logging.warning(f"重新加载页面失败: {e}")
                        
        except Exception as e:
            logging.error(f"CAPTCHA检测时出错: {e}")

    async def _simulate_human_browse_a_post_behavior(self, random_rate=0.2):
        """模拟人类浏览行为 - 随机点击帖子并浏览"""
        if random.random() >= random_rate:
            return
        logging.info("触发模拟浏览行为")
        try:
            # 一次性筛选出视口内有效的链接
            links = await self.page.query_selector_all('a[href*="/comments/"]:not([data-testid*="ad"]):not([data-adtype])')
            viewport_links = []
            
            for link in links:
                try:
                    href = await link.get_attribute("href")
                    if not href or "/user/" in href:
                        continue
                    
                    # 补全相对URL并验证格式
                    if href.startswith('/'):
                        href = "https://www.reddit.com" + href
                    if not re.match(r'https://www\.reddit\.com/r/[^/]+/comments/[a-zA-Z0-9]+/[^/]+/?$', href):
                        continue
                    
                    # 检查是否在视口内且可见
                    if await link.is_visible():
                        in_viewport = await link.evaluate('''
                            element => {
                                const rect = element.getBoundingClientRect();
                                return rect.bottom > 0 && rect.top < window.innerHeight && 
                                       rect.right > 0 && rect.left < window.innerWidth;
                            }
                        ''')
                        if in_viewport:
                            viewport_links.append((link, href))
                except Exception:
                    continue
            
            if not viewport_links:
                return
            
            # 选择并点击链接
            chosen_link, href = viewport_links[-1]
            logging.info(f"选择链接进行模拟浏览{href}")
            
            if not await chosen_link.is_enabled():
                logging.info("选择的链接不可点击")
                return
            
            # 执行点击、浏览、返回流程
            await chosen_link.scroll_into_view_if_needed()
            await self.page.wait_for_timeout(self.delays['action_min'])
            await chosen_link.click(force=True)
            await self.page.wait_for_load_state('domcontentloaded', timeout=10000)
            await self.page.wait_for_timeout(random.randint(self.delays['page_min'], self.delays['page_max']))
            
            if "/comments/" not in self.page.url:
                logging.info(f"点击后未跳转到正确页面: {self.page.url}")
                return
            
            # 模拟浏览
            for _ in range(random.randint(1, 3)):
                scroll_distance = random.randint(200, 5000)
                direction = 1 if random.random() < 0.8 else -1
                await self.page.evaluate(f"window.scrollBy(0, {scroll_distance * direction});")
                await self.page.wait_for_timeout(random.randint(self.delays['action_min'], self.delays['action_max']))
            
            # 返回列表页
            logging.info("返回列表页")
            await self.page.go_back()
            await self.page.wait_for_load_state('domcontentloaded')
            await self.page.wait_for_timeout(random.randint(self.delays['page_min'], self.delays['page_max']))
            
        except Exception as e:
            logging.info(f"模拟浏览行为时出错: {e}")
            try:
                if '/comments/' in self.page.url:
                    await self.page.go_back()
                    await self.page.wait_for_load_state('domcontentloaded')
            except:
                pass

    def save_progress(self, current_index, url_list):
        """保存爬取进度（完整保存，主要用于第一阶段）"""
        try:
            state_data = {
                "current_post_index": current_index,
                "collected_urls_with_source": url_list,
                "total_collected": len(url_list),
                "total_crawled_count": self.total_crawled_count,
                "subreddit_name": self.subreddit_name,
                "max_posts": self.max_posts,
                "sampling_ratios": self.sampling_ratios,
                "last_updated": datetime.datetime.now().isoformat()
            }
            
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logging.warning(f"保存进度失败: {e}")
    
    def update_progress_index(self, current_index):
        """只更新当前处理索引（轻量级更新，主要用于第二阶段）"""
        try:
            # 读取现有状态文件
            if not os.path.exists(self.state_file):
                logging.error("状态文件不存在，无法更新进度索引")
                raise FileNotFoundError("状态文件不存在，无法更新进度索引")
                
            with open(self.state_file, 'r', encoding='utf-8') as f:
                state_data = json.load(f)
            
            # 更新关键字段
            state_data["current_post_index"] = current_index
            state_data["total_crawled_count"] = self.total_crawled_count
            state_data["last_updated"] = datetime.datetime.now().isoformat()
            
            # 写回文件
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logging.error(f"更新进度索引失败: {e}")

    def load_progress(self):
        """加载爬取进度"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                
                current_index = state_data.get('current_post_index', 1)
                url_list = state_data.get('collected_urls_with_source', [])
                self.total_crawled_count = state_data.get('total_crawled_count', 0)
                
                if url_list and current_index <= len(url_list):
                    logging.info(f"恢复进度: {current_index}/{len(url_list)}，已爬取 {self.total_crawled_count} 条帖子")
                    return current_index, url_list
        except Exception:
            pass
        
        return 1, []

    def save_data(self, current_index):
        """保存数据到JSON文件"""
        try:
            # 如果没有新数据需要保存，直接返回
            if not self.all_posts_data:
                return
                
            # 读取现有数据
            existing_data = []
            if os.path.exists(self.output_file):
                try:
                    with open(self.output_file, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                except Exception:
                    pass
            
            # 合并数据
            all_data = existing_data + self.all_posts_data
            
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=4)
            
            # 更新总计数器
            self.total_crawled_count += len(self.all_posts_data)
            
            logging.info(f"保存数据成功，新增 {len(self.all_posts_data)} 条，文件总计 {len(all_data)} 条，本次运行已爬取 {self.total_crawled_count} 条帖子")
            
            # 清空已保存的数据，避免重复保存
            self.all_posts_data.clear()

            # 更新进度索引 - 使用轻量级方法
            self.update_progress_index(current_index)
            
        except Exception as e:
            logging.error(f"保存数据失败: {e}")

    async def collect_post_urls(self, target_url):
        """收集帖子URL - 两阶段爬取的第一阶段"""
        if "/comments/" in target_url:
            return [{"url": target_url, "source": "single_post"}]
        
        # 计算采样数量
        sampling_counts = {}
        remaining = self.max_posts
        sorted_types = self.sampling_ratios.keys()
        
        for i, source_type in enumerate(sorted_types):
            if i == len(sorted_types) - 1:
                sampling_counts[source_type] = remaining
            else:
                count = int(self.max_posts * self.sampling_ratios[source_type])
                sampling_counts[source_type] = count
                remaining -= count
        
        logging.info(f"采样计划: {sampling_counts}")
        
        # URL映射
        source_url_mapping = {
            "new": "/new/",
            "top_year": "/top/?t=year",
            "best": "/best/",
        }
        
        # 收集URL
        collected_urls = []
        seen_post_ids = set()
        base_url = target_url.rstrip('/')
        
        for source_type, count in sampling_counts.items():
            if count <= 0 or source_type not in source_url_mapping:
                continue
                
            url_suffix = source_url_mapping[source_type]
            sampling_url = base_url + url_suffix
            
            try:
                urls_from_source = await self._collect_from_source(
                    sampling_url, source_type, count, seen_post_ids
                )
                collected_urls.extend(urls_from_source)
                logging.info(f"收集类别：{source_type}: 收集到 {len(urls_from_source)} 个帖子")
            except Exception as e:
                logging.error(f"收集类别：{source_type} 收集失败: {e}")
        
        return collected_urls
    
    async def _collect_from_source(self, source_url, source_type, target_count, seen_post_ids):
        """从单个源收集URL"""
        collected = []
        
        await self.page.goto(source_url, wait_until='domcontentloaded', timeout=30000)
        await self.page.wait_for_timeout(random.randint(self.delays['page_min'], self.delays['page_max']))
        
        # 检查CAPTCHA或登录验证
        await self._check_and_handle_captcha_or_login(source_url)
        logging.info(f"收集类别：{source_type}: 开始收集帖子链接...")
        
        # 检查是否重定向到登录页
        if 'login' in self.page.url.lower():
            logging.error(f"{source_type}: 重定向到登录页")
            return []
        
        attempts = 0
        while len(collected) < target_count and attempts < 5:
            try:
                # links = await self.page.query_selector_all('a[href*="/comments/"]')
                links = await self.page.query_selector_all('a[href*="/comments/"]:not([data-testid*="ad"]):not([data-adtype])')
                new_found = 0
                
                for link in links:
                    href = await link.get_attribute("href")
                    logging.debug(f"发现链接: {href}")

                    if href and "/user/" not in href:
                        if href.startswith('/'):
                            href = "https://www.reddit.com" + href
                        
                        # 验证是否是完整的Reddit帖子链接格式
                        # 应该是: /r/subreddit/comments/post_id/post_title/ 格式
                        post_pattern = r'/r/[^/]+/comments/([a-zA-Z0-9]+)/[^/]+/?$'
                        post_match = re.search(post_pattern, href)
                        
                        if post_match:
                            post_id = post_match.group(1)
                            if post_id not in seen_post_ids:
                                logging.debug(f"新增帖子链接: {href}")
                                seen_post_ids.add(post_id)
                                collected.append({"url": href, "source": source_type})
                                new_found += 1
                                
                                if len(collected) >= target_count:
                                    break
                            else:
                                logging.debug(f"重复帖子，已跳过: {post_id}")
                        else:
                            logging.debug(f"链接格式不完整，跳过: {href}")
                logging.info(f"收集类别：{source_type}: 当前已收集 {len(collected)}/{target_count} 个帖子，新增 {new_found} 个")
                
                if new_found == 0:
                    attempts += 1
                else:
                    attempts = 0
                
                # 模拟人类浏览行为 - 有概率点击帖子浏览
                await self._simulate_human_browse_a_post_behavior()
                
                # 滚动到页面底部加载更多内容
                await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                await self.page.wait_for_timeout(random.randint(self.delays['scroll_min'], self.delays['scroll_max']))
                    
            except Exception as e:
                logging.error(f"收集类别：{source_type} 收集链接出错: {e}")
                attempts += 1
                await self.page.wait_for_timeout(random.randint(self.delays['action_min'], self.delays['action_max']))
        
        return collected[:target_count]

    async def fetch_post_json(self, post_url, source_type):
        """获取单个帖子的JSON数据 - 两阶段爬取的第二阶段"""
        try:
            # 构造JSON API URL
            base_url = post_url.split('?')[0]
            json_url = base_url.rstrip('/') + ".json"
            
            await self.page.goto(json_url, wait_until='domcontentloaded', timeout=15000)
            await self.page.wait_for_timeout(random.randint(self.delays['api_min'], self.delays['api_max']))
            
            # 检查CAPTCHA或登录验证
            await self._check_and_handle_captcha_or_login(json_url)
            
            # 获取JSON内容
            pre_element = await self.page.query_selector("pre")
            if not pre_element:
                return None
                
            json_content = await pre_element.text_content()
            if not json_content:
                return None
            
            # 解析JSON
            raw_data = json.loads(json_content)
            post_info = raw_data[0]['data']['children'][0]['data']
            comments_tree = raw_data[1]['data']['children']
            
            # 提取帖子数据
            post_data = {
                "post_id": self._extract_post_id(post_url),
                "url": post_info.get("url", post_url),
                "source_type": source_type,
                "subreddit": post_info.get("subreddit", ""),
                "title": post_info.get("title", "N/A"),
                "body": post_info.get("selftext", ""),
                "author": post_info.get("author", "[Deleted]"),
                "created_time": self._convert_time(post_info.get("created_utc", 0)),
                "score": post_info.get("score", 0),
                "upvote_ratio": post_info.get("upvote_ratio", 0.0),
                "num_comments": post_info.get("num_comments", 0),
                "num_crossposts": post_info.get("num_crossposts", 0),
                "total_awards_received": post_info.get("total_awards_received", 0),
                "pinned": post_info.get("pinned", False),
                "distinguished": post_info.get("distinguished", None),
                "flair_text": post_info.get("link_flair_text", ""),
                "content_categories": post_info.get("content_categories", []),
                "category": post_info.get("category", ""),
                "pwls": post_info.get("pwls", -1),
                "wls": post_info.get("wls", -1),
                "user_reports": post_info.get("user_reports", []),
                "mod_reports": post_info.get("mod_reports", []),
                "author_patreon_flair": post_info.get("author_patreon_flair", 0),
                "comments": []
            }
            
            # 解析评论
            for comment_node in comments_tree:
                parsed_comment = self._parse_comment(comment_node)
                if parsed_comment:
                    post_data["comments"].append(parsed_comment)
            
            return post_data
            
        except Exception as e:
            logging.error(f"获取帖子JSON失败: {e}")
            return None

    def _parse_comment(self, comment_data):
        """解析评论数据"""
        if comment_data.get('kind') == 'more':
            return None

        data = comment_data.get('data', {})
        utc_timestamp = data.get("created_utc", 0)
        
        parsed = {
            "author": data.get("author", "[Deleted]"),
            "text": data.get("body", "[无文本]"),
            "score": data.get("score", 0),
            "created_time": self._convert_time(utc_timestamp),
            "replies": [],
            "reply_count": 0
        }

        # 递归处理回复
        replies_raw = data.get("replies")
        if isinstance(replies_raw, dict):
            children = replies_raw.get('data', {}).get('children', [])
            for child in children:
                child_parsed = self._parse_comment(child)
                if child_parsed:
                    parsed["replies"].append(child_parsed)

        parsed["reply_count"] = len(parsed["replies"])
        return parsed

    def _convert_time(self, timestamp):
        """转换时间戳为可读格式"""
        if not timestamp:
            return "N/A"
        try:
            return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return "N/A"

    def _extract_post_id(self, url):
        """提取帖子ID"""
        match = re.search(r'/comments/([a-zA-Z0-9]+)/', url)
        return match.group(1) if match else None

    async def crawl_posts(self):
        """主要爬取流程"""
        completed_normally = False
        
        try:
            await self.init_browser()
            
            # 加载进度
            current_index, existing_urls = self.load_progress()
            
            if existing_urls:
                url_list = existing_urls
            else:
                # 第一阶段：收集帖子URL
                url_list = await self.collect_post_urls(self.subreddit_url)
                if not url_list:
                    logging.error("未收集到任何帖子链接")
                    return
                
                # 第一阶段完成后，保存进度（从index=1开始处理）
                self.save_progress(1, url_list)
                logging.info(f"第一阶段完成，已收集 {len(url_list)} 个帖子URL，进度已保存")
            
            total_posts = len(url_list)
            logging.info(f"开始爬取 {total_posts} 个帖子，从第 {current_index} 个开始")
            
            # 第二阶段：遍历帖子获取详细数据
            consecutive_failures = 0
            
            for index in range(current_index - 1, total_posts):
                url_item = url_list[index]
                url = url_item["url"]
                source_type = url_item["source"]
                current_index = index + 1
                
                logging.info(f"[{current_index}/{total_posts}] 处理[{source_type}]: {url}")
                                
                try:
                    post_data = await self.fetch_post_json(url, source_type)
                    
                    if post_data:
                        self.all_posts_data.append(post_data)
                        consecutive_failures = 0
                        # 每10个帖子保存一次数据，传递当前进度信息
                        if len(self.all_posts_data) % 10 == 0:
                            self.save_data(current_index)
                    else:
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            logging.error("连续失败过多，停止爬取")
                            break
                    
                    # 延迟
                    await self.page.wait_for_timeout(random.randint(self.delays['action_min'], self.delays['action_max']))
                    
                except Exception as e:
                    consecutive_failures += 1
                    logging.error(f"处理帖子出错: {e}")
                    
                    if consecutive_failures >= 3:
                        break
                    
                    await self.page.wait_for_timeout(random.randint(self.delays['action_min'], self.delays['action_max']))
            
            # 检查是否正常完成
            if current_index >= total_posts:
                completed_normally = True
                logging.info("所有帖子处理完成")
            
        except KeyboardInterrupt:
            logging.info("用户中断，进度已保存")
        except Exception as e:
            logging.error(f"爬取过程出错: {e}")
        finally:
            # 保存最终数据
            if self.all_posts_data:
                self.save_data()
            
            # 显示最终统计
            logging.info(f"爬取结束，本次运行总共爬取了 {self.total_crawled_count} 条帖子")
            
            # 正常完成时清理状态文件
            if completed_normally:
                try:
                    os.remove(self.state_file)
                    logging.info("任务完成，已清理状态文件")
                except:
                    pass
            
            await self.cleanup()

    async def cleanup(self):
        """清理资源"""
        try:
            if self.page:
                await self.page.close()
            if self.browser:
                await self.browser.close()
            if self.playwright:
                await self.playwright.stop()
        except Exception:
            pass

async def main():
    """主函数"""
    setup_logger()
    
    # 配置参数
    target_url = "https://www.reddit.com/r/dogs/"
    max_posts = 5000
    headless = False
    use_system_browser = 'chrome'  # 'chrome', 'edge', 或 None
    custom_sampling_ratios = {
        'new': 0.65,
        'top_year': 0.25,
        'best': 0.1
    }
    
    crawler = RedditCrawler(
        subreddit_url=target_url,
        max_posts=max_posts,
        headless=headless,
        use_system_browser=use_system_browser,
        sampling_ratios=custom_sampling_ratios,
        delays={
            'page_min': 3000, 'page_max': 5000,
            'action_min': 3000, 'action_max': 8000,
            'scroll_min': 5000, 'scroll_max': 10000,
            'api_min': 2000, 'api_max': 4000
        }
    )
    
    await crawler.crawl_posts()
    logging.info("Reddit爬取完成！")

if __name__ == "__main__":
    asyncio.run(main())