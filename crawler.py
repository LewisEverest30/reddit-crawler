# coding=utf-8
import json
import logging
import random
import time
import asyncio
import os
from pathlib import Path
from playwright.async_api import async_playwright
import datetime
import traceback
from urllib.parse import urlparse, parse_qs

def setup_logger():
    """设置日志记录"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("reddit_crawler_playwright.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

class RedditCrawler:
    def __init__(self, subreddit_url, output_file="reddit_data_with_time.json", 
                 max_posts=1000, headless=False, max_failures=3, 
                 delays=None, viewport=None, user_agent=None, 
                 user_data_dir=None, resume_from_post=1, sampling_ratios=None):
        """
        初始化Reddit爬虫
        
        Args:
            subreddit_url: Reddit子版块URL或具体帖子URL
            output_file: 输出JSON文件名
            max_posts: 最大爬取帖子数量
            headless: 是否无头模式
            max_failures: 最大连续失败次数
            delays: 延迟配置字典
            viewport: 视窗大小配置
            user_agent: 用户代理
            user_data_dir: 浏览器用户数据目录
            resume_from_post: 从第几个帖子开始爬取
            sampling_ratios: 采样比例配置字典，例如 {'new': 0.65, 'top_year': 0.25, 'best': 0.10}
        """
        self.subreddit_url = subreddit_url
        self.output_file = output_file
        self.max_posts = max_posts
        self.headless = headless
        self.max_failures = max_failures
        self.resume_from_post = resume_from_post
        self.user_data_dir = user_data_dir or "./reddit_browser_data"
        
        # 配置延迟策略
        self.delays = delays or {
            'page_min': 2000, 'page_max': 5000,
            'action_min': 500, 'action_max': 1500,
            'scroll_min': 1000, 'scroll_max': 3000,
            'api_min': 1000, 'api_max': 2000
        }
        
        # 配置采样比例策略
        self.sampling_ratios = sampling_ratios or {
            'new': 0.65,        # 65% - 最新帖子
            'top_year': 0.25,   # 25% - 年度热门
            'best': 0.10        # 10% - 最佳帖子
        }
        
        # 验证采样比例总和
        total_ratio = sum(self.sampling_ratios.values())
        if abs(total_ratio - 1.0) > 0.01:  # 允许小的浮点误差
            logging.warning(f"采样比例总和为 {total_ratio:.3f}，不等于1.0，将自动归一化")
            # 归一化比例
            for key in self.sampling_ratios:
                self.sampling_ratios[key] /= total_ratio
        
        logging.info(f"采样比例配置: {self.sampling_ratios}")
        
        # 配置浏览器参数
        self.viewport = viewport or {'width': 1920, 'height': 1080}
        self.user_agent = user_agent or 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        
        # 提取subreddit名称并创建对应目录
        self.subreddit_name = self.extract_subreddit_name(subreddit_url)
        self.subreddit_dir = f"./outputs/{self.subreddit_name}"
        
        # 为该subreddit创建专用目录
        Path(self.subreddit_dir).mkdir(parents=True, exist_ok=True)
        
        # 进度状态文件（合并进度和URL列表）
        self.state_file = os.path.join(self.subreddit_dir, "reddit_crawler_state.json")
        
        # 如果输出文件没有指定路径，也保存在subreddit目录下
        if not os.path.dirname(output_file):
            self.output_file = os.path.join(self.subreddit_dir, output_file)
        else:
            self.output_file = output_file
        
        # 存储爬取的数据
        self.all_posts_data = []
        self.collected_urls = set()
        
        # 创建用户数据目录
        Path(self.user_data_dir).mkdir(exist_ok=True)
        
        # 记录当前配置
        logging.info(f"Subreddit: {self.subreddit_name}")
        logging.info(f"数据目录: {self.subreddit_dir}")
        logging.info(f"输出文件: {self.output_file}")
        
        # 初始化playwright相关变量
        self.browser = None
        self.context = None
        self.page = None

    def extract_subreddit_name(self, url):
        """从Reddit URL中提取subreddit名称"""
        try:
            import re
            # 匹配 /r/subreddit_name/ 格式
            match = re.search(r'/r/([^/]+)', url)
            if match:
                return match.group(1)
            # 如果是帖子详情页，也尝试提取
            if '/comments/' in url:
                parts = url.split('/r/')
                if len(parts) > 1:
                    subreddit_part = parts[1].split('/')[0]
                    return subreddit_part
            return "unknown_subreddit"
        except Exception as e:
            logging.warning(f"提取subreddit名称失败: {e}")
            return "unknown_subreddit"
    
    def extract_post_id(self, url):
        """从Reddit URL中提取帖子ID用于去重"""
        try:
            import re
            match = re.search(r'/comments/([a-zA-Z0-9]+)/', url)
            return match.group(1) if match else None
        except Exception as e:
            logging.debug(f"提取帖子ID失败: {e}")
            return None

    def convert_time(self, timestamp):
        """将UTC时间戳转换为可读时间格式"""
        if not timestamp:
            return "N/A"
        try:
            return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except:
            return "N/A"

    def parse_json_comment(self, comment_data):
        """
        递归解析Reddit API返回的评论JSON数据
        
        Args:
            comment_data: Reddit API返回的评论数据字典
            
        Returns:
            dict: 解析后的评论数据
        """
        if comment_data.get('kind') == 'more':
            return None

        data = comment_data.get('data', {})
        
        # 获取时间戳
        utc_timestamp = data.get("created_utc", 0)
        
        # 提取核心内容
        parsed = {
            "author": data.get("author", "[Deleted]"),
            "text": data.get("body", "[无文本]"),
            "votes": data.get("score", 0),
            "created_utc": utc_timestamp,
            "created_time": self.convert_time(utc_timestamp),
            "replies": [],
            "reply_count": 0
        }

        # 递归处理回复
        replies_raw = data.get("replies")
        
        if isinstance(replies_raw, dict):
            children = replies_raw.get('data', {}).get('children', [])
            for child in children:
                child_parsed = self.parse_json_comment(child)
                if child_parsed:
                    parsed["replies"].append(child_parsed)

        # 计算直接子回复的数量
        parsed["reply_count"] = len(parsed["replies"])

        return parsed

    async def init_browser(self):
        """初始化浏览器和页面"""
        try:
            logging.info("正在初始化浏览器...")
            self.playwright = await async_playwright().start()
            
            # 启动浏览器，使用用户数据目录持久化状态
            self.browser = await self.playwright.chromium.launch_persistent_context(
                user_data_dir=self.user_data_dir,
                headless=self.headless,
                user_agent=self.user_agent,
                viewport=self.viewport,
                locale='en-US',
                timezone_id='America/New_York',
                args=[
                    '--no-sandbox',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--disable-dev-shm-usage',
                    '--no-first-run',
                    '--disable-notifications'
                ]
            )
            
            self.context = self.browser
            
            # 隐藏webdriver特征
            await self.context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined,
                });
                
                // 删除webdriver标识
                delete navigator.__proto__.webdriver;
                
                // 修改插件信息
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                
                // 修改语言信息
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en'],
                });
            """)
            
            # 创建新页面
            self.page = await self.context.new_page()
            
            # 设置额外的请求头
            await self.page.set_extra_http_headers({
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1'
            })
            
            logging.info("✅ 浏览器初始化成功")
            
        except Exception as e:
            logging.error(f"浏览器初始化失败: {e}")
            raise

    async def simulate_human_behavior(self):
        """模拟人类浏览行为"""
        try:
            # 随机鼠标移动
            await self.page.mouse.move(
                random.randint(100, self.viewport['width']-100), 
                random.randint(100, self.viewport['height']-100)
            )
            await self.page.wait_for_timeout(random.randint(100, 500))
            
            # 随机滚动
            scroll_distance = random.randint(200, 800)
            await self.page.mouse.wheel(0, scroll_distance)
            await self.page.wait_for_timeout(random.randint(500, 1000))
            
            # 模拟阅读时间
            reading_time = random.randint(1000, 3000)
            await self.page.wait_for_timeout(reading_time)
            
        except Exception as e:
            logging.warning(f"模拟人类行为时出现异常: {e}")

    def save_progress(self, current_post_index, collected_urls_with_source):
        """保存当前爬虫状态（进度+URL列表+来源信息）"""
        try:
            state_data = {
                "current_post_index": current_post_index,
                "collected_urls_with_source": collected_urls_with_source,  # [{"url": url, "source": source}]
                "total_collected": len(collected_urls_with_source),
                "subreddit_name": self.subreddit_name,
                "max_posts": self.max_posts,
                "sampling_ratios": self.sampling_ratios,  # 保存采样配置
                "last_updated": datetime.datetime.now().isoformat(),
                "version": "1.0"
            }
            
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=2)
                
            logging.info(f"已保存爬虫状态: 第 {current_post_index}/{len(collected_urls_with_source)} 个帖子")
            
        except Exception as e:
            logging.warning(f"保存爬虫状态失败: {e}")

    def load_progress(self):
        """加载爬虫状态（进度+URL列表+来源信息）"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                
                current_index = state_data.get('current_post_index', self.resume_from_post)
                last_updated = state_data.get('last_updated', 'Unknown')
                
                # 获取URL列表
                collected_urls_with_source = state_data.get('collected_urls_with_source', [])
                
                # 验证状态数据的完整性
                if len(collected_urls_with_source) == 0:
                    logging.warning("状态文件中URL列表为空，将重新收集")
                    return self.resume_from_post, []
                
                # 验证当前index是否有效
                if current_index > len(collected_urls_with_source):
                    logging.warning(f"进度索引({current_index})超出URL列表长度({len(collected_urls_with_source)})，重置为1")
                    current_index = 1
                
                # 恢复采样配置（如果状态文件中有保存）
                if 'sampling_ratios' in state_data:
                    saved_ratios = state_data['sampling_ratios']
                    if saved_ratios != self.sampling_ratios:
                        logging.info(f"状态文件中的采样比例: {saved_ratios}")
                        logging.info(f"当前配置的采样比例: {self.sampling_ratios}")
                        logging.info("使用当前配置的采样比例继续爬取")
                
                logging.info(f"读取到爬虫状态 - 进度: {current_index}/{len(collected_urls_with_source)}, 更新时间: {last_updated}")
                return max(current_index, self.resume_from_post), collected_urls_with_source
            else:
                return self.resume_from_post, []
                
        except Exception as e:
            logging.warning(f"加载爬虫状态失败: {e}")
            return self.resume_from_post, []

    def save_data(self):
        """保存数据到JSON文件"""
        try:
            logging.info(f"正在保存 {len(self.all_posts_data)} 条数据到 {self.output_file}...")
            
            # 如果文件已存在，先读取现有数据
            existing_data = []
            if os.path.exists(self.output_file):
                try:
                    with open(self.output_file, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                    logging.info(f"读取到现有数据 {len(existing_data)} 条")
                except Exception as e:
                    logging.warning(f"读取现有数据失败: {e}")
            
            # 合并数据（避免重复）
            all_data = existing_data + self.all_posts_data
            
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(all_data, f, ensure_ascii=False, indent=4)
                
            logging.info(f"✅ 数据保存成功，总计 {len(all_data)} 条帖子")
            
        except Exception as e:
            logging.error(f"保存数据失败: {e}")

    async def collect_post_urls(self, target_url):
        """收集帖子URL链接 - 支持非均匀采样"""
        logging.info("开始收集帖子链接（非均匀采样模式）...")
        
        # 判断是详情页还是列表页
        if "/comments/" in target_url:
            logging.info("检测到当前为帖子详情页，只处理这一个帖子")
            return [{"url": target_url, "source": "single_post"}]
        
        # 计算各种排序方式需要收集的帖子数量（基于配置的比例）
        sampling_counts = {}
        remaining_count = self.max_posts
        
        # 按配置比例计算每种类型的数量
        sorted_types = sorted(self.sampling_ratios.keys())  # 确保顺序一致
        for i, source_type in enumerate(sorted_types):
            if i == len(sorted_types) - 1:  # 最后一个类型取剩余的全部
                sampling_counts[source_type] = remaining_count
            else:
                count = int(self.max_posts * self.sampling_ratios[source_type])
                sampling_counts[source_type] = count
                remaining_count -= count
        
        # 打印采样计划
        plan_parts = []
        for source_type, count in sampling_counts.items():
            percentage = (count / self.max_posts) * 100
            plan_parts.append(f"{source_type}({count}, {percentage:.1f}%)")
        logging.info(f"采样计划: {' + '.join(plan_parts)} = {self.max_posts}")
        
        # 存储结果：[{"url": url, "source": source_type}]
        collected_urls_with_source = []
        seen_post_ids = set()  # 用于去重
        
        # 构建基础URL（移除可能的路径后缀）
        base_subreddit_url = target_url.rstrip('/')
        if base_subreddit_url.endswith(('/hot', '/new', '/top', '/best')):
            base_subreddit_url = '/'.join(base_subreddit_url.split('/')[:-1])
        
        # 采样配置：映射source_type到URL后缀
        source_url_mapping = {
            "new": "/new/",
            "top_year": "/top/?t=year",
            "best": "/best/",
            "hot": "/hot/",  # 支持更多类型
            "rising": "/rising/"
        }
        
        # 构建实际的采样配置列表
        sampling_configs = []
        for source_type, count in sampling_counts.items():
            if source_type in source_url_mapping and count > 0:
                url_suffix = source_url_mapping[source_type]
                sampling_configs.append((source_type, url_suffix, count))
            else:
                logging.warning(f"未知的采样类型或数量为0: {source_type}({count})")
        
        for source_type, url_suffix, target_count in sampling_configs:
            if target_count <= 0:
                continue
                
            sampling_url = base_subreddit_url + url_suffix
            logging.info(f"开始从 {source_type} 收集 {target_count} 个帖子: {sampling_url}")
            
            try:
                collected_from_source = await self._collect_from_single_source(
                    sampling_url, source_type, target_count, seen_post_ids
                )
                collected_urls_with_source.extend(collected_from_source)
                
                logging.info(f"从 {source_type} 成功收集到 {len(collected_from_source)} 个帖子")
                
            except Exception as e:
                logging.error(f"从 {source_type} 收集帖子时出错: {e}")
                continue
        
        logging.info(f"🎯 非均匀采样完成，总共收集到 {len(collected_urls_with_source)} 个唯一帖子")
        
        # 打印采样统计
        source_stats = {}
        for item in collected_urls_with_source:
            source = item["source"]
            source_stats[source] = source_stats.get(source, 0) + 1
        
        for source, count in source_stats.items():
            percentage = (count / len(collected_urls_with_source)) * 100 if collected_urls_with_source else 0
            logging.info(f"  {source}: {count} 个帖子 ({percentage:.1f}%)")
        
        return collected_urls_with_source
    
    async def _collect_from_single_source(self, source_url, source_type, target_count, seen_post_ids):
        """从单个排序页面收集指定数量的帖子URL"""
        collected_urls = []
        
        try:
            # 访问目标页面
            await self.page.goto(source_url, wait_until='domcontentloaded', timeout=30000)
            await self.page.wait_for_timeout(random.randint(2000, 4000))
            
            no_new_data_count = 0
            scroll_count = 0
            
            while len(collected_urls) < target_count and no_new_data_count < 3:
                # 模拟人类行为
                await self.simulate_human_behavior()
                
                # 提取当前可见的所有帖子链接
                try:
                    links = await self.page.query_selector_all('a[href*="/comments/"]')
                    new_found_count = 0
                    
                    for link in links:
                        try:
                            href = await link.get_attribute("href")
                            if href and "/user/" not in href:
                                # 转换相对路径为绝对路径
                                if href.startswith('/'):
                                    href = "https://www.reddit.com" + href
                                
                                # 提取帖子ID进行去重
                                post_id = self.extract_post_id(href)
                                if post_id and post_id not in seen_post_ids:
                                    seen_post_ids.add(post_id)
                                    collected_urls.append({"url": href, "source": source_type})
                                    new_found_count += 1
                                    
                                    if len(collected_urls) >= target_count:
                                        break
                                        
                        except Exception as e:
                            logging.debug(f"处理链接时出错: {e}")
                            continue
                    
                    current_count = len(collected_urls)
                    logging.info(f"  {source_type}: {current_count}/{target_count} (本轮新增: {new_found_count})")
                    
                    # 检查是否获取到新数据
                    if new_found_count == 0:
                        no_new_data_count += 1
                    else:
                        no_new_data_count = 0
                    
                    # 如果已经收集够了，就停止
                    if current_count >= target_count:
                        break
                    
                    # 滚动页面加载更多内容
                    scroll_count += 1
                    await self.page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    
                    # 等待新内容加载
                    scroll_delay = random.randint(self.delays['scroll_min'], self.delays['scroll_max'])
                    await self.page.wait_for_timeout(scroll_delay)
                        
                except Exception as e:
                    logging.error(f"{source_type}: 收集链接时出错: {e}")
                    no_new_data_count += 1
                    await self.page.wait_for_timeout(2000)
            
            return collected_urls[:target_count]  # 确保不超过目标数量
            
        except Exception as e:
            logging.error(f"从 {source_type} 收集帖子时发生错误: {e}")
            return collected_urls

    async def fetch_post_json(self, post_url, source_type):
        """获取单个帖子的JSON数据"""
        try:
            # 构造JSON API URL
            base_url = post_url.split('?')[0]
            if base_url.endswith('/'):
                json_url = base_url[:-1] + ".json"
            else:
                json_url = base_url + ".json"
            
            logging.debug(f"请求API: {json_url}")
            
            # 访问JSON API
            await self.page.goto(json_url, wait_until='domcontentloaded', timeout=15000)
            
            # 等待随机时间
            api_delay = random.randint(self.delays['api_min'], self.delays['api_max'])
            await self.page.wait_for_timeout(api_delay)
            
            # 获取JSON内容
            pre_element = await self.page.query_selector("pre")
            if not pre_element:
                logging.warning("未找到JSON内容")
                return None
                
            json_content = await pre_element.text_content()
            if not json_content:
                logging.warning("JSON内容为空")
                return None
            
            # 解析JSON
            raw_data = json.loads(json_content)
            
            # 提取帖子信息
            post_info_raw = raw_data[0]['data']['children'][0]['data']
            comments_tree_raw = raw_data[1]['data']['children']
            
            # 获取帖子时间戳
            post_utc = post_info_raw.get("created_utc", 0)

            post_data = {
                "title": post_info_raw.get("title", "N/A"),
                "url": post_info_raw.get("url", post_url),
                "body": post_info_raw.get("selftext", ""),
                "upvotes": post_info_raw.get("score", 0),
                "created_utc": post_utc,
                "created_time": self.convert_time(post_utc),
                "total_comments_count": post_info_raw.get("num_comments", 0),
                "source_type": source_type,  # 新增：标记来源类型
                "post_id": self.extract_post_id(post_url),  # 新增：帖子ID
                "comments": []
            }
            
            logging.info(f"解析帖子[{source_type}]: {post_data['title'][:50]}...")
            logging.info(f"发布时间: {post_data['created_time']}")
            
            # 解析评论
            for comment_node in comments_tree_raw:
                parsed_node = self.parse_json_comment(comment_node)
                if parsed_node:
                    post_data["comments"].append(parsed_node)
            
            logging.info(f"解析完成，包含 {len(post_data['comments'])} 条一级评论")
            return post_data
            
        except json.JSONDecodeError as e:
            logging.error(f"JSON解析失败: {e}")
            return None
        except Exception as e:
            logging.error(f"获取帖子数据时出错: {e}")
            return None

    async def crawl_posts(self):
        """主要的爬取流程"""
        completed_normally = False
        current_post_index = 1
        consecutive_failures = 0
        collected_urls_with_source = []
        
        try:
            await self.init_browser()
            
            # 加载进度
            current_post_index, existing_urls_with_source = self.load_progress()
            
            if existing_urls_with_source:
                collected_urls_with_source = existing_urls_with_source
                logging.info(f"从进度文件恢复，已有 {len(collected_urls_with_source)} 个URL")
            else:
                # 收集帖子链接
                collected_urls_with_source = await self.collect_post_urls(self.subreddit_url)
            
            if not collected_urls_with_source:
                logging.error("没有收集到任何帖子链接")
                return
            
            # 开始爬取帖子数据
            total_posts = len(collected_urls_with_source)
            logging.info(f"开始爬取 {total_posts} 个帖子，从第 {current_post_index} 个开始")
            
            for index in range(current_post_index - 1, total_posts):
                url_item = collected_urls_with_source[index]
                url = url_item["url"]
                source_type = url_item["source"]
                current_post_index = index + 1
                
                logging.info(f"\n[{current_post_index}/{total_posts}] 正在处理[{source_type}]: {url}")
                
                # 保存进度
                self.save_progress(current_post_index, collected_urls_with_source)
                
                try:
                    post_data = await self.fetch_post_json(url, source_type)
                    
                    if post_data:
                        self.all_posts_data.append(post_data)
                        consecutive_failures = 0  # 重置失败计数
                        logging.info("✅ 帖子处理成功")
                        
                        # 定期保存数据
                        if len(self.all_posts_data) % 10 == 0:
                            self.save_data()
                            
                    else:
                        consecutive_failures += 1
                        logging.warning(f"❌ 帖子处理失败，连续失败次数: {consecutive_failures}")
                        
                        if consecutive_failures >= self.max_failures:
                            logging.error("连续失败次数过多，停止爬取")
                            break
                    
                    # 随机延迟
                    delay = random.randint(self.delays['page_min'], self.delays['page_max'])
                    logging.info(f"等待 {delay/1000:.1f} 秒后继续...")
                    await self.page.wait_for_timeout(delay)
                    
                except Exception as e:
                    consecutive_failures += 1
                    logging.error(f"处理帖子时发生错误: {e}")
                    traceback.print_exc()
                    
                    if consecutive_failures >= self.max_failures:
                        logging.error("连续失败次数过多，停止爬取")
                        break
                    
                    # 错误后等待更长时间
                    await self.page.wait_for_timeout(random.randint(5000, 10000))
            
            # 如果完整处理了所有帖子，标记为正常完成
            if current_post_index >= total_posts:
                completed_normally = True
                logging.info("🎉 所有帖子处理完成")
            
        except KeyboardInterrupt:
            logging.info("用户中断爬取，进度已保存")
            self.save_progress(current_post_index, collected_urls_with_source)
        except Exception as e:
            logging.error(f"爬取过程中发生错误: {e}")
            traceback.print_exc()
            self.save_progress(current_post_index, collected_urls_with_source)
        finally:
            # 保存最终数据
            if self.all_posts_data:
                self.save_data()
            
            # 只有在正常完成时才清理状态文件
            if completed_normally:
                try:
                    if os.path.exists(self.state_file):
                        os.remove(self.state_file)
                    logging.info("爬取任务完成，已清理状态文件")
                except:
                    pass
            else:
                logging.info("保留状态文件以便下次继续爬取")
            
            await self.cleanup()

    async def cleanup(self):
        """清理资源"""
        try:
            if self.page:
                await self.page.close()
            if self.browser:
                await self.browser.close()
            if hasattr(self, 'playwright'):
                await self.playwright.stop()
            logging.info("资源清理完成")
        except Exception as e:
            logging.warning(f"清理资源时发生错误: {e}")

async def main():
    """主函数"""
    setup_logger()
    
    # ================= 配置区域 =================
    # 替换为你想爬取的Reddit子版块URL或具体帖子URL
    target_url = "https://www.reddit.com/r/dogs/"  # 示例：dogs子版块
    
    # 输出文件名（将自动保存到 ./data/dogs/ 目录下）
    output_file = "reddit_data_with_time.json"
    
    # 最大爬取帖子数量
    max_posts = 100
    
    # 是否使用无头模式（建议调试时设为False）
    headless = False
    
    # 自定义采样比例（可选，默认为65%新/25%热门/10%最佳）
    custom_sampling_ratios = {
        'new': 0.6,       # 60% 最新帖子
        'top_year': 0.3,  # 30% 年度热门
        'best': 0.1       # 10% 最佳帖子
    }
    
    # 创建爬虫实例
    crawler = RedditCrawler(
        subreddit_url=target_url,
        output_file=output_file,
        max_posts=max_posts,
        headless=headless,
        max_failures=3,
        sampling_ratios=custom_sampling_ratios,  # 传入自定义采样比例
        delays={
            'page_min': 2000, 'page_max': 5000,
            'action_min': 500, 'action_max': 1500,
            'scroll_min': 3000, 'scroll_max': 8000,  # Reddit加载比较慢，增加滚动延迟
            'api_min': 1000, 'api_max': 2000
        }
    )
    
    # 开始爬取
    await crawler.crawl_posts()
    
    logging.info("🎉 Reddit爬取完成！")

if __name__ == "__main__":
    asyncio.run(main())