# coding=utf-8
"""
Reddit爬虫 - 第二阶段:使用PullPush API获取帖子详细内容
"""
import json
import logging
import random
import asyncio
import os
import re
import datetime
import time
import sqlite3
import requests
from pathlib import Path


def setup_logger(debug=False):
    level = logging.DEBUG if debug else logging.INFO
    format_str = '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    
    logging.basicConfig(
        level=level,
        format=format_str,
        handlers=[
            logging.FileHandler("reddit_crawler_stage2.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


class PostCrawler:
    """第二阶段：使用PullPush API获取帖子详细内容"""

    def __init__(self, subreddit_url, delays=None,
                 start_index=None, end_index=None):

        self.subreddit_url = subreddit_url
        
        # 区间爬取参数（0-based索引，包含边界）
        self.start_index = start_index
        self.end_index = end_index
        
        # API请求延迟配置（毫秒）
        self.delays = delays or {
            'api_min': 100,
            'api_max': 500,
            'retry_min': 1000,
            'retry_max': 3000
        }
        
        # 提取subreddit名称并创建目录
        self.subreddit_name = self._extract_subreddit_name(subreddit_url)
        self.subreddit_dir = f".\\outputs\\{self.subreddit_name}"
        Path(self.subreddit_dir).mkdir(parents=True, exist_ok=True)
        
        # URL列表文件（第一阶段生成，第二阶段只读）
        self.urls_file = os.path.join(self.subreddit_dir, f"{self.subreddit_name}_urls.json")
        
        # 结果文件路径和进度文件路径（根据区间动态设置，在load_url_list后确定）
        self.output_file = None
        self.progress_file = None
        
        # 存储数据
        self.all_posts_data = []
        self.total_crawled_count = 0
        
        # SQLite数据库路径（全局唯一）
        self.db_path = "./outputs/reddit_posts.sqlite"
        Path("./outputs").mkdir(parents=True, exist_ok=True)
        self._init_database()

    def _init_database(self):
        """初始化SQLite数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='posts'")
        table_exists = cursor.fetchone() is not None
        
        if not table_exists:
            # 创建新表
            cursor.execute('''
                CREATE TABLE posts (
                    index_in_list INTEGER,
                    post_id TEXT PRIMARY KEY,
                    subreddit TEXT,
                    collect_source TEXT,
                    url TEXT,
                    title TEXT,
                    body TEXT,
                    author TEXT,
                    created_time TEXT,
                    score INTEGER,
                    upvote_ratio REAL,
                    num_comments INTEGER,
                    num_crossposts INTEGER,
                    num_comments_filtered INTEGER,
                    total_awards_received INTEGER,
                    pinned INTEGER,
                    distinguished TEXT,
                    flair_text TEXT,
                    content_categories TEXT,
                    category TEXT,
                    pwls INTEGER,
                    wls INTEGER,
                    user_reports TEXT,
                    mod_reports TEXT,
                    author_patreon_flair INTEGER,
                    comments TEXT,
                    crawled_at TEXT,
                    is_valid INTEGER DEFAULT 1,
                    llm_analyze_result TEXT
                )
            ''')
            logging.info("创建新的posts表")
        else:
            # 检查并添加缺失的列
            cursor.execute("PRAGMA table_info(posts)")
            existing_columns = [col[1] for col in cursor.fetchall()]
            
            # 添加index_in_list列（如果不存在）
            if 'index_in_list' not in existing_columns:
                try:
                    cursor.execute('ALTER TABLE posts ADD COLUMN index_in_list INTEGER')
                    logging.info("添加index_in_list列到posts表")
                except sqlite3.OperationalError as e:
                    logging.warning(f"添加index_in_list列失败: {e}")
            
            # 添加is_valid列（如果不存在）
            if 'is_valid' not in existing_columns:
                try:
                    cursor.execute('ALTER TABLE posts ADD COLUMN is_valid INTEGER DEFAULT 1')
                    logging.info("添加is_valid列到posts表")
                except sqlite3.OperationalError as e:
                    logging.warning(f"添加is_valid列失败: {e}")
            
            # 添加llm_analyze_result列（如果不存在）
            if 'llm_analyze_result' not in existing_columns:
                try:
                    cursor.execute('ALTER TABLE posts ADD COLUMN llm_analyze_result TEXT')
                    logging.info("添加llm_analyze_result列到posts表")
                except sqlite3.OperationalError as e:
                    logging.warning(f"添加llm_analyze_result列失败: {e}")
        
        # 创建索引以提高查询效率
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_subreddit ON posts(subreddit)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_collect_source ON posts(collect_source)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_index_in_list ON posts(index_in_list)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_valid ON posts(is_valid)')
        
        conn.commit()
        conn.close()
        logging.info(f"SQLite数据库初始化完成: {self.db_path}")

    def _extract_subreddit_name(self, url):
        """从Reddit URL中提取subreddit名称"""
        match = re.search(r'/r/([^/]+)', url)
        return match.group(1) if match else "unknown_subreddit"

    def _atomic_write_json(self, file_path, data, indent=2):
        """原子写入JSON文件，避免中断时文件被截断"""
        temp_file = file_path + ".tmp"
        try:
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=indent)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file, file_path)
        except Exception as e:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass
            raise e

    def load_url_list(self):
        """加载URL列表（第一阶段生成的索引文件，只读）并设置区间"""
        if not os.path.exists(self.urls_file):
            logging.error(f"URL索引文件不存在: {self.urls_file}")
            logging.error("请先运行第一阶段收集URL")
            return None
        
        try:
            with open(self.urls_file, 'r', encoding='utf-8') as f:
                urls_data = json.load(f)
            
            if not urls_data.get('is_complete', False):
                logging.error("URL收集未完成，请先完成第一阶段")
                return None
            
            url_list = urls_data.get('collected_urls', [])
            if not url_list:
                logging.error("URL列表为空")
                return None
            
            total_count = len(url_list)
            
            # 设置区间边界（0-based索引）
            if self.start_index is None:
                self.start_index = 0
            if self.end_index is None:
                self.end_index = total_count - 1
            
            # 边界检查
            if self.start_index < 0:
                self.start_index = 0
            if self.end_index >= total_count:
                self.end_index = total_count - 1
            if self.start_index > self.end_index:
                logging.error(f"无效区间: start={self.start_index}, end={self.end_index}")
                return None
            
            # 设置区间相关的文件路径
            self._setup_range_files()
            
            logging.info(f"已加载URL索引: 共 {total_count} 个帖子，本次爬取区间 [{self.start_index}, {self.end_index}]")
            return url_list
            
        except Exception as e:
            logging.error(f"读取URL索引文件失败: {e}")
            return None
    
    def _setup_range_files(self):
        """根据区间设置结果文件和进度文件路径"""
        range_suffix = f"{self.start_index}_{self.end_index}"
        self.output_file = os.path.join(
            self.subreddit_dir, 
            f"{self.subreddit_name}_data_{range_suffix}.json"
        )
        self.progress_file = os.path.join(
            self.subreddit_dir, 
            f"{self.subreddit_name}_crawl_progress_{range_suffix}.json"
        )

    def _get_crawled_indexes_from_db(self):
        """从数据库查询当前subreddit在指定区间内已爬取的所有index"""
        crawled_indexes = set()
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 查询该subreddit在区间内已爬取的index_in_list
            cursor.execute('''
                SELECT index_in_list FROM posts 
                WHERE subreddit = ? 
                AND index_in_list >= ? 
                AND index_in_list <= ?
            ''', (self.subreddit_name, self.start_index, self.end_index))
            
            rows = cursor.fetchall()
            crawled_indexes = {row[0] for row in rows if row[0] is not None}
            
            conn.close()
            logging.info(f"数据库中区间 [{self.start_index}, {self.end_index}] 已爬取 {len(crawled_indexes)} 条记录")
            
        except Exception as e:
            logging.warning(f"查询数据库已爬取记录失败: {e}")
        
        return crawled_indexes

    def load_crawl_progress(self):
        """加载爬取进度，以数据库实际爬取情况为准，返回未爬取的index列表"""
        # 从数据库获取已爬取的index集合
        crawled_indexes = self._get_crawled_indexes_from_db()
        
        # 计算区间内所有需要爬取的index
        all_indexes_in_range = set(range(self.start_index, self.end_index + 1))
        
        # 找出未爬取的index（需要补爬的）
        pending_indexes = sorted(all_indexes_in_range - crawled_indexes)
        
        # 已爬取数量以数据库为准
        total_crawled = len(crawled_indexes)
        
        # 读取进度文件（仅用于日志对比）
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                progress_crawled = progress.get('total_crawled', 0)
                progress_index = progress.get('current_index', self.start_index)
                
                if progress_crawled != total_crawled:
                    logging.warning(f"进度文件记录已爬取 {progress_crawled} 条，数据库实际 {total_crawled} 条，以数据库为准")
                if progress_index < self.end_index and (progress_index + 1) in crawled_indexes:
                    # 进度文件的下一个已经在数据库中，说明进度文件落后了
                    logging.info(f"进度文件记录到索引 {progress_index}，但数据库显示更多记录已爬取")
            except Exception as e:
                logging.warning(f"读取进度文件失败: {e}")
        
        if not pending_indexes:
            logging.info(f"区间 [{self.start_index}, {self.end_index}] 所有 {len(all_indexes_in_range)} 条记录均已爬取完成")
        else:
            logging.info(f"区间 [{self.start_index}, {self.end_index}] 共 {len(all_indexes_in_range)} 条，已爬取 {total_crawled} 条，待爬取 {len(pending_indexes)} 条")
            if len(pending_indexes) <= 20:
                logging.info(f"待爬取索引: {pending_indexes}")
            else:
                logging.info(f"待爬取索引(前20个): {pending_indexes[:20]}...")
        
        return pending_indexes, total_crawled

    def save_crawl_progress(self, current_index):
        """保存爬取进度"""
        try:
            progress = {
                "subreddit": self.subreddit_name,
                "range_start": self.start_index,
                "range_end": self.end_index,
                "current_index": current_index,
                "total_crawled": self.total_crawled_count,
                "last_updated": datetime.datetime.now().isoformat()
            }
            self._atomic_write_json(self.progress_file, progress)
        except Exception as e:
            logging.error(f"保存进度失败: {e}")

    def save_data(self, current_index):
        """保存数据到SQLite数据库"""
        try:
            # 如果没有新数据需要保存，直接返回
            if not self.all_posts_data:
                return
            
            # 保存到SQLite数据库
            self._save_to_sqlite(self.all_posts_data)
            
            # 更新总计数器
            self.total_crawled_count += len(self.all_posts_data)
            
            logging.info(f"保存数据成功，新增 {len(self.all_posts_data)} 条，本次运行已爬取 {self.total_crawled_count} 条帖子")
            
            # 清空已保存的数据，避免重复保存
            self.all_posts_data.clear()

            # 保存爬取进度
            self.save_crawl_progress(current_index)
            
        except Exception as e:
            logging.error(f"保存数据失败: {e}")

    def _save_to_sqlite(self, posts_data):
        """保存帖子数据到SQLite数据库（重复post_id时覆盖）"""
        if not posts_data:
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        crawled_at = datetime.datetime.now().isoformat()
        inserted_count = 0
        updated_count = 0
        error_count = 0
        
        for post in posts_data:
            try:
                # 先检查是否存在
                post_id = post.get("post_id")
                cursor.execute('SELECT 1 FROM posts WHERE post_id = ?', (post_id,))
                exists = cursor.fetchone() is not None
                
                # 准备数据，确保所有字段都有默认值
                insert_data = (
                    post.get("index", 0),  # index_in_list
                    post_id,  # post_id
                    post.get("subreddit", self.subreddit_name),  # subreddit
                    post.get("collect_source", "unknown"),  # collect_source
                    post.get("url", ""),  # url
                    post.get("title", ""),  # title
                    post.get("body", ""),  # body
                    post.get("author", ""),  # author
                    post.get("created_time", ""),  # created_time
                    post.get("score", 0),  # score
                    post.get("upvote_ratio", 0.0),  # upvote_ratio
                    post.get("num_comments", 0),  # num_comments
                    post.get("num_crossposts", 0),  # num_crossposts
                    post.get("num_comments_filtered", 0),  # num_comments_filtered
                    post.get("total_awards_received", 0),  # total_awards_received
                    1 if post.get("pinned") else 0,  # pinned
                    post.get("distinguished", ""),  # distinguished
                    post.get("flair_text", ""),  # flair_text
                    json.dumps(post.get("content_categories", []), ensure_ascii=False),  # content_categories
                    post.get("category", ""),  # category
                    post.get("pwls", -1),  # pwls
                    post.get("wls", -1),  # wls
                    json.dumps(post.get("user_reports", []), ensure_ascii=False),  # user_reports
                    json.dumps(post.get("mod_reports", []), ensure_ascii=False),  # mod_reports
                    post.get("author_patreon_flair", 0),  # author_patreon_flair
                    json.dumps(post.get("comments", []), ensure_ascii=False),  # comments
                    crawled_at,  # crawled_at
                    1 if post.get("is_valid", True) else 0  # is_valid
                )
                
                cursor.execute('''
                    INSERT OR REPLACE INTO posts (
                        index_in_list, post_id, subreddit, collect_source, url, title, body, author,
                        created_time, score, upvote_ratio, num_comments, num_crossposts,
                        num_comments_filtered, total_awards_received, pinned, distinguished, flair_text,
                        content_categories, category, pwls, wls, user_reports,
                        mod_reports, author_patreon_flair, comments, crawled_at, is_valid
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', insert_data)
                
                if exists:
                    updated_count += 1
                else:
                    inserted_count += 1
                    
            except Exception as e:
                error_count += 1
                logging.error(f"保存帖子到SQLite失败 (post_id={post.get('post_id', 'Unknown')}): {e}")
                logging.debug(f"失败的帖子数据: {post}")
        
        try:
            conn.commit()
        except Exception as e:
            logging.error(f"SQLite事务提交失败: {e}")
            conn.rollback()
        finally:
            conn.close()
        
        # 输出统计信息
        if error_count > 0:
            logging.warning(f"SQLite: 新增 {inserted_count} 条，更新 {updated_count} 条，失败 {error_count} 条")
        elif updated_count > 0:
            logging.info(f"SQLite: 新增 {inserted_count} 条，更新 {updated_count} 条已存在记录")
        else:
            logging.info(f"SQLite: 新增 {inserted_count} 条")

    async def fetch_post_json(self, post_url, url_index, source="unknown"):
        """使用PullPush API获取单个帖子的JSON数据"""
        try:
            post_id = self._extract_post_id(post_url)
            if not post_id:
                logging.error(f"无法从URL提取post_id: {post_url}")
                return None
            
            # 步骤1: 获取帖子详情
            await asyncio.sleep(random.uniform(self.delays['api_min'] / 1000, self.delays['api_max'] / 1000))
            submission_url = f"https://api.pullpush.io/reddit/search/submission/?ids={post_id}"
            
            resp = requests.get(submission_url, timeout=30)
            resp.raise_for_status()
            submission_data = resp.json().get('data', [])
            
            if not submission_data:
                logging.error(f"未找到帖子数据 (post_id={post_id})")
                return None
            
            post_info = submission_data[0]
            logging.info(f"获取到帖子 [{post_id}]: {post_info.get('title', 'N/A')[:50]}...")
            
            # 步骤2: 获取评论
            all_comments = await self._fetch_all_comments_from_pullpush(post_id)
            total_comments_count = self._count_comments_recursively(all_comments)
            logging.info(f"获取到评论，评论数: 根评论 {len(all_comments)}，总计（含回复） {total_comments_count}")

            # 提取帖子数据
            post_data = {
                "index": url_index,
                "post_id": post_id,
                "url": post_url,
                "subreddit": post_info.get("subreddit", ""),
                "collect_source": source,
                "title": post_info.get("title", "N/A"),
                "body": post_info.get("selftext", ""),
                "author": post_info.get("author", "[deleted]"),
                "created_time": self._convert_time(post_info.get("created_utc", 0)),
                "score": post_info.get("score", 0),
                "upvote_ratio": post_info.get("upvote_ratio", 0.0),
                "num_comments": post_info.get("num_comments", 0),
                "num_comments_filtered": total_comments_count,
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
                "comments": all_comments,
                "is_valid": self._is_post_valid(post_info.get("title", ""))
            }
                        
            return post_data
            
        except requests.RequestException as e:
            logging.error(f"PullPush API请求失败 (post_id={post_id}): {e}")
            return None
        except Exception as e:
            logging.error(f"获取帖子JSON失败: {e}")
            return None

    def _count_comments_recursively(self, comments):
        """递归统计评论总数（包括所有层级的回复）"""
        count = 0
        for comment in comments:
            count += 1
            if comment.get("replies"):
                count += self._count_comments_recursively(comment["replies"])
        return count

    async def _fetch_all_comments_from_pullpush(self, post_id):
        """从PullPush API获取帖子评论（按score降序）"""
        flat_comments = []
        params = {
            "link_id": post_id,
            "size": 500,
            "sort": "desc",
            "sort_type": "score"
        }
        
        try:
            await asyncio.sleep(random.uniform(self.delays['api_min'] / 1000, self.delays['api_max'] / 1000))
            resp = requests.get("https://api.pullpush.io/reddit/search/comment/", params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json().get('data', [])
            
            if not data:
                return []
            
            # 过滤并解析评论
            for comment_data in data:
                parsed = self._parse_pullpush_comment(comment_data, include_ids=True)
                if parsed:
                    flat_comments.append(parsed)
            
            # 构建树形结构
            return self._build_comment_tree(flat_comments, post_id)
            
        except requests.RequestException as e:
            logging.error(f"评论抓取失败: {e}")
        except Exception as e:
            logging.error(f"评论解析失败: {e}")
        
        return []

    def _parse_pullpush_comment(self, comment_data, include_ids=False):
        """解析PullPush API返回的评论数据（扁平结构）"""
        try:
            author = comment_data.get("author", "[deleted]")
            body = comment_data.get("body", "")
            
            # 过滤版主/机器人/删除的评论
            if self._is_bot_or_mod_comment_or_deleted(author, body):
                return None
            
            parsed = {
                "author": author,
                "text": body if body else "[无文本]",
                "score": comment_data.get("score", 0),
                "created_time": self._convert_time(comment_data.get("created_utc", 0)),
                "replies": [],
                "reply_count": 0
            }
            
            # 如果需要构建树形结构，保存ID信息
            if include_ids:
                parsed["comment_id"] = comment_data.get("id", "")
                parsed["parent_id"] = comment_data.get("parent_id", "")
            
            return parsed
            
        except Exception as e:
            logging.debug(f"解析评论失败: {e}")
            return None

    def _is_bot_or_mod_comment_or_deleted(self, author, body):
        """判断是否为版主/机器人评论或已删除评论"""
        # 检查评论人是否为 AutoModerator
        if author == "AutoModerator" or author == "[deleted]":
            return True
        
        # 检查评论内容是否包含机器人/版主特征, 或者删除
        body_lower = body.lower() if body else ""
        if "i am a bot" in body_lower or "moderator" in body_lower or "[deleted]" in body_lower:
            return True
        
        return False

    def _build_comment_tree(self, flat_comments, post_id):
        """将扁平化的评论列表构建成树形结构"""
        comment_dict = {}
        root_comments = []
        
        # 创建所有评论节点
        for comment in flat_comments:
            comment_id = comment.get("comment_id")
            if not comment_id:
                continue
                
            comment_dict[comment_id] = {
                "data": {
                    "author": comment["author"],
                    "text": comment["text"],
                    "score": comment["score"],
                    "created_time": comment["created_time"],
                    "replies": [],
                    "reply_count": 0
                },
                "parent_id": comment.get("parent_id", "")
            }
        
        # 建立父子关系
        for comment_id, comment_info in comment_dict.items():
            parent_id = comment_info["parent_id"]
            
            if parent_id.startswith("t3_"):
                # 直接回复帖子
                root_comments.append(comment_info["data"])
            elif parent_id.startswith("t1_"):
                # 回复评论
                parent_comment_id = parent_id[3:]
                if parent_comment_id in comment_dict:
                    comment_dict[parent_comment_id]["data"]["replies"].append(comment_info["data"])
                else:
                    # 父评论被过滤，作为根评论
                    root_comments.append(comment_info["data"])
        
        # 递归更新reply_count
        def update_counts(comment):
            comment["reply_count"] = len(comment["replies"])
            for reply in comment["replies"]:
                update_counts(reply)
        
        for comment in root_comments:
            update_counts(comment)
        
        return root_comments

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
    
    def _is_post_valid(self, title):
        """检查帖子是否有效（未被删除或移除）"""
        if not title:
            return False
        
        title_lower = title.lower()
        invalid_keywords = ["deleted by", "removed by"]
        
        for keyword in invalid_keywords:
            if keyword in title_lower:
                return False
        
        return True

    async def crawl_posts(self):
        """主要爬取流程"""
        completed_normally = False
        current_index = self.start_index
        
        try:
            # 加载URL索引（只读）
            url_list = self.load_url_list()
            if not url_list:
                return
            
            # 加载爬取进度，获取待爬取的index列表
            pending_indexes, self.total_crawled_count = self.load_crawl_progress()
            
            # 检查是否还有待爬取的帖子
            if not pending_indexes:
                logging.info(f"区间 [{self.start_index}, {self.end_index}] 爬取已完成，无需补爬")
                return
            
            total_pending = len(pending_indexes)
            logging.info(f"开始爬取区间 [{self.start_index}, {self.end_index}]，待爬取 {total_pending} 个帖子")
            
            # 遍历待爬取的index列表
            consecutive_failures = 0
            crawled_this_session = 0
            
            for i, index in enumerate(pending_indexes):
                url_item = url_list[index]
                url = url_item["url"]
                source = url_item.get("source", "unknown")
                current_index = index
                
                logging.info(f"[{i+1}/{total_pending}] 索引 {index} (区间{self.start_index}-{self.end_index}) 处理: {url}")
                                
                try:
                    post_data = await self.fetch_post_json(url, index, source)
                    
                    if post_data:
                        self.all_posts_data.append(post_data)
                        consecutive_failures = 0
                        crawled_this_session += 1
                        # 每10个帖子保存一次数据
                        if len(self.all_posts_data) % 10 == 0:
                            self.save_data(current_index)
                    else:
                        consecutive_failures += 1
                        logging.warning(f"索引 {index} 爬取失败，将在下次运行时重试")
                        if consecutive_failures >= 5:
                            logging.error("连续失败过多，停止爬取")
                            break
                    
                except Exception as e:
                    consecutive_failures += 1
                    logging.error(f"处理帖子出错 (索引 {index}): {e}")
                    if consecutive_failures >= 5:
                        break
                    await asyncio.sleep(random.uniform(self.delays['retry_min'] / 1000, self.delays['retry_max'] / 1000))
            
            # 检查是否正常完成（所有待爬取的都处理完了）
            if i == total_pending - 1 and consecutive_failures < 5:
                completed_normally = True
                logging.info(f"区间 [{self.start_index}, {self.end_index}] 本次待爬取的 {total_pending} 个帖子全部处理完成")
            
        except KeyboardInterrupt:
            logging.info("用户中断，进度已保存")
        except Exception as e:
            logging.error(f"爬取过程出错: {e}")
        finally:
            # 保存最终数据
            if self.all_posts_data:
                self.save_data(current_index)
            
            # 显示最终统计
            logging.info(f"爬取结束，本次运行爬取了 {self.total_crawled_count} 条帖子")
            
            # 完成后保留进度文件，方便查看爬取状态
            if completed_normally:
                logging.info("任务完成，进度文件已保留")


async def main():
    """主函数"""
    setup_logger()
    
    # 配置参数
    target_url = "https://www.reddit.com/r/dogs/"
    
    # 区间爬取参数（0-based索引，包含边界）
    # 设置为None表示不限制，爬取全部
    # 例如: start_index=0, end_index=99 表示爬取第1到第100个帖子
    start_index = None  # 起始位置，None表示从第1个开始
    end_index = None    # 结束位置，None表示爬到最后
    
    crawler = PostCrawler(
        subreddit_url=target_url,
        start_index=start_index,
        end_index=end_index,
        delays={
            'api_min': 50,
            'api_max': 200,
            'retry_min': 1000,
            'retry_max': 3000
        }
    )
    
    await crawler.crawl_posts()
    logging.info("Reddit爬取完成！")
    time.sleep(100)


if __name__ == "__main__":
    asyncio.run(main())
