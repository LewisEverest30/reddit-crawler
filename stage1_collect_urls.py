# coding=utf-8
"""
Reddit爬虫 - 第一阶段：使用PullPush API收集帖子URL
"""
import json
import logging
import os
import re
import random
import requests
import time
import datetime
from pathlib import Path


def setup_logger(debug=False):
    level = logging.DEBUG if debug else logging.INFO
    format_str = '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    
    logging.basicConfig(
        level=level,
        format=format_str,
        handlers=[
            logging.FileHandler("reddit_crawler_stage1.log", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )


class URLCollector:
    """第一阶段：收集Reddit帖子URL"""

    def __init__(self, subreddit_url, max_posts=100, before_timestamp=None, api_delay=None):
        self.subreddit_url = subreddit_url
        self.max_posts = max_posts
        self.before_timestamp = before_timestamp or int(time.time())
        
        # API调用延迟配置（秒）
        self.api_delay = api_delay or {'min': 2, 'max': 5}
        
        # 提取subreddit名称并创建目录
        self.subreddit_name = self._extract_subreddit_name(subreddit_url)
        self.subreddit_dir = f".\\outputs\\{self.subreddit_name}"
        Path(self.subreddit_dir).mkdir(parents=True, exist_ok=True)
        
        # 状态记录路径
        self.state_file = os.path.join(self.subreddit_dir, f"{self.subreddit_name}_crawler_state.json")

    def _extract_subreddit_name(self, url):
        """从Reddit URL中提取subreddit名称"""
        match = re.search(r'/r/([^/]+)', url)
        return match.group(1) if match else "unknown_subreddit"

    def _extract_post_id(self, url):
        """提取帖子ID"""
        match = re.search(r'/comments/([a-zA-Z0-9]+)/', url)
        return match.group(1) if match else None

    def save_progress(self, current_index, url_list, is_collection_complete=False, collection_progress=None):
        """保存爬取进度（完整保存，主要用于第一阶段）"""
        try:
            state_data = {
                "current_post_index": current_index,
                "collected_urls": url_list,
                "total_collected": len(url_list),
                "total_crawled_count": 0,
                "subreddit_name": self.subreddit_name,
                "max_posts": self.max_posts,
                "last_updated": datetime.datetime.now().isoformat()
            }
            
            # 如果提供了URL收集进度，也保存
            if collection_progress:
                state_data["url_collection_progress"] = collection_progress
            
            # 保存是否完成URL收集的标志
            state_data["is_collection_complete"] = is_collection_complete
            
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            logging.warning(f"保存进度失败: {e}")

    def save_url_collection_progress(self, collected_urls, before_timestamp, target_count):
        """保存URL收集阶段的进度"""
        try:
            latest_post_timestamp = None
            collected_post_ids = []
            
            if collected_urls:
                # 获取最新帖子时间戳
                latest_post_timestamp = collected_urls[-1].get('created_utc', before_timestamp)
                
                # 提取所有帖子ID
                collected_post_ids = [self._extract_post_id(item.get('url', '')) 
                                     for item in collected_urls]
                collected_post_ids = [pid for pid in collected_post_ids if pid]  # 过滤空值
            
            collection_progress = {
                "collected_count": len(collected_urls),
                "target_count": target_count,
                "latest_post_timestamp": latest_post_timestamp or before_timestamp,
                "before_timestamp": before_timestamp,
                "collected_post_ids": collected_post_ids,
                "last_collection_time": datetime.datetime.now().isoformat(),
                "is_collection_complete": len(collected_urls) >= target_count
            }
            
            is_complete = len(collected_urls) >= target_count
            self.save_progress(1, collected_urls, is_complete, collection_progress)
            
            logging.info(f"URL收集进度已保存: {len(collected_urls)}/{target_count}，"
                        f"最新帖子时间: {latest_post_timestamp}，已收集ID数: {len(collected_post_ids)}")
            
        except Exception as e:
            logging.error(f"保存URL收集进度失败: {e}")

    def load_progress(self) -> tuple:
        """加载爬取进度"""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)
                
                url_list = state_data.get('collected_urls', [])
                collection_progress = state_data.get('url_collection_progress', None)
                
                # 判断URL收集是否完成
                is_collection_complete = len(url_list) >= self.max_posts
                
                if is_collection_complete:
                    logging.info(f"URL收集已完成: {len(url_list)} 个帖子")
                    return url_list, True, collection_progress
                else:
                    # URL收集未完成，需要继续收集
                    if collection_progress:
                        latest_time = collection_progress.get('latest_post_timestamp', 'N/A')
                        logging.info(f"恢复URL收集进度: {len(url_list)}/{self.max_posts}，"
                                    f"最新帖子时间: {latest_time}")
                    return url_list, False, collection_progress
        except Exception as e:
            logging.warning(f"加载进度失败: {e}")
        
        return [], False, {}

    def _initialize_collection_state(self, existing_urls, collection_progress):
        """初始化URL收集状态"""
        collected_urls = existing_urls or []
        collected_post_ids = set()
        before = self.before_timestamp
        
        if collection_progress:
            # 恢复已收集的帖子ID集合
            existing_ids = collection_progress.get('collected_post_ids', [])
            collected_post_ids.update(existing_ids)
            
            # 使用before_timestamp作为起始点，避免漏掉同时间戳帖子
            before = collection_progress.get('before_timestamp', self.before_timestamp)
            latest_time = collection_progress.get('latest_post_timestamp', self.before_timestamp)
            
            logging.info(f"恢复URL收集进度: {len(collected_urls)}个帖子，{len(collected_post_ids)}个ID")
            logging.info(f"从时间戳 {before} 继续，最新: {latest_time}")
        else:
            # 初始化时提取已有URL的ID
            for url_data in collected_urls:
                post_id = self._extract_post_id(url_data.get('url', ''))
                if post_id:
                    collected_post_ids.add(post_id)
        
        return collected_urls, collected_post_ids, before

    def _is_deleted_post(self, post):
        """检查帖子是否已被删除"""
        # 检查作者是否已删除
        author = post.get("author", "")
        if author in ("[deleted]", "[removed]"):
            return True
        
        # 检查是否被版主/管理员删除
        removed_by = post.get("removed_by_category")
        if removed_by:  # 如果有值，说明被删除了
            return True
        
        # 检查帖子内容是否被删除（对于 self post）
        selftext = post.get("selftext", "")
        if selftext in ("[deleted]", "[removed]"):
            return True
        
        return False

    def _process_api_response(self, new_posts, collected_urls, collected_post_ids, max_posts):
        """处理API响应数据"""
        new_count = 0
        duplicate_count = 0
        deleted_count = 0
        
        for post in new_posts:
            if len(collected_urls) >= max_posts:
                break
            
            post_id = post.get("id")
            permalink = post.get("permalink")
            created_utc = post.get("created_utc")
            
            if not (post_id and permalink and created_utc):
                continue
                
            if post_id in collected_post_ids:
                duplicate_count += 1
                continue
            
            # 过滤已删除的帖子
            if self._is_deleted_post(post):
                deleted_count += 1
                collected_post_ids.add(post_id)  # 仍然记录ID避免重复检查
                continue
            
            post_url = f"https://www.reddit.com{permalink}"
            collected_urls.append({
                "url": post_url,
                "created_utc": created_utc
            })
            collected_post_ids.add(post_id)
            new_count += 1
        
        return new_count, duplicate_count, deleted_count

    def collect_post_urls(self, existing_urls=None, collection_progress=None):
        """收集帖子URL - 使用Pullpush API，支持断点续爬"""
        target_url = self.subreddit_url
        
        if "/comments/" in target_url:
            return [{"url": target_url}]
        
        # 提取subreddit名称
        subreddit_name = self._extract_subreddit_name(target_url)
        if not subreddit_name or subreddit_name == "unknown_subreddit":
            logging.error(f"无法从URL提取subreddit名称: {target_url}")
            return []
        
        # 初始化收集状态
        collected_urls, collected_post_ids, before = self._initialize_collection_state(
            existing_urls, collection_progress)
        
        if not collection_progress:
            logging.info(f"开始收集 r/{subreddit_name} 的帖子，目标数量: {self.max_posts}")
        
        consecutive_errors = 0
        consecutive_zero_new = 0  # 连续获取0个新URL的次数
        last_save_count = len(collected_urls)
        
        try:
            while len(collected_urls) < self.max_posts:
                try:
                    # 构造并发送API请求
                    api_url = "https://api.pullpush.io/reddit/search/submission/"
                    params = {
                        "subreddit": subreddit_name,
                        "size": min(100, self.max_posts - len(collected_urls) + 1),
                        "sort": "desc",
                        "sort_type": "created_utc"
                    }
                    if before:
                        params["before"] = before
                    
                    logging.info(f"请求API: {len(collected_urls)}/{self.max_posts} 个帖子")
                    response = requests.get(api_url, params=params, timeout=30)
                    
                    if response.status_code != 200:
                        logging.error(f"API请求失败: HTTP {response.status_code}")
                        consecutive_errors += 1
                        if consecutive_errors >= 3:
                            logging.error("连续请求失败，停止收集")
                            break
                        time.sleep(5)
                        continue
                    
                    data = response.json()
                    new_posts = data.get("data", [])
                    
                    if not new_posts:
                        logging.info("无更多数据，URL收集完成")
                        break
                    
                    # 处理新获取的帖子
                    new_count, duplicate_count, deleted_count = self._process_api_response(
                        new_posts, collected_urls, collected_post_ids, self.max_posts)
                    
                    # 检测连续多次获取0个新URL的情况
                    if new_count == 0:
                        consecutive_zero_new += 1
                        if consecutive_zero_new >= 3:
                            logging.info("连续多次未获取到新URL，可能已无更多数据，URL收集完成")
                            break
                    else:
                        consecutive_zero_new = 0  # 重置计数器
                    
                    # 更新时间戳用于下一页
                    if new_posts:
                        before = new_posts[-1]["created_utc"]
                    
                    consecutive_errors = 0  # 成功请求，重置错误计数
                    logging.info(f"获取 {new_count} 个新URL，跳过 {duplicate_count} 个重复，过滤 {deleted_count} 个已删除，总计 {len(collected_urls)}")
                    
                    # 定期保存进度
                    if (len(collected_urls) - last_save_count >= 50 or 
                        len(collected_urls) % 250 == 0):
                        self.save_url_collection_progress(collected_urls, before, self.max_posts)
                        last_save_count = len(collected_urls)
                    
                    # 随机延迟避免被封
                    delay = random.uniform(self.api_delay['min'], self.api_delay['max'])
                    logging.debug(f"等待 {delay:.1f} 秒后继续...")
                    time.sleep(delay)
                    
                except (requests.exceptions.Timeout, 
                        requests.exceptions.RequestException, 
                        json.JSONDecodeError) as e:
                    consecutive_errors += 1
                    logging.error(f"请求错误: {e}")
                    time.sleep(3)
                    if consecutive_errors >= 5:
                        logging.error("连续错误过多，保存进度并退出")
                        break
                    continue
                except KeyboardInterrupt:
                    logging.info("用户中断，保存进度...")
                    raise
                except Exception as e:
                    consecutive_errors += 1
                    logging.error(f"收集URL时出错: {e}")
                    if consecutive_errors >= 3:
                        break
                    time.sleep(2)
        
        except KeyboardInterrupt:
            logging.info("收集被中断，保存进度...")
            self.save_url_collection_progress(collected_urls, before, self.max_posts)
            raise
        
        # 最终保存进度
        if len(collected_urls) != last_save_count or len(collected_urls) >= self.max_posts:
            self.save_url_collection_progress(collected_urls, before, self.max_posts)
            status = "完成" if len(collected_urls) >= self.max_posts else "未完成"
            logging.info(f"URL收集{status}: {len(collected_urls)}/{self.max_posts}，"
                        f"去重后有效ID: {len(collected_post_ids)}")
        
        return collected_urls[:self.max_posts]

    def run(self):
        """运行URL收集"""
        try:
            # 加载进度
            existing_urls, is_collection_complete, collection_progress = self.load_progress()
            
            if is_collection_complete:
                logging.info(f"URL收集已完成，共 {len(existing_urls)} 个帖子")
                logging.info("如需重新收集，请删除状态文件后重试")
                return existing_urls
            
            # 继续或开始收集
            url_list = self.collect_post_urls(existing_urls, collection_progress)
            
            if not url_list:
                logging.error("未收集到任何帖子链接")
                return []
            
            logging.info(f"URL收集完成，共 {len(url_list)} 个帖子")
            return url_list
            
        except KeyboardInterrupt:
            logging.info("用户中断，进度已保存")
            return []
        except Exception as e:
            logging.error(f"收集过程出错: {e}")
            return []


def main():
    """主函数"""
    setup_logger()
    
    # 配置参数
    target_url = "https://www.reddit.com/r/dogs/"
    max_posts = 50000
    
    collector = URLCollector(
        subreddit_url=target_url,
        max_posts=max_posts,
        before_timestamp=int(datetime.datetime(2026, 1, 22).timestamp()),
        api_delay={'min': 2, 'max': 5}  # API调用间隔（秒）
    )
    
    url_list = collector.run()
    logging.info(f"第一阶段完成！收集到 {len(url_list)} 个URL")


if __name__ == "__main__":
    main()
