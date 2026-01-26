# coding=utf-8
"""
LLM分析器 - 用于分析Reddit帖子数据
场景一：从数据库读取帖子并分析
场景二：直接分析爬虫获取的帖子数据
"""
import json
import logging
import sqlite3
import time
import openai
import yaml
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path


# ==================== 全局常量：从配置文件加载 ====================
def _load_llm_config() -> Dict[str, str]:
    """从 llm_config.yaml 加载配置"""
    config_file = Path(__file__).parent / "llm_config.yaml"
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
            return {
                "system_prompt": config.get("system_prompt", ""),
                "first_message": config.get("first_message", ""),
                "api_key": config.get("api_key", "")
            }
    except FileNotFoundError:
        logging.warning(f"配置文件不存在: {config_file}，使用默认空配置")
        return {"system_prompt": "", "first_message": "", "api_key": ""}
    except Exception as e:
        logging.error(f"读取配置文件失败: {e}")
        return {"system_prompt": "", "first_message": "", "api_key": ""}

_LLM_CONFIG = _load_llm_config()
SYSTEM_PROMPT = _LLM_CONFIG["system_prompt"]
USER_MESSAGE_TEMPLATE = _LLM_CONFIG["first_message"]
DEFAULT_API_KEY = _LLM_CONFIG["api_key"]


class LLMAnalyzer:
    """LLM分析器，用于分析Reddit帖子数据"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "deepseek-chat",
                 base_url: Optional[str] = None,
                 db_path: str = "./outputs/reddit_posts.sqlite",
                 max_retries: int = 3):
        # 如果未提供 api_key，从配置文件加载
        if api_key is None:
            api_key = DEFAULT_API_KEY
        
        self.config = {
            "api_key": api_key,
            "model": model,
            "base_url": base_url
        }
        self.db_path = db_path
        self.max_retries = max_retries
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("llm_analyzer.log", encoding="utf-8"),
                logging.StreamHandler()
            ]
        )
        logging.info(f"LLM分析器初始化: model={model}")
    
    def get_posts_from_db(self, subreddit: Optional[str] = None, 
                         post_ids: Optional[List[str]] = None) -> List[Dict]:
        """从数据库获取帖子"""
        if not Path(self.db_path).exists():
            logging.error(f"数据库文件不存在: {self.db_path}")
            return []
        
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        try:
            if post_ids:
                placeholders = ','.join('?' * len(post_ids))
                query = f"SELECT * FROM posts WHERE post_id IN ({placeholders}) AND is_valid = 1"
                cursor.execute(query, post_ids)
            elif subreddit:
                query = "SELECT * FROM posts WHERE subreddit = ? AND is_valid = 1 ORDER BY created_time DESC"
                cursor.execute(query, (subreddit,))
            else:
                logging.error("必须提供subreddit或post_ids参数")
                return []
            
            rows = cursor.fetchall()
            posts = [dict(row) for row in rows]
            logging.info(f"获取 {len(posts)} 条帖子")
            return posts
            
        except Exception as e:
            logging.error(f"从数据库获取帖子失败: {e}")
            return []
        finally:
            conn.close()
    
    def _build_user_message(self, post: Dict) -> str:
        """
        构造 User Message（符合 DeepSeek Prompt Caching 要求）
        严格遵循 System-User 分离：所有动态数据只出现在 User Message
        """
        # 提取前10个根评论，并清理不必要的字段
        comments = []
        comments_data = post.get("comments", "")
        if comments_data:
            try:
                comments_list = json.loads(comments_data) if isinstance(comments_data, str) else comments_data
                if isinstance(comments_list, list):
                    comments = [self._clean_comment(c) for c in comments_list[:10]]
            except (json.JSONDecodeError, TypeError):
                pass
        
        # 构造符合 System Prompt 中定义的 Input Data Schema 的帖子数据
        post_data = {
            "title": post.get("title", ""),
            "selftext": post.get("body", ""),  # 对应 System Prompt 中的 selftext
            "top_comments": comments,
            "flair_text": post.get("flair_text", ""),
            "created_time": post.get("created_time", ""),
            "score": post.get("score", 0)
        }
        
        # 序列化为 JSON 字符串
        json_string = json.dumps(post_data, ensure_ascii=False, indent=2)
        
        # 使用全局常量模板
        user_message = USER_MESSAGE_TEMPLATE.format(post_json=json_string)
        
        return user_message
    
    def _clean_comment(self, comment: Dict) -> Dict:
        """层层递归，清理评论树数据，移除不必要的字段"""
        cleaned = {
            "text": comment.get("text", comment.get("body", "")),
            "score": comment.get("score", 0)
        }
        
        # 递归处理子评论
        if "replies" in comment and isinstance(comment["replies"], list):
            cleaned["replies"] = [self._clean_comment(reply) for reply in comment["replies"]]
        
        return cleaned
    
    def _call_llm_once(self, user_message: str, max_tokens: int = 4000, 
                      temperature: float = 0.7) -> Tuple[bool, str, float, str]:
        """
        调用LLM一次（无状态单轮问答，符合 Prompt Caching 要求）
        
        每次调用都重新构造独立的 messages 数组：
        1. System Message: 使用全局常量 SYSTEM_PROMPT（静态，可被缓存）
        2. User Message: 包含当前帖子的动态数据
        """
        # 每次调用都重新初始化 messages，确保无状态
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message}
        ]
        logging.info("调用LLM进行单轮问答, 输入内容：" + user_message + "...")
        return self._call_sdk(messages, max_tokens, temperature)
    
    def _call_sdk(self, messages: List[Dict], 
                  max_tokens: int, temperature: float) -> Tuple[bool, str, float, str]:
        """使用OpenAI SDK调用"""
        try:
            client = openai.OpenAI(
                api_key=self.config['api_key'],
                base_url=self.config.get('base_url')
            )
            
            start_time = time.time()
            response = client.chat.completions.create(
                model=self.config['model'],
                messages=messages,  # type: ignore
                max_tokens=max_tokens,
                temperature=temperature
            )
            elapsed_time = time.time() - start_time
            
            content = response.choices[0].message.content or ""

            logging.info(f"SDK调用成功，耗时: {elapsed_time:.3f}s")
            return True, content, elapsed_time, ""
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"SDK调用失败: {error_msg}")
            return False, "", 0, error_msg
    
    def _parse_json_response(self, response_content: str) -> Tuple[bool, Optional[Dict], str]:
        """解析LLM返回的JSON回复"""
        # 直接解析
        try:
            data = json.loads(response_content)
            return True, data, ""
        except json.JSONDecodeError:
            pass
        
        import re
        # 提取JSON代码块
        matches = re.findall(r'```(?:json)?\s*\n?(.*?)\n?```', response_content, re.DOTALL)
        if matches:
            for match in matches:
                try:
                    return True, json.loads(match.strip()), ""
                except json.JSONDecodeError:
                    continue
        
        # 提取JSON对象
        matches = re.findall(r'\{.*\}', response_content, re.DOTALL)
        if matches:
            for match in sorted(matches, key=len, reverse=True):
                try:
                    return True, json.loads(match), ""
                except json.JSONDecodeError:
                    continue
        
        return False, None, "无法从回复中解析JSON数据"
    
    def analyze_post(self, post: Dict, max_tokens: int = 4000, 
                    temperature: float = 0.7) -> Dict[str, Any]:
        """
        分析单个帖子（带重试机制）
        """
        post_id = post.get("post_id", "unknown")
        
        # 构造符合 Prompt Caching 要求的 User Message
        user_message = self._build_user_message(post)
        
        retry_count = 0
        last_error = ""
        
        for attempt in range(self.max_retries):
            logging.info(f"分析帖子 {post_id}，尝试 {attempt + 1}/{self.max_retries}")
            
            success, response, llm_time, error = self._call_llm_once(user_message, max_tokens, temperature)
            
            if not success:
                last_error = f"LLM调用失败: {error}"
                retry_count += 1
                time.sleep(2)
                continue
            
            parse_success, data, parse_error = self._parse_json_response(response)
            
            if parse_success:
                logging.info(f"帖子 {post_id} 分析成功")
                return {
                    "success": True,
                    "post_id": post_id,
                    "data": data,
                    "raw_response": response,
                    "error": "",
                    "llm_time": llm_time,
                    "retry_count": retry_count
                }
            else:
                last_error = f"JSON解析失败: {parse_error}"
                retry_count += 1
                if attempt < self.max_retries - 1:
                    # 在重试时添加额外提示（仍然在 User Message 中）
                    user_message += "\n\nIMPORTANT: Please ensure you return ONLY the raw JSON object without any markdown formatting or additional text."
                    time.sleep(1)
        
        logging.error(f"帖子 {post_id} 分析失败，已重试 {retry_count} 次")
        return {
            "success": False,
            "post_id": post_id,
            "data": None,
            "raw_response": response if 'response' in locals() else "",
            "error": last_error,
            "llm_time": 0,
            "retry_count": retry_count
        }
    
    def analyze_posts_from_db(self, subreddit: Optional[str] = None,
                             post_ids: Optional[List[str]] = None,
                             max_tokens: int = 4000,
                             temperature: float = 0.7,
                             delay_between_posts: float = 1.0) -> List[Dict[str, Any]]:
        """场景一：从数据库读取帖子并依次分析"""
        posts = self.get_posts_from_db(subreddit, post_ids)
        if not posts:
            logging.warning("没有获取到帖子")
            return []
        
        logging.info(f"准备分析 {len(posts)} 条帖子")
        for post in posts:
            logging.info(f"帖子ID: {post.get('post_id', 'unknown')}, 标题: {post.get('title', '')[:30]}...")
        
        results = []
        
        for i, post in enumerate(posts, 1):
            logging.info(f"处理进度: {i}/{len(posts)}")
            result = self.analyze_post(post, max_tokens, temperature)
            results.append(result)
            
            if i < len(posts) and delay_between_posts > 0:
                time.sleep(delay_between_posts)
        
        success_count = sum(1 for r in results if r["success"])
        logging.info(f"分析完成: 成功 {success_count}/{len(results)}")
        
        return results
    
    def analyze_post_directly(self, post_data: Dict,
                            max_tokens: int = 4000,
                            temperature: float = 0.7) -> Dict[str, Any]:
        """场景二：直接分析传入的帖子数据（用于爬虫实时调用）"""
        logging.info(f"直接分析帖子: {post_data.get('post_id', 'unknown')}")
        return self.analyze_post(post_data, max_tokens, temperature)


def main():
    """示例用法"""
    
    # 初始化分析器（配置从 llm_config.yaml 自动加载）
    # 方式1：使用配置文件中的 api_key
    analyzer = LLMAnalyzer(
        model="deepseek-chat",  # 或 "deepseek-reasoner"
        base_url="https://api.deepseek.com/v1"  # DeepSeek API 地址
    )
    
    # 方式2：手动指定 api_key（优先级更高）
    # analyzer = LLMAnalyzer(
    #     api_key="your-api-key-here",
    #     model="deepseek-chat",
    #     base_url="https://api.deepseek.com/v1"
    # )
    
    # 场景一：从数据库读取并分析
    results = analyzer.analyze_posts_from_db(subreddit="dogs", post_ids=["1kia2bg"])
    
    for result in results:
        if result["success"]:
            print(f"\n帖子 {result['post_id']} 分析成功")
            print(json.dumps(result["data"], ensure_ascii=False, indent=2))
            print(f"耗时: {result['llm_time']:.2f}s, 重试: {result['retry_count']}")
        else:
            print(f"\n帖子 {result['post_id']} 失败: {result['error']}")
    
    # 场景二：直接分析帖子数据
    '''
    post_data = {
        "post_id": "test123",
        "title": "My dog has severe separation anxiety",
        "body": "When I leave for work, my dog howls and destroys the door...",
        "flair_text": "Help",
        "created_time": "2026-01-25",
        "score": 150,
        "comments": [
            {"author": "user1", "body": "Try crate training!", "score": 20},
            {"author": "user2", "body": "My dog had the same issue.", "score": 15}
        ]
    }
    
    result = analyzer.analyze_post_directly(post_data)
    
    if result["success"]:
        print("\n分析成功:")
        print(json.dumps(result["data"], ensure_ascii=False, indent=2))
        # 预期输出：category_code: "C-01", category_name: "分离焦虑"等
    '''

if __name__ == "__main__":
    main()
