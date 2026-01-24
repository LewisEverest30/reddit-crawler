# coding=utf-8
"""
Reddit爬虫 - 区间结果合并工具
用于合并各个区间爬取的JSON结果文件
"""
import json
import os
import re
import argparse
from pathlib import Path


def find_range_files(subreddit_dir, subreddit_name):
    """查找所有区间结果文件"""
    pattern = re.compile(rf'^{re.escape(subreddit_name)}_data_(\d+)_(\d+)\.json$')
    range_files = []
    
    for filename in os.listdir(subreddit_dir):
        match = pattern.match(filename)
        if match:
            start = int(match.group(1))
            end = int(match.group(2))
            filepath = os.path.join(subreddit_dir, filename)
            range_files.append({
                'path': filepath,
                'start': start,
                'end': end,
                'filename': filename
            })
    
    # 按起始位置排序
    range_files.sort(key=lambda x: x['start'])
    return range_files


def merge_json_files(range_files, output_path, deduplicate=True):
    """合并JSON文件"""
    all_posts = []
    seen_post_ids = set()
    stats = {
        'total_files': len(range_files),
        'total_posts': 0,
        'duplicates_removed': 0
    }
    
    for file_info in range_files:
        filepath = file_info['path']
        print(f"读取: {file_info['filename']} (区间 {file_info['start']}-{file_info['end']})")
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                posts = json.load(f)
            
            for post in posts:
                post_id = post.get('post_id')
                if deduplicate and post_id in seen_post_ids:
                    stats['duplicates_removed'] += 1
                    continue
                
                seen_post_ids.add(post_id)
                all_posts.append(post)
                stats['total_posts'] += 1
                
        except Exception as e:
            print(f"  警告: 读取失败 - {e}")
    
    # 保存合并结果
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_posts, f, ensure_ascii=False, indent=4)
    
    return stats


def check_coverage(range_files, total_count=None):
    """检查区间覆盖情况"""
    if not range_files:
        return {'covered': [], 'gaps': [], 'overlaps': []}
    
    # 排序
    sorted_files = sorted(range_files, key=lambda x: x['start'])
    
    covered_ranges = [(f['start'], f['end']) for f in sorted_files]
    gaps = []
    overlaps = []
    
    # 检查间隙和重叠
    for i in range(1, len(sorted_files)):
        prev_end = sorted_files[i-1]['end']
        curr_start = sorted_files[i]['start']
        
        if curr_start > prev_end + 1:
            gaps.append((prev_end + 1, curr_start - 1))
        elif curr_start <= prev_end:
            overlaps.append((curr_start, min(prev_end, sorted_files[i]['end'])))
    
    # 检查首尾
    first_start = sorted_files[0]['start']
    last_end = sorted_files[-1]['end']
    
    if first_start > 1:
        gaps.insert(0, (1, first_start - 1))
    
    if total_count and last_end < total_count:
        gaps.append((last_end + 1, total_count))
    
    return {
        'covered': covered_ranges,
        'gaps': gaps,
        'overlaps': overlaps,
        'first_start': first_start,
        'last_end': last_end
    }


def main():
    parser = argparse.ArgumentParser(description='合并Reddit爬虫的区间结果文件')
    parser.add_argument('subreddit', help='Subreddit名称 (例如: dogs)')
    parser.add_argument('--output-dir', default='./outputs', help='输出目录 (默认: ./outputs)')
    parser.add_argument('--no-deduplicate', action='store_true', help='不进行去重')
    parser.add_argument('--check-only', action='store_true', help='仅检查区间覆盖，不合并')
    parser.add_argument('--total', type=int, help='URL列表总数（用于检查覆盖完整性，0-based索引）')
    
    args = parser.parse_args()
    
    subreddit_name = args.subreddit
    subreddit_dir = os.path.join(args.output_dir, subreddit_name)
    
    if not os.path.exists(subreddit_dir):
        print(f"错误: 目录不存在 - {subreddit_dir}")
        return 1
    
    # 查找区间文件
    range_files = find_range_files(subreddit_dir, subreddit_name)
    
    if not range_files:
        print(f"未找到区间结果文件")
        print(f"期望格式: {subreddit_name}_data_<start>_<end>.json")
        return 1
    
    print(f"\n找到 {len(range_files)} 个区间结果文件:")
    for f in range_files:
        print(f"  - {f['filename']} (区间 {f['start']}-{f['end']})")
    
    # 检查覆盖情况
    coverage = check_coverage(range_files, args.total)
    
    print(f"\n区间覆盖分析:")
    print(f"  覆盖范围: {coverage['first_start']} - {coverage['last_end']}")
    
    if coverage['gaps']:
        print(f"  ⚠️  存在间隙:")
        for gap in coverage['gaps']:
            print(f"      {gap[0]} - {gap[1]}")
    else:
        print(f"  ✓ 无间隙")
    
    if coverage['overlaps']:
        print(f"  ⚠️  存在重叠:")
        for overlap in coverage['overlaps']:
            print(f"      {overlap[0]} - {overlap[1]}")
    else:
        print(f"  ✓ 无重叠")
    
    if args.check_only:
        return 0
    
    # 合并文件
    output_path = os.path.join(subreddit_dir, f"{subreddit_name}_data_merged.json")
    print(f"\n开始合并...")
    
    stats = merge_json_files(range_files, output_path, deduplicate=not args.no_deduplicate)
    
    print(f"\n合并完成!")
    print(f"  输出文件: {output_path}")
    print(f"  处理文件: {stats['total_files']} 个")
    print(f"  帖子总数: {stats['total_posts']} 条")
    if stats['duplicates_removed'] > 0:
        print(f"  去重数量: {stats['duplicates_removed']} 条")
    
    return 0


if __name__ == "__main__":
    exit(main())
