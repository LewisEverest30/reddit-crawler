# coding=utf-8
"""
JSON文件更新工具 - 用于修改已有的URL收集JSON文件
"""
import json
import os
import sys
from pathlib import Path


def atomic_write_json(file_path, data, indent=2):
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


def add_source_field(json_file_path, source_value="pullpush", backup=True):
    """
    为JSON文件中的collected_urls添加source字段
    
    Args:
        json_file_path: JSON文件路径
        source_value: source字段的值，默认为"pullpush"
        backup: 是否创建备份文件，默认为True
    
    Returns:
        成功处理的URL数量
    """
    if not os.path.exists(json_file_path):
        print(f"错误: 文件不存在 - {json_file_path}")
        return 0
    
    try:
        # 读取JSON文件
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 检查是否有collected_urls字段
        if 'collected_urls' not in data:
            print(f"警告: 文件中没有找到 'collected_urls' 字段 - {json_file_path}")
            return 0
        
        collected_urls = data['collected_urls']
        
        # 检查是否为列表
        if not isinstance(collected_urls, list):
            print(f"错误: 'collected_urls' 不是列表类型 - {json_file_path}")
            return 0
        
        # 创建备份
        if backup:
            backup_file = json_file_path + ".backup"
            if not os.path.exists(backup_file):
                atomic_write_json(backup_file, data)
                print(f"已创建备份文件: {backup_file}")
        
        # 更新URL对象，添加source字段
        updated_count = 0
        for url_obj in collected_urls:
            if isinstance(url_obj, dict):
                if 'source' not in url_obj:
                    url_obj['source'] = source_value
                    updated_count += 1
        
        # 保存更新后的文件
        atomic_write_json(json_file_path, data)
        
        print(f"成功更新 {json_file_path}")
        print(f"  - 总URL数: {len(collected_urls)}")
        print(f"  - 新增source字段: {updated_count}")
        print(f"  - 已有source字段: {len(collected_urls) - updated_count}")
        
        return updated_count
        
    except json.JSONDecodeError as e:
        print(f"错误: JSON解析失败 - {json_file_path}")
        print(f"  详细信息: {e}")
        return 0
    except Exception as e:
        print(f"错误: 处理文件失败 - {json_file_path}")
        print(f"  详细信息: {e}")
        return 0


def batch_update_directory(directory_path, source_value="pullpush", pattern="*_urls.json", backup=True):
    """
    批量更新目录下的所有JSON文件
    
    Args:
        directory_path: 目录路径
        source_value: source字段的值
        pattern: 文件名匹配模式
        backup: 是否创建备份
    
    Returns:
        成功处理的文件数量
    """
    directory = Path(directory_path)
    
    if not directory.exists():
        print(f"错误: 目录不存在 - {directory_path}")
        return 0
    
    if not directory.is_dir():
        print(f"错误: 不是有效的目录 - {directory_path}")
        return 0
    
    # 查找所有匹配的JSON文件
    json_files = list(directory.rglob(pattern))
    
    if not json_files:
        print(f"警告: 在目录中没有找到匹配 '{pattern}' 的文件 - {directory_path}")
        return 0
    
    print(f"找到 {len(json_files)} 个文件待处理\n")
    
    success_count = 0
    for json_file in json_files:
        print(f"处理: {json_file}")
        try:
            result = add_source_field(str(json_file), source_value, backup)
            if result >= 0:
                success_count += 1
            print()  # 空行分隔
        except Exception as e:
            print(f"跳过文件 {json_file}: {e}\n")
            continue
    
    print(f"批量更新完成: {success_count}/{len(json_files)} 个文件成功处理")
    return success_count


def main():
    """主函数 - 命令行接口"""
    if len(sys.argv) < 2:
        print("用法:")
        print("  单个文件: python update_json_schema.py <json文件路径> [source值]")
        print("  批量处理: python update_json_schema.py --batch <目录路径> [source值]")
        print()
        print("示例:")
        print('  python update_json_schema.py outputs/dogs/dogs_urls.json "pullpush"')
        print('  python update_json_schema.py --batch outputs/ "pullpush"')
        sys.exit(1)
    
    if sys.argv[1] == "--batch":
        # 批量模式
        if len(sys.argv) < 3:
            print("错误: 批量模式需要指定目录路径")
            sys.exit(1)
        
        directory = sys.argv[2]
        source_value = sys.argv[3] if len(sys.argv) > 3 else "pullpush"
        
        batch_update_directory(directory, source_value)
    else:
        # 单文件模式
        json_file = sys.argv[1]
        source_value = sys.argv[2] if len(sys.argv) > 2 else "pullpush"
        
        add_source_field(json_file, source_value)


if __name__ == "__main__":
    main()
