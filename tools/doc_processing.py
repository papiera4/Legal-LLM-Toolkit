import json
import csv
import os
import re
import time
import threading
from pathlib import Path
import concurrent.futures
from openai import OpenAI, APIError, APIConnectionError, RateLimitError
from dotenv import load_dotenv

# 常量定义
INPUT_DIR = Path('XXX') # 存储文书txt文件的目录
OUTPUT_DIR = Path('XXX') # 输出目录
LOG_FILE = OUTPUT_DIR / 'processed.log'
CSV_PATH = OUTPUT_DIR / 'results.csv'
MODEL_DEPLOYMENT = "XXX"  # 替换为你的模型

# 正则表达式预编译
LINK_PATTERN = re.compile(r'(https?://[^\s]+)')
PKULAW_PATTERN = re.compile(r'.*\(([^)]+)\)[^()]*\.txt$')

# 提示词模板
SYSTEM_PROMPT = """
你是一个专业的法律文书信息提取和结构化助手，专门处理中国XXXX文书。你的任务是从给定的文书中准确、系统地提取关键信息，并按照标准化的格式进行输出。
"""

USER_PROMPT = """
从行政处罚文书提取这些信息：文书编号；年份；省份，如果文书中没有提省份，但提了市或者县，则根据该市位置判断省份，直接输出该省简称（如北京、内蒙古、江苏，而不是北京市、内蒙古自治区、江苏省）；
是否给予警告，是=1，否=0。
如果某些信息无法确定，使用0表示。
输出为json格式，除了格式化的json外，不要输出任何文字。如果有需要说明的，放在“其他”字段，如果没有需要说明的，则该字段不需要有内容。
json的key为：
{"文书编号", "年份", "省份",
"警告",……
"其他"。}
行政处罚文书内容如下：
"""

# CSV表头
CSV_HEADERS = [
    "法宝引证码", "原文链接", "文书编号", 
    "年份", "省份",
    "警告",……
    "其他"
]

# 初始化环境
load_dotenv()
if not (api_key := os.getenv("OPENAI_API_KEY")) or not (api_base := os.getenv("OPENAI_API_BASE")):
    raise EnvironmentError("缺少必要的环境变量配置")

client = OpenAI(api_key=api_key, base_url=api_base)

# 线程安全锁
file_lock = threading.Lock()
log_lock = threading.Lock()

def load_processed_files() -> set[str]:
    """加载已处理文件记录"""
    processed = set()
    if LOG_FILE.exists():
        with LOG_FILE.open('r', encoding='utf-8') as f:
            processed.update(line.strip() for line in f)
    return processed

def save_processed_file(file_path: str) -> None:
    """保存已处理文件记录"""
    with log_lock:
        with LOG_FILE.open('a', encoding='utf-8') as f:
            f.write(f"{file_path}\n")

def extract_link(text: str) -> str:
    """从文本提取URL"""
    match = LINK_PATTERN.search(text.replace('\n', ' '))
    return match.group(1) if match else "N/A"

def extract_pkulaw_id(file_path: Path) -> str:
    """从文件名提取引证码"""
    match = PKULAW_PATTERN.search(file_path.name)
    return match.group(1) if match else "N/A"

def process_file(file_path: Path) -> bool:
    """处理单个文件"""
    content = None
    try:
        # 读取文件内容
        with file_path.open('r', encoding='utf-8') as f:
            text = f.read()

        # API请求（带重试机制）
        for attempt in range(3):
            try:
                response = client.chat.completions.create(
                    model=MODEL_DEPLOYMENT,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": USER_PROMPT + text}
                    ],
                    timeout=30
                )
                content = response.choices[0].message.content
                break
            except (APIError, APIConnectionError, RateLimitError) as e:
                if attempt == 2:
                    raise
                time.sleep((attempt+1)*5)

        # 解析结果
        data = json.loads(content)
        row = {
            "法宝引证码": extract_pkulaw_id(file_path),
            "原文链接": extract_link(text),
            **{k: data.get(k, "N/A") for k in CSV_HEADERS[2:]}
        }

        # 写入CSV
        with file_lock:
            with CSV_PATH.open('a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
                writer.writerow(row)

        save_processed_file(str(file_path))
        return True

    except json.JSONDecodeError as e:
        print(f"[JSON解析失败] {file_path.name}: {e}\n响应内容: {content}")
    except Exception as e:
        print(f"[处理失败] {file_path.name}: {type(e).__name__} - {str(e)}")
        if content: print(f"响应内容: {content}")
    return False

def main():
    # 初始化目录
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # 初始化CSV文件
    if not CSV_PATH.exists():
        with CSV_PATH.open('w', newline='', encoding='utf-8') as f:
            csv.DictWriter(f, fieldnames=CSV_HEADERS).writeheader()

    # 获取待处理文件
    processed = load_processed_files()
    tasks = [f for f in INPUT_DIR.glob('*.txt') if str(f) not in processed]

    # 并行处理
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(process_file, f): f for f in tasks}
        for future in concurrent.futures.as_completed(futures):
            if future.exception():
                f = futures[future]
                print(f"异常处理文件 {f.name}: {future.exception()}")

if __name__ == "__main__":
    main()