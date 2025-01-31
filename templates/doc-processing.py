# 这个代码模板用于处理从北大法宝数据库批量下载的法律文书，下载时选择保存为“纯文本”，下载后的压缩包无需解压，全部放在{input_directory}目录下

import json
import csv
import zipfile
import os
import re
from dotenv import load_dotenv
load_dotenv()

# input目录下存储压缩包，压缩包内为txt文件
input_directory = 'XXXX'
output_directory = 'XXXX'

# from openai import AzureOpenAI  
# endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
# subscription_key = os.getenv("AZURE_OPENAI_API_KEY")
# api_version=os.getenv("AZURE_OPENAI_API_VERSION")
# client = AzureOpenAI(  
#     azure_endpoint=endpoint,  
#     api_key=subscription_key,  
#     api_version=api_version,  
# )

# from openai import OpenAI
# apiKey = os.getenv("OPENAI_API_KEY")
# client = OpenAI(
#     api_key=apiKey,
# )

deployment = "XXXX" # 替换为你的模型, example: gpt-4

system_prompt = """
你是一个专业的法律文书信息提取和结构化助手，专门处理中国XXXX文书。你的任务是从给定的文书中准确、系统地提取关键信息，并按照标准化的格式进行输出。
"""

user_prompt = """
从行政处罚文书提取这些信息：文书编号；原文链接；年份；省份，如果文书中没有提省份，但提了市或者县，则根据该市位置判断省份，直接输出该省简称（如北京、内蒙古、江苏，而不是北京市、内蒙古自治区、江苏省）；
是否给予警告，是=1，否=0；
……。
如果某些信息无法确定，使用0表示。
输出为json格式，除了格式化的json外，不要输出任何文字。如果有需要说明的，放在“其他”字段，如果没有需要说明的，则该字段不需要有内容。
json的key为：
{"文书编号", "原文链接", "年份", "省份",
"警告", ……
"其他"。}
行政处罚文书内容如下：
"""

csv_headers = [
    "文书编号", "原文链接", "年份", "省份",
    "警告", ……
    "其他"
]

# 以下没有需要修改的
for filename in os.listdir(input_directory):
    if filename.endswith('.zip'):      
        zip_path = os.path.join(input_directory, filename)
        output_csv = os.path.join(output_directory, filename.replace('.zip', '.csv'))
        
        with open(output_csv, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
            csv_writer.writeheader()
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for doc_file in zip_ref.namelist():
                    if doc_file.endswith('.txt'):
                        try:
                            with zip_ref.open(doc_file) as file:
                                document_text = file.read().decode('utf-8')
                            
                            prompts = [
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": user_prompt + document_text}
                            ]

                            responses = client.chat.completions.create(  
                                model=deployment,
                                messages=prompts,  
                                temperature=0.7,  
                                top_p=0.95,  
                                frequency_penalty=0,  
                                presence_penalty=0,  
                                stop=None,  
                                stream=False  
                            ) 

                            content = responses.choices[0].message.content
                            parsed_data = json.loads(content)

                            csv_writer.writerow(parsed_data)

                            match = re.search(r'\(.*\)\.txt$', doc_file)
                            if match:
                                display_filename = match.group(0)
                                print(f"成功处理文件: {display_filename} 压缩包 {filename}")
                            else:
                                print(f"成功处理文件: {doc_file} 压缩包 {filename}")
                        
                        except Exception as e:
                            print(f"处理文件 {doc_file} 压缩包 {filename} 时出错: {e}")
                            print(content)
        print(f"压缩包{filename}已处理")
print(f"任务完成，结果保存在{output_directory}目录")
