"""
北大法宝（pkulaw.com）xls批量处理工具

功能：
将从北大法宝下载的若干xls文件合并为单个xlsx文件。

使用示例：
python3 xls_pkulaw.py

"""
import os
import pandas as pd
import xlrd
from openpyxl import Workbook

def is_number(text):
    try:
        float(text)
        return True
    except ValueError:
        return False

def merge_xls_files(directory, output_file):
    merged_data = pd.DataFrame()
    columns = ['序号', '标题', '时效性', '效力位阶', '制定机关', '发文字号', 
               '公布日期', '实施日期', '法宝引证码', '哈希值']

    for filename in os.listdir(directory):
        if filename.endswith('.xls'):
            filepath = os.path.join(directory, filename)
            try:
                workbook = xlrd.open_workbook(filepath)
                sheet = workbook.sheet_by_index(0)
                headers = [sheet.cell_value(2, col) for col in range(sheet.ncols)]
                data_rows = []

                for row_idx in range(3, sheet.nrows):
                    first_cell_value = sheet.cell_value(row_idx, 0)
                    if isinstance(first_cell_value, str) and not is_number(first_cell_value.strip()):
                        print(f"文件 {filename} 第{row_idx+1}行第一个单元格不是数字（内容: '{first_cell_value}'），停止处理该文件的后续行")
                        break
                    elif not isinstance(first_cell_value, (int, float)):
                        print(f"文件 {filename} 第{row_idx+1}行第一个单元格不是数字（类型: {type(first_cell_value)}），停止处理该文件的后续行")
                        break
                    data_rows.append([sheet.cell_value(row_idx, col) for col in range(sheet.ncols)])

                if data_rows:
                    df = pd.DataFrame(data_rows, columns=headers)
                    df['法宝引证码'] = ''
                    df['哈希值'] = ''
                    merged_data = pd.concat([merged_data, df], ignore_index=True)
                print(f"已处理文件: {filename}（有效行数: {len(data_rows)}）")

            except Exception as e:
                print(f"处理文件 {filename} 时出错: {str(e)}")

    if not merged_data.empty:
        merged_data = merged_data[[col for col in columns if col in merged_data.columns]]
        wb = Workbook()
        ws = wb.active
        ws.append(columns)
        for _, row in merged_data.iterrows():
            ws.append(row.tolist())
        wb.save(output_file)
        print(f"合并完成，结果已保存到: {output_file}（总行数: {len(merged_data)}）")
    else:
        print("没有找到可合并的有效xls文件")

def main():
    xls_directory = 'your_xls_directory'  # 替换为你的xls文件目录
    output_file = 'merged_output.xlsx'  # 替换为你想要的输出文件名
    merge_xls_files(xls_directory, output_file)

if __name__ == "__main__":
    main()