"""
北大法宝（pkulaw.com）压缩包解压工具

功能：
1. 支持批量解压指定目录下的所有ZIP文件
2. 支持命令行参数指定输入/输出目录

使用示例：
python3 unzip_pkulaw.py --input ./zips --output ./unzipped

"""

import zipfile
from pathlib import Path
from typing import Union, List
from pathvalidate import sanitize_filename

def decode_filename(original: str) -> str:
    cp437_bytes = original.encode('cp437')
    for encoding in ['gb18030', 'utf-8', 'latin1']:
        return cp437_bytes.decode(encoding, errors='replace')

def safe_extract(zip_path: Path, target_path: Path) -> bool:
    with zipfile.ZipFile(zip_path) as zf:
        for file_info in zf.filelist:
            decoded_name = decode_filename(file_info.filename)
            safe_name = sanitize_filename(decoded_name)
            dest = target_path / safe_name
            dest.write_bytes(zf.read(file_info.filename))
            print(f"Extracted: {safe_name}")
    return True

def extract_all_zips(source_dir: Union[str, Path], target_dir: Union[str, Path]) -> List[str]:
    source_path = Path(source_dir)
    target_path = Path(target_dir)
    target_path.mkdir(parents=True, exist_ok=True)
    return [
        str(zip_path) for zip_path in source_path.glob('*.zip')
        if safe_extract(zip_path, target_path)
    ]

def main():
    import argparse
    parser = argparse.ArgumentParser(description='北大法宝ZIP文件批量解压工具')
    parser.add_argument('--input',  required=True, help='压缩包存储目录')
    parser.add_argument('--output', required=True, help='解压目录')
    args = parser.parse_args()

    processed = extract_all_zips(args.input, args.output)
    print(f"Successfully processed {len(processed)} ZIP files")

if __name__ == "__main__":
    main()
    