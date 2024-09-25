import subprocess
import argparse
import re
from datetime import datetime

# コマンドライン引数の解析
parser = argparse.ArgumentParser(description="Generate and execute DBT commands")
parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them")
parser.add_argument("--input-file", type=str, required=True, help="Path to the input file containing table names")
args = parser.parse_args()
source_dataset = "dl_salesforce_snapshots"

# 入力ファイルから表名リストを読み込む関数
def load_input_list(file_path):
    try:
        with open(file_path, 'r') as file:
            return [line.strip() for line in file if line.strip()]
    except FileNotFoundError:
        print(f"Error: Input file '{file_path}' not found.")
        return []
    except IOError:
        print(f"Error: Unable to read input file '{file_path}'.")
        return []

# 入力リストの取得
input_list = load_input_list(args.input_file)

# camelCase を snake_case に変換する関数
def camel_to_snake(name):
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    return pattern.sub('_', name).lower()

# 日付を抽出してフォーマットを変更する関数
def format_date(table_name):
    date_str = table_name[-8:]
    try:
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        return date_obj.strftime("%Y-%m-%d")
    except ValueError:
        print(f"Warning: Unable to parse date from {table_name}. Using original string.")
        return date_str

# コマンドを生成する関数
def generate_command(table_name):
    underscore_index = table_name.rfind('_')
    prefix = camel_to_snake(table_name[:underscore_index].replace('_AE_491',''))
    session_date = format_date(table_name)
    return f'dbt snapshot --select sf_{prefix}_snapshots --vars \'{{"session_date": "{session_date}", "source_dataset": "{source_dataset}", "source_table": "{table_name}"}}\''

# コマンドを実行する関数
def execute_command(command, dry_run=False):
    if dry_run:
        print(f"[Dry run] Command to execute:\n{command}")
    else:
        try:
            result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True)
            print(f"Command executed successfully:\n{command}")
            print(result.stdout)
            print(result.stderr)
        except subprocess.CalledProcessError as e:
            print(f"Error executing command:\n{command}")
            print(e.stdout)
            print(e.stderr)

# 各要素に対してコマンドを生成し実行
for item in input_list:
    item = item.strip()  # 余分な空白を削除
    if item:  # 空の行をスキップ
        command = generate_command(item)
        print(f"target table: {item}")
        print(f"attempt to execute command: {command}")
        execute_command(command, args.dry_run)
        print("\n" + "="*50 + "\n")  # 区切り線
