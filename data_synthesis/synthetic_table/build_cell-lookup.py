import random

import fire, copy
import pdb
from tqdm import tqdm
import sys, json, argparse, os, fnmatch

from gpt4_score import server_by_gpt4

import transformers

print(transformers.__file__)
from concurrent.futures import ThreadPoolExecutor, as_completed


def generate_table_repr(data):
    keys = ["8 X 8", "12 X 12", "16 X 16", "20 X 20", "24 X 24", "28 X 28", "32 X 32"]
    cnt = 0

    def list_to_markdown_table(data):
        # Get the length of each column based on the longest item
        col_widths = [max(len(str(item)) for item in col) for col in zip(*data)]

        # Create the format string for each row
        row_format = "| " + " | ".join(f"{{:<{width}}}" for width in col_widths) + " |"
        # print(row_format)
        # Generate the table rows
        column_lenth = max([len(row) for row in data])

        for row in data:
            diff = column_lenth - len(row)
            row += [""] * diff
            if diff:
                print(row)
                print(diff)
                # pdb.set_trace()

        rows = [row_format.format(*row) for row in data]

        # Add the header separator
        header_separator = "| " + " | ".join("-" * width for width in col_widths) + " |"
        rows.insert(1, header_separator)

        return "\n".join(rows) + "\n\n"

    ret_data = []
    for k in keys:
        for data_sample in data[k]:
            clipped = data_sample["clipped_table_rows"]
            # print(list_to_markdown_table(clipped))
            data_sample.update({"clipped_table_repr": list_to_markdown_table(clipped)})
            temp = copy.deepcopy(data_sample)
            ret_data.append(temp)
            cnt += 1
    print(cnt)
    return ret_data
    # write_path = "/Users/oliver/Documents/wlr/meituan/NeedleInATable/data/NIAT_tables_8-32-1.json"
    # with open(write_path, 'w', encoding='utf-8') as fw:
    #     json.dump(data, fw)


def find_markdown_json_files(directory):
    markdown_json_files = []
    for root, dirs, files in os.walk(directory):
        for filename in fnmatch.filter(files, '*train*Markdown*.json'):
            markdown_json_files.append(os.path.join(root, filename))
    return markdown_json_files


def find_md_tables(dir_path):
    train_md_files = find_markdown_json_files(dir_path)
    table_json = []
    table_rows_set = set()
    for file_name in train_md_files:
        print(file_name)
        json_file = json.load(open(file_name, 'r', encoding="utf-8"))
        '''dict_keys(['item_id', 'input', 'output', 'table_rows', 'table_repr', 'table_repr_type', 'ori_query', 'answer_list', 'task_type', 'dataset_name', 'table_type', 'table_title'])'''
        for data_sample in json_file:
            if data_sample["table_repr_type"] == "Markdown":
                table_size = f"{len(data_sample['table_rows'])} X {len(data_sample['table_rows'][0])}"
                if len(data_sample['table_rows']) > 8 and len(data_sample['table_rows'][0]) > 8 and data_sample['table_repr'] not in table_rows_set:
                    column_lenth = max([len(row) for row in data_sample['table_rows']])
                    for row in data_sample['table_rows']:
                        diff = column_lenth - len(row)
                        row += [""] * diff

                    table_rows_set.add(data_sample['table_repr'])
                    data_sample.update({"table_size": table_size})
                    # print(table_size)
                    table_json.append(data_sample)

    print(len(table_json))
    return table_json


'''
单个cell定位：行列表头确认➡️定位cell内容

行表头确认、第N列元素提取➡️某行的第N个元素

列表头确认，第N行元素提取➡️某列的第N个元素

多个a的b之间找最小/最大值 （ 几趟列车的到站时间、离站时间对比:A列车的离开时间是否比B列车的到站时间更晚？)

出现某个关键字的某行/某列的行/列表头信息（全部）检索

某个只出现过一个关键字的相对位置单元格检索，某个内容的前N行后M列信息
'''


def multiple_ab_minmax(data_sample):
    print("multiple_ab_minmax")


def locate_single_cell(data_sample):
    # print("locate_single_cell")
    func_name = "locate_single_cell"
    row_headers = [row[0] for row in data_sample["table_rows"]]  # 行表头：每行第一个元素组成的列表
    column_headers = data_sample["table_rows"][0]  # 每列第一个元素组成的列表，即表格的第一行
    from collections import Counter

    row_count = Counter(row_headers)
    column_count = Counter(column_headers)

    unique_row_elements = [num for num, cnt in row_count.items() if cnt == 1]
    unique_column_elements = [num for num, cnt in column_count.items() if cnt == 1]
    try:
        row_header = list(unique_row_elements)[random.randrange(len(unique_row_elements))]
        column_header = list(unique_column_elements)[random.randrange(len(unique_column_elements))]
    except:
        return None, None, None
        # pdb.set_trace()

    row_header_last_index = len(row_headers) - 1 - row_headers[::-1].index(row_header)

    column_header_last_index = len(column_headers) - 1 - column_headers[::-1].index(column_header)
    try:
        ground_truth = data_sample['table_rows'][row_header_last_index][column_header_last_index]
    except:
        pdb.set_trace()
    query = f"In the table above, what is the element located in the cell at the intersection of the row header {row_header} and the column header {column_header}?"

    return query, ground_truth, func_name


def col_header_find_N_cell(data_sample):  # find the Nth cell in a certain col
    # print("col_header_find_N_cell")
    func_name = "col_header_find_N_cell"
    col_headers = data_sample["table_rows"][0]  # 每列第一个元素组成的列表，即表格的第一行
    from collections import Counter
    col_count = Counter(col_headers)

    unique_col_elements = [num for num, cnt in col_count.items() if cnt == 1]
    try:
        col_header = list(unique_col_elements)[random.randrange(len(unique_col_elements))]
    except:
        return None, None, None
    col_header_index = col_headers.index(col_header)

    N = random.randrange(len(data_sample["table_rows"]))
    try:
        ground_truth = data_sample['table_rows'][N][col_header_index]
    except:
        pdb.set_trace()
    query = f"What is the {N}th cell value with header col {col_header}?"

    return query, ground_truth, func_name


def row_header_find_N_cell(data_sample):  # find the Nth row in a certain column
    # print("row_header_find_N_cell")
    func_name = "row_header_find_N_cell"
    row_headers = [row[0] for row in data_sample["table_rows"]]  # 行表头：每行第一个元素组成的列表
    from collections import Counter

    row_count = Counter(row_headers)

    unique_row_elements = [num for num, cnt in row_count.items() if cnt == 1]

    try:
        row_header = list(unique_row_elements)[random.randrange(len(unique_row_elements))]
    except:
        return None, None, None

    row_header_last_index = len(row_headers) - 1 - row_headers[::-1].index(row_header)

    # N_row = [row[0] for row in data_sample["table_rows"]]

    N = random.randrange(len(data_sample["table_rows"][0]))  # 一共有N行，即二位表格的长度
    try:
        ground_truth = data_sample['table_rows'][row_header_last_index][N]
    except:
        pdb.set_trace()

    query = f"What is the {N}th cell value with header row {row_header}?"

    return query, ground_truth, func_name


def certain_key_all_location(data_sample):
    # print("certain_key_all_location")
    func_name = "certain_key_all_location"
    table_rows = data_sample["table_rows"]
    element_positions = {}
    for i, row in enumerate(table_rows):
        for j, element in enumerate(row):
            if element not in element_positions:
                element_positions[element] = []
            element_positions[element].append((i + 1, j + 1))

    element_positions_keys = list(element_positions.keys())
    random_element = random.choice(element_positions_keys)
    # rdm_idx = random.randint(element_positions_keys)

    query = f'How many cell contains the value {random_element}? Please provide all row IDs and column IDs of all cells contain  "{random_element}".'

    ground_truth = f"There are {len(element_positions[random_element])} cells contains cell value {random_element}, here they are: "
    for pos in element_positions[random_element]:
        ground_truth += f"({pos[0], pos[0]}), "

    return query, ground_truth, func_name


def certain_key_count(data_sample):
    func_name = "certain_key_count"
    from collections import Counter
    all_cell = []
    for row in data_sample["table_rows"]:
        all_cell += row

    cell_count = Counter(all_cell)

    cell_to_ask = random.choice(list(cell_count.keys()))

    ground_truth = cell_count[cell_to_ask]

    query = f"In the table above, how many cells contain the value {cell_to_ask}?"
    return query, ground_truth, func_name


def certain_cell_navigate(data_sample):
    # print("certain_cell_navigate")
    func_name = "certain_cell_navigate"
    table_repr = data_sample["table_rows"]
    element_positions = {}
    for i, row in enumerate(table_repr):
        for j, element in enumerate(row):
            if element not in element_positions:
                element_positions[element] = []
            element_positions[element].append((i + 1, j + 1))

    unique_element_positions = {}
    for k in element_positions:
        if len(element_positions[k]) == 1:
            unique_element_positions[k] = list(element_positions[k])[0]
    # print(unique_element_positions)
    target_cell, cell_position = random.choice(list(unique_element_positions.items()))
    base_position = cell_position
    base_row, base_col = base_position
    table_rows, table_columns = len(table_repr), len(table_repr[0])

    max_row_offset = table_rows - base_row - 1
    min_row_offset = -base_row
    max_col_offset = table_columns - base_col - 1
    min_col_offset = -base_col

    relative_row = random.randint(min_row_offset, max_row_offset)
    relative_col = random.randint(min_col_offset, max_col_offset)

    # query = f"How many cell contains the value {cell_to_ask}?"
    query = ("You are provided with a two-dimensional table and need to locate the content of a specific cell. The following information is given:\n\nBase Position:\n\n"
             f"Row index: {base_row}\nColumn index: {base_col}\nRelative Position:\n\nRow offset: {relative_row}\nColumn offset: {relative_col}\nSearch Instructions:\n\n"
             f"If relative_row > 0, move downwards from the base row index.\nIf relative_row < 0, move upwards from the base row index.\nIf relative_col > 0, move rightwards from the base column index.\nIf relative_col < 0, move leftwards from the base column index.\n"
             f"Calculate the new target position (new_row, new_col) using these offsets from the base position, and return the content of the cell located at this new position")
    try:
        ground_truth = table_repr[relative_row + base_row][relative_col + base_col]
    except:
        pdb.set_trace()

    return query, ground_truth, func_name


#     '''You are provided with a two-dimensional table and need to locate the content of a specific cell. The following information is given:
#
# Base Position:
#
# Row index: base_row
# Column index: base_col
# Relative Position:
#
# Row offset: {relative_row}\nColumn offset: {relative_col}\nSearch Instructions:
#
# If relative_row > 0, move downwards from the base row index.
# If relative_row < 0, move upwards from the base row index.
# If relative_col > 0, move rightwards from the base column index.
# If relative_col < 0, move leftwards from the base column index.
# Calculate the new target position (new_row, new_col) using these offsets from the base position, and return the content of the cell located at this new position'''


func = {
    0: locate_single_cell,
    1: row_header_find_N_cell,
    2: col_header_find_N_cell,
    3: certain_key_all_location,
    4: certain_key_count,
    5: certain_cell_navigate,
}


def list_to_markdown_table(data):
    # Get the length of each column based on the longest item
    col_widths = [max(len(str(item)) for item in col) for col in zip(*data)]

    # Create the format string for each row
    row_format = "| " + " | ".join(f"{{:<{width}}}" for width in col_widths) + " |"
    # print(row_format)
    # Generate the table rows
    column_lenth = max([len(row) for row in data])

    for row in data:
        diff = column_lenth - len(row)
        row += [""] * diff
        if diff:
            print(row)
            print(diff)
            # pdb.set_trace()

    rows = [row_format.format(*row) for row in data]

    # Add the header separator
    header_separator = "| " + " | ".join("-" * width for width in col_widths) + " |"
    rows.insert(1, header_separator)

    return "\n".join(rows) + "\n\n"


switch_func_idx_dict = {}


def main(
        read_file_path: str = r"/Users/oliver/Documents/wlr/meituan/NeedleInATable/data/NIAT_tables_from_public_datasets.json",
        dir_path: str = r"/Users/oliver/Documents/wlr/meituan/IFT-Data-For-Tabular-Tasks/",
        output_file_name: str = "1108_wtq_gpt4o_cotprocess_train.json",
):
    # data_json = generate_table_repr(json.load(open(read_file_path, 'r', encoding="utf-8")))
    # random.shuffle(data_json)
    # data_sample = data_json[0]
    # table_rows = data_sample["clipped_table_rows"]

    md_tables_json = find_md_tables(dir_path)
    random.shuffle(md_tables_json)
    # certain_cell_navigate(md_tables_json[0])
    cnt = 0
    '''dict_keys(['item_id', 'input', 'output', 'table_rows', 'table_repr', 'table_repr_type', 'ori_query', 'answer_list', 'task_type', 'dataset_name', 'table_type', 'table_title'])'''

    def process_data_sample(data_sample):
        switch_func_idx = int(random.random() * 6)
        global switch_func_idx_dict
        switch_func_idx_dict[switch_func_idx] = switch_func_idx_dict.get(switch_func_idx, 0) + 1
        query, gt, func_name = func[switch_func_idx](data_sample)
        if query and gt:
            checked_table_repr = list_to_markdown_table(data_sample['table_rows'])
            data_sample.update({"table_repr": checked_table_repr})
            checked_table_repr = list_to_markdown_table(data_sample['table_rows'])
            gpt_prompt = f"Carefully review the table, question, and provided ground truth answer. \n\nTable:{checked_table_repr}\n\nQuestion: {query}\n\nGround truth Answer: {gt}\n Generate a detailed, step-by-step reasoning process to accurately answer the question. Conclude with the final answer."
            messages = [{"role": "user", "content": gpt_prompt}]
            gpt_output = server_by_gpt4(messages)
            data_sample.update({"gpt4o_output": gpt_output, "gpt4o_input": gpt_prompt, "gpt4o_ground_truth": gt, "lookup_type": func_name,"Question":query})
            output_json.append(copy.deepcopy(data_sample))

    output_json = []
    cnt = 0
    # md_tables_json = md_tables_json[:500]
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_data_sample, data_sample): data_sample for data_sample in md_tables_json}

        for future in tqdm(as_completed(futures), total=len(md_tables_json)):
            future.result()
            cnt += 1

    with open(output_file_name, 'w', encoding='utf-8') as f:
        json.dump(output_json, f, indent=4)
    global switch_func_idx_dict
    print(switch_func_idx_dict)
    # for data_sample in md_tables_json:
    #     # certain_cell_navigate(data_sample)
    #     # 0-5均匀随机数
    #
    #     query, gt = func[int(random.random() * 6)](data_sample)
    #     if query and gt:
    #         checked_table_repr = list_to_markdown_table(data_sample['table_rows'])
    #         data_sample.update({"table_repr": checked_table_repr})
    #         gpt_prompt = f"Carefully review the table, question, and provided ground truth answer. \n\nTable:{checked_table_repr}\n\nQuestion: {query}\n\nGround truth Answer: {gt}\n Generate a detailed, step-by-step reasoning process to accurately answer the question. Conclude with the final answer."
    #         messages = [{"role": "user", "content": gpt_prompt}]
    #         gpt_output = server_by_gpt4(messages)
    #         data_sample.update({"gpt4o_output": gpt_output})
    #         cnt += 1
    # print(cnt)


if __name__ == "__main__":
    fire.Fire(main)
