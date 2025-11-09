import random

import fire, copy
import pdb
from tqdm import tqdm
import sys, json, argparse, os
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures

local_transformers_path = "/mnt/dolphinfs/hdd_pool/docker/user/hadoop-aipnlp/internship/wlr_test/"
sys.path.insert(0, local_transformers_path)
from gpt4_score import server_by_gpt4

# local_transformers_path = "/mnt/dolphinfs/hdd_pool/docker/user/hadoop-aipnlp/internship/wlr_test/pkgs/transformers/src"
# sys.path.insert(0, local_transformers_path)
import transformers
from lookup_build import locate_single_cell, col_header_find_N_cell, row_header_find_N_cell, certain_key_all_location, certain_key_count, certain_cell_navigate

print(transformers.__file__)
switch_func_idx_dict = {}

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


def generate_table_repr(data):
    cnt = 0

    ret_data = []
    for data_sample in data:
        clipped = data_sample["table_rows"]
        # print(list_to_markdown_table(clipped))
        data_sample.update({"table_repr": list_to_markdown_table(clipped)})
        temp = copy.deepcopy(data_sample)
        ret_data.append(temp)
        cnt += 1
    print(cnt)
    return ret_data


def niat_generate_cot_wo_demos(data_json, output_file_name,):
    output_json = []
    cnt = 0
    processed_json = []
    for data_sample in data_json:
        markdown_table_str, table_rows = list_to_markdown_table(data_sample['table_rows']), data_sample['table_rows']
        # print(data_sample.keys())
        # pdb.set_trace()
        tab_len = len(table_rows)
        row_num,col_num = len(table_rows),len(table_rows[0])
        # row_num = data_sample['row_num']
        # col_num = data_sample['col_num']

        for _ in range(tab_len * 8):
            i, j = random.randint(1, row_num), random.randint(1, col_num)
            temp = copy.deepcopy(data_sample)
            try:
                gt = table_rows[i - 1][j - 1]
            except:
                continue
                # print(table_rows)
                # pdb.set_trace()
            query_input = f"Now, generate the thinking process to find the target cell value in detail. The target cell to be retrieved is located at the position where the row_ID is {i} and the column_ID is {j}, and the target cell value is {gt}."
            instruction = 'You are an expert in analyzing and extracting information from tabular data. You will receive a table in Markdown format, where each row is separated by a newline, and each column within a row is delimited by the “|” symbol. Your task is to read the table carefully and retrieve the value from a specified cell, based on provided row and column IDs.\n\n**Instructions**\n\n1.	Row and Column IDs: Both row and column IDs start from 1.\n2.	Header Row: The first row (row_ID 1) is the header row and should be counted when locating the target row.\n3.	Empty Cells: If the target cell is empty, return an empty string ("") as the cell_value.\n\n'
            prompt_template = f"{instruction}Carefully review the table, question, and provided ground truth answer. \n\nTable:\n\n{markdown_table_str}\n\nQuestion: {query_input}\n\nGenerate a detailed, step-by-step reasoning process to accurately answer the question. Conclude with the final answer."
            # print(prompt_template)
            # pdb.set_trace()
            messages = [
                {"role": "user", "content": prompt_template}
            ]
            temp.update({"gpt_output": messages, "pos_id": f"{i}_{j}"})
            processed_json.append(temp)
    random.shuffle(processed_json)
    print(f"len of processed_json: {len(processed_json)}")

    # processed_json = processed_json[:data_num]

    def process_data_sample(data_sample):
        messages = data_sample['gpt_output']
        gpt_output = server_by_gpt4(messages)
        data_sample.update({"gpt_output": gpt_output})
        return data_sample

    output_json = []
    cnt = 0

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_data_sample, data_sample): data_sample for data_sample in processed_json}

        for future in tqdm(as_completed(futures), total=len(processed_json)):
            data_sample = future.result()
            output_json.append(copy.deepcopy(data_sample))


    with open(output_file_name, 'w', encoding='utf-8') as f:
        json.dump(output_json, f, indent=4)
    print(f"len_output_json: {len(output_json)}")


def lookup_generate_cot(md_tables_json,output_file_name):
    random.shuffle(md_tables_json)
    cnt = 0
    '''dict_keys(['item_id', 'input', 'output', 'table_rows', 'table_repr', 'table_repr_type', 'ori_query', 'answer_list', 'task_type', 'dataset_name', 'table_type', 'table_title'])'''
    func = {
        0: locate_single_cell,
        1: row_header_find_N_cell,
        2: col_header_find_N_cell,
        3: certain_key_all_location,
        4: certain_key_count,
        5: certain_cell_navigate,
    }

    def process_data_sample(data_sample):
        global switch_func_idx_dict
        for _ in range(32 * 16):
            switch_func_idx = int(random.random() * 6)
            switch_func_idx_dict[switch_func_idx] = switch_func_idx_dict.get(switch_func_idx, 0) + 1


            query, gt, func_name = func[switch_func_idx](data_sample)
            if query and gt:
                checked_table_repr = list_to_markdown_table(data_sample['table_rows'])
                data_sample.update({"table_repr": checked_table_repr})
                checked_table_repr = list_to_markdown_table(data_sample['table_rows'])
                gpt_prompt = f"Carefully review the table, question, and provided ground truth answer. \n\nTable:{checked_table_repr}\n\nQuestion: {query}\n\nGround truth Answer: {gt}\n Generate a detailed, step-by-step reasoning process to accurately answer the question. Conclude with the final answer."
                messages = [{"role": "user", "content": gpt_prompt}]
                gpt_output = server_by_gpt4(messages)
                data_sample.update({"gpt4o_output": gpt_output, "gpt4o_input": gpt_prompt, "gpt4o_ground_truth": gt, "lookup_type": func_name, "Question": query})
                output_json.append(copy.deepcopy(data_sample))

    output_json = []
    cnt = 0
    # md_tables_json = md_tables_json[:500]
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_data_sample, data_sample): data_sample for data_sample in md_tables_json}

        for future in tqdm(as_completed(futures), total=len(md_tables_json)):
            future.result()
            cnt += 1
    print(len(output_json))
    with open(output_file_name, 'w', encoding='utf-8') as f:
        json.dump(output_json, f, indent=4)
    global switch_func_idx_dict
    print(switch_func_idx_dict)


def main(
        task_name: str = "NIAT",
        read_file_path: str = r"/mnt/dolphinfs/hdd_pool/docker/user/hadoop-aipnlp/FMG/internship/wlr_test/NeedleInATable/data/synthesized_tables_MarkDown_3795_samples.json",
        niat_output_file_name: str = r"/mnt/dolphinfs/hdd_pool/docker/user/hadoop-aipnlp/FMG/internship/wlr_test/NeedleInATable/data/gpt4/1128_synth_niat_gpt4o_build_cot_output.json",
        lookup_output_file_name: str = r"/mnt/dolphinfs/hdd_pool/docker/user/hadoop-aipnlp/FMG/internship/wlr_test/NeedleInATable/data/gpt4/1128_synth_lookup_gpt4o_build_cot_output.json",
        data_num: int = 2000,
        batch_size: int = 32,
):


    json_object = json.load(open(read_file_path, 'r', encoding="utf-8"))
    data_json = [sample for sample in json_object if "table_32_32" in sample['id']]
    data_json = generate_table_repr(data_json)

    # random.shuffle(data_json)
    niat_data_json = data_json[:12]
    niat_generate_cot_wo_demos(niat_data_json, niat_output_file_name)

    lookup_data_json = data_json[12:]
    lookup_generate_cot(lookup_data_json, lookup_output_file_name)


if __name__ == "__main__":
    fire.Fire(main)
