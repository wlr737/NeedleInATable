import random

import fire, copy
import pdb
from tqdm import tqdm
import sys, json, argparse, os
from concurrent.futures import ThreadPoolExecutor, as_completed
import concurrent.futures
from gpt4_score import server_by_gpt4

import transformers


def select_demon(prompt):
    data_json = json.load(open(file_path, 'r', encoding="utf-8"))
    generate_table_repr()
    keys = ["8 X 8", "12 X 12", "16 X 16", "20 X 20", "24 X 24", "28 X 28", "32 X 32"]
    output_json = []
    for k in keys:
        for data_sample in data_json[k]:
            table_len = int(k.split(" ")[-1])
            for i in range(table_len):
                for j in range(table_len):
                    markdown_table_str, table_rows = data_sample['clipped_table_repr'], data_sample['clipped_table_rows']
                    query_input = f"Now, retrieve the cell value at the position where the row_ID is {i + 1} and the column_ID is {j + 1}. As this work is crucial to my career, please proceed thoughtfully with careful attention to detail, and provide your reasoning process in detail.\n\nThe answer is:"
                    prompt_template = prompt.replace("{markdown_table_str}", markdown_table_str).replace("{i+1}", str(i + 1)).replace("{j+1}", str(j + 1))
                    # print(prompt_template)
                    # pdb.set_trace()
                    messages = [
                        {"role": "user", "content": prompt_template}
                    ]
                    try:
                        gt = table_rows[i][j]
                    except:
                        print(table_rows)
                        pdb.set_trace()
                    if i == 13 and j == 21 and k == "24 X 24":
                        print(prompt_template)
                        pdb.set_trace()
                    # print(data_sample.keys())
                    # pdb.set_trace()
                    # output_json.append({"table_rows": table_rows, "markdown_table_str": markdown_table_str,
                    #                     'table_size': data_sample['table_size'], "input": tempate_prompt, "table_id": data_sample['table_id'], "table_type": data_sample['table_type']})

    pass


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

def generate_with_demos(prompt_num,read_file_path,output_file_name,data_num):
    prompt_1 = '''You are an expert in analyzing and extracting information from tabular data. You will receive a table in Markdown format, where each row is separated by a newline, and each column within a row is delimited by the “|” symbol. Your task is to read the table carefully and retrieve the value from a specified cell, based on provided row and column IDs.

    **Instructions**

    	1.	Row and Column IDs: Both row and column IDs start from 1.
    	2.	Header Row: The first row (row_ID 1) is the header row and should be counted when locating the target row.
    	3.	Empty Cells: If the target cell is empty, return an empty string ("") as the cell_value.

    **Example**

    Given table:
    | 3        | 19  | 8      | Team                     | 14  | Rank | 6      | 9      | 1        | 18   | 2        | 13     | Chassis      | Points | 17   | Engine        | Year             | 12    | 7      | 15   | 16   | 10     | 5       | 11     |
    | -------- | --- | ------ | ------------------------ | --- | ---- | ------ | ------ | -------- | ---- | -------- | ------ | ------------ | ------ | ---- | ------------- | ---------------- | ----- | ------ | ---- | ---- | ------ | ------- | ------ |
    | KTY 1    |     |        |                          |     |      |        |        | PPIR 26  |      | ATL 2    |        | INDY 2       |        |      | TXS 7         | Dallara          |       |        |      |      |        |         |        |
    |          |     |        | Riley & Scott            |     |      |        |        | LVS 22   |      | INDY DNQ |        | WDW 2        |        |      | PHX 1         | 2000             |       |        |      |      |        | 290     |        |
    | LBH      | FON | TXS    | Lazier Partners Racing   | MDO | 38th | DET    | MIL    | STP      | HOU  | ALA      | TOR    | Dallara DW12 | 8      | HOU  | Chevrolet     | 2013             | TOR   | DET    | SNM  | BAL  | IOW    | INDY 31 | POC    |
    | DOV 2    |     | LVS 3  | WDW 15                   |     |      | ATL 17 |        | TXS 11   |      | NHA 7    |        | PHX 28       |        |      | INDY 2        | 1998             |       | TXS 6  |      |      | 5th    | PPIR 7  | 262    |
    | INDY DNQ |     | FON    | STP                      |     |      | TXS    | MIL    | ALA      |      | IMS      | SNM    | NLA          |        |      | LBH           | 2015             | POC   | TOR    | NC   | –    | IOW    | DET     | MDO    |
    | INDY 12  |     | NSH    | Dreyer & Reinbold Racing |     |      | RIR 16 | MIL    | STP 14   |      | MOT 14   | CHI    | Honda        |        |      | HMS 14        | 2006             | SNM   | KAN 15 | 18th | 122  | MIS 15 | TXS 19  | KTY    |
    | NHA 12   |     |        |                          |     |      |        |        | PPIR 8   |      | CLT 1    |        | INDY 4       |        |      | TXS 17        | Oldsmobile       |       |        |      |      |        |         |        |
    | IMS      |     | IOW    | Lazier Burns Racing      | SNM |      | DET    | TOR    | LBH      |      | ALA      | WGL    | STP          |        | 12   | PHX           | 2016             | TXS   | RDA    |      | 35th | MDO    | DET     | POC    |
    | INDY 15  |     | NSH 12 | Chevrolet                |     |      | RIR 18 | MIS 13 | FON 7    |      | NAZ 23   | TXS 7  | HMS 22       |        |      | PHX 7         | 2002             | CHI 3 | KAN 7  | 8th  | 305  | KTY 3  | PPIR 15 | GTW 15 |
    | NSH      |     | NAZ    |                          |     |      | KTY    | CHI    | RIR      |      | KAN      |        | INDY 23      |        |      | TXS           | Hemelgarn Racing |       | PPIR   |      |      | FON    | MIS     | TXS    |
    | DET      |     | POC    | STP                      | SNM |      | HOU    | IOW    | IMS      | 11   | INDY 32  | MIL    | LBH          |        | 35th | ALA           | 2014             | MDO   | HOU    | FON  |      | TOR    | TXS     | TOR    |
    | INDY 1   |     |        | Hemelgarn Racing         |     |      | 159    |        | WDW 17   |      | PHX DNS  |        | Reynard      |        |      | Ford Cosworth | 1996             |       |        |      |      |        | 14th    |        |
    |          |     |        | Dreyer & Reinbold Racing |     |      |        |        | MOT      |      | INDY DNQ |        | HMS          |        |      | PHX           | 2004             |       |        |      |      |        | 12      |        |
    |          |     |        | NHA 19                   |     |      |        |        | 8th      |      | 209      |        | LVS 24       |        |      |               | 1996–97          |       |        |      |      |        |         |        |
    | INDY 5   |     | MIL 18 | Panther Racing           | WGL |      | KAN    | MIS 6  | STP      | 140  | MOT      | CHI 10 | HMS          |        | 23rd | PHX           | 2005             | SNM   | NSH 9  | FON  |      | KTY 6  | RIR     | PPIR   |
    | TXS      |     | EDM    | STP                      | HMS |      | WGL    | KTY    | INDY DNQ |      | MIL      | MOT    | LBH          |        | –    | KAN           | 2009             | CHI   | TOR    |      | NC   | MDO    | RIR     | SNM    |
    | PPIR 1   |     | GTW 13 | PHX 3                    |     |      | NSH 1  | CHI 11 | INDY 18  |      | TXS 4    | 398    | HMS 20       |        |      | ATL 6         | 2001             | 2nd   | KTY 1  |      |      | TXS 17 | KAN 5   |        |
    | IMS      |     | ROA    | Lazier Partners Racing   | WGL |      | DET    | IOW    | ALA      | 14   | PHX      | GTW    | STP          |        | 37th | LBH           | 2017             | POC   | TXS    | SNM  |      | TOR    | DET     | MDO    |
    | INDY 19  |     | WGL    | Sam Schmidt Motorsports  | DET |      | IOW    | NSH    | MOT      | 12   | KAN      | SNM    | HMS          |        | 28th | STP           | 2007             | KTY   | RIR    | CHI  |      | MDO    | TXS     | MIS    |
    | PPIR 5   |     | TXS 10 | WDW 10                   |     |      | PPIR 4 |        | INDY 7   |      | TXS 14   |        | PHX 18       |        |      | CLT1 C        | 1999             |       | LVS 11 |      |      | 6th    | DOV 2   | 224    |
    | KAN      | 13  | RIR    | Hemelgarn-Johnson Racing | SNM |      | TXS    | WGL    | MOT1     | 37th | LBH1     | KTY    | HMS          |        | SRF2 | STP           | 2008             | EDM   | IOW    | DET  | CHI  | NSH    | MIL     | MDO    |
    | PPIR 10  |     | GTW 11 | HMS Wth                  |     |      | NSH 14 | KTY 16 | INDY 21  |      | TXS 13   | TXS    | PHX 11       |        |      | MOT 19        | 2003             | FON   | MIS 12 | 19th | 201  | NAZ 13 | KAN 13  | CHI 16 |
    |          |     |        | Infiniti                 |     |      |        |        | PHX 21   |      |          |        |              |        |      | WDW 5         | Dallara          |       |        |      |      |        |         |        |


    Now, retrieve the cell value at the position where the row_ID is 14 and the column_ID is 22. As this work is crucial to my career, please proceed thoughtfully and with careful attention to detail. Please provide your reasoning process in detail.

    The answer is:

    To retrieve the cell value at row_ID 14 and column_ID 22, I will carefully follow these steps:

    1. Understand the Table Structure

    	•	Rows: Each line separated by a newline character represents a row.
    	•	Columns: Within each row, columns are separated by the | symbol.
    	•	Row and Column Indexing:
    	•	row_IDs start from 1, so the first row (the header) is row_ID 1.
    	•	column_IDs also start from 1, with the first column in each row being column_ID 1.

    2. Locate the Target Row (row_ID 14)

    	•	Since the header row is row_ID 1 and the separator row is row_ID 2, row 14 in the table corresponds to the 14th line from the top.
    	•	The content of the 14th row in the table is:

    |          |     |        | NHA 19                   |     |      |        |        | 8th      |      | 209      |        | LVS 24       |        |      |               | 1996–97          |       |        |      |      |        |         |        |



    3. Determine the Target Column (column_ID 22)

    	•	Total Columns: According to the header row, there are 24 columns in each row.
    	•	Therefore, column_ID 22 refers to the 22nd position in this row.

    4. Parse the Row into Columns

    	•	Splitting the 14th row by |, we get the following columns (after trimming whitespace):
    	1.	(Empty)
    	2.	(Empty)
    	3.	(Empty)
    	4.	NHA 19
    	5.	(Empty)
    	6.	(Empty)
    	7.	(Empty)
    	8.	(Empty)
    	9.	8th
    	10.	(Empty)
    	11.	209
    	12.	(Empty)
    	13.	LVS 24
    	14.	(Empty)
    	15.	(Empty)
    	16.	(Empty)
    	17.	1996–97
    	18.	(Empty)
    	19.	(Empty)
    	20.	(Empty)
    	21.	(Empty)
    	22.	(Empty)  ← Target Cell
    	23.	(Empty)
    	24.	(Empty)

    5. Retrieve the Value

    	•	The value at row_ID 14 and column_ID 22 is an empty string ("").

    Final Result

    {
      "row_ID": "14",
      "column_ID": "22",
      "cell_value": ""
    }


    Now, carefully read the table provided below, follow these steps, and output your result as a JSON object in the format: {"row_ID": "m", "column_ID": "n", "cell_value": "<Corresponding Cell Value>"}

    Given Table:

    <<<table_input>>>
    <<<query_input>>>
    '''

    # prompt_2: 先找到列表头再同列提取信息
    prompt_2 = ''''You are an expert in analyzing and extracting information from tabular data. You will receive a table in Markdown format, where each row is separated by a newline, and each column within a row is delimited by the “|” symbol. Your task is to read the table carefully and retrieve the value from a specified cell, based on provided row and column IDs.

    **Instructions**

        1.	Row and Column IDs: Both row and column IDs start from 1.
        2.	Header Row: The first row (row_ID 1) is the header row and should be counted when locating the target row.
        3.	Empty Cells: If the target cell is empty, return an empty string ("") as the cell_value.

    **Example**

    Given table:
    | Region | Country      | Location                                                            | Size (m)  | Payload (metric tonnes) | Degrees of Freedom | X Horiz Disp (mm) | Y Horiz Disp (mm) | Z Vert Disp (mm) | X Horiz Vel (mm/s) | Y Horiz vel (mm/s) | Z Vert vel (mm/s) | X Horiz accel (m/s2) | Y Horiz accel (m/s2) | Z Vert accel (m/s2) | Max Freq (Hz) | Details checked | Max Freq (Hz) | Z Vert vel (mm/s) | Payload (metric tonnes) | Details checked | Z Vert accel (m/s2) | Country      | Degrees of Freedom | Region | Location                                                            | X Horiz Disp (mm) | X Horiz accel (m/s2) | Z Vert vel (mm/s) | X Horiz accel (m/s2) | X Horiz Disp (mm) | Y Horiz Disp (mm) |
    | ------ | ------------ | ------------------------------------------------------------------- | --------- | ----------------------- | ------------------ | ----------------- | ----------------- | ---------------- | ------------------ | ------------------ | ----------------- | -------------------- | -------------------- | ------------------- | ------------- | --------------- | ------------- | ----------------- | ----------------------- | --------------- | ------------------- | ------------ | ------------------ | ------ | ------------------------------------------------------------------- | ----------------- | -------------------- | ----------------- | -------------------- | ----------------- | ----------------- |
    | Africa | Algeria      | CGS Laboratory (in construction)                                    | 6.1 x 6.1 | 60                      | 6                  | ±150              | ±250              | ±100             | ±1100              | ±1100              | ±1000             | ±10                  | ±10                  | ±8                  | 100           | 30/6/2010       | 100           | ±1000             | 60                      | 30/6/2010       | ±8                  | Algeria      | 6                  | Africa | CGS Laboratory (in construction)                                    | ±150              | ±10                  | ±1000             | ±10                  | ±150              | ±250              |
    | Africa | South Africa | University of Witwatersrand                                         | 4 x 4     | 10                      | 1                  | ±750              | n/a               | n/a              | ±1000              | n/a                | n/a               | ±10                  | n/a                  | n/a                 | 40            | 17/7/2009       | 40            | n/a               | 10                      | 17/7/2009       | n/a                 | South Africa | 1                  | Africa | University of Witwatersrand                                         | ±750              | ±10                  | n/a               | ±10                  | ±750              | n/a               |
    | Asia   | China        | China Academy of Building Research, Beijing                         | 6.1 x 6.1 | 60                      | 6                  | ±150              | ±250              | ±100             | ±1000              | ±1200              | ±800              | ±15                  | ±10                  | ±8                  | 50            | ?               | 50            | ±800              | 60                      | ?               | ±8                  | China        | 6                  | Asia   | China Academy of Building Research, Beijing                         | ±150              | ±15                  | ±800              | ±15                  | ±150              | ±250              |
    | Asia   | China        | Guangzhou University                                                | 3 x 3     | 20                      | 6                  | ±100              | ±100              | ±50              | ±1000              | ±1000              | ±1000             | ±26                  | ±26                  | ±50                 | 50            | 10/7/2008       | 50            | ±1000             | 20                      | 10/7/2008       | ±50                 | China        | 6                  | Asia   | Guangzhou University                                                | ±100              | ±26                  | ±1000             | ±26                  | ±100              | ±100              |
    | Asia   | China        | Nanjing University of Technology                                    | 3 x 5     | 15                      | 3                  | ±120              | ±120              | ±120             | ±500               | ±500               | ±500              | ±10                  | ±10                  | ±10                 | 50            | ?               | 50            | ±500              | 15                      | ?               | ±10                 | China        | 3                  | Asia   | Nanjing University of Technology                                    | ±120              | ±10                  | ±500              | ±10                  | ±120              | ±120              |
    | Asia   | China        | Tongji University, Shanghai                                         | 4 x 4     | 25                      | 6                  | ±100              | ±50               | ±50              | ±1000              | ±600               | ±600              | ±40                  | ±20                  | ±40                 | 50            | ?               | 50            | ±600              | 25                      | ?               | ±40                 | China        | 6                  | Asia   | Tongji University, Shanghai                                         | ±100              | ±40                  | ±600              | ±40                  | ±100              | ±50               |
    | Asia   | China        | Xi'a University of Architecture & Technology                        | 2 x 2     | ?                       | ?                  | ?                 | ?                 | ?                | ?                  | ?                  | ?                 | ?                    | ?                    | ?                   | ?             | ?               | ?             | ?                 | ?                       | ?               | ?                   | China        | ?                  | Asia   | Xi'a University of Architecture & Technology                        | ?                 | ?                    | ?                 | ?                    | ?                 | ?                 |
    | Asia   | Singapore    | Nanyang Technological University                                    | 3 x 3     | 10                      | 1                  | ±120              | n/a               | n/a              | ±650               | n/a                | n/a               | ±20                  | n/a                  | n/a                 | 50            | 23/7/2008       | 50            | n/a               | 10                      | 23/7/2008       | n/a                 | Singapore    | 1                  | Asia   | Nanyang Technological University                                    | ±120              | ±20                  | n/a               | ±20                  | ±120              | n/a               |
    | Asia   | Hong Kong    | City University of Hong Kong                                        | ?         | ?                       | ?                  | ?                 | ?                 | ?                | ?                  | ?                  | ?                 | ?                    | ?                    | ?                   | ?             | ?               | ?             | ?                 | ?                       | ?               | ?                   | Hong Kong    | ?                  | Asia   | City University of Hong Kong                                        | ?                 | ?                    | ?                 | ?                    | ?                 | ?                 |
    | Asia   | Hong Kong    | Hong Kong Polytechnic University                                    | 3 x 3     | 10                      | ?                  | ?                 | ?                 | ?                | ?                  | ?                  | ?                 | 10                   | ?                    | ?                   | ?             | ?               | ?             | ?                 | 10                      | ?               | ?                   | Hong Kong    | ?                  | Asia   | Hong Kong Polytechnic University                                    | ?                 | 10                   | ?                 | 10                   | ?                 | ?                 |
    | Asia   | India        | Jamia Millia Islamia,New Delhi                                      | 5 x 5     | 20                      | 1                  | ±500              | n/a               | n/a              | ?                  | ?                  | ?                 | ±20                  | n/a                  | n/a                 | 100           | ?               | 100           | ?                 | 20                      | ?               | n/a                 | India        | 1                  | Asia   | Jamia Millia Islamia,New Delhi                                      | ±500              | ±20                  | ?                 | ±20                  | ±500              | n/a               |
    | Asia   | India        | IIT Guwahati                                                        | 2.5 x 2.5 | 5                       | 1                  | ±500              | n/a               | n/a              | ?                  | ?                  | ?                 | ±20                  | n/a                  | n/a                 | 100           | ?               | 100           | ?                 | 5                       | ?               | n/a                 | India        | 1                  | Asia   | IIT Guwahati                                                        | ±500              | ±20                  | ?                 | ±20                  | ±500              | n/a               |
    | Asia   | India        | CPRI Bangalore,Karnataka                                            | 3 x 3     | 10                      | 6                  | ?                 | ?                 | ?                | ?                  | ?                  | ?                 | ?                    | ?                    | ?                   | ?             | ?               | ?             | ?                 | 10                      | ?               | ?                   | India        | 6                  | Asia   | CPRI Bangalore,Karnataka                                            | ?                 | ?                    | ?                 | ?                    | ?                 | ?                 |
    | Asia   | India        | IISc, Bangalore                                                     | 1 x 1     | 0.5                     | 6                  | ±220              | ±220              | ±100             | ±570               | ±570               | ±570              | ±30                  | ±30                  | ±20                 | 50            | 23/7/2008       | 50            | ±570              | 0.5                     | 23/7/2008       | ±20                 | India        | 6                  | Asia   | IISc, Bangalore                                                     | ±220              | ±30                  | ±570              | ±30                  | ±220              | ±220              |
    | Asia   | India        | SERC, Chennai (1 of 3), Tamil Nadu                                  | 4 x 4     | 30                      | 3                  | ?                 | ?                 | ?                | ?                  | ?                  | ?                 | ?                    | ?                    | ?                   | ?             | ?               | ?             | ?                 | 30                      | ?               | ?                   | India        | 3                  | Asia   | SERC, Chennai (1 of 3), Tamil Nadu                                  | ?                 | ?                    | ?                 | ?                    | ?                 | ?                 |
    | Asia   | India        | SERC, Chennai (2 of 3),Tamil Nadu                                   | 2 x 2     | 5                       | 3                  | ?                 | ?                 | ?                | ?                  | ?                  | ?                 | ?                    | ?                    | ?                   | ?             | ?               | ?             | ?                 | 5                       | ?               | ?                   | India        | 3                  | Asia   | SERC, Chennai (2 of 3),Tamil Nadu                                   | ?                 | ?                    | ?                 | ?                    | ?                 | ?                 |
    | Asia   | India        | SERC, Chennai (3 of 3),Tamil Nadu                                   | 3 x 3     | 10                      | 6                  | ?                 | ?                 | ?                | ?                  | ?                  | ?                 | ?                    | ?                    | ?                   | ?             | ?               | ?             | ?                 | 10                      | ?               | ?                   | India        | 6                  | Asia   | SERC, Chennai (3 of 3),Tamil Nadu                                   | ?                 | ?                    | ?                 | ?                    | ?                 | ?                 |
    | Asia   | India        | Indira Gandhi Centre for Atomic Research(IGCAR), Chennai,Tamil Nadu | 3 x 3     | 10                      | 6                  | ±100              | ±100              | ±100             | 300                | 300                | ?                 | ±14.715              | ±14.715              | 9.81                | 100           | ?               | 100           | ?                 | 10                      | ?               | 9.81                | India        | 6                  | Asia   | Indira Gandhi Centre for Atomic Research(IGCAR), Chennai,Tamil Nadu | ±100              | ±14.715              | ?                 | ±14.715              | ±100              | ±100              |
    | Asia   | India        | IIT Roorkee,Uttarakhand                                             | 3.5 x 3.5 | 20                      | 2                  | ?                 | n/a               | ?                | ?                  | n/a                | ?                 | ?                    | n/a                  | ?                   | ?             | ?               | ?             | ?                 | 20                      | ?               | ?                   | India        | 2                  | Asia   | IIT Roorkee,Uttarakhand                                             | ?                 | ?                    | ?                 | ?                    | ?                 | n/a               |
    | Asia   | India        | IIT Kanpur,Uttar Pradesh                                            | 1.2 x 1.8 | 4                       | 1                  | ±75               | n/a               | n/a              | ±1500              | n/a                | n/a               | ±50                  | n/a                  | n/a                 | 50            | 25/6/2009       | 50            | n/a               | 4                       | 25/6/2009       | n/a                 | India        | 1                  | Asia   | IIT Kanpur,Uttar Pradesh                                            | ±75               | ±50                  | n/a               | ±50                  | ±75               | n/a               |
    | Asia   | India        | Bengal Engineering and Science University, Shibpur,West Bengal      | 1.5 x 1.5 | ?                       | 1                  | ?                 | n/a               | n/a              | ?                  | n/a                | n/a               | ?                    | n/a                  | n/a                 | ?             | 19/11/2009      | ?             | n/a               | ?                       | 19/11/2009      | n/a                 | India        | 1                  | Asia   | Bengal Engineering and Science University, Shibpur,West Bengal      | ?                 | ?                    | n/a               | ?                    | ?                 | n/a               |
    | Asia   | Iran         | Iran University of Science & Technology (IUST)                      | 2 x 0.5   | 5                       | 1                  | ±60               | n/a               | n/a              | ?                  | n/a                | n/a               | ±6.5                 | n/a                  | n/a                 | ?             | ?               | ?             | n/a               | 5                       | ?               | n/a                 | Iran         | 1                  | Asia   | Iran University of Science & Technology (IUST)                      | ±60               | ±6.5                 | n/a               | ±6.5                 | ±60               | n/a               |
    | Asia   | Iran         | Sharif University of Technology (SUT))                              | 4 x 4     | 30                      | 3                  | ±125              | ±200              | ?                | ±500               | ±800               | ?                 | ±50                  | ±40                  | ?                   | 50            | 7/19/2011       | 50            | ?                 | 30                      | 7/19/2011       | ?                   | Iran         | 3                  | Asia   | Sharif University of Technology (SUT))                              | ±125              | ±50                  | ?                 | ±50                  | ±125              | ±200              |
    | Asia   | Japan        | Aichi Institute of Technology                                       | 11 x 6    | 136                     | 1                  | ±150              | ?                 | ?                | ±1000              | ?                  | ?                 | ±10                  | ?                    | ?                   | 50            | ?               | 50            | ?                 | 136                     | ?               | ?                   | Japan        | 1                  | Asia   | Aichi Institute of Technology                                       | ±150              | ±10                  | ?                 | ±10                  | ±150              | ?                 |
    | Asia   | Japan        | Building Research Institute                                         | 3 x 4     | 20                      | 3                  | ?                 | ?                 | ?                | ?                  | ?                  | ?                 | ?                    | ?                    | ?                   | ?             | ?               | ?             | ?                 | 20                      | ?               | ?                   | Japan        | 3                  | Asia   | Building Research Institute                                         | ?                 | ?                    | ?                 | ?                    | ?                 | ?                 |
    | Asia   | Japan        | Central Research Institute of Electric Power Industry               | 5 x 5     | 60                      | 1                  | ±500              | n/a               | n/a              | ±1500              | n/a                | n/a               | ±10                  | n/a                  | n/a                 | 30            | 12/3/2008       | 30            | n/a               | 60                      | 12/3/2008       | n/a                 | Japan        | 1                  | Asia   | Central Research Institute of Electric Power Industry               | ±500              | ±10                  | n/a               | ±10                  | ±500              | n/a               |
    | Asia   | Japan        | NIED ‘E-Defence’ Laboratory, Miki City                              | 20 x 15   | 1200                    | 6                  | ±1000             | ±1000             | ±500             | ±2000              | ±2000              | ±700              | ±9                   | ±9                   | ±15                 | 50            | 3/3/2008        | 50            | ±700              | 1200                    | 3/3/2008        | ±15                 | Japan        | 6                  | Asia   | NIED ‘E-Defence’ Laboratory, Miki City                              | ±1000             | ±9                   | ±700              | ±9                   | ±1000             | ±1000             |

    To retrieve the specified cell, I’ll proceed by following these steps carefully:

    	1.	Identify the Target Cell:
    	•	The row_ID is 2, which corresponds to the second row in the table, including the header row.
    	•	The column_ID is 17, meaning I should count to the 17th column in the second row.
    	2.	Count Columns in the Header and Locate Column 17:
    	•	The first row is the header row. By counting across the header, I locate column 17, which is titled “Details checked”.
    	3.	Find the Value in Row 2, Column 17:
    	•	Moving to row 2 under this column title, I locate the value, which is “30/6/2010”.

    Thus, the value in the cell at row_ID 2 and column_ID 17 is:

    Answer: 30/6/2010

    Returning this as a JSON object:

    {
      "row_ID": "2",
      "column_ID": "16",
      "cell_value": "30/6/2010"
    }

    Now, carefully read the table provided below, follow these steps, and output your result as a JSON object in the format: {"row_ID": "m", "column_ID": "n", "cell_value": "<Corresponding Cell Value>"}

    Given Table:

    <<<table_input>>>
    <<<query_input>>>
    '''

    prompt_3 = '''You are an expert in analyzing and extracting information from tabular data. You will receive a table in Markdown format, where each row is separated by a newline, and each column within a row is delimited by the “|” symbol. Your task is to read the table carefully and retrieve the value from a specified cell, based on provided row and column IDs.

    **Instructions**

    	1.	Row and Column IDs: Both row and column IDs start from 1.
    	2.	Header Row: The first row (row_ID 1) is the header row and should be counted when locating the target row.
    	3.	Empty Cells: If the target cell is empty, return an empty string ("") as the cell_value.

    **Example**

    Given table:

    | -         | -                     | -      | Regular season | Regular season | Regular season | Regular season | Regular season | Playoffs | Playoffs | Playoffs | Playoffs | Playoffs | -   | -   | Playoffs | Playoffs | Regular season | Regular season | Regular season | -                     | -      | Playoffs | Regular season | Playoffs | Regular season | -   | Playoffs | -   | Regular season | Playoffs | Playoffs |
    | --------- | --------------------- | ------ | -------------- | -------------- | -------------- | -------------- | -------------- | -------- | -------- | -------- | -------- | -------- | --- | --- | -------- | -------- | -------------- | -------------- | -------------- | --------------------- | ------ | -------- | -------------- | -------- | -------------- | --- | -------- | --- | -------------- | -------- | -------- |
    | Season    | Team                  | League | -              | GP             | G              | A              | Pts            | PIM      | -        | GP       | G        | A        | Pts | PIM | GP       | PIM      | G              | -              | A              | Team                  | League | -        | Pts            | A        | GP             | Pts | G        | Pts | A              | G        | -        |
    | 1982–83   | Prince Albert Mintos  | SMHL   | -              | 28             | 11             | 11             | 22             | 170      | -        | —        | —        | —        | —   | —   | —        | 170      | 11             | -              | 11             | Prince Albert Mintos  | SMHL   | -        | 22             | —        | 28             | —   | —        | —   | 11             | —        | -        |
    | 1982–83   | Prince Albert Raiders | WHL    | -              | 6              | 0              | 1              | 1              | 9        | -        | —        | —        | —        | —   | —   | —        | 9        | 0              | -              | 1              | Prince Albert Raiders | WHL    | -        | 1              | —        | 6              | —   | —        | —   | 1              | —        | -        |
    | 1983–84   | Prince Albert Raiders | WHL    | -              | 70             | 2              | 7              | 9              | 233      | -        | 5        | 0        | 0        | 0   | 4   | 5        | 233      | 2              | -              | 7              | Prince Albert Raiders | WHL    | -        | 9              | 0        | 70             | 0   | 0        | 0   | 7              | 0        | -        |
    | 1984–85   | Prince Albert Raiders | WHL    | -              | 72             | 8              | 30             | 38             | 247      | -        | 13       | 1        | 0        | 1   | 34  | 13       | 247      | 8              | -              | 30             | Prince Albert Raiders | WHL    | -        | 38             | 0        | 72             | 1   | 1        | 1   | 30             | 1        | -        |
    | 1984–85   | Prince Albert Raiders | M-Cup  | -              | —              | —              | —              | —              | —        | -        | 5        | 0        | 1        | 1   | 10  | 5        | —        | —              | -              | —              | Prince Albert Raiders | M-Cup  | -        | —              | 1        | —              | 1   | 0        | 1   | —              | 0        | -        |
    | 1985–86   | Prince Albert Raiders | WHL    | -              | 70             | 14             | 34             | 48             | 177      | -        | 20       | 1        | 8        | 9   | 63  | 20       | 177      | 14             | -              | 34             | Prince Albert Raiders | WHL    | -        | 48             | 8        | 70             | 9   | 1        | 9   | 34             | 1        | -        |
    | 1986–87   | Chicago Blackhawks    | NHL    | -              | 63             | 1              | 8              | 9              | 146      | -        | 3        | 0        | 0        | 0   | 10  | 3        | 146      | 1              | -              | 8              | Chicago Blackhawks    | NHL    | -        | 9              | 0        | 63             | 0   | 0        | 0   | 8              | 0        | -        |
    | 1987–88   | Chicago Blackhawks    | NHL    | -              | 54             | 1              | 6              | 7              | 185      | -        | 5        | 0        | 0        | 0   | 27  | 5        | 185      | 1              | -              | 6              | Chicago Blackhawks    | NHL    | -        | 7              | 0        | 54             | 0   | 0        | 0   | 6              | 0        | -        |
    | 1987–88   | Saginaw Hawks         | IHL    | -              | 6              | 0              | 3              | 3              | 37       | -        | —        | —        | —        | —   | —   | —        | 37       | 0              | -              | 3              | Saginaw Hawks         | IHL    | -        | 3              | —        | 6              | —   | —        | —   | 3              | —        | -        |
    | 1988–89   | Chicago Blackhawks    | NHL    | -              | 79             | 18             | 36             | 54             | 352      | -        | 16       | 0        | 8        | 8   | 84  | 16       | 352      | 18             | -              | 36             | Chicago Blackhawks    | NHL    | -        | 54             | 8        | 79             | 8   | 0        | 8   | 36             | 0        | -        |
    | 1989–90   | Chicago Blackhawks    | NHL    | -              | 59             | 5              | 23             | 28             | 301      | -        | 20       | 2        | 4        | 6   | 46  | 20       | 301      | 5              | -              | 23             | Chicago Blackhawks    | NHL    | -        | 28             | 4        | 59             | 6   | 2        | 6   | 23             | 2        | -        |
    | 1990–91   | Chicago Blackhawks    | NHL    | -              | 75             | 14             | 15             | 29             | 191      | -        | 6        | 0        | 1        | 1   | 36  | 6        | 191      | 14             | -              | 15             | Chicago Blackhawks    | NHL    | -        | 29             | 1        | 75             | 1   | 0        | 1   | 15             | 0        | -        |
    | 1991–92   | Edmonton Oilers       | NHL    | -              | 79             | 15             | 32             | 47             | 220      | -        | 16       | 3        | 9        | 12  | 44  | 16       | 220      | 15             | -              | 32             | Edmonton Oilers       | NHL    | -        | 47             | 9        | 79             | 12  | 3        | 12  | 32             | 3        | -        |
    | 1992–93   | Edmonton Oilers       | NHL    | -              | 83             | 15             | 30             | 45             | 210      | -        | —        | —        | —        | —   | —   | —        | 210      | 15             | -              | 30             | Edmonton Oilers       | NHL    | -        | 45             | —        | 83             | —   | —        | —   | 30             | —        | -        |
    | 1993–94   | Edmonton Oilers       | NHL    | -              | 57             | 3              | 13             | 16             | 140      | -        | —        | —        | —        | —   | —   | —        | 140      | 3              | -              | 13             | Edmonton Oilers       | NHL    | -        | 16             | —        | 57             | —   | —        | —   | 13             | —        | -        |
    | 1993–94   | Winnipeg Jets         | NHL    | -              | 13             | 1              | 4              | 5              | 51       | -        | —        | —        | —        | —   | —   | —        | 51       | 1              | -              | 4              | Winnipeg Jets         | NHL    | -        | 5              | —        | 13             | —   | —        | —   | 4              | —        | -        |
    | 1994–95   | Winnipeg Jets         | NHL    | -              | 44             | 3              | 15             | 18             | 139      | -        | —        | —        | —        | —   | —   | —        | 139      | 3              | -              | 15             | Winnipeg Jets         | NHL    | -        | 18             | —        | 44             | —   | —        | —   | 15             | —        | -        |
    | 1995–96   | Winnipeg Jets         | NHL    | -              | 82             | 7              | 23             | 30             | 205      | -        | 6        | 2        | 1        | 3   | 30  | 6        | 205      | 7              | -              | 23             | Winnipeg Jets         | NHL    | -        | 30             | 1        | 82             | 3   | 2        | 3   | 23             | 2        | -        |
    | 1996–97   | Phoenix Coyotes       | NHL    | -              | 66             | 3              | 17             | 20             | 164      | -        | —        | —        | —        | —   | —   | —        | 164      | 3              | -              | 17             | Phoenix Coyotes       | NHL    | -        | 20             | —        | 66             | —   | —        | —   | 17             | —        | -        |
    | 1996–97   | Montréal Canadiens    | NHL    | -              | 9              | 1              | 1              | 2              | 23       | -        | 5        | 0        | 0        | 0   | 17  | 5        | 23       | 1              | -              | 1              | Montréal Canadiens    | NHL    | -        | 2              | 0        | 9              | 0   | 0        | 0   | 1              | 0        | -        |
    | 1997–98   | Montréal Canadiens    | NHL    | -              | 81             | 4              | 30             | 34             | 122      | -        | 10       | 0        | 1        | 1   | 14  | 10       | 122      | 4              | -              | 30             | Montréal Canadiens    | NHL    | -        | 34             | 1        | 81             | 1   | 0        | 1   | 30             | 0        | -        |
    | 1998–99   | Montréal Canadiens    | NHL    | -              | 11             | 0              | 2              | 2              | 48       | -        | —        | —        | —        | —   | —   | —        | 48       | 0              | -              | 2              | Montréal Canadiens    | NHL    | -        | 2              | —        | 11             | —   | —        | —   | 2              | —        | -        |
    | 1998–99   | Chicago Blackhawks    | NHL    | -              | 64             | 6              | 15             | 21             | 107      | -        | —        | —        | —        | —   | —   | —        | 107      | 6              | -              | 15             | Chicago Blackhawks    | NHL    | -        | 21             | —        | 64             | —   | —        | —   | 15             | —        | -        |
    | 1999–2000 | Chicago Blackhawks    | NHL    | -              | 37             | 0              | 7              | 7              | 40       | -        | —        | —        | —        | —   | —   | —        | 40       | 0              | -              | 7              | Chicago Blackhawks    | NHL    | -        | 7              | —        | 37             | —   | —        | —   | 7              | —        | -        |
    | 1999–2000 | Dallas Stars          | NHL    | -              | 26             | 1              | 2              | 3              | 22       | -        | 23       | 0        | 0        | 0   | 33  | 23       | 22       | 1              | -              | 2              | Dallas Stars          | NHL    | -        | 3              | 0        | 26             | 0   | 0        | 0   | 2              | 0        | -        |
    | 2000–01   | Toronto Maple Leafs   | NHL    | -              | 74             | 4              | 7              | 11             | 93       | -        | 2        | 0        | 0        | 0   | 2   | 2        | 93       | 4              | -              | 7              | Toronto Maple Leafs   | NHL    | -        | 11             | 0        | 74             | 0   | 0        | 0   | 7              | 0        | -        |


    Now, retrieve the cell value at the position where the row_ID is 23 and the column_ID is 7. As this work is crucial to my career, please proceed thoughtfully and with careful attention to detail. Please provide your reasoning process in detail.

    The answer is:

    ### Steps to Retrieve the Correct Value:

    1. **Identify the Header and Data Rows**: The first row is the header row, so data starts from the second row.

    2. **Locate the Specific Row**: We need to find the 23rd row, which means including the header row, it's the 23rd line in the table.

    3. **Locate the Specific Column**: We need to find the value in column 7.

    ### Applying the Steps:

    - **Header Row (Row 1)**: This is the first line in the table.
    - **Data Row 23**: This is the 23rd line in the table.

    The 23rd line in the table is:
    ```
    | 1997–98   | Montréal Canadiens    | NHL    | -              | 81             | 4              | 30             | 34             | 122      | -        | 10       | 0        | 1        | 1   | 14  | 10       | 122      | 4              | -              | 30             | Montréal Canadiens    | NHL    | -        | 34             | 1        | 81             | 1   | 0        | 1   | 30             | 0        | -        |
    ```

    - **Column 7**: The 7th column in this row corresponds to the value under the "A" (Regular season) header.

    The value in this cell is `30`.

    Therefore, the correct cell value at row_ID 23 and column_ID 7 is `30`.

    Now, carefully read the table provided below, follow these steps, and output your result as a JSON object in the format: {"row_ID": "m", "column_ID": "n", "cell_value": "<Corresponding Cell Value>"}

    Given Table:

    <<<table_input>>>
    <<<query_input>>>'''
    data_json = json.load(open(read_file_path, 'r', encoding="utf-8"))
    # print(f"len_data_json: {len(data_json)}")
    data_json = generate_table_repr(data_json)
    random.shuffle(data_json)
    # print(f"len_data_json: {len(data_json)}")
    # req_json = data_json[:2000]
    # output_file_name = "/mnt/dolphinfs/hdd_pool/docker/user/hadoop-aipnlp/internship/wlr_test/NeedleInATable/data/gpt4/1028_gpt4o_build_cot_output_2.json"
    output_json = []
    processed_json = []
    for data_sample in data_json:
        markdown_table_str, table_rows = data_sample['clipped_table_repr'], data_sample['clipped_table_rows']
        # print(data_sample.keys())
        # pdb.set_trace()
        tab_len = len(table_rows)
        for _ in range(tab_len * tab_len // 4):
            i, j = random.randint(1, tab_len), random.randint(1, tab_len)
            temp = copy.deepcopy(data_sample)
            try:
                gt = table_rows[i - 1][j - 1]
            except:
                print(table_rows)
                pdb.set_trace()
            query_input = f"Now, generate the thinking process to find the target cell value in detail. The target cell to be retrieved is located at the position where the row_ID is {i} and the column_ID is {j}, and the target cell value is {gt}. As this work is crucial to my career, please proceed thoughtfully with careful attention to detail, and provide your reasoning process in detail.\n\nThinking process:"
            prompt_template = prompt_3.replace("<<<table_input>>>", markdown_table_str).replace("<<<query_input>>>", query_input)
            messages = [
                {"role": "user", "content": prompt_template}
            ]
            temp.update({"gpt_output": messages})
            processed_json.append(temp)
    random.shuffle(processed_json)
    print(f"len of processed_json: {len(processed_json)}")

    cnt = 0
    for data_sample in processed_json:
        messages = data_sample['gpt_output']
        gpt_output = server_by_gpt4(messages)
        data_sample.update({"gpt_output": gpt_output})
        output_json.append(copy.deepcopy(data_sample))
        if cnt >= data_num: break
        # if cnt % 20 == 0: print(f"cnt: {cnt}")
        cnt += 1
        print(cnt)
        if cnt % 100 == 0:
            with open(output_file_name, 'w', encoding='utf-8') as f:
                json.dump(output_json, f, indent=4)
    with open(output_file_name, 'w', encoding='utf-8') as f:
        json.dump(output_json, f, indent=4)
    print(f"len_output_json: {len(output_json)}")

def generate_wo_demos(read_file_path,output_file_name,data_num):
    data_json = generate_table_repr(json.load(open(read_file_path, 'r', encoding="utf-8")))
    random.shuffle(data_json)
    print(f"len_data_json: {len(data_json)}")
    # req_json = data_json[:2000]
    # output_file_name = "/mnt/dolphinfs/hdd_pool/docker/user/hadoop-aipnlp/internship/wlr_test/NeedleInATable/data/gpt4/1028_gpt4o_build_cot_output_2.json"
    output_json = []
    cnt = 0
    processed_json = []
    for data_sample in data_json:
        markdown_table_str, table_rows = data_sample['clipped_table_repr'], data_sample['clipped_table_rows']
        # print(data_sample.keys())
        # pdb.set_trace()
        tab_len = len(table_rows)
        for _ in range(tab_len *  4):
            i, j = random.randint(1, tab_len), random.randint(1, tab_len)
            temp = copy.deepcopy(data_sample)
            try:
                gt = table_rows[i - 1][j - 1]
            except:
                print(table_rows)
                pdb.set_trace()
            query_input = f"Now, generate the thinking process to find the target cell value in detail. The target cell to be retrieved is located at the position where the row_ID is {i} and the column_ID is {j}, and the target cell value is {gt}."
            instruction = 'You are an expert in analyzing and extracting information from tabular data. You will receive a table in Markdown format, where each row is separated by a newline, and each column within a row is delimited by the “|” symbol. Your task is to read the table carefully and retrieve the value from a specified cell, based on provided row and column IDs.\n\n**Instructions**\n\n1.	Row and Column IDs: Both row and column IDs start from 1.\n2.	Header Row: The first row (row_ID 1) is the header row and should be counted when locating the target row.\n3.	Empty Cells: If the target cell is empty, return an empty string ("") as the cell_value.\n\n'
            prompt_template = f"{instruction}Carefully review the table, question, and provided ground truth answer. \n\nTable:\n\n{markdown_table_str}\n\nQuestion: {query_input}\n\nGenerate a detailed, step-by-step reasoning process to accurately answer the question. Conclude with the final answer."
            # print(prompt_template)
            # pdb.set_trace()
            messages = [
                {"role": "user", "content": prompt_template}
            ]
            temp.update({"gpt_output": messages,"pos_id":f"{i}_{j}"})
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
            # cnt += 1
            # if cnt >= data_num:
            #     break
            # if cnt % 100 == 0:
            #     with open(output_file_name, 'w', encoding='utf-8') as f:
            #         json.dump(output_json, f, indent=4)

    with open(output_file_name, 'w', encoding='utf-8') as f:
        json.dump(output_json, f, indent=4)
    print(f"len_output_json: {len(output_json)}")



def main(
        task_name: str = "NIAT",
        read_file_path: str = r"/Users/oliver/Documents/wlr/meituan/NeedleInATable/data/NIAT_tables_from_public_datasets.json",
        output_file_name: str = r"/mnt/dolphinfs/hdd_pool/docker/user/hadoop-aipnlp/internship/wlr_test/NeedleInATable/data/gpt4/1028_gpt4o_build_cot_output_2.json",
        data_num: int = 2000,
        batch_size: int = 32,
):
    # random.shuffle(data_json)
    # generate_with_demos(2, read_file_path, output_file_name, data_num)
    generate_wo_demos(read_file_path,output_file_name,data_num)



if __name__ == "__main__":
    fire.Fire(main)
