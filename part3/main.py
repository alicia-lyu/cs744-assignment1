from task import task
import sys

task_num = sys.argv[1]
data_file_name = sys.argv[2]
output_dir = sys.argv[3]

task(task_num, data_file_name, output_dir)