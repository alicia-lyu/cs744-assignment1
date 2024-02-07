from task_big import task_big
import sys

task_num = sys.argv[1]
experiment_num = sys.argv[2]
output_dir = sys.argv[3]

task_big(task_num, experiment_num, output_dir)