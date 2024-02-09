echo "IMPORTANT: Make sure the repo is located in /mnt/data/cs744-assignment1 before you run this script"
echo "Alternatively, you can run `run.sh` or `make run` from /part3 for all 4 tasks"
cd /mnt/data/cs744-assignment1/part3
echo "Running task 1 with web-BerkStan.txt"
./run_small.sh 1 web-BerkStan.txt || echo "Failed to run task 1 with web-BerkStan.txt"
echo "Running task 1 with enwiki-pages-articles"
./run_big.sh 1 1 || echo "Failed to run task 1 with enwiki-pages-articles"
