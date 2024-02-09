cd /mnt/data/cs744-assignment1/part3
echo "Running task 4 with web-BerkStan.txt, but you have to manually remove a worker"
./run_small.sh 4 web-BerkStan.txt || echo "Failed to run task 4 with web-BerkStan.txt"
echo "Running task 4 with enwiki-pages-articles, but you have to manually remove a worker"
./run_big.sh 4 1 || echo "Failed to run task 4 with enwiki-pages-articles"
