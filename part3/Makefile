run:
	echo "Running task 1 with web-BerkStan.txt"
	-./run_small.sh 1 web-BerkStan.txt
	echo "Running task 2 with web-BerkStan.txt"
	-./run_small.sh 2 web-BerkStan.txt
	echo "Running task 3 with web-BerkStan.txt"
	-./run_small.sh 3 web-BerkStan.txt
	echo "Task 4 is basically task 1, requiring you manually drop a worker"
	echo "Skipping task 4 with web-BerkStan.txt"
	echo "Running task 1 with enwiki-pages-articles"
	-./run_big.sh 1 1
	echo "Running task 2 with enwiki-pages-articles, with partition_num=243"
	-./run_big.sh 2 4
	echo "Running task 3 with enwiki-pages-articles"
	-./run_big.sh 3 1
	echo "Task 4 is basically task 1, requiring you manually drop a worker"
	echo "Skipping task 4 with enwiki-pages-articles"

run-web:
	-./run_small 1 web-BerkStan.txt
	-./run_small 2 web-BerkStan.txt
	-./run_small 3 web-BerkStan.txt

run-big:
	-./run_big.sh 1 1
	-make run-big-2
	-./run_big.sh 3 1
	./run_big.sh 3 2 # Comparing task 3 with task 1
	-./run_big.sh 4 1 # To drop a worker

run-big-2:
	./run_big.sh 2 5
	./run_big.sh 2 4
	./run_big.sh 2 3
	./run_big.sh 2 2
	./run_big.sh 2 1

run-all:
	make run-web
	make run-wiki
	make run-big

test-1-deprecated:
	-./run_deprecated 1 web-BerkStan.txt

test-2-deprecated:
	-./run_deprecated 2 web-BerkStan.txt

test-3-deprecated:
	-./run_deprecated 3 web-BerkStan.txt

run-web-deprecated:
	make test-1
	make test-2
	make test-3

test-wiki-deprecated:
	-./run_deprecated 3 enwiki-pages-articles

run-wiki-deprecated:
	-./run_deprecated 1 enwiki-pages-articles
	-./run_deprecated 2 enwiki-pages-articles
	-./run_deprecated 3 enwiki-pages-articles