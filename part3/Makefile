run-web:
	-./run_small 1 web-BerkStan.txt
	-./run_small 2 web-BerkStan.txt
	-./run_small 3 web-BerkStan.txt

run-big:
	-./run_big.sh 3 1
	-./run_big.sh 1 1
	-make run-big-2

run-big-2:
	./run_big.sh 2 5
	./run_big.sh 2 4
	./run_big.sh 2 3
	./run_big.sh 2 2
	./run_big.sh 2 1

rest:
	./run_big.sh 4 1 # To drop a worker
	./run_big.sh 3 2 # Comparing task 3 with task 1
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