test-1:
	./run.sh 1 web-BerkStan.txt

test-2:
	./run.sh 2 web-BerkStan.txt

test-3:
	./run.sh 3 web-BerkStan.txt

run-web:
	make test-1
	make test-2
	make test-3

test-wiki:
	./run.sh 1 enwiki-pages-articles

run-wiki:
	./run.sh 1 enwiki-pages-articles
	./run.sh 2 enwiki-pages-articles
	./run.sh 3 enwiki-pages-articles

run-big:
	./run_big.sh 1 1
	for i in {1..5}; do ./run_big.sh 2 $$i; done
	./run_big.sh 3 1

run-all:
	make run-all-web
	make run-all-wiki
	make run-big
