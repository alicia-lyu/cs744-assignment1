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

run-web:
	-./run.sh 1 web-BerkStan.txt
	-./run.sh 2 web-BerkStan.txt
	-./run.sh 3 web-BerkStan.txt

run-wiki:
	-./run.sh 3 enwiki-pages-articles
	-./run.sh 2 enwiki-pages-articles
	-./run.sh 1 enwiki-pages-articles

run-big:
	-./run_big.sh 3 1
	-@for i in {5..1}; do ./run_big.sh 2 $$i; done
	-./run_big.sh 1 1

run-all:
	make run-web
	make run-wiki
	make run-big