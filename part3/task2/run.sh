echo "Running task 2 with web-BerkStan.txt, with partition_num=15"
../run_small 2 web-BerkStan.txt || echo "Failed to run task 2 with web-BerkStan.txt"
echo "Running task 2 with enwiki-pages-articles, with partition_num=243"
../run_big 2 4 || echo "Failed to run task 2 with enwiki-pages-articles"
