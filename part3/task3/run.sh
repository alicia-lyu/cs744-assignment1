echo "Running task 3 with web-BerkStan.txt"
../run_small 3 web-BerkStan.txt || echo "Failed to run task 3 with web-BerkStan.txt"
echo "Running task 1 with enwiki-pages-articles"
../run_big 3 1 || echo "Failed to run task 3 with enwiki-pages-articles"