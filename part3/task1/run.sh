echo "Running task 1 with web-BerkStan.txt"
../run_small 1 web-BerkStan.txt || echo "Failed to run task 1 with web-BerkStan.txt"
echo "Running task 1 with enwiki-pages-articles"
../run_big 1 1 || echo "Failed to run task 1 with enwiki-pages-articles"
