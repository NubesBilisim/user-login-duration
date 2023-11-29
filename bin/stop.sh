pid=$(head -n 1 .pid)
echo $pid
kill -9 $pid
rm -rf .pid