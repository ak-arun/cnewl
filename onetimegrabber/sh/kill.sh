pid=`ps -ef | awk '/[H]iveDDLOnetimeGrabber/{print$2}'`
if [ -n "$pid" ] && [ "$pid" -eq "$pid" ] 2>/dev/null; then
  kill -9 $pid
 echo "Job Killed"
else
  echo "Job already completed/killed"
fi
