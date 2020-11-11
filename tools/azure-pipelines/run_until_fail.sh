#!/bin/bash
# This scripts runs a given test in a loop until it failes.
# All the logs are redirected to a log file and will be deleted if the test has passed.

if [ $# -ne 2 ];then
	echo "USAGE: run_until_fail.sh TEST_DIR TEST_CLASS"
	exit
fi

test_dir=$1
test_class=$2

echo "Installing Flink"
mvn clean install -B -Dfast -DskipTests -Pskip-webui-build -pl $test_dir -am

echo "running test in $test_dir $test_class"
cd $test_dir

success=0
round=0

mkdir $FLINK_ARTIFACT_DIR
log=$FLINK_ARTIFACT_DIR/test_log_$test_class
echo "log will be written to dir $log"

while [ $success == 0 ]
do
	echo "Running tests: Run $round"
	mvn test -B -Dfast -Dtest=$2 2>&1 > $log
	success=$?
	if [ $success == 0 ]; then
		rm $log
	else
		echo "\nTest finished with return code $success."
		echo "Check log at $log."
	fi
	round=$(($round + 1))
done;

