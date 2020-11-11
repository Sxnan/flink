#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

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

