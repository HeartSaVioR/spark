#!/bin/bash

COUNT=0
while true; do
  build/sbt "sql/testOnly *ContinuousSuite -- -z \"query without test harness\"" > test-run-console.log 2>&1;
  RET=$?
  grep "are not a superset of" test-run-console.log
  if [ $RET -ne 0 ]; then
    echo "Test failed with $COUNT times!"
    cp test-run-console.log failed-test-run-console.log
    echo "Copied console output log to failed-test-run-console.log ..."
    cp sql/core/target/unit-tests.log failed-test-log.log
    echo "Copied unit test log to failed-test-log.log ... exiting..."
    exit $?
  fi
  COUNT=$((COUNT + 1))
  if [ $((COUNT % 10)) -eq 0 ]; then
    echo "Passed $COUNT times! date: "`date`
  fi
done
