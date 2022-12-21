#!/usr/bin/env bash

#
# basic map-reduce test
#

#RACE=

# comment this to run the tests without the Go race detector.
RACE=-race

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mrapps && go build $RACE -buildmode=plugin wc.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin mtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin rtiming.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin early_exit.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin crash.go) || exit 1
(cd ../../mrapps && go build $RACE -buildmode=plugin nocrash.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1
(cd .. && go build $RACE mrsequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
../mrsequential ../../mrapps/wc.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

timeout -k 2s 180s ../mrcoordinator ../pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &
timeout -k 2s 180s ../mrworker ../../mrapps/wc.so &

# wait for the coordinator to exit.
wait $pid

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
	  echo '---' wc test: PASS
  else
	    echo '---' wc output is not the same as mr-correct-wc.txt
	      echo '---' wc test: FAIL
	        failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait
