#!/bin/sh
set -x

LINKBENCH_DIR="/linkbench/"

FILE_LOG="log/linkbench.log"

TARANTOOL_DIR=${TARANTOOL_DIR:-/tarantool/}
# Launch server

cd ${LINKBENCH_DIR}src/tarantool_scripts
${TARANTOOL_DIR}src/tarantool app.lua & echo $! > ./nd.pid
cd ${LINKBENCH_DIR}

#here we specify the test parameters
TARANTOOL_PROPS="${LINKBENCH_DIR}config/LinkConfigTarantool.properties"

LINKBENCH_CMD="sh ${LINKBENCH_DIR}bin/linkbench -c $TARANTOOL_PROPS -L $FILE_LOG"

#Load Phase
ok=`(($LINKBENCH_CMD -l 2>&1) | awk '{for(i=1;i<=NF;i++){ if($i=="ERROR"){print $0; exit(1)} } }')`
if ! test -z $ok
then
    echo 'Error in Load Phase'
    tail -20 $FILE_LOG
    exit 1
fi

#Request Phase
ok=`(($LINKBENCH_CMD -r 2>&1) | awk '{for(i=1;i<=NF;i++){ if($i=="ERROR" && $NF!="conflict"){print $0; exit(1)}}}')`
if ! test -z $ok
then
    echo 'Error in Request Phase'
    tail -20 $FILE_LOG
    exit 1
fi
echo "OK"
tail $FILE_LOG
res=`(tail -n 1 $FILE_LOG | awk '{print $NF}')`
echo "QPS = $res" | tee result.log

cat ./nd.pid | xargs kill -s TERM; rm nd.pid
if [ -e /credentials/auth.conf ]
then
	pip install requests
	python /linkbench/export.py /credentials/auth.conf result.log linkbench linkbench
fi
