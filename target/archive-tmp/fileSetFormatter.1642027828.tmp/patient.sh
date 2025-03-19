#!/bin/sh

if [ -n "$JAVA_HOME" ]; then
    #JAVA_HOME is not empty"
exec="$JAVA_HOME/bin/java -jar Patient_Data-*.jar"
else
	#JAVA_HOME is empty
	echo "Warning: sent JAVA_HOME"
	exec="java -jar Patient_Data-*.jar"
fi

BASEDIR=$(dirname $0)
CURDIR=`pwd`
cspidfile="$CURDIR/$BASEDIR/../patient.pid"

start(){
    # check to see if it's already running
    CSRUNNING=0
    if [ -f "$cspidfile" ]; then
        CSPID=`cat "$cspidfile" 2>/dev/null`
        if [ -n "$CSPID" ] ; then
        	kill -0 "$CSPID" 2>/dev/null
        	if [ $? = 0 ]; then
            	CSRUNNING=1
            fi
        fi
    fi
    if [ $CSRUNNING = 1 ] && [ $? = 0 ]; then
		echo "Patient_Data already running with PID $CSPID."
	else
		cd "$BASEDIR/../"
		ulimit -n 8192
		$exec &
    	echo $! > $cspidfile
    	echo "Started Patient_Data with pid $!"
    	cd "$CURDIR"
		ret=0;
    fi
    return $ret
}

status(){
    # check to see if it's already running
    CSRUNNING=0
    if [ -f "$cspidfile" ]; then
        CSPID=`cat "$cspidfile" 2>/dev/null`
        if [ -n "$CSPID" ] ; then
        	kill -0 "$CSPID" 2>/dev/null
        	if [ $? = 0 ]; then
            	CSRUNNING=1
            fi
        fi
    fi
    if [ $CSRUNNING = 1 ] && [ $? = 0 ]; then
		echo "Patient_Data running with PID $CSPID."
		ret=0;
	else
		echo "Patient_Data is not running."
		ret=0;
    fi
    return $ret
}

stop(){
        if [ ! -f "$cspidfile" ]; then
            # not running; per LSB standards this is "ok"
            action $"Stopping $prog: " /bin/true
            return 0
        fi
        CSPID=`cat "$cspidfile" 2>/dev/null`
        if [ -n "$CSPID" ]; then
        	kill -0 "$CSPID" 2>/dev/null
        	if [ $? = 0 ]; then
            	/bin/kill "$CSPID" >/dev/null 2>&1
            	ret=$?
            	if [ $ret = 0 ]; then
            		echo "Patient_Data with pid $CSPID stopped."
            	else
            		echo "Unable to stop Patient_Data pid $CSPID"
            	fi
            else
            	echo "Patient_Data is not running."
            	ret=4
            fi

        else
            ret=4
        fi
        return $ret
}

restart(){
    stop
    sleep 1m
    start
}

# See how we were called.
case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  status)
    status
    ;;
  restart)
    restart
    ;;
  *)
    echo $"Usage: $0 {start|stop|status|restart}"
    exit 2
esac

exit $?