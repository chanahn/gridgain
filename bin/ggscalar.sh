#!/bin/bash
#
# Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html
#  _________        _____ __________________        _____
#  __  ____/___________(_)______  /__  ____/______ ____(_)_______
#  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
#  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
#  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
#
# Version: 4.0.1c.07042012
#

#
# Starts Scala REPL with GridGain on the classpath.
#

#
# Check JAVA_HOME.
#
if [ "$JAVA_HOME" = "" ]; then
    echo $0", ERROR: JAVA_HOME environment variable is not found."
    echo $0", ERROR: Please create JAVA_HOME variable pointing to location of JDK 1.6 or JDK 1.7."
    echo $0", ERROR: You can also download latest JDK at: http://java.sun.com/getjava"

    exit 1
fi

JAVA=${JAVA_HOME}/bin/java

#
# Check JDK.
#
if [ ! -e "$JAVA" ]; then
    echo $0", ERROR: The JAVA is not found in $JAVA_HOME."
    echo $0", ERROR: Please modify your script so that JAVA_HOME would point"
    echo $0", ERROR: to valid location of Java installation."

    exit 1
fi

JAVA_VER=`$JAVA -version 2>&1 | egrep "1\.[67]\."`

if [ "$JAVA_VER" == "" ]; then
    echo $0", ERROR: The version of JAVA installed in $JAVA_HOME is incorrect."
    echo $0", ERROR: Please install JDK 1.6 or 1.7."
    echo $0", ERROR: You can also download latest JDK at: http://java.sun.com/getjava"

    exit 1
fi

#
# Set property JAR name during the Ant build.
#
ANT_AUGMENTED_GGJAR=gridgain-4.0.1c.jar

osname=`uname`

GRIDGAIN_HOME_TMP=

case $osname in
    Darwin*)
        export GRIDGAIN_HOME_TMP=$(dirname $(dirname $(cd ${0%/*} && echo $PWD/${0##*/})))
        ;;
    *)
        export GRIDGAIN_HOME_TMP="$(dirname $(readlink -f $0))"/..
        ;;
esac

#
# Set GRIDGAIN_HOME, if needed.
#
if [ "${GRIDGAIN_HOME}" = "" ]; then
    echo $0", WARN: GRIDGAIN_HOME environment variable is not found."

    export GRIDGAIN_HOME=${GRIDGAIN_HOME_TMP}
elif [ "${GRIDGAIN_HOME}" != "${GRIDGAIN_HOME_TMP}" ] && [ "${GRIDGAIN_HOME}/bin/.." != "${GRIDGAIN_HOME_TMP}" ]; then
    echo $0", WARN: GRIDGAIN_HOME environment variable may be pointing to wrong folder: $GRIDGAIN_HOME"
fi

#
# Check GRIDGAIN_HOME
#
if [ ! -d "${GRIDGAIN_HOME}/config" ]; then
    echo $0", ERROR: GRIDGAIN_HOME environment variable is not found or is not valid."
    echo $0", ERROR: GRIDGAIN_HOME variable must point to GridGain installation folder."

    exit 1
fi

#
# Set GRIDGAIN_LIBS.
#
. "${GRIDGAIN_HOME}"/bin/setenv.sh

#
# OS specific support.
#
SEPARATOR=":";

case $osname in
    CYGWIN*)
        SEPARATOR=";";
        ;;
esac

CP="${GRIDGAIN_LIBS}${SEPARATOR}${GRIDGAIN_HOME}/${ANT_AUGMENTED_GGJAR}"

QUIET="-DGRIDGAIN_QUIET=true"

while [ $# -gt 0 ]
do
    case "$1" in
        -v) QUIET="-DGRIDGAIN_QUIET=false";;
    esac
    shift
done

#
# JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp
# for more details. Note that default settings use ** PARALLEL GC**.
#
# NOTE
# ====
# ASSERTIONS ARE DISABLED BY DEFAULT SINCE VERSION 3.5.
# IF YOU WANT TO ENABLE THEM - ADD '-ea' TO JVM_OPTS VARIABLE
#
# ADD YOUR ADDITIONAL PARAMETERS/OPTIONS HERE
#
JVM_OPTS="-Xms512m -Xmx512m -XX:NewSize=64m -XX:MaxNewSize=64m -XX:PermSize=128m -XX:MaxPermSize=128m \
-XX:SurvivorRatio=128 -XX:MaxTenuringThreshold=0 -XX:+UseTLAB -XX:+UseParNewGC -XX:+UseConcMarkSweepGC \
-XX:+CMSClassUnloadingEnabled"

# Uncomment if you get StackOverflowError.
# On 64 bit systems this value can be larger, e.g. -Xss16m
# JVM_OPTS="${JVM_OPTS} -Xss4m"

# Uncomment to set preference for IPv4 stack.
# JVM_OPTS="${JVM_OPTS} -Djava.net.preferIPv4Stack=true"

#
# Save terminal setting. Used to restore terminal on finish.
#
SAVED_STTY=`stty -g 2>/dev/null`

#
# Restores terminal.
#
function restoreSttySettings() {
    stty ${SAVED_STTY}
}

#
# Trap that restores terminal in case script execution is interrupted.
#
trap restoreSttySettings INT

#
# Start REPL.
#
${JAVA_HOME}/bin/java ${JVM_OPTS} ${QUIET}  -DGRIDGAIN_SCRIPT \
-DGRIDGAIN_HOME="${GRIDGAIN_HOME}" -DGRIDGAIN_PROG_NAME="$0" -cp "${CP}" \
scala.tools.nsc.MainGenericRunner -usejavacp -Yrepl-sync -i ${GRIDGAIN_HOME}/bin/scalar.scala

#
# Restore terminal.
#
restoreSttySettings
