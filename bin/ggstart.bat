::
:: Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html
:: _________        _____ __________________        _____
:: __  ____/___________(_)______  /__  ____/______ ____(_)_______
:: _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
:: / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
:: \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
::
:: Version: 4.0.3c.14052012
::

::
:: Grid command line loader.
::

@echo off

if "%OS%" == "Windows_NT"  setlocal

:: Check JAVA_HOME.
if not "%JAVA_HOME%" == "" goto checkJdk
    echo %0, ERROR: JAVA_HOME environment variable is not found.
    echo %0, ERROR: Please create JAVA_HOME variable pointing to location of JDK 1.6 or JDK 1.7.
    echo %0, ERROR: You can also download latest JDK at: http://java.sun.com/getjava
goto error_finish

:checkJdk
:: Check that JDK is where it should be.
if exist "%JAVA_HOME%\bin\java.exe" goto checkJdkVersion
    echo %0, ERROR: The JDK is not found in %JAVA_HOME%.
    echo %0, ERROR: Please modify your script so that JAVA_HOME would point to valid location of JDK.
goto error_finish

:checkJdkVersion
"%JAVA_HOME%\bin\java.exe" -version 2>&1 | findstr "1\.[67]\." > nul
if %ERRORLEVEL% equ 0 goto checkGridGainHome1
    echo %0, ERROR: The version of JAVA installed in %JAVA_HOME% is incorrect.
    echo %0, ERROR: Please install JDK 1.6 or 1.7.
    echo %0, ERROR: You can also download latest JDK at: http://java.sun.com/getjava
goto error_finish

:: Check GRIDGAIN_HOME.
:checkGridGainHome1
if not "%GRIDGAIN_HOME%" == "" goto checkGridGainHome2
    echo %0, WARN: GRIDGAIN_HOME environment variable is not found.
    pushd "%~dp0"/..
    set GRIDGAIN_HOME=%CD%
    popd

:checkGridGainHome2
:: remove all trailing slashes from GRIDGAIN_HOME.
if %GRIDGAIN_HOME:~-1,1% == \ goto removeTrailingSlash
if %GRIDGAIN_HOME:~-1,1% == / goto removeTrailingSlash
goto checkGridGainHome3
:removeTrailingSlash
set GRIDGAIN_HOME=%GRIDGAIN_HOME:~0,-1%
goto checkGridGainHome2

:checkGridGainHome3
if exist "%GRIDGAIN_HOME%\config" goto checkGridGainHome4
    echo %0, ERROR: GRIDGAIN_HOME environment variable is not valid installation home.
    echo %0, ERROR: GRIDGAIN_HOME variable must point to GridGain installation folder.
    goto error_finish

:checkGridGainHome4
set GRIDGAIN_HOME_LOWER=%GRIDGAIN_HOME%
call :toLowerCase GRIDGAIN_HOME_LOWER

set SCRIPT_DIR=%~dp0
call :toLowerCase SCRIPT_DIR

if "%GRIDGAIN_HOME_LOWER%\bin\" == "%SCRIPT_DIR%" goto parseArgs
    echo %0, WARN: GRIDGAIN_HOME environment variable may be pointing to wrong folder: %GRIDGAIN_HOME%

::
:: Process command lime arguments.
::
:parseArgs
if "%1" == "" goto run

if "%1" == "-i" (
    set INTERACTIVE=1
) else if "%1" == "-v" (
    set QUIET=-DGRIDGAIN_QUIET=false
) else if "%1" == "-np" (
    set NO_PAUSE=1
) else (
    set CONFIG=%1
)

shift

goto parseArgs

:run

if "%CONFIG%" == "" set CONFIG=%GRIDGAIN_HOME%\config\default-spring.xml

:: This is Ant-augmented variable.
set ANT_AUGMENTED_GGJAR=gridgain-4.0.3c.jar

::
:: Set GRIDGAIN_LIBS
::
call "%GRIDGAIN_HOME%\bin\setenv.bat"

set CP=%GRIDGAIN_LIBS%;%GRIDGAIN_HOME%\%ANT_AUGMENTED_GGJAR%

::
:: Process 'restart'.
::
set RESTART_SUCCESS_FILE="%GRIDGAIN_HOME%\work\gridgain_success_%RANDOM%%TIME:~6,2%%TIME:~9,2%"
set RESTART_SUCCESS_OPT=-DGRIDGAIN_SUCCESS_FILE=%RESTART_SUCCESS_FILE%

::
:: Find available port for JMX
::
for /F "tokens=*" %%A in ('""%JAVA_HOME%\bin\java" -cp "%GRIDGAIN_HOME%\%ANT_AUGMENTED_GGJAR%" org.gridgain.grid.tools.portscanner.GridPortScanner"') do set JMX_PORT=%%A

::
:: This variable defines necessary parameters for JMX
:: monitoring and management.
:: ADD YOUR ADDITIONAL PARAMETERS/OPTIONS HERE
::
set JMX_MON=-Dcom.sun.management.jmxremote

::
:: This enables remote unsecure access to JConsole or VisualVM.
:: ADD YOUR ADDITIONAL PARAMETERS/OPTIONS HERE
::
set JMX_MON=%JMX_MON% -Dcom.sun.management.jmxremote.port=%JMX_PORT% -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false

::
:: JVM options. See http://java.sun.com/javase/technologies/hotspot/vmoptions.jsp
:: for more details. Note that default settings use **PARALLEL GC**.
::
:: ADD YOUR/CHANGE ADDITIONAL OPTIONS HERE
::
set JVM_OPTS=-Xms512m -Xmx512m -XX:NewSize=64m -XX:MaxNewSize=64m -XX:PermSize=128m -XX:MaxPermSize=128m -XX:SurvivorRatio=128 -XX:MaxTenuringThreshold=0 -XX:+UseTLAB -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled

::
:: Uncomment if you get StackOverflowError.
:: On 64 bit systems this value can be larger, e.g. -Xss16m
::
:: set JVM_OPTS=%JVM_OPTS% -Xss4m

::
:: Uncomment to set preference to IPv4 stack.
::
:: set JVM_OPTS=%JVM_OPTS% -Djava.net.preferIPv4Stack=true

::
:: Assertions are disabled by default since version 3.5.
:: If you want to enable them - set 'ENABLE_ASSERTIONS' flag to '1'.
::
set ENABLE_ASSERTIONS=0

::
:: Set '-ea' options if assertions are enabled.
::
if %ENABLE_ASSERTIONS% == 1 set JVM_OPTS=%JVM_OPTS% -ea

::
:: Set program name.
::
set PROG_NAME=gridgain.bat
if "%OS%" == "Windows_NT" set PROG_NAME=%~nx0%

:run_java

::
:: Remote debugging (JPDA).
:: Uncomment and change if remote debugging is required.
:: set JVM_OPTS=-Xdebug -Xrunjdwp:transport=dt_socket,address=8787,server=y,suspend=n %JVM_OPTS%
::

if "%INTERACTIVE%" == "1" (
    "%JAVA_HOME%\bin\java.exe" %JVM_OPTS% %QUIET% %RESTART_SUCCESS_OPT% %JMX_MON% -DGRIDGAIN_SCRIPT  -DGRIDGAIN_HOME="%GRIDGAIN_HOME%" -DGRIDGAIN_PROG_NAME="%PROG_NAME%" -cp "%CP%" org.gridgain.grid.loaders.cmdline.GridCommandLineLoader
) else (
    "%JAVA_HOME%\bin\java.exe" %JVM_OPTS% %QUIET% %RESTART_SUCCESS_OPT% %JMX_MON% -DGRIDGAIN_SCRIPT  -DGRIDGAIN_HOME="%GRIDGAIN_HOME%" -DGRIDGAIN_PROG_NAME="%PROG_NAME%" -cp "%CP%" org.gridgain.grid.loaders.cmdline.GridCommandLineLoader "%CONFIG%"
)

set JAVA_ERRORLEVEL=%ERRORLEVEL%

:: errorlevel 130 if aborted with Ctrl+c
if %JAVA_ERRORLEVEL%==130 goto finish

:: Exit if first run unsuccessful (Loader must create file).
if not exist %RESTART_SUCCESS_FILE% goto error_finish
del %RESTART_SUCCESS_FILE%

goto run_java

:finish
if not exist %RESTART_SUCCESS_FILE% goto error_finish
del %RESTART_SUCCESS_FILE%

:error_finish
:error_finish

if not "%NO_PAUSE%" == "1" pause

goto :EOF

:toLowerCase
for %%i in ("A=a" "B=b" "C=c" "D=d" "E=e" "F=f" "G=g" "H=h" "I=i" "J=j" "K=k" "L=l" "M=m" "N=n" "O=o" "P=p" "Q=q" "R=r" "S=s" "T=t" "U=u" "V=v" "W=w" "X=x" "Y=y" "Z=z") do call set "%1=%%%1:%%~i%%"
goto :EOF
