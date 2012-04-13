::
:: Copyright (C) GridGain Systems Licensed under GPLv3, http://www.gnu.org/licenses/gpl.html
:: _________        _____ __________________        _____
:: __  ____/___________(_)______  /__  ____/______ ____(_)_______
:: _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
:: / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
:: \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
::
:: Version: 4.0.2c.12042012
::

::
:: Exports GRIDGAIN_LIBS variable containing classpath for GridGain.
:: Expects GRIDGAIN_HOME to be set.
:: Can be used like:
::      call %GRIDGAIN_HOME%\bin\setenv.bat
:: in other scripts to set classpath using exported GRIDGAIN_LIBS variable.
::

@echo off

:: USER_LIBS variable can optionally contain user's JARs/libs.
:: set USER_LIBS=

::
:: Check GRIDGAIN_HOME.
::
if not "%GRIDGAIN_HOME%" == "" goto run
    echo %0, ERROR: GRIDGAIN_HOME environment variable is not found.
goto finish

:run
:: The following libraries are required for GridGain.
set GRIDGAIN_LIBS=%USER_LIBS%;%GRIDGAIN_HOME%\config\userversion;%GRIDGAIN_HOME%\libs\*

:: Comment these jars if you do not wish to use Hyperic SIGAR licensed under GPL
:: Note that starting with GridGain 3.0 - Community Edition is licensed under GPLv3.
set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%GRIDGAIN_HOME%\libs\sigar.jar

:: Uncomment if using JBoss.
:: JBOSS_HOME must point to JBoss installation folder.
:: set JBOSS_HOME=

:: set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%JBOSS_HOME%\lib\jboss-common.jar
:: set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%JBOSS_HOME%\lib\jboss-jmx.jar
:: set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%JBOSS_HOME%\lib\jboss-system.jar
:: set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%JBOSS_HOME%\server\all\lib\jbossha.jar
:: set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%JBOSS_HOME%\server\all\lib\jboss-j2ee.jar
:: set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%JBOSS_HOME%\server\all\lib\jboss.jar
:: set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%JBOSS_HOME%\server\all\lib\jboss-transaction.jar
:: set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%JBOSS_HOME%\server\all\lib\jmx-adaptor-plugin.jar
:: set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%JBOSS_HOME%\server\all\lib\jnpserver.jar

:: If using JBoss AOP following libraries need to be downloaded separately
:: set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%JBOSS_HOME%\lib\jboss-aop-jdk50.jar
:: set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%JBOSS_HOME%\lib\jboss-aspect-library-jdk50.jar

:: Set user external libraries
call :setallext "%GRIDGAIN_HOME%\libs\ext" %*
goto finish

:setallext
if .%1.==.. goto finish
set dir=%1
set dir=%dir:"=%
if not "%GRIDGAIN_LIBS%"=="" set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%dir%
if "%GRIDGAIN_LIBS%"=="" set GRIDGAIN_LIBS=%dir%
for %%i in ("%dir%\*.jar") do call :setoneext "%%i"
for %%i in ("%dir%\*.zip") do call :setoneext "%%i"
shift
goto setallext

:setoneext
set file=%1
set file=%file:"=%
set GRIDGAIN_LIBS=%GRIDGAIN_LIBS%;%file%

:finish
