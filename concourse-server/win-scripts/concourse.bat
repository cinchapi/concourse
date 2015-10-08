@if "%DEBUG%" == "" @echo off

@rem Options that are passed to Java to configure the Concourse Server runtime.
@rem Before changing these parameters, please check the documentation for
@rem concourse.prefs to see if the desired functionality is configured there (i.e
@rem the Concourse Server heap size is specified using the `heap_size` preference)
@rem because those take precedence.
set JVMOPTS=""
set JVMOPTS=%JVMOPTS%-Xms1024m
set JVMOPTS=%JVMOPTS% -Xmx1024m
set JVMOPTS=%JVMOPTS% -Dcom.sun.management.jmxremote
set JVMOPTS=%JVMOPTS% -Dcom.sun.management.jmxremote.port=9010
set JVMOPTS=%JVMOPTS% -Dcom.sun.management.jmxremote.local.only=false
set JVMOPTS=%JVMOPTS% -Dcom.sun.management.jmxremote.authenticate=false
set JVMOPTS=%JVMOPTS% -Dcom.sun.management.jmxremote.ssl=false
set JVMOPTS=%JVMOPTS% -XX:+UseThreadPriorities
set JVMOPTS=%JVMOPTS% -XX:ThreadPriorityPolicy=42
set JVMOPTS=%JVMOPTS% -XX:CompileThreshold=500

@rem Set the APP_HOME to the parent directory
set DIRNAME=%~dp0
set OWD=%CD%
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
CD %DIRNAME%
CD ..
CD ..
set APP_HOME=%CD%
CD %OWD%

@rem Set the classpath
set CLASSPATH="%APP_HOME%\lib\*"

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if "%ERRORLEVEL%" == "0" goto main

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.


:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto main

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

:main
@rem get the first command line argument as the action
set ACTION=%1
shift
@rem store the rest of the command line arguments
set CMD_LINE_ARGS=
goto setArgs

:setArgs
if ""%1""=="""" goto doneSetArgs
set CMD_LINE_ARGS=%CMD_LINE_ARGS%%1
shift
goto setArgs

:doneSetArgs
@rem ensure that we operate from APP_HOME
cd %APP_HOME%
@rem handle the user specified action
if "%ACTION%" == "" set ACTION=start
set LABEL_EXISTS=""
2>NUL CALL :CASE_%ACTION%
IF ERRORLEVEL 1 IF %LABEL_EXISTS% == "" CALL :DEFAULT_CASE
cd %OWD%
EXIT /b

:CASE_console
  "%JAVA_EXE%" %JVMOPTS% -classpath "%CLASSPATH%" org.cinchapi.concourse.server.ConcourseServer %CMD_LINE_ARGS%
  goto :END_CASE
:CASE_start
  echo this is the start case
  goto :END_CASE
:DEFAULT_CASE
  echo i don't know what you're talking about bruh
  goto :EOF
:END_CASE
  set LABEL_EXISTS="true"
  goto :EOF
