@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem
@rem  concourse-server startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

@rem Add default JVM options here. You can also use JAVA_OPTS and CONCOURSE_SERVER_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if "%ERRORLEVEL%" == "0" goto init

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:init
@rem Get command-line arguments, handling Windowz variants

if not "%OS%" == "Windows_NT" goto win9xME_args
if "%@eval[2+2]" == "4" goto 4NT_args

:win9xME_args
@rem Slurp the command line arguments.
set CMD_LINE_ARGS=
set _SKIP=2

:win9xME_args_slurp
if "x%~1" == "x" goto execute

set CMD_LINE_ARGS=%*
goto execute

:4NT_args
@rem Get arguments from the 4NT Shell from JP Software
set CMD_LINE_ARGS=%$

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\concourse-server-0.1.0.1377709198.377.jar;%APP_HOME%\lib\guava-13.0.1.jar;%APP_HOME%\lib\mockito-all-1.9.5.jar;%APP_HOME%\lib\commons-codec-1.8.jar;%APP_HOME%\lib\jsr305-2.0.1.jar;%APP_HOME%\lib\slf4j-api-1.7.5.jar;%APP_HOME%\lib\logback-classic-1.0.13.jar;%APP_HOME%\lib\joda-time-2.2.jar;%APP_HOME%\lib\annotations-12.0.jar;%APP_HOME%\lib\libthrift-0.9.0.jar;%APP_HOME%\lib\commons-configuration-1.9.jar;%APP_HOME%\lib\jetbridge-1.0-SNAPSHOT.jar;%APP_HOME%\lib\concourse-0.1.0.1377709198.3.jar;%APP_HOME%\lib\perf4j-0.9.16.jar;%APP_HOME%\lib\aspectjweaver-1.7.3.jar;%APP_HOME%\lib\aspectjrt-1.7.3.jar;%APP_HOME%\lib\commons-jexl-1.1.jar;%APP_HOME%\lib\commons-logging-1.1.3.jar;%APP_HOME%\lib\logback-core-1.0.13.jar;%APP_HOME%\lib\httpcore-4.1.4.jar;%APP_HOME%\lib\httpclient-4.1.3.jar;%APP_HOME%\lib\commons-lang-2.6.jar;%APP_HOME%\lib\hamcrest-core-1.3.jar;%APP_HOME%\lib\junit-4.11.jar

@rem Execute concourse-server
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %CONCOURSE_SERVER_OPTS%  -classpath "%CLASSPATH%" org.cinchapi.concourse.server.ConcourseServer %CMD_LINE_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if "%ERRORLEVEL%"=="0" goto mainEnd

:fail
rem Set variable CONCOURSE_SERVER_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
if  not "" == "%CONCOURSE_SERVER_EXIT_CONSOLE%" exit 1
exit /b 1

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
