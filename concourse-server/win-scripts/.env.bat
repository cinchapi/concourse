@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem
@rem  Configuration for Concourse CLIs on Windows
@rem
@rem ##########################################################################

@rem Add default JVM options here. You can also use JAVA_OPTS and GRADLE_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

@rem Set the APP_HOME to the parent directory
set DIRNAME=%~dp0
set CWD=%CD%
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
CD %DIRNAME%
CD ..
set APP_HOME=%CD%
CD %CWD%

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


:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

:init
@rem Set the classpath
set CLASSPATH="%APP_HOME%\lib\*"

@rem get the command line arguments
set CMD_LINE_ARGS=
goto setArgs

:setArgs
if ""%1""=="""" goto doneSetArgs
set CMD_LINE_ARGS=%CMD_LINE_ARGS% %1
shift
goto setArgs

:doneSetArgs