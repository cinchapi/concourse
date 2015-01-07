@if "%DEBUG%" == "" @echo off
@rem this config will setup all the environment variables and check
@rem that paths are proper
call .env.bat %*

@rem run the program
"%JAVA_EXE%" -classpath "%CLASSPATH%" org.cinchapi.concourse.shell.ConcourseShell %CMD_LINE_ARGS%