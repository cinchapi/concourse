@if "%DEBUG%" == "" @echo off
@rem This config will setup all the enviornment variables and check that
@rem paths are proper
call .env.bat %*

@rem run the program
"%JAVA_EXE%" -classpath "%CLASSPATH%"org.cinchapi.concourse.server.cli.ManageUsersCli %CMD_LINE_ARGS%