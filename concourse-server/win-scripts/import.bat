@if "%DEBUG%" == "" @echo off
@rem This config will setup all the enviornment variables and check that
@rem paths are proper
call .env.bat %*

@rem Specify the fully qualified name of the java CLI class to run. 
SET CLI="org.cinchapi.concourse.importer.cli.GeneralCsvImportCli"


@rem run the program
"%JAVA_EXE%" -classpath "%CLASSPATHorg.cinchapi.concourse.cli.CommandLineInterfaceRunner "%CLI%" %CMD_LINE_ARGS%