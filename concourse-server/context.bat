@if "%DEBUG%" == "" @echo off
set FILE="context.txt"
del %FILE% /f >nul 2>&1
for /f "delims=" %%i in ('git rev-parse HEAD') do set Last_COMMIT=%%i
echo Last Commit: %LAST_COMMIT% > %FILE%
git status >> %FILE%
for /f "delims=" %%i in ('git diff --name-only ../') do (
  git diff ../%%i >> %FILE%
  echo. >> %FILE%
  echo. >> %FILE%
  echo. >> %FILE%
)

EXIT /B 0
