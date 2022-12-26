@echo off

C:

REM Google search rank investigation start.
REM *** now on seraching... ***

set FOLDER_PATH=%~dp0

Python %FOLDER_PATH%\getASINAPI.py 2> log.txt
