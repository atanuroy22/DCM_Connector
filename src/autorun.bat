@echo off
:: Update the file path
::=============================================================
set "base_path=C:\Users\Atanu Roy\Downloads\DCM_py"
::=============================================================



set "py_file_path=%base_path%\app.py"
set "vbs_file_path=%base_path%\script.vbs"
set "startup_folder=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup"


(
    echo Set objShell = CreateObject("WScript.Shell")
    echo objShell.Run "%py_file_path%", 0, False
    echo WScript.Sleep 10000
    echo WScript.Quit
) > "%vbs_file_path%"

:: Copy the VBS file to the startup folder
copy /Y "%vbs_file_path%" "%startup_folder%"

:: start the py in background
start /b pythonw "%py_file_path%"