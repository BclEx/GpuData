@echo off
pushd ..\GpuStructs
echo Building GpuStructs:
PowerShell -Command ".\psake.ps1"
popd

::
echo GpuStructs
mkdir packages\GpuStructs.1.0.0
pushd packages\GpuStructs.1.0.0
set SRC=..\..\..\BclEx-Abstract\Release
xcopy %SRC%\BclEx-Abstract.1.0.0.nupkg . /Y/Q
..\..\tools\7za.exe x -y BclEx-Abstract.1.0.0.nupkg -ir!lib
popd
::pause