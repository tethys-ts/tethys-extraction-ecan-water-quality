set root=D:\programs\Anaconda3_32bit
call %root%\Scripts\activate.bat %root%
call activate HydstraRec
call python %~dp0\site_data.py
call python %~dp0\ts_data.py
