FOR /L %%i IN (1,1,3) DO (
    start cmd.exe /k "cd./venv/Scripts && activate && cd ..\.. && python -m replica"
)