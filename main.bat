start cmd.exe /k "cd./venv/Scripts && activate && cd ..\.. && python -m Pyro4.naming"
start cmd.exe /k "cd./venv/Scripts && activate && cd ..\.. && python -m frontend"
timeout 10
FOR /L %%i IN (1,1,3) DO (
    start cmd.exe /k "cd./venv/Scripts && activate && cd ..\.. && python -m replica"
)
cls
cd./venv/Scripts && activate && cd ..\.. && python -m client