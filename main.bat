SET "num_replicas=3"
SET "num_frontends=1"

IF NOT "%1"=="" (

    SET "num_frontends=%1"

)

IF NOT "%2"=="" (

    SET "num_replicas=%2"

)

start cmd.exe /k "cd./venv/Scripts && activate && cd ..\.. && python -m Pyro4.naming"
FOR /L %%i IN (1,1,%num_frontends%) DO (
    start cmd.exe /k "cd./venv/Scripts && activate && cd ..\.. && python -m frontend"
)
cls
timeout 15
FOR /L %%i IN (1,1,%num_replicas%) DO (
    start cmd.exe /k "cd./venv/Scripts && activate && cd ..\.. && python -m replica"
)
cls
cd./venv/Scripts && activate && cd ..\.. && python -m client