SET "num_replicas=3"
SET "num_frontends=1"
SET "user_id=1"

IF NOT "%1"=="" (

    SET "num_frontends=%1"

)

IF NOT "%2"=="" (

    SET "num_replicas=%2"

)

IF NOT "%3"=="" (

    SET "user_id=%3"

)

start cmd.exe /k "cd./venv/Scripts&&activate&&cd ..\..&&python -m Pyro4.naming"
FOR /L %%i IN (1,1,%num_frontends%) DO (
    start cmd.exe /k "cd./venv/Scripts&&activate&&cd ..\..&&python -m frontend"
)
cls
FOR /L %%i IN (1,1,%num_replicas%) DO (
    start cmd.exe /k "cd./venv/Scripts&&activate&&cd ..\..&&python -m replica"
)
cls
cd./venv/Scripts&&activate&&cd ..\..&&python -m client %3
cls