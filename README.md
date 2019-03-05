# Distributed Systems Coursework

This is my submission for the Distributed Systems coursework of the Networks and Systems module at Durham University, 2019.

## Running

To run the system, use `main.bat`, located in the base repository. By default, this launches 5 terminal windows - the 
Pyro name server, 1 frontend (FE), and 3 replicas (RM). The original terminal window serves as the client. By entering
commands as the client, the propagation of requests and queries can be traced through the FE and the RMs. `main.bat`
accepts 2 arguments: the number of FEs, and the number of RMs. To launch 3 FEs and 2 RMs, for example, run `main.bat 3 2`.

In order to ensure consistency, the virtual environment `venv` used in development has been packaged in this repository. 
To run a Python module, the following command must be used: `cd./venv/Scripts && activate && cd ..\..`. This will
activate the virtual environment and return the CLI to the base directory, from which Python commands can be run using
`python -m frontend`, for example.

Each replica updates its value only in memory, but all initialise their database from the same file, 
`database/ratings.sqlite`. As a result, changes between sessions are not saved, but this ensures that the gossip
architecture provably works.

By default, each FE sends requests to 2 separate RMs. The system is therefore tolerant of 1 failure. In order to test
this, a replica may be shut down mid-operation. This parameter is encoded as `FAULT_TOLERANCE` in `frontend.py`.

All ratings are associated with a user ID. There are 611 user IDs in the ratings DB: if no ID is inputted, a random ID
in the range `0 - 611` is selected: this is displayed on operations.

Unit tests for the more complex Timestamp and Log functionality can be found in
`tests`. They may be run using `python -m tests`.