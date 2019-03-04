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

By default, each FE sends requests to 2 separate RMs. The system is therefore tolerant of 1 failure. In order to test
this, a replica may be shut down mid-operation. 