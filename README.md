# Distributed Systems Coursework

This is my submission for the Distributed Systems coursework of the Networks and Systems module at Durham University, 2019.

## venv

In order to ensure consistency, the virtual environment `venv` used in development has been packaged in this repository. 
To run Python, `venv` must be activated using `cd ./venv/Scripts && activate`. 

## Running

To run the system, use `main.bat`, located in the base repository. `main.bat` accepts three arguments: the number of
frontends (FEs) to launch, the number of replica managers (RMs) to launch, and the user ID of the client. For example, `main.bat 2 4 42`
will launch `2` frontends, `4` replicas, and assigns the client an ID of `42`. These values respectively default to `1`, 
`3` (as per the specification) and a random number between `1` and `611` (the maximum user ID in the database). The 
original terminal window serves as the client. By entering commands as the client, the propagation of requests, queries,
 gossip, and timestamps can be observed between the client, the FEs, and the RMs.


#### If main.bat doesn't work

Activate `venv` and run `python -m Pyro4.naming` to launch the Pyro Name Server, `python -m frontend` to 
launch a frontend, `python -m replica` to launch a replica, and `python -m client` to launch the client. These must be 
in separate terminal windows.

## Notes

Each replica initialises their database from the same file, `database/data.sqlite`, but only modifies an in-memory copy.
As a result, changes between sessions are not saved, but this ensures replicas only modify their copy of the data, 
rather than the (centralised) original. 

By default, each FE sends requests to `2` separate RMs. This parameter is encoded as `FAULT_TOLERANCE` in `frontend.py`.
The system is therefore tolerant of `1` failure. Replicas respond with arbitrary statuses (`ACTIVE`, `OVERLOADED`, 
`OFFLINE`), as per the specification, when queried. The system is structured in such a way that a replica actually being
 offline is identical to a replica responding with a status of `OFFLINE`. 
 
Detailed descriptions of the functionality of each component may be found in the source code.
 
The system should in theory work with multiple FEs, but this has been tested considerably less thoroughly than the 
stated case of just `1`. Use at your own risk!

The requested distributed replication system diagram can be found at `documents/ds.pdf`.

##Tests

Unit tests for the more complex Timestamp and Log functionality can be found in `tests`, and run using `python -m tests`.