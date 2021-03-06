# Distributed Systems Coursework

This is my submission for the Distributed Systems coursework of the Networks and Systems module at Durham University, 2019.

## Diagram

The distributed system diagram, showing major workflows among servers, a client, and the frontend, can be found at `documents/diagram.pdf`.

## Running

#### Packages and version

This project requires `Pyro4` to run. It may be installed using the instructions found [here](https://pythonhosted.org/Pyro4/install.html).
The project may also require `msgpacker`, which can be installed using `python -m pip install msgpacker`. The project was
tested using Python `3.6.3`, on a Windows system.

#### main.bat

To run the system, use `main.bat`, located in the base repository. `main.bat` accepts three arguments: the number of
frontends (FEs) to launch, the number of replica managers (RMs) to launch, and the user ID of the client. For example, `main.bat 2 4 42`
will launch `2` frontends, `4` replicas, and assigns the client an ID of `42`. These values respectively default to `1`, 
`3` (as per the specification) and a random number between `1` and `611` (the maximum user ID in the database). The 
original terminal window serves as the client. By entering commands as the client, the propagation of requests, queries,
 gossip, and timestamps can be observed between the client, the FEs, and the RMs.


#### If main.bat doesn't work

Run `python -m Pyro4.naming` to launch the Pyro Name Server, `python -m frontend` to 
launch a frontend, `python -m replica` to launch a replica, and `python -m client` to launch the client. These must be 
in separate terminal windows, and the Pyro Name Server must be running before any other DS components can be launched.

## Functionality

There are 6 commands available. These are listed by the client:

1. Get average rating. QUERY. Returns the average rating of the movie `(movieID)`.
2. Create user rating. UPDATE. Inserts a new rating `(userID, movieID, rating)`.
3. Get user rating. QUERY. Returns this user's rating of the movie with the inputted `movieID`.
4. Update user rating. UPDATE. Updates an existing user rating `(userID, movieID, rating)`.
5. Delete user rating. UPDATE. Deletes an existing user rating `(userID, movieID)`.
6. Get all user ratings. QUERY. Returns all ratings that this user has made `(userID)`

## Notes

* Each replica initialises their database from the same file, `database/data.sqlite`, but only modifies an in-memory copy.
As a result, changes between sessions are not saved, but this ensures replicas only modify their copy of the data, 
rather than the (centralised) original. 
* If a RM receives a query  or update and has out-dated information, it immediately requests the information from an RM with
the correct information. In this way, the user **never** receives out-of-date information, the strictest possible consistency control.
Updates are causally ordered.
* By default, each FE sends requests to `2` separate RMs. This parameter is encoded as `FAULT_TOLERANCE` in `frontend.py`.
The system is therefore tolerant of `1` failure. Replicas respond with arbitrary statuses (`ACTIVE`, `OVERLOADED`, 
`OFFLINE`), as per the specification, when queried. The system is structured in such a way that a replica actually being
 offline is identical to a replica responding with a status of `OFFLINE`. 
* Detailed descriptions of the functionality of each component may be found in the source code.
* The system should in theory work with multiple FEs, but this has been tested considerably less thoroughly than the 
stated case of just `1`. Use at your own risk!
* The system is designed in such a way that it should continue to function successfully if new RMs and FEs are added during 
operation. As before, this is a much stronger guarantee for RMs than FEs!

##Tests

Unit tests for the more complex Timestamp and Log functionality can be found in `tests`, and run using `python -m tests`.