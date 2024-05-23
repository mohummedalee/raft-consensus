## Raft : Implementation in Go
CS 7610: Distributed Systems

December 3, 2018
Muhammad Ali and Meredith Accum


### Building

All source code `*.go` files are stored in a single folder called raft.
There is a makefile included for building, and for cleaning up
any files that are used to save the persistent state of the Raft instances.

To build :

```
cd raft
go build
```

or 

```
cd raft
make
```

### Running

After building:

To run the client:
From any machine that isn't one of the raft instances, run this command:
`./raft -client`

To run a raft instance from a fresh state:
```
make fresh
./raft
```

or 

```
rm savedState*
./raft
```

To run a raft instance from a saved state:
`./raft`

### Testing

There is an included `hostfile.txt` with the addresses of five
linux machines 030-034. 

Log in to each linux machine and start the raft program with 

```
make clean
make 
./raft
```

Log in to a 6th machine and start the client
`./raft -client`

Interact with the client by typing commands. The client will contact
nodes in the Raft cluster. 

To test leader elections, kill various Raft instances with Ctrl-C and 
then restart them, if desired.

We've hardcoded a port into the code for testing convenience. There
should be no need to change this unless multiple people are trying to
test the program at the same time.

Note that a raft instance will try to load its saved state from
a file, if one is found. To start from a completely fresh state,
run `make clean` before running `./raft`. 
