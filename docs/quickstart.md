### Quickstart

To start the Demo-UI and CompoDBs backend server a few steps first need to be done. In order to experiment 
with the StackOverflow benchmark, run the following commands:

```
$ cd compodb-core/tests/
$ ./download_stackoverflow.sh
```
Be aware that this can take some time and needs a stable connection (loads about 50GB).

To start the Project, you can simply run:
```
$ ./start.sh

## Additional flags

# With log-output
$ ./start.sh -dev

# To set the scale factor of the TPC-H/DS Benchmarks, add the desired SF-Number (e.g. 10 or 5, default is 1)
$ ./start.sh -dev 10
$ ./start.sh 5
```
