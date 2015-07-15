# aio_test

The purpose of this test is to get an idea of AIO performance for a particular system/disk given a dynamic range of parameters.

The test is highly configurable in that the total number of AIO writes can be configured, as well as the size of each write and 
the total number of writes the test will perform. 

The test results include averga throughput to disk, any reported AIO errors, and min, max, and avergae AIO write delays. 

Future versions of this test should include simultaneous reads from the files being written to for more functional performance measuring. 

aio_test usage:

Available options:
  -b, --batch          size of maximum batch (default is no batching)
  -d, --directory      Directory to write files
  -f, --fds            number of file descriptors
  -h. --help           print help
  -l, --length         length of each batch write
  -p, --pool           pool AIO's to specific fd's
  -r, --reads          number of max outstanding reads
  -t, --total          total number of writes
  -v, --verbose        verbose printing
  -w, --writes         number of max outstanding writes
  -W, --wait           wait for W seconds for writes/reads to coplete

By default, the test will attempt to write to disk 100000 times using 16 AIO's, 1024 bytes per write to a single file. The command line 
would look like this:

`
./aio_test -l 1024 -w 16 -t 100000 -f 1
`

Example output looks like this:

> Results
> ********************************************************************
>        Errors (write fails + out. req's):      0
>        Maximum Write Time:                     20.25 ms
>        Minimum Write Time:                     0.02 ms
>        Average Time Per Write:                 0.7388 ms
> Total Time: 5.505 seconds. Total Written: 102.40 mbytes
> Average Writes/Second: 18166.23 (Write Size: 1024 bytes)
> Average Throughput/Second: 18.60 mbytes
> ********************************************************************

More advanced options for this test are the concepts of batching and pooling. 

Since the test allows multiple AIO attempts to multiple files, pooling ensures that the same AIO attempts are always attempted to the same FD. 
This creates a more realistic test scenario with regard to how AIO is typically used within an application.

Batching allows multiple writes to batch up in the pool in the event that all AIO's are currently used up, which can increase throughput for bursts
of small amount of data being attempted. 
