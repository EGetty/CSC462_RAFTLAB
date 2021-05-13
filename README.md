# CSC462_RAFTLAB

RAFT code in Golang for CSC462 Summer 2020. Base code provided. Modified code was completed under src\kvraft, src\raft, and src\shardkv. 

RAFT is based on the following paper https://raft.github.io/raft.pdf by Diego Ongaro and John Ousterhout from Stanford University.

Build code under folder kvraft with: 

```
go build
```

Then run the code with:

```
go test
```

Code will output the following when run:

![Sample output tests](https://github.com/EGetty/CSC462_RAFTLAB/blob/main/sample_output_tests.png)

If test prints are used, output will look similar to the following:

![Sample output with values printed](https://github.com/EGetty/CSC462_RAFTLAB/blob/main/sample_output_with_prints.png)

![Sample output using existing ID](https://github.com/EGetty/CSC462_RAFTLAB/blob/main/sample_output_with_prints_existing_id.png)
