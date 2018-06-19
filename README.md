# fastmq

FastMQ is a toy project written in kotlin and java , so it was supposed to be a distributed message queue.

## Architecture
The storage engine is based on Apache BookKeeper, and communication between broker and client is built on top of netty using
Google Protocol Buffer ,so it's gonna be tremendously easy to write different programming language SDK based on this.
The best advantage of this architecture is scalability, you can add more computation node or storage node based on your need.

