# AccountStream

## Overview

AccountStream streams updated account information to our contact discovery
service.  It pulls information from two places:

* The DynamoDB accounts table
* A filtered stream of DynamoDB updates

As AccountStream streams data down to a client, it also streams down
continuation tokens regularly.  A client may request a new stream with
a previous continuation token, and in so doing only receive updates from
when that continuation token was initially provided to them.

See the GRPC streaming service at `src/main/proto/accountstream.proto`
for the specific interface.

This system relies on an AWS lambda (code in the `filter-cds-updates`
subdirectory), which receives a DynamoDB update stream from the Accounts
table and filters out just the subset of updates that contact
discovery would find useful.  It drops those into a Kinesis stream, which
AccountStream pulls from.

Should a client not have a continuation token, it will receive all relevant
account records, then a continuous set of updates.
AccountStream's GRPC streams will never willingly close; a connected client
will receive updates ad infinitum.  Only a client close or a server
error will cause the stream to stop.

## Building

AccountStream is a Micronaut process.  It can be locally built with

```
mvn clean verify
```

and locally run with

```
mvn mn:run
```

Lambda can be built/tested with

```
cd filter-cds-updates
mvn clean verify
```
