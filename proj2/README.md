# 2nd Project

- João Ricardo Cardoso
- João Pires
- Miguel Romariz
- Nuno Marques

## Important note

The scripts may have a CRLF end of line sequence when checked out from the repository, which might bring problems when running in a Linux system. To correct this problem, the conversion to LF must be done.

## Compile

Run the compile.sh script inside the src/ folder of our project.

## Cleanup

Run the cleanup.sh inside the build/ folder, using as parameter the peer id to which the cleanup should be made.

## Run

Inside the build/ folder, run, by order, the following scripts:

 - rmi.sh (no parameters needed)
 - tracker.sh (or tracker_default.sh)
 - peer.sh (or peer_default.sh)

## Test the protocols

Run the test.sh script inside the build/ folder with the following parameters:

test.sh <peer_id> BACKUP|RESTORE|DELETE|RECLAIM|STATE [<opnd_1>] [<optnd_2]

## Parameters for the scripts

tracker.sh <protocol_version> <self_tracker_ID> <self_port> (<target_tracker_ip><target_tracker_port> <target_tracker_id>)*

tracker_default.sh <node_id> <port>

peer.sh <protocol_version> <peer_ID> <port> <tracker_ip> <tracker_port> <tracker_id>

peer_default.sh <node_ID>
