#! /bin/bash

python3 /home/ec2-user/sbmd/sbahnmuc03b_TransferDB.py
echo "Finished 03 Transfer"

python3 /home/ec2-user/sbmd/sbahnmuc04b_TransferDB.py
echo "Finished 04 Transfer"

python3 /home/ec2-user/sbmd/sbahnmuc05b_TransferDB.py
echo "Finished 05 Transfer"
