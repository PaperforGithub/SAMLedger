# SAMLedger: an efficient sharded blockchain ledger based on simulated account migration
## Deployment and Testing
**Deployment** 
- We recommend deploying SAMLedger on Ubuntu 20.0. Nodes are recommended to have a configuration of at least 4 CPU cores and 16 GB of memory.
  
**Testing**
- We have provided test scripts in folder "./SAMLedger/scripts/" for different parameters, including ETH-workload, Zipfian-Workload, and Hotspot-Workload. Testing can be initiated by executing the command "python3 ./scripts/exp_run.py Result", and the corresponding test results will be automatically saved to the folder "~/Result".
- System parameters can be configured by modifying the macro variables in file "./SAMLedger/config.h".
