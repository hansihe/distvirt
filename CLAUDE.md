# distvirt

## Components

### distvirt-orchestrator
Orchestrator manages the distvirt cluster. Distvirt is meant for staging environments, there is a singular orchestrator. If lost, the cluster loses all state.

Orchestrator has a few layers of tests:
* Unit tests
* Integration tests
* `stateright` model tests
* Scenario tests, tests full orchestrator with a harness

### distvirt-worker
Multiple workers can connect to an orchestrator. Workers are fairly dumb, they execute commands from the orchestrator.

They host containers (in microvms) and the userspace network fabric.

Worker has a few layers of tests:
* Unit tests
* Integration tests
* E2E tests. Since these spin up VMs, they require root to run

### distvirt-*-protocol
Contains protocol definitions for communication between the different components.

## Guidelines
* If you want to check, build and test your code, just run the test command. Running the test command also compiles the code, no need for both.
