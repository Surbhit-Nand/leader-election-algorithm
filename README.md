# CS324 Assignment 1 — Distributed Worker System

## Package structure
- `com.cs324.common` — shared RMI interfaces (`WorkerRemote`, `BootstrapRemote`) and `Constants`
- `com.cs324.bootstrap` — Bootstrap Node (worker registry only, no election/job role)
- `com.cs324.worker` — Worker nodes (election, coordination, job execution)
- `com.cs324.client` — Client GUI (job submission)

## Requirements
- JDK 24
- Maven (or use NetBeans/VS Code's built-in Maven support)

## Build
```
mvn compile
```

## Run (once implemented)
Start each component in its own terminal, from the project root:

1. Bootstrap Node:
   ```
   mvn exec:java -Dexec.mainClass=com.cs324.bootstrap.BootstrapNode
   ```
2. Worker Node(s) — repeat with different IDs/ports for each worker:
   ```
   mvn exec:java -Dexec.mainClass=com.cs324.worker.WorkerNode -Dexec.args="<id> <port>"
   ```
3. Client(s):
   ```
   mvn exec:java -Dexec.mainClass=com.cs324.client.ClientApp
   ```

## Status
Project skeleton only — `common` interfaces and placeholder classes are in place.
Implementation (election logic, job splitting, GUI) is in progress.

## Team
- Shamal Prasad — S11219545
- Ravinesh Narayan — S11197324
- Isaiah Narayan — S11175115
