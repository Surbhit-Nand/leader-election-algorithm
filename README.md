# CS324 Assignment 1 (2026) - Distributed Worker Cluster

## First-Time Setup

Clone the repo:
```
git clone <your-repo-URL>
```

Move into the project folder:
```
cd A1_CS324
```

Switch to your branch (e.g. `suri`):
```
git checkout <name>
```

Build the project:
```
mvn compile
```

Run ClientApp.java by right clicking the file.


---

## Every Time You Work On This (AFTER IMPLEMENTING WORKERS)

Pull the latest changes first:
```
git pull origin <name>
```

Run the Bootstrap Node (start first, one only):
```
mvn exec:java -Dexec.mainClass=com.cs324.bootstrap.BootstrapNode -Dexec.args="1099"
```

Run a Worker Node (one per terminal, unique id + port each time):
```
mvn exec:java -Dexec.mainClass=com.cs324.worker.WorkerNode -Dexec.args="5 3500 localhost 1099"
```

Run a Client (Swing GUI):
```
mvn exec:java -Dexec.mainClass=com.cs324.client.ClientApp
```

When you're done, stage your changes:
```
git add -A
```

Commit them:
```
git commit -m "your message here"
```

Push them:
```
git push origin <name>
```

---

## Design Decisions

These are settled interpretations of ambiguous spec points. Keep implementations consistent with these across all worker/election/job-dispatch code.

- **JAC increments per worker assignment, not per client job.** Per the spec: *"The counter is incremented each time that worker, while acting as coordinator, assigns a job to another worker."* Taken literally, if a single client job is split across 3 other workers, the coordinator's JAC increases by 3 in that one request — not by 1. A term (5 increments) can therefore end well before 5 separate client jobs have been processed, depending on how many workers are active.
- **The coordinator participates in every job's computation.** It does not only delegate — it takes its own share of the workload (its own sub-range/sub-list) alongside the workers it assigns tasks to.
- **Coordinator failure detection uses heartbeats.** Workers periodically ping the current coordinator; a missed response is what triggers a new election. This is separate from the election-termination mechanism (echo-based, see election design notes).
- **Fault tolerance is out of scope for this assignment.** No requirement to handle a worker or coordinator dying mid-job or mid-election — that's Assignment 2's focus.
- **Neighbour connections are bidirectional.** When worker A connects to worker B (via a peer the Bootstrap Node introduced), both A and B add each other to their local neighbour lists — a successful RMI call is treated as the acknowledgement, no separate ack message needed.

---

## Packages
- `common` - shared RMI interfaces + constants
- `bootstrap` - Bootstrap Node
- `worker` - Worker nodes (election + job execution)
- `client` - Client GUI