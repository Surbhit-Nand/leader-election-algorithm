# CS324 Assignment 1 (2026) — Distributed Worker Cluster

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

## Packages
- `common` — shared RMI interfaces + constants
- `bootstrap` — Bootstrap Node
- `worker` — Worker nodes (election + job execution)
- `client` — Client GUI
