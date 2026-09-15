```markdown
# CS324 Assignment 1 (2026) — Distributed Worker Cluster

## Clone the repo

```
git clone <your-repo-URL>
cd A1_CS324
```

## Switch to your branch

```
git checkout aryan
```
(or `suri` / `alvin`)

If the branch isn't showing up locally:
```
git fetch origin
git checkout -b aryan origin/aryan
```

## Push your changes

```
git add -A
git commit -m "your message here"
git push origin aryan
```

## Build

```
mvn compile
```

## Run

**1. Bootstrap Node** (start first, one only):
```
mvn exec:java -Dexec.mainClass=com.cs324.bootstrap.BootstrapNode -Dexec.args="1099"
```

**2. Worker Node** (one per terminal, unique id + port each time):
```
mvn exec:java -Dexec.mainClass=com.cs324.worker.WorkerNode -Dexec.args="5 3500 localhost 1099"
```

**3. Client** (Swing GUI):
```
mvn exec:java -Dexec.mainClass=com.cs324.client.ClientApp
```

## Packages
- `common` — shared RMI interfaces + constants
- `bootstrap` — Bootstrap Node
- `worker` — Worker nodes (election + job execution)
- `client` — Client GUI
```