# CS324 Assignment 1 (2026) - Distributed Worker Cluster

## Prerequisites

- **JDK 25 (or newer)** — the project targets Java 25 (latest LTS). You must have JDK 25+ installed or the build will fail with a "release version not supported" error.
  - Check your version: `java -version`
  - If you have an older JDK, install JDK 25 (e.g. from [Adoptium](https://adoptium.net/)) and make sure it is selected in your IDE / `JAVA_HOME`.
- **Maven 3.9+** — used to build and run the project.

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

To run the Bootstrap Node, the Workers and the Client, see [How to Run](#how-to-run-any-ide-or-terminal) below.

---

## How to Run (any IDE or terminal)

The project has no runtime dependencies (JUnit is `test`-scoped), so the runtime classpath is just `target/classes`. These commands behave the same in VS Code, Apache NetBeans, IntelliJ, Eclipse and a plain terminal. Every node is a separate process, so start each one in its own terminal.

Bootstrap Node (start first, one only):

```
java -cp target/classes com.cs324.bootstrap.BootstrapNode 1099
```

Worker Node (one per terminal, unique id + port each time). The arguments are `workerId rmiPort bootstrapHost bootstrapPort`:

```
java -cp target/classes com.cs324.worker.WorkerNode 1 3500 localhost 1099
java -cp target/classes com.cs324.worker.WorkerNode 2 3501 localhost 1099
```

Client (Swing GUI - as many as you like):

```
java -cp target/classes com.cs324.client.ClientApp
```

### VS Code

Press **F5** (Run and Debug) and pick a configuration from `.vscode/launch.json`:

| Configuration | Main class | Arguments |
| --- | --- | --- |
| `Bootstrap Node (port 1099)` | `com.cs324.bootstrap.BootstrapNode` | `1099` |
| `Worker 1 (id 1, port 3500)` | `com.cs324.worker.WorkerNode` | `1 3500 localhost 1099` |
| `Worker 2 (id 2, port 3501)` | `com.cs324.worker.WorkerNode` | `2 3501 localhost 1099` |
| `Worker 3 (id 3, port 3502)` | `com.cs324.worker.WorkerNode` | `3 3502 localhost 1099` |
| `Client App` | `com.cs324.client.ClientApp` | - |

Every configuration names an explicit main class, so the IDE never has to guess it.

### Apache NetBeans

- **F6** (*Run Project*) and **Shift+F6** (*Run File*) work as they are - NetBeans resolves the main class itself and starts the program in a forked JVM.
- Choose the main class under *Project Properties -> Run -> Main Class* (arguments and VM options live there too). NetBeans stores that in `nbactions.xml` in the project root - commit it so the whole team shares the same run setup.
- Register a **JDK 25 or newer** under *Tools -> Java Platforms*, because the pom targets `release 25`.

### Command line with Maven (optional)

The `client`, `bootstrap` and `worker` profiles only set the default `exec.mainClass` / `exec.args`, so the short form is enough for a quick look at a node:

```
mvn -Pclient exec:java
mvn -Pbootstrap exec:java
mvn -Pworker exec:java -Dexec.args="2 3501 localhost 1099"
```

For anything that has to keep running, fork a JVM with `exec:exec` (`%classpath` is replaced by Maven with the project classpath):

```
mvn exec:exec -Dexec.executable=java -Dexec.args="-classpath %classpath com.cs324.bootstrap.BootstrapNode 1099"
```

**Why fork:** `exec:java` runs the program inside the Maven JVM and only tracks threads created in its own thread group. RMI (`LocateRegistry.createRegistry`, exported remote objects) creates non-daemon threads in the *system* thread group, so a node started that way can disappear as soon as the build finishes - measured on this repo: `mvn -Pbootstrap exec:java` printed `Bootstrap Node running on port 1099 as BootstrapRegistry` and then exited, while the `exec:exec` form was still listening on port 1099 after 30 seconds and released the port when Maven was stopped.

### Troubleshooting

- `Error: Could not find or load main class ${packageClassName}` - the IDE ran NetBeans' default Maven "run" action without substituting `${packageClassName}` (this is what the Oracle Java extension for VS Code did). That placeholder is filled in by the IDE, never by Maven, so it must not reach the command line. Use one of the recipes above instead.
- `release version 25 not supported` - the IDE or Maven is running on a JDK older than 25. Point it at JDK 25+.

---

## Every Time You Work On This (AFTER IMPLEMENTING WORKERS)

Pull the latest changes first:

```
git pull origin <name>
```

Rebuild after changing code:

```
mvn -q compile
```

Start the Bootstrap Node first (one only), then one Worker per terminal (unique id + port each time) and as many Clients as you like - the full list is in [How to Run](#how-to-run-any-ide-or-terminal):

```
java -cp target/classes com.cs324.bootstrap.BootstrapNode 1099
java -cp target/classes com.cs324.worker.WorkerNode 1 3500 localhost 1099
java -cp target/classes com.cs324.client.ClientApp
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

- **JAC increments per worker assignment, not per client job.** Per the spec: _"The counter is incremented each time that worker, while acting as coordinator, assigns a job to another worker."_ Taken literally, if a single client job is split across 3 other workers, the coordinator's JAC increases by 3 in that one request — not by 1. A term (5 increments) can therefore end well before 5 separate client jobs have been processed, depending on how many workers are active.
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
