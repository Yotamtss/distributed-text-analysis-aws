# Distributed Text Analysis on AWS

A distributed text-analysis system built on AWS EC2, SQS, and S3. A local client uploads a text file and a small fleet of cloud workers analyzes it in parallel using the Stanford CoreNLP toolkit, coordinated by a manager node.

Built by Shir Ben Aderet and Yotam Tsur as an assignment for a Distributed Systems Programming course.

## Architecture

The system has three components, each running on a different machine, communicating asynchronously over SQS:

- **LocalApp** — runs on the local machine. Uploads the input file to S3, launches a Manager EC2 instance if none is running, sends a task request over SQS, and waits for the result.
- **Manager** — runs on an EC2 instance. Downloads the input file from S3, splits it into analysis jobs, and dispatches them to Workers based on the requested parallelism (`n`). Tracks job state per client (keyed by a unique S3 bucket name) so multiple clients can be served concurrently, and scales the number of active Worker instances with the workload.
- **Worker** — runs on EC2 instances launched by the Manager. Pulls jobs from a shared SQS queue, runs the text analysis, uploads results to S3, and reports completion back to the Manager.

```
LocalApp --(SQS: task)--> Manager --(SQS: jobs)--> Workers
   ^                         |                         |
   |                         v                         v
   +---(SQS: done, S3 result)-+<----(SQS: done, S3)-----+
```

Once all jobs for a client finish, the Manager builds an HTML summary, uploads it to S3, and notifies the LocalApp, which downloads and saves it locally.

### Concurrency and fault tolerance

- The Manager runs a dedicated `ResultListener` thread for Worker→Manager messages, separate from its main loop that handles new client tasks and termination signals, so it never blocks on one while handling the other.
- Jobs use a 5-minute SQS visibility timeout: if a Worker stalls or crashes before deleting a message, it becomes visible to another Worker again.
- A message is only deleted after its result (or error) has been reported, so no job is silently lost. If analysis throws an exception, the Worker reports an error for that job and moves on to the next one.

### Termination

The LocalApp can request shutdown with a `terminate` flag. The Manager finishes any in-flight jobs, terminates all active Worker instances, and then terminates itself.

## Project layout

```
demo/                       Maven project (build this)
  pom.xml
  src/main/java/com/example/
    LocalApp.java            entry point run locally
    Manager.java              runs on the Manager EC2 instance
    Worker.java                runs on Worker EC2 instances
    S3Helper.java               shared S3 upload/download helpers
LocalApp.java, Manager.java,  reference copies of the same
Worker.java, S3Helper.java    sources kept at the repo root
README.txt                  original assignment write-up
Assignment 1 (2).pdf        assignment specification
```

## Building and running

Requirements: JDK 11, Maven, and an AWS account with EC2/S3/SQS access configured locally (e.g. via `aws configure`) and an EC2 key pair.

```bash
cd demo
mvn clean package
```

This produces a runnable shaded jar (`target/demo-1.4.jar`, entry point `com.example.LocalApp`). Copy the jar to the AMI used for Manager/Worker instances, then run the client locally:

```bash
java -jar demo-1.4.jar <inputFileName> <outputFileName> <n> [terminate]
```

- `inputFileName` — text file to analyze
- `outputFileName` — where to save the resulting HTML summary
- `n` — controls how many Worker instances the Manager launches for the job
- `terminate` (optional) — shuts down the Manager and all Workers once this job completes

## Security note

AWS credentials are expected to already be configured in the local environment (`aws configure`) and are never embedded in the code. No secrets, keys, or credentials are included in this repository.
