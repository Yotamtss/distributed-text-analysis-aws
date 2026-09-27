Shir Ben Aderet 212074512
Yotam Tsur 322209164

Project Description and Execution

Instructions on How to Run the Project:
1. AWS Setup: AWS CLI is configured with valid credentials and the appropriate .pem file for the key pair is available for EC2 access.
2. Compile Java code and package it into a .jar file. This file contains the Worker and Manager classes. 
3. Scp the jar file into the instance.
4. Create an AMI that has the current setup, any code needed for the Worker and Manager to run, and the .jar file.
5. Execution: Run the LocalApp from your local machine using the following command format:
    java -jar yourjar.jar <inputFileName> <outputFileName> <n> [terminate]


Explanation of Program Components and Flow:
This program implements a text analysis system, managed by a central Manager on an EC2 instance and executed by Worker EC2 instances.
The program uses AWS SQS for communication and AWS S3 for storage.

Program flow:
* Local App is initialized, and launced Manager if there is not one running. Next it aploads the input file into a S3 bucket, and sends a task message to the Manager through SQS.
* Manager downloads input file from S3, and parses the file into seperate analysis tasks, while using the n value to decide how many workes to launch for the tasks.
* Manager sends analysis task through SQS to Workers. 
* Workers poll tasks from SQS, each Worker preforms the anlysis and uploads the results to the S3 bucket.
* Worker sends a "done" message to Manager through SQS and deletes job message from queue.
* Once all jobs are complete, the Manager generates an HTML summary, uploads it to S3, and notifies the LocalApp's unique response queue
* The LocalApp receives the notification, downloads the final HTML summary from S3, saves it locally , and cleans up its queue.
* If requested, the LocalApp sends a terminate signal to the Manager, which then shuts down all Workers and terminates itself.


Security:
* Credentials are pre configured in the environment using aws configure command.

scalability:
* Multiple Clients: The Manager uses a hash map keyed by the unique bucketName (which includes a UUID), and it's value is the task's state, to manage the state of multiple, simultaneous client requests in parallel.
* The number of Workers scales dynamically based on the current workload (but there is a max active worker count).

Presistance:
* Worker.java uses visibilityTimeout of 5 minutes when receiving a message. If a Worker crashes or stalls for more than 5 minutes before deleting the message, SQS will make the message visible to another Worker.
* Messages are deleted only after the job is done and the results/errors are reported to the Manager.
* Worker Exception: If the analysis throws an exception, the Worker catches it, sends an ERROR message to the Manager, and then deletes the job message, allowing the Worker to continue processing other jobs.

Threads in Application:
* Manager: A dedicated ResultListener thread is used to process messages from the Worker To Manager SQS while the main thread handles messages from the Local To Manager SQS (new tasks and terminate signals).
Adding thread to the application is a good idea, and it is crucial for concurrency and responsiveness. The threads use allows Manager to simultaneously listen for new client tasks, while processing worker results without blocking.

Termination Process:
1. Local App sends terminate signal to the Manager through SQS.
2. Manager receives the signal, sets terminateRequested = true.
3. The ResultListener thread continues to run until both terminateRequested is set and activeTasks is empty.
4. Manager terminates all active worker instances, and its own instance.
Terminates all active Worker instances.

Division of Labor in Wokers:
* The Workers use a shared SQS (ManagerToWorkersQueue), which means workers compete for jobs.
It is expected that some workers may process more messages than others (and some are "slacking" compared to others).

Is The System Distributed?
* The system is very distributed. The Local App, Manager and Workers are all on different machines (Local computer, and EC2 instances).
Communication between the machines is almost entirely asynchronous via SQS, meaning when a machine sends a message it can continue its work without waiting for an immediate reply from the receiver.
The only necessary synchronous wait is the Local App waiting for the final "done" message from Manager.