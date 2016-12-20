# How the project works:
## Local:
Expect this arguments:
-i with the path of the input file
-o with the path of the output file
-n the n number
-d the d number
-t (optional) terminate the manager when you are done

The input file should look like this (json file):
```
{
  "start-date": "2016-10-30",
  "end-date": "2017-01-27",
  "speed-threshold": 10,
  "diameter-threshold": 200,
  "miss-threshold": 0.3
}```

n and d dicteds the number of workers that will be created for the task:
    Let delta be the number of days in the tasks.
    Number of workers = delta / n*d

The local will upload the project code and input file to private S3 bucket, it will send an sqs message with the input file key (in s3).
It will then bring up a Manager instance in AWS (unless one is not already up) and will wait for answer on a queue.
Once it gets the answer it will download the output file, parse it to html and save.

## Manager:
Runs 2 theards, each with one flow (queue names are not real):
- Flow 1 (locals) : loop: read messages from local_to_manager queue -> download input files -> create task for each ->
-> create workers if needed -> create jobs for the task -> put jobs in manager_to_workers queue.

- Flow 2 (workers): loop: read messages from workers_to_manager queue -> update the task object in the manager state ->
if the task is done create a summery file, upload it to s3 and send to manager_to_local queue + check if we need to kill some
workers.

### How terminate works:
"locals" thread get a message from input file with terminate order:
    It notify the "workers" thread.
    It handle the input file task (create the jobs for the workers) and then exit.

When the "workers" thread is notified about terminate it keep working as usaual, but once all the tasks are finished it upload the statisicts of the workers to S3 and quit.

Note that once the locals thread gets the terminate order no more tasks will be added to the worker thread.

The task is splitted to 1-day jobs, and sent like this to the manager_to_workers sqs queue. This is done to ensure maxium parallarlirty.

## Worker:
Wait for messages in the manager_to_workers queue. On each message it make a NASA query, parse the result the according to the parameters in the message and return the answers to the manager.


## Reasurces: 
- S3:
	All the application use the same S3 bucket.
- SQS:
	Permenet queues: local_to_manager, manager_to_workers, workers_to_manager.
	Temporary queues: manager_to_local
- EC2:
	All the applications run ami-b73b63a0 (us-east-1)




## How to run the project
run the core/local.py file with the following parameter:
-i with the path of the input file
-o with the path of the output file
-n the n number
-d the d number
-t (optional) terminate the manager when you are done

The python packages in the requirements.txt file should be installed on the local machine (```run pip install -r requirements.txt```)

for example:
-i input_file.json -o output_file.html -n 10 -d 2 -t

## Test run:
We used d=2, n=10, input file:

```{
  "start-date": "2016-10-30",
  "end-date": "2017-01-27",
  "speed-threshold": 10,
  "diameter-threshold": 200,
  "miss-threshold": 0.3
}```

Full run took the program 2:34 minutes to run.

The manager created 4 workers, their statistics:

```{
    "ae467552-0c98-45a9-aff9-2dffefe51e44": {
        "num_of_dangerous": 31, 
        "total_asteroids": 218, 
        "num_of_safe": 187
    }, 
    "9af55022-ddbd-4729-ada4-360a8a7426d3": {
        "num_of_dangerous": 26, 
        "total_asteroids": 228, 
        "num_of_safe": 202
    }, 
    "d5a7f9e3-de73-4c16-b051-2fd464261ecc": {
        "num_of_dangerous": 8, 
        "total_asteroids": 106, 
        "num_of_safe": 98
    }, 
    "8c57721c-0926-4d8b-a92b-9b1b03aca062": {
        "num_of_dangerous": 14, 
        "total_asteroids": 125, 
        "num_of_safe": 111
    }
}```

Dvir Cohen - 304903347
Aviv Ben Haim - 305091787


