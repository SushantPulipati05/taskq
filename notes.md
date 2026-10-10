10th october;
seeded the jobs table with dummy jobs and ran two servers concurrently each requesting a job
- ran a plain select sql command on both and ended up assigning same job to both a and b 
- then wrote select command with a lock, this time each got assigned a different job
- then wrote select command with nowait instead of locked, this time b could not obtain lock on job 1 because a had it. No wait is needed instead of locked because b should be doing the job it is assigned instead of any job available and marked as queue

- the job is never set "running" forever, only till the locked until time runs out so that if the worker ever crash without finishing the job it gets back to the queue
- the done/dead jobs are added to the table with partial indexing so that postgres doesn't need to go through every job only the queued once, with normal index
- the select command is all single line instead of multiple steps to avoid the gap created between running two separate might cause another worker to sweep in and take the job(the first step checks for the job, the lock is not established until the second step)
- what Isolation actually promises. It means transactions don’t see each other’s uncommitted changes, and conflicting writes are handled safely. It doesn’t stop two transactions from reading the same committed row. Postgres uses MVCC: a plain SELECT reads a snapshot and takes no row lock (“readers don’t block writers, writers don’t block readers”).


- multiple go routines can, work on the jobs simultaneously now, the worker(go routine) that takes up the first job is completely dependent on the who reaches the session first, it's almost random
- after each failure of completion of the job, the job is kind of locked, its backoff for 2seconds during this time no worker can accept that job, even if the job queue is empty, it can only be accepted after the buffer timer
- This took a lot of time to understand and implement, the code wasn't that complex, but the understanding part took some time, its kinda easy once you understand

- the loop here uses ctx which listens when a ctrl c is pressed, so that the workers stop taking new jobs, the handler doesn't so that the jobs are finished before the session is terminate
- this is was necessary because terminating mid session caused job to stopped in the middle and getting marked as a failure, this was wastage of one good attempt and also when the job is sensitive and crucial this is not a  good 
- the shutdown timer is longer than the lease so that all the jobs finish before termination

- the queue here ensures that the job is done atleast once, but sometimes it runs more than once. Getting exactly one is practically impossible because there can be a slow worker or any other problem
- the reaper action is also counted as an attempt in the job. it is to save system from a poison job, basically a job that can kill other workers, one downside is that the workers that had a genuine problem also lose an attempt but they have been given enough head room to deal with it