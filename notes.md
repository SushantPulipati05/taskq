10th october;
seeded the jobs table with dummy jobs and ran two servers concurrently each requesting a job
- ran a plain select sql command on both and ended up assigning same job to both a and b 
- then wrote select command with a lock, this time each got assigned a different job
- then wrote select command with nowait instead of locked, this time b could not obtain lock on job 1 because a had it. No wait is needed instead of locked because b should be doing the job it is assigned instead of any job available and marked as queue

- the job is never set "running" forever, only till the locked until time runs out so that if the worker ever crash without finishing the job it gets back to the queue
- the done/dead jobs are added to the table with partial indexing so that postgres doesn't need to go through every job only the queued once, with normal index
- the select command is all single line instead of multiple steps to avoid the gap created between running two separate might cause another worker to sweep in and take the job(the first step checks for the job, the lock is not established until the second step)
- what Isolation actually promises. It means transactions don’t see each other’s uncommitted changes, and conflicting writes are handled safely. It doesn’t stop two transactions from reading the same committed row. Postgres uses MVCC: a plain SELECT reads a snapshot and takes no row lock (“readers don’t block writers, writers don’t block readers”).

