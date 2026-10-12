taskqdb=# begin;
BEGIN
Time: 6.244 ms
taskqdb=*# EXPLAIN(ANALYZE, BUFFERS)
taskqdb-*# UPDATE jobs
SET    status = 'running', locked_by = 'bench',
       locked_until = now() + interval '30 seconds',
       attempts = attempts + 1, updated_at = now()
WHERE  id = (
    SELECT id FROM jobs
    WHERE  status = 'queued' AND queue = 'default' AND run_at <= now()
    ORDER  BY priority DESC, run_at, id
    LIMIT  1
    FOR UPDATE SKIP LOCKED
);
                                                                     QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------------------------
 Update on jobs  (cost=2.52..10.55 rows=0 width=0) (actual time=0.767..0.768 rows=0 loops=1)
   Buffers: shared hit=26 dirtied=2 written=1
   InitPlan 1 (returns $1)
     ->  Limit  (cost=0.28..2.10 rows=1 width=26) (actual time=0.212..0.213 rows=1 loops=1)
           Buffers: shared hit=5
           ->  LockRows  (cost=0.28..789.54 rows=433 width=26) (actual time=0.184..0.185 rows=1 loops=1)
                 Buffers: shared hit=5
                 ->  Index Scan using jobs_claim_idx on jobs jobs_1  (cost=0.28..785.21 rows=433 width=26) (actual time=0.090..0.090 rows=1 loops=1)
                       Index Cond: ((queue = 'default'::text) AND (run_at <= now()))
                       Filter: (status = 'queued'::text)
                       Buffers: shared hit=3
   ->  Index Scan using jobs_pkey on jobs  (cost=0.42..8.45 rows=1 width=90) (actual time=0.305..0.307 rows=1 loops=1)
         Index Cond: (id = $1)
         Buffers: shared hit=9
 Planning:
   Buffers: shared hit=43
 Planning Time: 1.119 ms
 Execution Time: 2.048 ms
(18 rows)

Time: 18.298 ms
taskqdb=*# ROLLBACK;
ROLLBACK
Time: 0.740 ms
taskqdb=# BEGIN; EXPLAIN (ANALYZE, BUFFERS) UPDATE jobs SET status = 'running', locked_by = 'bench', locked_until = now() + interval '30 seconds', attempts = attempts + 1, updated_at = now() WHERE id = (SELECT id FROM jobs WHERE status = 'queued' AND queue = 'default' AND run_at <= now() ORDER BY priority DESC, run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED); ROLLBACK;
BEGIN
Time: 0.351 ms
                                                                     QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------------------------
 Update on jobs  (cost=2.52..10.55 rows=0 width=0) (actual time=0.258..0.259 rows=0 loops=1)
   Buffers: shared hit=20
   InitPlan 1 (returns $1)
     ->  Limit  (cost=0.28..2.10 rows=1 width=26) (actual time=0.081..0.082 rows=1 loops=1)
           Buffers: shared hit=4
           ->  LockRows  (cost=0.28..789.54 rows=433 width=26) (actual time=0.080..0.081 rows=1 loops=1)
                 Buffers: shared hit=4
                 ->  Index Scan using jobs_claim_idx on jobs jobs_1  (cost=0.28..785.21 rows=433 width=26) (actual time=0.044..0.044 rows=1 loops=1)
                       Index Cond: ((queue = 'default'::text) AND (run_at <= now()))
                       Filter: (status = 'queued'::text)
                       Buffers: shared hit=3
   ->  Index Scan using jobs_pkey on jobs  (cost=0.42..8.45 rows=1 width=90) (actual time=0.103..0.104 rows=1 loops=1)
         Index Cond: (id = $1)
         Buffers: shared hit=10
 Planning:
   Buffers: shared hit=1
 Planning Time: 0.414 ms
 Execution Time: 0.309 ms
(18 rows)

Time: 1.380 ms
ROLLBACK
Time: 0.190 ms
taskqdb=# BEGIN; EXPLAIN (ANALYZE, BUFFERS) UPDATE jobs SET status = 'running', locked_by = 'bench', locked_until = now() + interval '30 seconds', attempts = attempts + 1, updated_at = now() WHERE id = (SELECT id FROM jobs WHERE status = 'queued' AND queue = 'default' AND run_at <= now() ORDER BY priority DESC, run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED); ROLLBACK;
BEGIN
Time: 0.173 ms
                                                                     QUERY PLAN
-----------------------------------------------------------------------------------------------------------------------------------------------------
 Update on jobs  (cost=2.52..10.55 rows=0 width=0) (actual time=0.233..0.234 rows=0 loops=1)
   Buffers: shared hit=20
   InitPlan 1 (returns $1)
     ->  Limit  (cost=0.28..2.10 rows=1 width=26) (actual time=0.137..0.137 rows=1 loops=1)
           Buffers: shared hit=4
           ->  LockRows  (cost=0.28..789.54 rows=433 width=26) (actual time=0.136..0.136 rows=1 loops=1)
                 Buffers: shared hit=4
                 ->  Index Scan using jobs_claim_idx on jobs jobs_1  (cost=0.28..785.21 rows=433 width=26) (actual time=0.034..0.034 rows=1 loops=1)
                       Index Cond: ((queue = 'default'::text) AND (run_at <= now()))
                       Filter: (status = 'queued'::text)
                       Buffers: shared hit=3
   ->  Index Scan using jobs_pkey on jobs  (cost=0.42..8.45 rows=1 width=90) (actual time=0.152..0.154 rows=1 loops=1)
         Index Cond: (id = $1)
         Buffers: shared hit=10
 Planning Time: 0.155 ms
 Execution Time: 0.261 ms
(16 rows)

Time: 0.712 ms
ROLLBACK
Time: 0.143 ms



Measurement B

taskqdb=# DROP INDEX jobs_claim_idx;
DROP INDEX
Time: 17.593 ms
taskqdb=# BEGIN; EXPLAIN (ANALYZE, BUFFERS) UPDATE jobs SET status = 'running', locked_by = 'bench', locked_until = now() + interval '30 seconds', attempts = attempts + 1, updated_at = now() WHERE id = (SELECT id FROM jobs WHERE status = 'queued' AND queue = 'default' AND run_at <= now() ORDER BY priority DESC, run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED); ROLLBACK;
BEGIN
Time: 0.200 ms
                                                                 QUERY PLAN
--------------------------------------------------------------------------------------------------------------------------------------------
 Update on jobs  (cost=37244.60..37252.63 rows=0 width=0) (actual time=133.381..133.383 rows=0 loops=1)
   Buffers: shared hit=13668 read=3594
   InitPlan 1 (returns $1)
     ->  Limit  (cost=37244.17..37244.18 rows=1 width=26) (actual time=133.194..133.195 rows=1 loops=1)
           Buffers: shared hit=13649 read=3594
           ->  LockRows  (cost=37244.17..37249.58 rows=433 width=26) (actual time=133.192..133.193 rows=1 loops=1)
                 Buffers: shared hit=13649 read=3594
                 ->  Sort  (cost=37244.17..37245.25 rows=433 width=26) (actual time=133.178..133.178 rows=1 loops=1)
                       Sort Key: jobs_1.priority DESC, jobs_1.run_at, jobs_1.id
                       Sort Method: quicksort  Memory: 52kB
                       Buffers: shared hit=13648 read=3594
                       ->  Seq Scan on jobs jobs_1  (cost=0.00..37242.00 rows=433 width=26) (actual time=132.908..133.047 rows=500 loops=1)
                             Filter: ((status = 'queued'::text) AND (queue = 'default'::text) AND (run_at <= now()))
                             Rows Removed by Filter: 999500
                             Buffers: shared hit=13648 read=3594
   ->  Index Scan using jobs_pkey on jobs  (cost=0.42..8.45 rows=1 width=90) (actual time=133.213..133.215 rows=1 loops=1)
         Index Cond: (id = $1)
         Buffers: shared hit=13655 read=3594
 Planning:
   Buffers: shared hit=6 dirtied=1
 Planning Time: 0.479 ms
 Execution Time: 133.466 ms
(22 rows)

Time: 134.669 ms
ROLLBACK
Time: 0.412 ms
taskqdb=# BEGIN; EXPLAIN (ANALYZE, BUFFERS) UPDATE jobs SET status = 'running', locked_by = 'bench', locked_until = now() + interval '30 seconds', attempts = attempts + 1, updated_at = now() WHERE id = (SELECT id FROM jobs WHERE status = 'queued' AND queue = 'default' AND run_at <= now() ORDER BY priority DESC, run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED); ROLLBACK;
BEGIN
Time: 0.141 ms
                                                                QUERY PLAN
------------------------------------------------------------------------------------------------------------------------------------------
 Update on jobs  (cost=37244.60..37252.63 rows=0 width=0) (actual time=92.497..92.498 rows=0 loops=1)
   Buffers: shared hit=13697 read=3562
   InitPlan 1 (returns $1)
     ->  Limit  (cost=37244.17..37244.18 rows=1 width=26) (actual time=92.385..92.386 rows=1 loops=1)
           Buffers: shared hit=13681 read=3562
           ->  LockRows  (cost=37244.17..37249.58 rows=433 width=26) (actual time=92.384..92.385 rows=1 loops=1)
                 Buffers: shared hit=13681 read=3562
                 ->  Sort  (cost=37244.17..37245.25 rows=433 width=26) (actual time=92.371..92.372 rows=1 loops=1)
                       Sort Key: jobs_1.priority DESC, jobs_1.run_at, jobs_1.id
                       Sort Method: quicksort  Memory: 52kB
                       Buffers: shared hit=13680 read=3562
                       ->  Seq Scan on jobs jobs_1  (cost=0.00..37242.00 rows=433 width=26) (actual time=92.226..92.306 rows=500 loops=1)
                             Filter: ((status = 'queued'::text) AND (queue = 'default'::text) AND (run_at <= now()))
                             Rows Removed by Filter: 999500
                             Buffers: shared hit=13680 read=3562
   ->  Index Scan using jobs_pkey on jobs  (cost=0.42..8.45 rows=1 width=90) (actual time=92.402..92.403 rows=1 loops=1)
         Index Cond: (id = $1)
         Buffers: shared hit=13687 read=3562
 Planning Time: 0.198 ms
 Execution Time: 92.529 ms
(20 rows)

Time: 93.137 ms
ROLLBACK
Time: 0.204 ms
taskqdb=# BEGIN; EXPLAIN (ANALYZE, BUFFERS) UPDATE jobs SET status = 'running', locked_by = 'bench', locked_until = now() + interval '30 seconds', attempts = attempts + 1, updated_at = now() WHERE id = (SELECT id FROM jobs WHERE status = 'queued' AND queue = 'default' AND run_at <= now() ORDER BY priority DESC, run_at, id LIMIT 1 FOR UPDATE SKIP LOCKED); ROLLBACK;
BEGIN
Time: 0.178 ms
                                                                QUERY PLAN
------------------------------------------------------------------------------------------------------------------------------------------
 Update on jobs  (cost=37244.60..37252.63 rows=0 width=0) (actual time=88.634..88.635 rows=0 loops=1)
   Buffers: shared hit=13729 read=3530
   InitPlan 1 (returns $1)
     ->  Limit  (cost=37244.17..37244.18 rows=1 width=26) (actual time=88.542..88.542 rows=1 loops=1)
           Buffers: shared hit=13713 read=3530
           ->  LockRows  (cost=37244.17..37249.58 rows=433 width=26) (actual time=88.540..88.541 rows=1 loops=1)
                 Buffers: shared hit=13713 read=3530
                 ->  Sort  (cost=37244.17..37245.25 rows=433 width=26) (actual time=88.527..88.528 rows=1 loops=1)
                       Sort Key: jobs_1.priority DESC, jobs_1.run_at, jobs_1.id
                       Sort Method: quicksort  Memory: 52kB
                       Buffers: shared hit=13712 read=3530
                       ->  Seq Scan on jobs jobs_1  (cost=0.00..37242.00 rows=433 width=26) (actual time=88.384..88.462 rows=500 loops=1)
                             Filter: ((status = 'queued'::text) AND (queue = 'default'::text) AND (run_at <= now()))
                             Rows Removed by Filter: 999500
                             Buffers: shared hit=13712 read=3530
   ->  Index Scan using jobs_pkey on jobs  (cost=0.42..8.45 rows=1 width=90) (actual time=88.560..88.561 rows=1 loops=1)
         Index Cond: (id = $1)
         Buffers: shared hit=13719 read=3530
 Planning Time: 0.144 ms
 Execution Time: 88.665 ms
(20 rows)

Time: 89.232 ms
ROLLBACK
Time: 0.298 ms



                                                                           QUERY PLAN                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Update on jobs  (cost=200.95..208.97 rows=0 width=0) (actual time=363.966..363.967 rows=0 loops=1)
   Buffers: shared hit=17 read=23260 dirtied=6
   InitPlan 1 (returns $2)
     ->  Limit  (cost=0.43..200.52 rows=1 width=26) (actual time=363.810..363.811 rows=1 loops=1)
           Buffers: shared hit=2 read=23256 dirtied=1
           ->  LockRows  (cost=0.43..86640.76 rows=433 width=26) (actual time=363.809..363.810 rows=1 loops=1)
                 Buffers: shared hit=2 read=23256 dirtied=1
                 ->  Index Scan using jobs_claim_full_idx on jobs jobs_1  (cost=0.43..86636.43 rows=433 width=26) (actual time=363.796..363.797 rows=1 loops=1)
                       Index Cond: ((queue = 'default'::text) AND (run_at <= now()))
                       Filter: (status = 'queued'::text)
                       Rows Removed by Filter: 999500
                       Buffers: shared hit=1 read=23256 dirtied=1
   ->  Index Scan using jobs_pkey on jobs  (cost=0.42..8.45 rows=1 width=90) (actual time=363.828..363.831 rows=1 loops=1)
         Index Cond: (id = $2)
         Buffers: shared hit=8 read=23256 dirtied=1
 Planning Time: 0.209 ms
 Execution Time: 364.003 ms
(17 rows)

                                                                           QUERY PLAN                                                                       
----------------------------------------------------------------------------------------------------------------------------------------------------------------
 Update on jobs  (cost=200.95..208.97 rows=0 width=0) (actual time=305.825..305.827 rows=0 loops=1)
   Buffers: shared hit=31 read=23266 dirtied=4 written=3
   InitPlan 1 (returns $2)
     ->  Limit  (cost=0.43..200.52 rows=1 width=26) (actual time=303.689..303.690 rows=1 loops=1)
           Buffers: shared hit=3 read=23255 dirtied=1 written=3
           ->  LockRows  (cost=0.43..86640.76 rows=433 width=26) (actual time=303.688..303.689 rows=1 loops=1)
                 Buffers: shared hit=3 read=23255 dirtied=1 written=3
                 ->  Index Scan using jobs_claim_full_idx on jobs jobs_1  (cost=0.43..86636.43 rows=433 width=26) (actual time=303.665..303.665 rows=1 loops=1)
                       Index Cond: ((queue = 'default'::text) AND (run_at <= now()))
                       Filter: (status = 'queued'::text)
                       Rows Removed by Filter: 999500
                       Buffers: shared hit=2 read=23255 dirtied=1 written=3
   ->  Index Scan using jobs_pkey on jobs  (cost=0.42..8.45 rows=1 width=90) (actual time=303.727..303.730 rows=1 loops=1)
         Index Cond: (id = $2)
         Buffers: shared hit=9 read=23255 dirtied=1 written=3
 Planning:
   Buffers: shared hit=158 read=26
 Planning Time: 4.988 ms
 Execution Time: 306.497 ms
(19 rows)








when postgres synchronous is on 
workers      jobs       seconds      jobs/sec    
1            20000      113.99       175         
4            20000      19.79        1011        
8            20000      11.14        1796        
16           20000      7.90         2533        
32           20000      4.56         4383        

when postgres synchronous is switched off 
workers      jobs       seconds      jobs/sec    
1            20000      21.70        921         
4            20000      5.70         3511        
8            20000      3.83         5216        
16           20000      2.88         6947        


worker 1 is almost 5 times faster with synchronous switched off this means that worker one does 175 jobs/sec, so about 5.7 ms per job. Each job is 2 commits, so about 2.9 ms per commit and worker 1 with Sync off: 921 jobs/sec, so about 1.1 ms per job. So of the 5.7 ms each job took, about 4.6 ms (around 80%) was just waiting for the disk flush. The query itself, the network round trips and your Go code together took only about 1.1 ms.
Its taking so much time to flush the disk because postgres is running inside Docker Desktop, which on Windows is running inside a small Linux virtual machine (WSL2). Every flush has to pass through that virtual machine’s virtual disk down to my real SSD, which adds a few milliseconds each time. On a Linux server with a fast disk, that flush might take 0.1–0.5 ms.
the trade off with sync-off is the durability, in case of a crash, we might lose the commits made in the last fraction of second and the jobs done might revert back


also keep adding more workers just doesn't keep on reducing the time exponentially, it actually flattens the curve. this is because the bottleneck effect that happens when there is only one place the commits happen, so the worker get keep queued behind one other waiting to process their query to the single db