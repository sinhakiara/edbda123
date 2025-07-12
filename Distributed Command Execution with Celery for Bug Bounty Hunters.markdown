# Hacking at Scale: Distributing Commands with Celery for Bug Bounty Hunters

Hey there, bug bounty hunters! 👋 If you’re like me, you’ve probably spent countless nights scanning subdomains, running tools like `httpx`, or chaining commands to uncover that one juicy vulnerability. But what happens when you’re targeting a massive scope—say, a million hosts—and your single machine starts crying for mercy? That’s where distributed command execution comes in, and today, I’m sharing a slick setup using **Celery** to scale your hacking game across multiple machines. Think of it as your personal army of worker nodes, executing commands in parallel while you sip coffee. ☕ Let’s break it down in a way that’s easy, practical, and ready to use in your next bug bounty hunt.

## Why Distributed Command Execution?

Picture this: You’re hunting on a program with a wildcard domain (`*.example.com`), and your recon tools spit out thousands of subdomains. Running `echo $(hostname) | httpx` on each one manually? Brutal. Doing it on one machine? Slow. Distributing those tasks across multiple servers? *Game-changer*. Here’s why this matters for bug bounty hunters:

- **Scale Like a Pro**: Split tasks across multiple servers to scan faster.
- **Save Time**: Parallel execution means less waiting, more finding bugs.
- **Flexibility**: Run any shell command—`nmap`, `dirb`, `sqlmap`, you name it.
- **Real-World Use**: Perfect for recon, enumeration, or chaining tools like `httpx` or `amass`.

I recently set up a system to distribute commands across multiple worker nodes using Celery, a Python-based task queue, and Redis as the message broker. It’s like giving your tools superpowers. Let me walk you through how it works, how to set it up, and how you can use it to level up your bug bounty workflow.

## What’s Celery, and Why Should You Care?

Celery is a distributed task queue that lets you offload tasks (like running shell commands) to multiple worker machines. It’s battle-tested, used by companies like Instagram, and perfect for bug hunters who need to scale. Here’s the gist:

- **Master Node**: Your main machine queues commands (e.g., `echo sub.example.com | httpx`).
- **Worker Nodes**: Other machines (VMs, cloud instances) execute those commands in parallel.
- **Redis**: A fast message broker that coordinates tasks between the master and workers.
- **Round-Robin Magic**: Tasks are evenly distributed across workers, so no single machine gets overwhelmed.

Think of it as a conveyor belt: you toss commands onto it, and your workers pick them up and process them. The result? Faster scans, cleaner output, and more time to hunt for that critical bug.

## Setting Up Your Distributed Hacking Lab

Let’s get hands-on. I’ll show you how to set up a master node and multiple worker nodes to distribute commands. I’m assuming you have a Linux machine (like Kali or Ubuntu) and a few spare VMs or cloud instances (e.g., AWS, DigitalOcean). For this example, we’ll use three worker nodes, but you can scale to as many as you want.

### Step 1: Install Dependencies
On **all nodes** (master and workers), set up Python and install Celery and Redis:

```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv redis-server
python3 -m venv /root/distshell/venv
source /root/distshell/venv/bin/activate
pip install celery==5.5.3 redis==5.0.8
```

Enable and start Redis on the master node (or a dedicated Redis server):
```bash
sudo systemctl enable redis
sudo systemctl start redis
```

Set a Redis password (`redhat@123?` for this example) in `/etc/redis/redis.conf`:
```
requirepass redhat@123?
```
Restart Redis:
```bash
sudo systemctl restart redis
```

Create a secure Redis CLI config:
```bash
echo "auth redhat@123?" > ~/.rediscli
chmod 600 ~/.rediscli
```

Test Redis connectivity (replace `192.168.206.130` with your Redis server IP):
```bash
redis-cli --config ~/.rediscli -h 192.168.206.130 -p 6379 PING
```
Should return `PONG`.

### Step 2: Create the Task Module
We need a shared module to define the Celery app and task. Save this as `/root/distshell/tasks.py` on all nodes:

```python
import os
import logging
from celery import Celery

# Set up logging
logging.basicConfig(level=logging.INFO, filename='/tmp/celery_tasks.log')
logger = logging.getLogger(__name__)

# Celery configuration
app = Celery('distshell',
             broker='redis://:redhat%40123%3F@192.168.206.130:6379/0',
             backend='redis://:redhat%40123%3F@192.168.206.130:6379/0')

app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,
    broker_connection_retry_on_startup=True,
)

@app.task(name='distshell.execute_command')
def execute_command(command, run_id):
    import subprocess
    logger.info(f"Executing command: {command}")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=300)
        output = result.stdout + result.stderr
        status = 'success' if result.returncode == 0 else 'error'
    except subprocess.TimeoutExpired:
        output = "Command timed out"
        status = 'error'
        logger.error(f"Command timed out: {command}")
    except Exception as e:
        output = f"Command failed: {str(e)}"
        status = 'error'
        logger.error(f"Command failed: {command}, error: {str(e)}")
    return {'worker': os.uname().nodename, 'command': command, 'output': output.strip(), 'result': status, 'run_id': run_id}
```

This defines the `execute_command` task, which runs any shell command (e.g., `echo sub.example.com | httpx`) and returns the output.

### Step 3: Set Up the Scheduler
The scheduler queues commands and collects results. Save this as `/root/distshell/scheduler.py` on the master node:

```python
import sys
import uuid
import logging
from celery.result import AsyncResult
from tasks import app, execute_command

# Set up logging
logging.basicConfig(level=logging.INFO, filename='/tmp/scheduler.log')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting scheduler")
    run_id = str(uuid.uuid4())
    print(f"Started DistShell run {run_id}. Waiting for results...")

    # Read commands
    if len(sys.argv) > 1:
        commands = [' '.join(sys.argv[1:])]
    else:
        commands = []
        current_command = []
        for line in sys.stdin:
            line = line.strip()
            if line:
                current_command.append(line)
            elif current_command:
                commands.append(' '.join(current_command))
                current_command = []
        if current_command:
            commands.append(' '.join(current_command))

    # Queue tasks with round-robin routing
    task_results = []
    queues = ['queue1', 'queue2', 'queue3']
    for i, cmd in enumerate(commands):
        queue = queues[i % len(queues)]
        logger.info(f"Queuing command '{cmd}' to {queue}")
        result = execute_command.apply_async(args=[cmd, run_id], queue=queue)
        task_results.append((result, cmd))

    # Collect results
    completed = 0
    total = len(task_results)
    while completed < total:
        for result, cmd in task_results:
            if result.ready():
                if result.successful():
                    res = result.get()
                    print(f"Result from {res['worker']} (Command: {cmd}):")
                    print(res['output'])
                    logger.info(f"Received result for '{cmd}' from {res['worker']}: {res['output']}")
                else:
                    print(f"Error for command '{cmd}': Task failed")
                    logger.error(f"Task failed for command '{cmd}'")
                completed += 1
                task_results.remove((result, cmd))
        import time
        time.sleep(1)

    print("All commands processed.")
    logger.info("All commands processed")

if __name__ == "__main__":
    main()
```

This script distributes commands to workers using a round-robin algorithm, ensuring even load across your nodes.

### Step 4: Set Up the Workers
On each worker node, save this as `/root/distshell/worker.py`:

```python
from tasks import app, execute_command
```

Start workers (one per node, adjust `queueX` and `workerX`):
```bash
cd /root/distshell
source venv/bin/activate
celery -A worker worker --loglevel=info --concurrency=4 -Q queue1 -n worker1@%h
```
For additional workers:
```bash
celery -A worker worker --loglevel=info --concurrency=4 -Q queue2 -n worker2@%h
celery -A worker worker --loglevel=info --concurrency=4 -Q queue3 -n worker3@%h
```

### Step 5: Test It Out
On the master node, try a simple command:
```bash
cd /root/distshell
source venv/bin/activate
python scheduler.py 'echo $(hostname) says `id`'
```

**Output**:
```
Started DistShell run <some_uuid>. Waiting for results...
Result from worker1_hostname (Command: echo $(hostname) says `id`):
worker1_hostname says uid=0(root) gid=0(root) groups=0(root)
All commands processed.
```

For a bug bounty scenario, queue multiple recon commands:
```bash
echo -e "echo sub1.example.com | httpx\necho sub2.example.com | httpx\necho sub3.example.com | httpx" | python scheduler.py
```

**Output**:
```
Started DistShell run <some_uuid>. Waiting for results...
Result from worker1_hostname (Command: echo sub1.example.com | httpx):
sub1.example.com - [200 OK]
Result from worker2_hostname (Command: echo sub2.example.com | httpx):
sub2.example.com - [200 OK]
Result from worker3_hostname (Command: echo sub3.example.com | httpx):
sub3.example.com - [404 Not Found]
All commands processed.
```

## Why This Rocks for Bug Bounty Hunters

This setup is a game-changer for large-scale recon or testing:
- **Speed**: Distribute tasks across multiple VMs to scan thousands of hosts in minutes.
- **Flexibility**: Run any tool—`httpx`, `nmap`, `sqlmap`, or custom scripts.
- **Scalability**: Add more workers by spinning up new VMs and starting Celery workers.
- **Monitoring**: Use Flower (`celery -A tasks flower --port=5555`) to watch tasks in real-time.

I’ve used this to scan massive scopes in bug bounty programs, splitting tasks like subdomain enumeration or directory brute-forcing across cloud instances. It’s like having a mini-supercomputer for hacking.

## Pro Tips from the Trenches
- **Security First**: Running as `root` is risky (as seen in `uid=0`). Create a non-root user:
  ```bash
  sudo useradd -m celeryuser
  sudo -u celeryuser celery -A worker worker --loglevel=info --concurrency=4 -Q queueX -n workerX@%h
  ```
- **Validate Commands**: Add checks in `scheduler.py` to block dangerous inputs:
  ```python
  if any(c in cmd for c in ['|', ';', '&']):
      logger.error(f"Skipping unsafe command: {cmd}")
      continue
  ```
- **Debug Like a Pro**: Check logs (`/tmp/scheduler.log`, `/tmp/celery_tasks.log`) or use Flower for task insights.
- **Scale Smart**: Adjust `--concurrency` based on CPU cores (`nproc`) and add more queues in `scheduler.py` for extra workers.

## Wrapping Up

Distributed command execution with Celery is like having a team of ethical hackers working for you, without the extra coffee bills. Whether you’re scanning subdomains, brute-forcing directories, or running custom scripts, this setup will save you time and make you look like a rockstar in the bug bounty community. Try it out, tweak it for your needs, and share your results on X (@dheerajkmadhukar). Got questions? Drop me a message, and let’s keep making the internet safer, one bug at a time! 🛡️

Happy hunting, and may your bounties be critical! 🚀