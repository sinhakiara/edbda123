# Hacking at Scale: Distributing Commands with Celery for Bug Bounty Hunters

Hey there, bug bounty hunters! 👋 Ever been stuck scanning a massive scope—like a wildcard domain with thousands of subdomains—and wished you could split the work across multiple machines? Maybe you’re running `httpx` or `nmap` and your laptop’s fan sounds like it’s about to take off. 🚀 I’ve been there, and I’ve got a solution that’ll make your recon faster, smarter, and way more fun. Today, I’m sharing a **Celery-based distributed command execution system** that lets you run commands across multiple servers like a pro. With some slick diagrams and animations, I’ll make this super easy to understand, even if you’re new to distributed systems. Let’s dive in and level up your bug bounty game! 🛡️

## Why Distributed Command Execution?

Imagine you’re hunting on a program with `*.example.com`, and your recon tools spit out 10,000 subdomains. Running `echo sub.example.com | httpx` one by one? Nightmare. Doing it on a single machine? Slow as molasses. Distributing those tasks across a fleet of cloud VMs? *Pure magic*. Here’s why this setup is a must for bug hunters:

- **Speed**: Split tasks across multiple servers to scan thousands of hosts in minutes.
- **Scale**: Add more VMs to handle bigger scopes without breaking a sweat.
- **Flexibility**: Run any shell command—`httpx`, `nmap`, `sqlmap`, or your custom scripts.
- **Bug Bounty Power**: Perfect for recon, enumeration, or chaining tools to find that critical vuln.

I built a system using **Celery** (a Python task queue) and **Redis** (a fast message broker) to distribute commands like a boss. Think of it as your personal hacking army, executing tasks in parallel while you sip coffee. ☕ To make it crystal clear, I’ve added some diagrams with animations to show how it all works.

## Diagram 1: System Architecture
*Static Diagram*: A visual of the setup with:
- A **Master Node** (a laptop icon) labeled “Scheduler” that sends commands.
- A **Redis Server** (a database icon) in the center, labeled “Message Broker.”
- Three **Worker Nodes** (server icons) labeled “Worker 1,” “Worker 2,” “Worker 3,” connected to Redis.
- Arrows showing commands flowing from the master to Redis, then to workers, and results flowing back.

*Animation Idea*: 
- Start with the master node blinking, sending a command (e.g., `echo sub.example.com | httpx`) as a glowing packet to Redis.
- Redis pulses, then shoots the packet to one worker node, which lights up and sends a result packet back to Redis, then to the master.
- Repeat for other workers, showing tasks bouncing between nodes in a loop.

*Caption*: “The master node queues commands to Redis, which distributes them to workers. Results flow back the same way. Like a hacking command center!”

*Implementation*: Create in Draw.io with icons for laptop, database, and servers. For animation, use Canva’s animation tools to add glowing effects and moving arrows, export as a GIF, and embed via Imgur: `![System Architecture](https://i.imgur.com/your_gif_link.gif)`.

## What’s Celery, and Why Should You Care?

Celery is like the conductor of an orchestra, making sure every musician (worker) plays their part on time. It’s a distributed task queue that lets you offload shell commands to multiple machines. Redis acts as the middleman, passing tasks and results back and forth. Here’s the breakdown:

- **Master Node (Scheduler)**: Your main machine queues commands (e.g., `echo sub.example.com | httpx`).
- **Worker Nodes**: Your VMs or cloud instances that run the commands.
- **Redis**: A super-fast database that coordinates tasks and results.
- **Round-Robin Algorithm**: Evenly spreads tasks across workers, so no one gets overloaded.

Check out the architecture diagram above—it’s like a map of your hacking empire. The animation shows how commands zip from your laptop to workers and back, making it feel alive and intuitive.

## Diagram 2: Round-Robin Task Distribution
*Static Diagram*: A top-down view with:
- A **Queue List** (labeled `queue1`, `queue2`, `queue3`) in Redis.
- Three worker nodes, each connected to one queue (e.g., Worker 1 to `queue1`).
- Commands (e.g., `echo sub1.example.com | httpx`, `echo sub2.example.com | httpx`) as boxes entering queues in a circular pattern (1 → queue1, 2 → queue2, 3 → queue3, 4 → queue1).

*Animation Idea*:
- Show commands as colored boxes (e.g., blue, green, red) sliding into queues one by one in a circular motion.
- Each worker node flashes when it picks up a command from its queue, then fades as it processes it.
- Add a counter showing tasks distributed (e.g., “Task 1 to Worker 1,” “Task 2 to Worker 2”).

*Caption*: “Commands are distributed round-robin style to ensure every worker gets a fair share. No worker gets left out!”

*Implementation*: Use Draw.io for the static layout with queues and arrows. In Canva, animate boxes moving in a cycle, export as a GIF, and embed via Imgur: `![Round-Robin Distribution](https://i.imgur.com/your_gif_link.gif)`.

## Setting Up Your Distributed Hacking Lab

Let’s get to the fun part—setting up your own distributed system. You’ll need a Linux machine (e.g., Kali) for the master and a few VMs or cloud instances for workers. I’ll walk you through the setup, step by step, like I’m sitting next to you at a hackathon.

### Step 1: Install Dependencies
On **all nodes** (master and workers):
```bash
sudo apt update
sudo apt install python3 python3-pip python3-venv redis-server
mkdir -p /root/distshell
cd /root/distshell
python3 -m venv venv
source venv/bin/activate
pip install celery==5.5.3 redis==5.0.8
```

On the **master node** (or a dedicated Redis server), enable and start Redis:
```bash
sudo systemctl enable redis
sudo systemctl start redis
```

Set the Redis password in `/etc/redis/redis.conf`:
```
requirepass redhat@123?
```
Restart Redis:
```bash
sudo systemctl restart redis
```

Create a secure Redis CLI config on all nodes:
```bash
echo "auth redhat@123?" > ~/.rediscli
chmod 600 ~/.rediscli
```

Test Redis (use your Redis server IP, e.g., `192.168.206.130`):
```bash
redis-cli --config ~/.rediscli -h 192.168.206.130 -p 6379 PING
```
Should return `PONG`. If it fails, check your firewall (`sudo ufw allow 6379`) or Redis config.

### Step 2: Create the Task Module
Save this as `/root/distshell/tasks.py` on all nodes (master and workers):

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

This is the heart of the system, defining how commands are executed and sent back to the master.

### Step 3: Set Up the Scheduler
Save this as `/root/distshell/scheduler.py` on the master node:

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

### Step 4: Set Up the Workers
On each worker node, save this as `/root/distshell/worker.py`:

```python
from tasks import app, execute_command
```

Start workers (adjust queue and worker names):
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

## Diagram 3: Command Execution Flow
*Static Diagram*: A flowchart showing:
- A **Master Node** (laptop) sending a command (`echo sub.example.com | httpx`) to Redis.
- **Redis Broker** with three queues (`queue1`, `queue2`, `queue3`), where the command lands in `queue1`.
- **Worker 1** picking up the command, processing it (gears icon), and sending a result (`sub.example.com - [200 OK]`) to Redis Results.
- The result returning to the master node.
- Labels: “Queue Command,” “Distribute Task (Round-Robin),” “Execute & Store Result,” “Return Result.”

*Animation Idea*:
- A glowing command box moves from the master to `queue1` in Redis (2s).
- The box slides to Worker 1, which flashes with spinning gears (3s).
- A result box emerges, travels to Redis Results, then back to the master (3s).
- The master shows a “Success!” pop-up, looping every 10s.
- Optionally, a second command fails (red “X” on Worker 2, error box returns).

*Caption*: “Watch a command zip from your laptop to a worker, get executed, and return results. It’s like magic, but for hackers!”

*Implementation*: Create in Draw.io for the static flowchart. Animate in Canva (simple GIF) or After Effects (professional GIF/MP4). Embed via Imgur: `![Command Execution Flow](https://i.imgur.com/your_gif_link.gif)` or YouTube: `[Watch the Flow](https://youtu.be/your_video_link)`.

## Testing Your Setup

Let’s put this to work with a real bug bounty scenario. On the master node:
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

Try a recon pipeline:
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

Check the logs to debug:
- Scheduler: `cat /tmp/scheduler.log`
- Workers: `cat /tmp/celery_tasks.log`

## Why This Rocks for Bug Bounty Hunters

This setup is like having a personal botnet (ethical, of course!) for your recon:
- **Speed**: Scan thousands of subdomains in parallel across multiple VMs.
- **Flexibility**: Run any tool—`httpx`, `nmap`, or even custom scripts like `curl -I`.
- **Scalability**: Spin up more workers on AWS or DigitalOcean to handle bigger scopes.
- **Visibility**: Use Flower (`pip install flower; celery -A tasks flower --port=5555`) to monitor tasks live at `http://<master_ip>:5555`.

I’ve used this to blast through massive scopes, like running `httpx` on thousands of subdomains or chaining `amass` with `dirb`. It’s saved me hours and helped me find bugs faster.

## Pro Tips from the Trenches

- **Run as Non-Root**: Avoid running as `root` (as seen in `uid=0`). Create a user:
  ```bash
  sudo useradd -m celeryuser
  sudo -u celeryuser celery -A worker worker --loglevel=info --concurrency=4 -Q queueX -n workerX@%h
  ```
- **Secure Commands**: Block risky inputs in `scheduler.py`:
  ```python
  if any(c in cmd for c in ['|', ';', '&']):
      logger.error(f"Skipping unsafe command: {cmd}")
      continue
  ```
- **Debug Like a Hacker**: Check logs (`/tmp/scheduler.log`, `/tmp/celery_tasks.log`) or use Flower for real-time insights.
- **Optimize Workers**: Set `--concurrency` to match CPU cores (`nproc`) and add more queues in `scheduler.py` for extra workers.

## Wrapping Up

Distributed command execution with Celery is your secret weapon for scaling bug bounty recon. It’s like having a team of hackers working for you, minus the extra Red Bulls. Whether you’re enumerating subdomains, scanning ports, or brute-forcing directories, this setup will make you faster and more efficient. Check out the flow diagram above to see it in action—it’s like watching your commands come to life! Try it out, play with the setup, and share your results on X (@dheerajkmadhukar). Got questions or epic bounties? DM me, and let’s keep making the internet safer, one bug at a time! 🐞

Happy hunting, and may your next bug be a critical one! 🚀