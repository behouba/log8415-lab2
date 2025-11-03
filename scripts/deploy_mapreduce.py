#!/usr/bin/env python3
import json, os, sys, subprocess, time

KEY_PATH = os.getenv("AWS_KEY_PATH")
if not KEY_PATH:
    sys.exit("Missing AWS_KEY_PATH. Run: set -a; source .env; set +a")

with open("artifacts/mapreduce_instances.json") as f:
    instances = json.load(f)

SSH_USER = "ubuntu"

SSH_BASE = [
    "ssh",
    "-o", "StrictHostKeyChecking=no",
    "-o", "BatchMode=yes",
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=3",
    "-o", "ConnectTimeout=20",
    "-o", "ConnectionAttempts=10",
]

def ssh(host, cmd, show_output=True):
    remote = f"bash -lc '{cmd}'"
    proc = subprocess.Popen(
        SSH_BASE + ["-i", KEY_PATH, f"{SSH_USER}@{host}", remote],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    output_lines = []
    try:
        if proc.stdout:
            for line in proc.stdout:
                output_lines.append(line)
                if show_output:
                    print(line, end="")
    finally:
        proc.wait()
    return proc.returncode, "".join(output_lines)

def scp_upload(host, local_path, remote_path):
    result = subprocess.run(
        ["scp", "-o", "StrictHostKeyChecking=no", "-i", KEY_PATH,
         local_path, f"{SSH_USER}@{host}:{remote_path}"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    return result

def wait_for_ssh(host):
    print(f"  Waiting for SSH on {host}...")
    for i in range(30):
        try:
            result = subprocess.run(
                SSH_BASE + ["-i", KEY_PATH, f"{SSH_USER}@{host}", "echo ready"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10
            )
            if result.returncode == 0:
                print(f"  SSH ready on {host}")
                return True
        except:
            pass
        time.sleep(5)
    return False

def setup_instance(host, role):
    print(f"\n  Setting up {host} ({role})...")
    code, _ = ssh(host, "python3 --version", show_output=False)
    if code == 0:
        print("  Python 3 already present; skipping install.")
    else:
        print("  Installing Python 3...")
        install_cmd = (
            "sudo apt-get update -y && "
            "sudo DEBIAN_FRONTEND=noninteractive "
            "apt-get install -y python3 python3-venv python3-pip"
        )
        code, output = ssh(host, install_cmd)
        if code != 0:
            print(f"  ERROR: Failed to install Python on {host}")
            print(output)
            return False

    ssh(host, "mkdir -p ~/mapreduce ~/data", show_output=False)

    print(f"  OK {host} setup complete")
    return True

# Deploy to all instances
print("=== Deploying MapReduce to instances ===\n")

all_hosts = []
for mapper in instances["mappers"]:
    all_hosts.append((mapper["public_ip"], "mapper"))
for reducer in instances["reducers"]:
    all_hosts.append((reducer["public_ip"], "reducer"))

print("Step 1: Waiting for SSH to be ready on all instances...")
for host, role in all_hosts:
    if not wait_for_ssh(host):
        sys.exit(f"ERROR: SSH did not become available on {host}")

print("\nStep 2: Setting up instances...")
for host, role in all_hosts:
    if not setup_instance(host, role):
        sys.exit(f"ERROR: Failed to setup {host}")

print("\nStep 3: Deploying mapper script to mapper instances...")
for mapper in instances["mappers"]:
    host = mapper["public_ip"]
    print(f"  Uploading mapper.py to {host}...")
    result = scp_upload(host, "app/mapper.py", "~/mapreduce/mapper.py")
    if result.returncode != 0:
        print(f"  ERROR: Failed to upload to {host}")
        print(result.stdout)
        sys.exit(1)
    ssh(host, "chmod +x ~/mapreduce/mapper.py", show_output=False)

print("\nStep 4: Deploying reducer script to reducer instances...")
for reducer in instances["reducers"]:
    host = reducer["public_ip"]
    print(f"  Uploading reducer.py to {host}...")
    result = scp_upload(host, "app/reducer.py", "~/mapreduce/reducer.py")
    if result.returncode != 0:
        print(f"  ERROR: Failed to upload to {host}")
        print(result.stdout)
        sys.exit(1)
    ssh(host, "chmod +x ~/mapreduce/reducer.py", show_output=False)

print("\nOK Deployment complete!")
print(f"Deployed to {len(instances['mappers'])} mappers, {len(instances['reducers'])} reducers")
