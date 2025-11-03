#!/usr/bin/env python3
import os, sys
import boto3

REGION = os.getenv("AWS_REGION", "us-east-1")

ec2 = boto3.resource("ec2", region_name=REGION)
ec2_client = boto3.client("ec2", region_name=REGION)

print("=== Lab 2 Teardown ===\n")

# Find all Lab 2 instances
print("Finding all Lab 2 instances...")
instances = list(ec2.instances.filter(
    Filters=[
        {"Name": "tag:Lab", "Values": ["lab2"]},
        {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]}
    ]
))

if not instances:
    print("No Lab 2 instances found.")
else:
    print(f"Found {len(instances)} instance(s):")
    for instance in instances:
        name = next((tag["Value"] for tag in (instance.tags or []) if tag["Key"] == "Name"), "unnamed")
        print(f"  - {instance.id} ({name}) - {instance.state['Name']}")

    print(f"\nTerminating {len(instances)} instance(s)...")
    instance_ids = [i.id for i in instances]
    ec2_client.terminate_instances(InstanceIds=instance_ids)
    print("OK Termination initiated")

    print("\nWaiting for instances to terminate...")
    for instance in instances:
        print(f"  Waiting for {instance.id}...")
        instance.wait_until_terminated()
    print("OK All instances terminated")

print("\nOK Teardown complete!")
