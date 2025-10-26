#!/usr/bin/env python3
"""
Provision EC2 instances for distributed MapReduce
- N mapper instances (t2.micro)
- M reducer instances (t2.micro)
"""
import json, os, sys, itertools
import boto3

REGION   = os.getenv("AWS_REGION", "us-east-1")
KEY_NAME = os.getenv("AWS_KEY_NAME")
SG_ID    = os.getenv("AWS_INSTANCE_SG_ID")
AMI_ID   = os.getenv("AWS_AMI_ID", "")
SUBNETS  = os.getenv("AWS_SUBNET_IDS", "").split(",") if os.getenv("AWS_SUBNET_IDS") else []

# Configuration
NUM_MAPPERS = 3   # Number of mapper instances
NUM_REDUCERS = 2  # Number of reducer instances

if not (KEY_NAME and SG_ID and SUBNETS):
    sys.exit("Missing one of: AWS_KEY_NAME, AWS_INSTANCE_SG_ID, AWS_SUBNET_IDS")

ec2 = boto3.resource("ec2", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

if not AMI_ID:
    print("AMI_ID not found in environment, resolving from AWS SSM...")
    try:
        AMI_ID = ssm.get_parameter(
            Name="/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
        )["Parameter"]["Value"]
    except Exception:
        AMI_ID = ssm.get_parameter(
            Name="/aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id"
        )["Parameter"]["Value"]
    print(f"Using Ubuntu 22.04 AMI: {AMI_ID}")

def create_instances(instance_type, count, role_tag):
    """Create multiple instances with a specific role"""
    subnet_cycle = itertools.cycle(SUBNETS)
    instances = []

    print(f"Creating {count} x {instance_type} instance(s) for {role_tag}...")

    for i in range(count):
        subnet = next(subnet_cycle)
        instance_group = ec2.create_instances(
            ImageId=AMI_ID,
            InstanceType=instance_type,
            MinCount=1, MaxCount=1,
            KeyName=KEY_NAME,
            NetworkInterfaces=[{
                "DeviceIndex": 0,
                "SubnetId": subnet,
                "AssociatePublicIpAddress": True,
                "Groups": [SG_ID],
            }],
            TagSpecifications=[{
                "ResourceType": "instance",
                "Tags": [
                    {"Key": "Name", "Value": f"lab2-{role_tag}-{i+1}"},
                    {"Key": "Lab", "Value": "lab2"},
                    {"Key": "Part", "Value": "mapreduce"},
                    {"Key": "Role", "Value": role_tag},
                ],
            }],
        )
        instances.extend(instance_group)

    return instances

print(f"Provisioning {NUM_MAPPERS} mappers and {NUM_REDUCERS} reducers...")
print()

# Create mapper instances
mapper_instances = create_instances("t2.micro", NUM_MAPPERS, "mapper")

# Create reducer instances
reducer_instances = create_instances("t2.micro", NUM_REDUCERS, "reducer")

all_instances = mapper_instances + reducer_instances

print(f"\nWaiting for all {len(all_instances)} instances to enter the 'running' state...")
for i in all_instances:
    print(f"  Waiting for {i.id}...")
    i.wait_until_running()
    i.load()

print("\n✅ All instances are running. Details:")

output_data = {
    "mappers": [],
    "reducers": [],
}

for i in mapper_instances:
    role = "mapper"
    print(f"  - {i.id} | {i.instance_type} | {role} | {i.public_ip_address}")
    output_data["mappers"].append({
        "id": i.id,
        "type": i.instance_type,
        "state": i.state["Name"],
        "public_ip": i.public_ip_address,
        "private_ip": i.private_ip_address,
        "role": role,
    })

for i in reducer_instances:
    role = "reducer"
    print(f"  - {i.id} | {i.instance_type} | {role} | {i.public_ip_address}")
    output_data["reducers"].append({
        "id": i.id,
        "type": i.instance_type,
        "state": i.state["Name"],
        "public_ip": i.public_ip_address,
        "private_ip": i.private_ip_address,
        "role": role,
    })

os.makedirs("artifacts", exist_ok=True)
with open("artifacts/mapreduce_instances.json", "w") as f:
    json.dump(output_data, f, indent=2)

print("\n✅ Wrote instance details to artifacts/mapreduce_instances.json")
print(f"\nSummary:")
print(f"  Mappers:  {len(mapper_instances)}")
print(f"  Reducers: {len(reducer_instances)}")
print(f"  Total:    {len(all_instances)}")
print("\nNext steps:")
print("  1. Wait ~30 seconds for SSH to become available")
print("  2. Run: python scripts/deploy_mapreduce.py")
