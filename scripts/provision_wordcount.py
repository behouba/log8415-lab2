#!/usr/bin/env python3
import json, os, sys
import boto3

REGION   = os.getenv("AWS_REGION", "us-east-1")
KEY_NAME = os.getenv("AWS_KEY_NAME")
SG_ID    = os.getenv("AWS_INSTANCE_SG_ID")
AMI_ID   = os.getenv("AWS_AMI_ID", "")
SUBNETS  = os.getenv("AWS_SUBNET_IDS", "").split(",") if os.getenv("AWS_SUBNET_IDS") else []

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

print("Creating 1 x t2.large instance for WordCount benchmarking...")
instances = ec2.create_instances(
    ImageId=AMI_ID,
    InstanceType="t2.large",
    MinCount=1, MaxCount=1,
    KeyName=KEY_NAME,
    NetworkInterfaces=[{
        "DeviceIndex": 0,
        "SubnetId": SUBNETS[0],
        "AssociatePublicIpAddress": True,
        "Groups": [SG_ID],
    }],
    TagSpecifications=[{
        "ResourceType": "instance",
        "Tags": [
            {"Key": "Name", "Value": "lab2-wordcount-t2.large"},
            {"Key": "Lab", "Value": "lab2"},
            {"Key": "Part", "Value": "wordcount"},
        ],
    }],
    # Increase storage for datasets
    BlockDeviceMappings=[{
        "DeviceName": "/dev/sda1",
        "Ebs": {
            "VolumeSize": 30,  # 30GB for Hadoop/Spark and datasets
            "VolumeType": "gp3",
            "DeleteOnTermination": True
        }
    }]
)

instance = instances[0]
print(f"\nWaiting for instance {instance.id} to enter the 'running' state...")
instance.wait_until_running()
instance.load()

print("\n✅ Instance is running. Details:")
print(f"  - ID:         {instance.id}")
print(f"  - Type:       {instance.instance_type}")
print(f"  - Public IP:  {instance.public_ip_address}")
print(f"  - Private IP: {instance.private_ip_address}")

output_data = {
    "id": instance.id,
    "type": instance.instance_type,
    "state": instance.state["Name"],
    "public_ip": instance.public_ip_address,
    "private_ip": instance.private_ip_address,
}

os.makedirs("artifacts", exist_ok=True)
with open("artifacts/wordcount_instance.json", "w") as f:
    json.dump(output_data, f, indent=2)

print("\n✅ Wrote instance details to artifacts/wordcount_instance.json")
