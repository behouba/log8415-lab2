#!/usr/bin/env python3
import json, os, sys, subprocess, time, urllib.request, shlex

import boto3
from botocore.exceptions import ClientError

KEY_PATH = os.getenv("AWS_KEY_PATH")
if not KEY_PATH:
    sys.exit("Missing AWS_KEY_PATH. Run: set -a; source .env; set +a")

with open("artifacts/wordcount_instance.json") as f:
    instance = json.load(f)

HOST = instance["public_ip"]
SSH_USER = "ubuntu"
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SG_ID = os.getenv("AWS_INSTANCE_SG_ID")

SSH_BASE = [
    "ssh",
    "-o", "StrictHostKeyChecking=no",
    "-o", "BatchMode=yes",
    "-o", "ServerAliveInterval=15",
    "-o", "ServerAliveCountMax=3",
    "-o", "ConnectTimeout=20",
    "-o", "ConnectionAttempts=10",
]

def ensure_ssh_access():
    if not SG_ID:
        print("WARN: AWS_INSTANCE_SG_ID not set; skipping security group authorization.")
        return
    try:
        myip = urllib.request.urlopen("https://checkip.amazonaws.com", timeout=10).read().decode().strip()
    except Exception as exc:
        print(f"WARN: Unable to determine public IP for SSH authorization: {exc}")
        return
    cidr = f"{myip}/32"
    print(f"Ensuring SSH access for {cidr} on security group {SG_ID}...")
    ec2 = boto3.client("ec2", region_name=AWS_REGION)
    try:
        ec2.authorize_security_group_ingress(
            GroupId=SG_ID,
            IpPermissions=[{
                "IpProtocol": "tcp",
                "FromPort": 22,
                "ToPort": 22,
                "IpRanges": [{
                    "CidrIp": cidr,
                    "Description": "Lab2 automation SSH access"
                }]
            }]
        )
        print(f"Authorized SSH from {cidr}.")
    except ClientError as err:
        if "InvalidPermission.Duplicate" in str(err):
            print(f"SSH rule for {cidr} already exists.")
        else:
            print(f"WARN: Failed to authorize SSH {cidr}: {err}")

def ssh(cmd, show_output=True):
    remote = f"bash -lc {shlex.quote(cmd)}"
    proc = subprocess.Popen(
        SSH_BASE + ["-i", KEY_PATH, f"{SSH_USER}@{HOST}", remote],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
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
    if proc.returncode != 0:
        print(f"ERROR: Command failed with exit code {proc.returncode}")
        if show_output:
            sys.exit(1)
        else:
            print("".join(output_lines))
        sys.exit(1)
    return "".join(output_lines)

ensure_ssh_access()

print(f"Setting up Hadoop and Spark on {HOST}...")
print("\n=== Step 1: Wait for SSH to be ready ===")
for i in range(30):
    try:
        result = subprocess.run(
            SSH_BASE + ["-i", KEY_PATH, f"{SSH_USER}@{HOST}", "echo ready"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10
        )
        if result.returncode == 0:
            print("SSH is ready!")
            break
        else:
            msg = result.stderr.strip() if result.stderr else result.stdout.strip()
            if msg:
                print(f"SSH attempt {i+1} failed: {msg}")
    except Exception as exc:
        print(f"SSH attempt {i+1} exception: {exc}")
    print(f"Waiting for SSH... ({i+1}/30)")
    time.sleep(10)
else:
    sys.exit("SSH did not become available")

print("\n=== Step 2: Update system and install dependencies ===")
ssh("sudo apt-get update -y")
ssh("sudo DEBIAN_FRONTEND=noninteractive apt-get install -y openjdk-11-jdk wget curl")

print("\n=== Step 3: Verify Java installation ===")
ssh("java -version")

print("\n=== Step 4: Download and install Hadoop 3.3.6 ===")
ssh("""
set -e
cd ~
HADOOP_TGZ=hadoop-3.4.2.tar.gz
HADOOP_URL=https://dlcdn.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz
DL_OPTS="--fail --location --retry 5 --retry-all-errors --retry-delay 5 --connect-timeout 20 --speed-limit 10240 --speed-time 30 --continue-at -"
if [ -d ~/hadoop ]; then
  echo "[HADOOP] Existing ~/hadoop directory detected; skipping download."
  du -sh ~/hadoop || true
else
  if [ -f "$HADOOP_TGZ" ]; then
    echo "[HADOOP] Reusing existing $HADOOP_TGZ"
    ls -lh "$HADOOP_TGZ"
  else
    echo "[HADOOP] Downloading from $HADOOP_URL with curl"
    if ! curl $DL_OPTS -o "$HADOOP_TGZ" "$HADOOP_URL"; then
      status=$?
      echo "[HADOOP] curl download failed (exit $status); retrying with wget..."
      rm -f "$HADOOP_TGZ"
      if ! wget -O "$HADOOP_TGZ" --tries=5 --timeout=60 --waitretry=5 --continue --progress=dot:giga "$HADOOP_URL"; then
        status=$?
        echo "[HADOOP] Failed to download tarball via wget (exit $status)" >&2
        exit 1
      fi
    fi
    echo "[HADOOP] Download completed"
    ls -lh "$HADOOP_TGZ"
  fi
  echo "[HADOOP] Verifying tarball..."
  if ! tar -tzf "$HADOOP_TGZ" >/dev/null 2>&1; then
    echo "[HADOOP] Tarball appears corrupt. Delete $HADOOP_TGZ and rerun." >&2
    exit 1
  fi
  echo "[HADOOP] Extracting..."
  rm -rf hadoop-3.4.2
  tar -xzf "$HADOOP_TGZ"
  rm -rf hadoop
  mv hadoop-3.4.2 hadoop
  rm "$HADOOP_TGZ"
fi
""")

print("\n=== Step 5: Configure Hadoop environment ===")
ssh("""
cat > ~/.cloud_lab_env.sh <<'ENV_EOF'
# Cloud Computing Lab environment
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export HADOOP_HOME=/home/ubuntu/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
ENV_EOF
""")

ssh("""
if ! grep -Fq '. ~/.cloud_lab_env.sh' ~/.bashrc && ! grep -Fq 'source ~/.cloud_lab_env.sh' ~/.bashrc; then
  printf '\n# Load Cloud Computing Lab environment\n[ -f ~/.cloud_lab_env.sh ] && . ~/.cloud_lab_env.sh\n' >> ~/.bashrc
fi
""")

ssh("""
if [ ! -f ~/.profile ]; then
  printf '#!/bin/sh\n' > ~/.profile
fi
if grep -Eq '(\\. ~/.bashrc|source ~/.bashrc)' ~/.profile; then
  :
elif ! grep -Fq '. ~/.cloud_lab_env.sh' ~/.profile && ! grep -Fq 'source ~/.cloud_lab_env.sh' ~/.profile; then
  printf '\n# Load Cloud Computing Lab environment\n[ -f ~/.cloud_lab_env.sh ] && . ~/.cloud_lab_env.sh\n' >> ~/.profile
fi
""")

print("\n=== Step 6: Configure Hadoop core-site.xml ===")
ssh("""
cat > ~/hadoop/etc/hadoop/core-site.xml << 'XML_EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>/home/ubuntu/hadoop/tmp</value>
    </property>
</configuration>
XML_EOF
""")

print("\n=== Step 7: Configure Hadoop hdfs-site.xml ===")
ssh("""
cat > ~/hadoop/etc/hadoop/hdfs-site.xml << 'XML_EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>/home/ubuntu/hadoop/data/namenode</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>/home/ubuntu/hadoop/data/datanode</value>
    </property>
</configuration>
XML_EOF
""")

print("\n=== Step 8: Configure Hadoop mapred-site.xml ===")
ssh("""
cat > ~/hadoop/etc/hadoop/mapred-site.xml << 'XML_EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapreduce.framework.name</name>
        <value>yarn</value>
    </property>
    <property>
        <name>mapreduce.application.classpath</name>
        <value>$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*:$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*</value>
    </property>
</configuration>
XML_EOF
""")

print("\n=== Step 9: Configure Hadoop yarn-site.xml ===")
ssh("""
cat > ~/hadoop/etc/hadoop/yarn-site.xml << 'XML_EOF'
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>yarn.nodemanager.aux-services</name>
        <value>mapreduce_shuffle</value>
    </property>
    <property>
        <name>yarn.nodemanager.env-whitelist</name>
        <value>JAVA_HOME,HADOOP_COMMON_HOME,HADOOP_HDFS_HOME,HADOOP_CONF_DIR,CLASSPATH_PREPEND_DISTCACHE,HADOOP_YARN_HOME,HADOOP_HOME,PATH,LANG,TZ,HADOOP_MAPRED_HOME</value>
    </property>
</configuration>
XML_EOF
""")

print("\n=== Step 10: Set JAVA_HOME in hadoop-env.sh ===")
ssh("""
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/hadoop/etc/hadoop/hadoop-env.sh
""")

print("\n=== Step 11: Create Hadoop directories ===")
ssh("""
mkdir -p ~/hadoop/tmp
mkdir -p ~/hadoop/data/namenode
mkdir -p ~/hadoop/data/datanode
""")

JAVA_HOME = "/usr/lib/jvm/java-11-openjdk-amd64"
env_prefix = f"JAVA_HOME={JAVA_HOME} "

print("\n=== Step 12: Configure passwordless SSH for Hadoop scripts ===")
ssh("""
mkdir -p ~/.ssh
chmod 700 ~/.ssh
if [ ! -f ~/.ssh/id_rsa ]; then
  ssh-keygen -t rsa -q -N "" -f ~/.ssh/id_rsa
fi
touch ~/.ssh/authorized_keys
if ! grep -qxF "$(cat ~/.ssh/id_rsa.pub)" ~/.ssh/authorized_keys; then
  cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
fi
chmod 600 ~/.ssh/authorized_keys
""")

print("\n=== Step 13: Format HDFS ===")
ssh(f"{env_prefix}~/hadoop/bin/hdfs namenode -format -force")

print("\n=== Step 14: Start Hadoop services ===")
ssh(f"{env_prefix}~/hadoop/bin/hdfs --daemon start namenode")
ssh(f"{env_prefix}~/hadoop/bin/hdfs --daemon start datanode")
ssh(f"{env_prefix}~/hadoop/bin/yarn --daemon start resourcemanager")
ssh(f"{env_prefix}~/hadoop/bin/yarn --daemon start nodemanager")

print("\n=== Step 15: Verify Hadoop is running ===")
time.sleep(5)
ssh(f"{env_prefix}~/hadoop/bin/hdfs dfsadmin -report")

print("\n=== Step 16: Download and install Spark 3.5.0 ===")
ssh("""
set -e
cd ~
SPARK_TGZ=spark-3.5.7-bin-hadoop3.tgz
SPARK_URL=https://dlcdn.apache.org/spark/spark-3.5.7/spark-3.5.7-bin-hadoop3.tgz
DL_OPTS="--fail --location --retry 5 --retry-all-errors --retry-delay 5 --connect-timeout 20 --speed-limit 10240 --speed-time 30 --continue-at -"
if [ -d ~/spark ]; then
  echo "[SPARK] Existing ~/spark directory detected; skipping download."
  du -sh ~/spark || true
else
  if [ -f "$SPARK_TGZ" ]; then
    echo "[SPARK] Reusing existing $SPARK_TGZ"
    ls -lh "$SPARK_TGZ"
  else
    echo "[SPARK] Downloading from $SPARK_URL with curl"
    if ! curl $DL_OPTS -o "$SPARK_TGZ" "$SPARK_URL"; then
      status=$?
      echo "[SPARK] curl download failed (exit $status); retrying with wget..."
      rm -f "$SPARK_TGZ"
      if ! wget -O "$SPARK_TGZ" --tries=5 --timeout=60 --waitretry=5 --continue --progress=dot:giga "$SPARK_URL"; then
        status=$?
        echo "[SPARK] Failed to download tarball via wget (exit $status)" >&2
        exit 1
      fi
    fi
    echo "[SPARK] Download completed"
    ls -lh "$SPARK_TGZ"
  fi
  echo "[SPARK] Verifying tarball..."
  if ! tar -tzf "$SPARK_TGZ" >/dev/null 2>&1; then
    echo "[SPARK] Tarball appears corrupt. Delete $SPARK_TGZ and rerun." >&2
    exit 1
  fi
  echo "[SPARK] Extracting..."
  rm -rf spark-3.5.7-bin-hadoop3
  tar -xzf "$SPARK_TGZ"
  rm -rf spark
  mv spark-3.5.7-bin-hadoop3 spark
  rm "$SPARK_TGZ"
fi
""")

print("\n=== Step 17: Configure Spark environment ===")
ssh("""
cat >> ~/.cloud_lab_env.sh <<'ENV_EOF'

# Spark Environment
export SPARK_HOME=/home/ubuntu/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=/usr/bin/python3
ENV_EOF
""")

print("\n=== Step 18: Verify Spark installation ===")
ssh(f"{env_prefix}~/spark/bin/spark-submit --version")

print("\n=== Step 19: Create HDFS input directory ===")
ssh(f"{env_prefix}~/hadoop/bin/hdfs dfs -mkdir -p /input")
ssh(f"{env_prefix}~/hadoop/bin/hdfs dfs -mkdir -p /output")

print("\n=== Step 20: Prepare HDFS staging directories ===")
ssh(f"{env_prefix}~/hadoop/bin/hdfs dfs -mkdir -p /tmp")
ssh(f"{env_prefix}~/hadoop/bin/hdfs dfs -chmod -R 1777 /tmp")
ssh(f"{env_prefix}~/hadoop/bin/hdfs dfs -mkdir -p /user/{SSH_USER}")
ssh(f"{env_prefix}~/hadoop/bin/hdfs dfs -chown -R {SSH_USER} /user/{SSH_USER}")

print("\nOK Hadoop and Spark installation complete!")
print("Hadoop NameNode: http://{}:9870".format(HOST))
print("YARN:            http://{}:8088".format(HOST))
