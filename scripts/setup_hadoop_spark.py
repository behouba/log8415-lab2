#!/usr/bin/env python3
import json, os, sys, subprocess, time

KEY_PATH = os.getenv("AWS_KEY_PATH")
if not KEY_PATH:
    sys.exit("Missing AWS_KEY_PATH. Run: set -a; source .env; set +a")

with open("artifacts/wordcount_instance.json") as f:
    instance = json.load(f)

HOST = instance["public_ip"]
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

def ssh(cmd, show_output=True):
    remote = f"bash -lc '{cmd}'"
    result = subprocess.run(
        SSH_BASE + ["-i", KEY_PATH, f"{SSH_USER}@{HOST}", remote],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
    )
    if show_output and result.stdout:
        print(result.stdout, end="")
    if result.returncode != 0:
        print(f"ERROR: Command failed with exit code {result.returncode}")
        print(result.stdout)
        sys.exit(1)
    return result

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
    except:
        pass
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
HADOOP_TGZ=hadoop-3.3.6.tar.gz
HADOOP_URL=https://archive.apache.org/dist/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
DL_OPTS="--fail --location --retry 5 --retry-all-errors --retry-delay 5 --connect-timeout 20 --max-time 600 --speed-limit 10240 --speed-time 30 --continue-at -"
echo "Downloading Hadoop from $HADOOP_URL"
curl $DL_OPTS -o "$HADOOP_TGZ" "$HADOOP_URL"
tar -xzf "$HADOOP_TGZ"
rm -rf hadoop
mv hadoop-3.3.6 hadoop
rm "$HADOOP_TGZ"
""")

print("\n=== Step 5: Configure Hadoop environment ===")
ssh("""
cat >> ~/.bashrc << 'BASHRC_EOF'

# Hadoop Environment
export HADOOP_HOME=/home/ubuntu/hadoop
export HADOOP_INSTALL=$HADOOP_HOME
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
BASHRC_EOF
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
ssh(f"{env_prefix}~/hadoop/sbin/hadoop-daemon.sh start namenode")
ssh(f"{env_prefix}~/hadoop/sbin/hadoop-daemon.sh start datanode")
ssh(f"{env_prefix}~/hadoop/sbin/yarn-daemon.sh start resourcemanager")
ssh(f"{env_prefix}~/hadoop/sbin/yarn-daemon.sh start nodemanager")

print("\n=== Step 15: Verify Hadoop is running ===")
time.sleep(5)
ssh(f"{env_prefix}~/hadoop/bin/hdfs dfsadmin -report")

print("\n=== Step 16: Download and install Spark 3.5.0 ===")
ssh("""
set -e
cd ~
SPARK_TGZ=spark-3.5.0-bin-hadoop3.tgz
SPARK_URL=https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
DL_OPTS="--fail --location --retry 5 --retry-all-errors --retry-delay 5 --connect-timeout 20 --max-time 600 --speed-limit 10240 --speed-time 30 --continue-at -"
echo "Downloading Spark from $SPARK_URL"
curl $DL_OPTS -o "$SPARK_TGZ" "$SPARK_URL"
tar -xzf "$SPARK_TGZ"
rm -rf spark
mv spark-3.5.0-bin-hadoop3 spark
rm "$SPARK_TGZ"
""")

print("\n=== Step 17: Configure Spark environment ===")
ssh("""
cat >> ~/.bashrc << 'BASHRC_EOF'

# Spark Environment
export SPARK_HOME=/home/ubuntu/spark
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
export PYSPARK_PYTHON=/usr/bin/python3
BASHRC_EOF
""")

print("\n=== Step 18: Verify Spark installation ===")
ssh(f"{env_prefix}~/spark/bin/spark-submit --version")

print("\n=== Step 19: Create HDFS input directory ===")
ssh(f"{env_prefix}~/hadoop/bin/hdfs dfs -mkdir -p /input")
ssh(f"{env_prefix}~/hadoop/bin/hdfs dfs -mkdir -p /output")

print("\n✅ Hadoop and Spark installation complete!")
print("Hadoop NameNode: http://{}:9870".format(HOST))
print("YARN:            http://{}:8088".format(HOST))
