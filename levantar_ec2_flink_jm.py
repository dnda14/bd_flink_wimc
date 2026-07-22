import boto3
import json
import time
import sys
import os

REGION = 'us-east-1'
INSTANCE_NAME = 'ec2-flink-server'
KEY_NAME = 'vockey' # Usamos la misma llave que ya tienes
INSTANCE_TYPE = 't3.medium' # Flink también necesita buena RAM
AMI_ID = 'ami-0c7217cdde317cfec' # Ubuntu

S3_BUCKET_NAME = "kafka-flink-bucket-dnda"

def create_security_group(ec2_client):
    sg_name = 'ec2-flink-sg'
    try:
        print("Creando Security Group para Flink...")
        response = ec2_client.create_security_group(
            GroupName=sg_name,
            Description='Acceso SSH y puertos para Web UI de Flink'
        )
        sg_id = response['GroupId']

        # Abrir puertos: 22 (SSH), 8081 (Web UI de Flink)
        ec2_client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                {'IpProtocol': 'tcp', 'FromPort': 8081, 'ToPort': 8081, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                {'IpProtocol': 'tcp', 'FromPort': 6123, 'ToPort': 6123, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}, # Flink RPC
                {'IpProtocol': 'tcp', 'FromPort': 0, 'ToPort': 65535, 'UserIdGroupPairs': [{'GroupId': sg_id}]}
            ]
        )

    except ec2_client.exceptions.ClientError as e:
        if 'InvalidGroup.Duplicate' in str(e):
            sgs = ec2_client.describe_security_groups(GroupNames=[sg_name])
            sg_id = sgs['SecurityGroups'][0]['GroupId']
        else:
            raise e

    return sg_id

def lanzar_ec2():
    ec2 = boto3.client('ec2', region_name=REGION)
    sg_id = create_security_group(ec2)
    
    print(f"Lanzando instancia {INSTANCE_TYPE} con clave {KEY_NAME} para FLINK...")
    
    user_data = """#!/bin/bash
apt-get update -y
apt-get install -y openjdk-17-jre wget

# 1. Obtener la IP Pública y Privada
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
PUBLIC_IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)
PRIVATE_IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/local-ipv4)

# 2. Descargar e instalar Apache Flink 2.x
cd /opt
wget https://archive.apache.org/dist/flink/flink-2.3.0/flink-2.3.0-bin-scala_2.12.tgz
tar -xzf flink-2.3.0-bin-scala_2.12.tgz
mv flink-2.3.0 flink

mkdir -p /opt/flink/plugins/s3-fs-hadoop
cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

# 3. Configurar Flink para permitir acceso web desde afuera
cat <<EOF > /opt/flink/conf/config.yaml
rest:
  address: $PUBLIC_IP
  bind-address: 0.0.0.0

jobmanager:
  rpc:
    address: $PRIVATE_IP
    port: 6123
  memory:
    process.size: 1600m

taskmanager:
  memory:
    process.size: 1728m

state:
  backend: rocksdb
  checkpoints:
    dir: s3://kafka-flink-bucket-dnda/flink-checkpoints
EOF

# 4. Dar permisos al usuario ubuntu
chown -R ubuntu:ubuntu /opt/flink

# 5. Iniciar el cluster de Flink localmente
sudo -u ubuntu /opt/flink/bin/jobmanager.sh start
"""

    response = ec2.run_instances(
        ImageId=AMI_ID,
        InstanceType=INSTANCE_TYPE,
        KeyName=KEY_NAME,
        MaxCount=1,
        MinCount=1,
        SecurityGroupIds=[sg_id],
        UserData=user_data,
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': INSTANCE_NAME}]
            }
        ]
    )
    
    instance_id = response['Instances'][0]['InstanceId']
    print(f"✅ Instancia de Flink creada con ID: {instance_id}")
    
    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance_id])
    
    desc = ec2.describe_instances(InstanceIds=[instance_id])
    ip_publica = desc['Reservations'][0]['Instances'][0].get('PublicIpAddress', 'No asignada')
    ip_privada = desc['Reservations'][0]['Instances'][0].get('PrivateIpAddress', 'No asignada')
    
    print("\n" + "=" * 60)
    print("RESUMEN DE LA INSTANCIA EC2 DE FLINK")
    print("=" * 60)
    print(f"  Instance ID:    {instance_id}")
    print(f"  IP Pública:     {ip_publica}")
    print(f"  Estado:         Running")
    print(f"  SSH Conexión:   ssh -i {KEY_NAME}.pem ubuntu@{ip_publica}")
    print(f"  Panel Flink UI: http://{ip_publica}:8081")
    print("=" * 60)
    print("Nota: Espera unos 2-3 minutos para que se instale todo antes de abrir el Panel Web.")
    
    info = {'instance_id': instance_id, 'ip_publica': ip_publica, 'ip_privada_jm': ip_privada}
    with open('ec2_flink_info.json', 'w') as f:
        json.dump(info, f, indent=4)
        
def terminar_ec2():
    try:
        with open('ec2_flink_info.json', 'r') as f:
            info = json.load(f)
    except FileNotFoundError:
        print("❌ No se encontró ec2_flink_info.json.")
        sys.exit(1)
        
    instance_id = info['instance_id']
    ec2 = boto3.client('ec2', region_name=REGION)
    
    print(f"Terminando instancia de Flink {instance_id}...")
    ec2.terminate_instances(InstanceIds=[instance_id])
    print("✅ Señal enviada. La EC2 de Flink se apagará.")
    
    if os.path.exists('ec2_flink_info.json'):
        os.remove('ec2_flink_info.json')

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--terminar':
        terminar_ec2()
    else:
        lanzar_ec2()
