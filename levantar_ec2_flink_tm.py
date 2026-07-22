import boto3
import json
import time
import sys
import os

REGION = 'us-east-1'
INSTANCE_NAME = 'ec2-flink-taskmanager' # Nombre distinto para el worker
KEY_NAME = 'vockey' 
INSTANCE_TYPE = 't3.medium' 
AMI_ID = 'ami-0c7217cdde317cfec' 

def get_security_group_id(ec2_client, sg_name='ec2-flink-sg'):
    # Busca el Security Group que ya creó el script del JobManager
    try:
        sgs = ec2_client.describe_security_groups(GroupNames=[sg_name])
        return sgs['SecurityGroups'][0]['GroupId']
    except ec2_client.exceptions.ClientError as e:
        print(f"❌ Error: No se encontró el Security Group {sg_name}. ¿Ya lanzaste el JobManager?")
        sys.exit(1)

def lanzar_taskmanager():
    # 1. Leer la IP privada del JobManager
    try:
        with open('ec2_flink_info.json', 'r') as f:
            jm_info = json.load(f)
            jm_private_ip = jm_info['ip_privada_jm']
    except FileNotFoundError:
        print("❌ Error: No se encontró 'ec2_flink_info.json'. Debes lanzar el JobManager primero.")
        sys.exit(1)
    except KeyError:
        print("❌ Error: El archivo JSON no contiene 'ip_privada_jm'.")
        sys.exit(1)

    ec2 = boto3.client('ec2', region_name=REGION)
    sg_id = get_security_group_id(ec2)
    
    print(f"Lanzando TaskManager ({INSTANCE_TYPE})... Se conectará al JobManager en {jm_private_ip}")
    
    # 2. Configurar el User Data
    # Inyectamos la variable jm_private_ip directamente en el YAML de Flink
    user_data = f"""#!/bin/bash
apt-get update -y
apt-get install -y openjdk-17-jre wget curl

# Descargar e instalar Apache Flink 2.3.0
cd /opt
wget https://archive.apache.org/dist/flink/flink-2.3.0/flink-2.3.0-bin-scala_2.12.tgz
tar -xzf flink-2.3.0-bin-scala_2.12.tgz
mv flink-2.3.0 flink

# Habilitar plugin de S3 (Obligatorio para los checkpoints de RocksDB)
mkdir -p /opt/flink/plugins/s3-fs-hadoop
cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

# Configurar Flink para que se reporte al JobManager
cat <<EOF > /opt/flink/conf/config.yaml
jobmanager:
  rpc:
    address: {jm_private_ip}
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

# Dar permisos al usuario ubuntu
chown -R ubuntu:ubuntu /opt/flink

# INICIAR SOLO EL TASKMANAGER
sudo -u ubuntu /opt/flink/bin/taskmanager.sh start
"""

    # OJO: Descomenta IamInstanceProfile si estás usando S3 y necesitas permisos de AWS Academy
    response = ec2.run_instances(
        ImageId=AMI_ID,
        InstanceType=INSTANCE_TYPE,
        KeyName=KEY_NAME,
        MaxCount=1,
        MinCount=1,
        SecurityGroupIds=[sg_id],
        UserData=user_data,
        # IamInstanceProfile={{'Name': 'LabInstanceProfile'}}, 
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [{'Key': 'Name', 'Value': INSTANCE_NAME}]
            }
        ]
    )
    
    instance_id = response['Instances'][0]['InstanceId']
    print(f"✅ Instancia TaskManager creada con ID: {instance_id}")
    
    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance_id])
    
    desc = ec2.describe_instances(InstanceIds=[instance_id])
    ip_publica = desc['Reservations'][0]['Instances'][0].get('PublicIpAddress', 'No asignada')
    
    print("\n" + "=" * 60)
    print("RESUMEN DE LA INSTANCIA EC2 - TASKMANAGER")
    print("=" * 60)
    print(f"  Instance ID:    {instance_id}")
    print(f"  IP Pública:     {ip_publica}")
    print(f"  Conectado a:    JobManager en {jm_private_ip}")
    print("=" * 60)
    print("Nota: Espera ~2 minutos. Luego revisa la Web UI del JobManager y verás un 'Task Manager' disponible.")
    
    info = {'instance_id': instance_id, 'ip_publica': ip_publica, 'conectado_a': jm_private_ip}
    
    # Guardamos la info del TaskManager en su propio archivo
    with open('ec2_taskmanager_info.json', 'w') as f:
        json.dump(info, f, indent=4)
        
def terminar_ec2():
    try:
        with open('ec2_taskmanager_info.json', 'r') as f:
            info = json.load(f)
    except FileNotFoundError:
        print("❌ No se encontró ec2_taskmanager_info.json.")
        sys.exit(1)
        
    instance_id = info['instance_id']
    ec2 = boto3.client('ec2', region_name=REGION)
    
    print(f"Terminando instancia TaskManager {instance_id}...")
    ec2.terminate_instances(InstanceIds=[instance_id])
    print("✅ Señal enviada.")
    
    if os.path.exists('ec2_taskmanager_info.json'):
        os.remove('ec2_taskmanager_info.json')

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--terminar':
        terminar_ec2()
    else:
        lanzar_taskmanager()    