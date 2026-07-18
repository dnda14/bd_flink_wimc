import boto3
import json
import time
import sys


REGION = 'us-east-1'
INSTANCE_NAME = 'ec2-kafka-server'
KEY_NAME = 'vockey'
INSTANCE_TYPE = 't3.medium' 
AMI_ID = 'ami-0c7217cdde317cfec' 

def create_security_group(ec2_client):
    sg_name = 'ec2-kafka-sg'
    try:
        print("Creando Security Group...")
        response = ec2_client.create_security_group(
            GroupName=sg_name,
            Description='Acceso SSH y puertos para Kafka'
        )
        sg_id = response['GroupId']

        ec2_client.authorize_security_group_ingress(
            GroupId=sg_id,
            IpPermissions=[
                {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                {'IpProtocol': 'tcp', 'FromPort': 9092, 'ToPort': 9092, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                {'IpProtocol': 'tcp', 'FromPort': 2181, 'ToPort': 2181, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
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
    
    print(f"Lanzando instancia {INSTANCE_TYPE} con clave {KEY_NAME}...")
    
    user_data = """#!/bin/bash
apt-get update -y
apt-get install -y openjdk-17-jre wget

# 2. Obtener la IP Pública de la instancia EC2 usando la API de metadatos de AWS
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
PUBLIC_IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)

cd /opt
wget https://downloads.apache.org/kafka/4.3.1/kafka_2.13-4.3.1.tgz
tar -xzf kafka_2.13-4.3.1.tgz
mv kafka_2.13-4.3.1 kafka

# 4. Configurar Kafka para que anuncie su IP Pública y formatear KRaft
sed -i "s|advertised.listeners=.*|advertised.listeners=PLAINTEXT://$PUBLIC_IP:9092|g" /opt/kafka/config/server.properties
KAFKA_CLUSTER_ID=$(/opt/kafka/bin/kafka-storage.sh random-uuid)
/opt/kafka/bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c /opt/kafka/config/server.properties --standalone

# 5. Crear servicio para Kafka (KRaft mode)
cat <<EOF > /etc/systemd/system/kafka.service
[Unit]
Description=Apache Kafka
After=network.target

[Service]
Type=simple
ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh
Restart=on-abnormal

[Install]
WantedBy=multi-user.target
EOF

# 6. Iniciar Kafka automáticamente
systemctl daemon-reload
systemctl enable kafka
systemctl start kafka
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
    print(f"✅ Instancia creada con ID: {instance_id}")
    
    waiter = ec2.get_waiter('instance_running')
    waiter.wait(InstanceIds=[instance_id])
    
    # Obtener IP Pública
    desc = ec2.describe_instances(InstanceIds=[instance_id])
    ip_publica = desc['Reservations'][0]['Instances'][0].get('PublicIpAddress', 'No asignada')
    
    print("RESUMEN DE LA INSTANCIA EC2")
    print("=" * 60)
    print(f"  Instance ID:    {instance_id}")
    print(f"  IP Pública:     {ip_publica}")
    print(f"  Estado:         Running")
    print(f"  ssh -i labsuser.pem ubuntu@{ip_publica}")
    
    info = {'instance_id': instance_id, 'ip_publica': ip_publica}
    with open('ec2_info.json', 'w') as f:
        json.dump(info, f, indent=4)
        
def terminar_ec2():
    import os
    try:
        with open('ec2_info.json', 'r') as f:
            info = json.load(f)
    except FileNotFoundError:
        print("❌ No se encontró ec2_info.json. ¿Seguro que creaste la instancia con este script?")
        sys.exit(1)
        
    instance_id = info['instance_id']
    ec2 = boto3.client('ec2', region_name=REGION)
    
    print(f"Terminando instancia {instance_id}...")
    ec2.terminate_instances(InstanceIds=[instance_id])
    print("✅ Señal de terminación enviada. La instancia se apagará pronto.")
    
    if os.path.exists('ec2_info.json'):
        os.remove('ec2_info.json')

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--terminar':
        terminar_ec2()
    else:
        lanzar_ec2()
