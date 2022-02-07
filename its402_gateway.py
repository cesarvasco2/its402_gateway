import sys
import decimal
import json
import paho.mqtt.client as mqtt
import boto3
import time
from datetime import date, datetime
import calendar
import ssl
sqs = boto3.resource('sqs', region_name='us-east-1')
queue = sqs.get_queue_by_name(QueueName='processador_entrada')
#configurações do broker:
Broker = 'servermqtt.duckdns.org'
PortaBroker = 1883 
Usuario = 'ghidro'
Senha = 'ghidro'
KeepAliveBroker = 60
TopicoSubscribe = '#' #Topico que ira se inscrever
#Callback - conexao ao broker realizada
def on_connect(client, userdata, flags, rc):
    print('[STATUS] Conectado ao Broker. Resultado de conexao: '+str(rc))
#faz subscribe automatico no topico
    client.subscribe(TopicoSubscribe)
#Callback - mensagem recebida do broker
def on_message(client, userdata, msg):
    MensagemRecebida = str(msg.payload.decode('utf-8'))
    #print("[MESAGEM RECEBIDA] Topico: "+msg.topic+" / Mensagem: "+MensagemRecebida)
    #programa principal:
    list_payload = json.loads(MensagemRecebida)
    dia_semana = date.today() 
    dict_payload = {}  
    lista_de_campos = [
        {'key':'sinal','type':'str','value_key':'v','fields':'rssi'},
        {'key':'sinal_ruido','type':'str','value_key':'v','fields':'snr'},
        {'key':'modelo','type':'str','value_key':'vs','fields':'model'},
        {'key':'versao','type':'str','value_key':'vs','fields':'version'},
        {'key':'voltagem_bateria','type':'str','value_key':'v','fields':'battery'},
        {'key':'temperatura','type':'str','value_key':'v','fields':'temperatura'},
        {'key':'umidade','type':'str','value_key':'v','fields':'umidade'},
        {'key':'rele','type':'array','value_key':'vs','fields':('emr_b3_relay','emr_b4_relay')},
        {'key':'entradas_4a20','type':'4a20','value_key':'v','fields':('emc_e1_curr','emc_e2_curr','emc_e3_curr','emc_e4_curr')},
        {'key':'pressao','type':'array','value_key':'v','fields':('pressure-E2','pressure-E3','pressure-E4')},
        {'key':'status','type':'array','value_key':'vb','fields':('c1_status','c2_status','emr_c3_status','emr_c4_status')},
        
     ]
    dict_payload['id_dispositivo'] = list_payload[0]['bn']
    data_e_hora_atuais = datetime.now()
    dict_payload['data_hora_dispositivo'] = data_e_hora_atuais.strftime('%Y-%m-%d %H:%M:%S')
    for campo in lista_de_campos:
        for camp_equip in list_payload:
            if 'n' in camp_equip:
                if campo['type']=='array' or campo['type']=='4a20':
                    for field in campo['fields']:
                        if camp_equip['n'] == field:
                            if campo['key'] == 'status':
                                if 'vb' in camp_equip:
                                    if not campo['key'] in dict_payload:
                                        dict_payload[campo['key']]=[]
                                    dict_payload[campo['key']].append(str(camp_equip[campo['value_key']]))
                            else:
                                if not campo['key'] in dict_payload:
                                    dict_payload[campo['key']]=[]
                                if campo['type']=='4a20':
                                    dict_payload[campo['key']].append(int(camp_equip[campo['value_key']]*100000))
                                else:    
                                    dict_payload[campo['key']].append(int(camp_equip[campo['value_key']]))
                else:
                    if camp_equip['n']==campo['fields']:
                        if campo['type']=='str':
                            dict_payload[campo['key']] = str(camp_equip[campo['value_key']])
                        elif campo['type']=='int':
                            dict_payload[campo['key']] = int(camp_equip[campo['value_key']])    
    dict_payload['codigo_produto'] = 16
    dict_payload['timestamp_servidor'] = int(datetime.now().timestamp())
    dict_payload['timestamp_dispositivo'] = int(list_payload[0]['bt'])
    dict_payload['dia_sem'] = calendar.day_name[dia_semana.weekday()] 
    remover_vazio = []
    for chave, valor in dict_payload.items():
        if dict_payload[chave] == []:
            remover_vazio.append(chave)
    for item in remover_vazio:
        dict_payload.pop(item)
    if 'entradas_4a20' in dict_payload:
        for v in range(len(dict_payload['entradas_4a20'])): 
                if dict_payload['entradas_4a20'][v] < 300: 
                    dict_payload['entradas_4a20'][v] = 0      
    if 'rele' in dict_payload:
        for v in range(len(dict_payload['rele'])): 
            if dict_payload['rele'][v] == 'NC': 
                dict_payload['rele'][v] = 1
            elif dict_payload['rele'][v] == 'NO': 
                dict_payload['rele'][v] = 0
    if 'status' in dict_payload:
        for v in range(len(dict_payload['status'])): 
            if dict_payload['status'][v] == 'True': 
                dict_payload['status'][v] = 1
            elif dict_payload['status'][v] == 'False': 
                dict_payload['status'][v] = 0          
    print(dict_payload)
    queue.send_message(MessageBody=str(json.dumps(dict_payload, ensure_ascii=False)))
try:
    print('[STATUS] Inicializando MQTT...')
    #inicializa MQTT:
    client = mqtt.Client('', True, None, mqtt.MQTTv311)
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(Usuario, Senha)
    # the key steps here
    #context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    # if you do not want to check the cert hostname, skip it
    # context.check_hostname = False
    #client.tls_set_context(context)
    client.connect(Broker, PortaBroker, KeepAliveBroker)
    client.loop_forever()

except KeyboardInterrupt:
    print ('\nCtrl+C pressionado, encerrando aplicacao e saindo...')
    sys.exit(0)