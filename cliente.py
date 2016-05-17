import json
import time
import boto

valido = 0
while (valido != 1):
    try:
        USER_ID = int(raw_input("Qual o seu ID de usuario "))
    except ValueError:
        print ("Digite apenas numeros")
        pass
    else:
        USER_ID = str(USER_ID)
        valido = 1

print("Tentando conectar... ")
SNSConnection = boto.connect_sns(aws_access_key_id="[redacted]]",
                                 aws_secret_access_key="[redacted]")
SQSConnection = boto.connect_sqs(aws_access_key_id="[redacted]",
                                 aws_secret_access_key="[redacted]")

print("Conectado!")

print("Conectando no topico...")
createResult = SNSConnection.create_topic(USER_ID)
topicARN = createResult["CreateTopicResponse"]["CreateTopicResult"]["TopicArn"]

print("Conectando a fila do usuario no topico...")
idFila = SQSConnection.create_queue(USER_ID)
SNSConnection.subscribe_sqs_queue(topicARN, idFila)

print("")
print("")
print("Usuario " + USER_ID + " no ar!")
if (idFila.count() > 0):
    print("Voce tem mensagens para ler!")

opcao = 0
while (opcao != 9):
    print("")
    print("")
    print("")
    print("=================================")
    print("MENU DE OPCOES")
    print("Usuario: " + USER_ID)
    print("")
    print("1. Ler mensagens")
    print("2. Enviar mensagem")
    print("3. Criar grupo")
    print("4. Mandar para grupo")
    print("9. Sair")
    try:
        opcao = int(raw_input("> "))
    except ValueError:
        print("Digite apenas numeros")
        opcao = 0
        pass

    if (opcao == 1):
        print("Verificando mensagens...")
        qtdMsg = idFila.count()
        if (qtdMsg > 0):
            i = 1;
            while (1):
                listaMensagens = SQSConnection.receive_message(idFila,
                                                               number_messages=1,
                                                               visibility_timeout=3)
                if (len(listaMensagens) == 0):
                    break
                print("Recuperando mensagem " + str(i) + " de aproximadamente " + str(qtdMsg))
                i += 1
                msgBody = json.loads(listaMensagens[0].get_body())
                mensagem = eval(msgBody["Message"])
                print("        De: " + str(mensagem["from"]))
                print("      Data: " + str(mensagem["timestamp"]))
                print("  Mensagem: " + str(mensagem["body"]))
                print("")
                SQSConnection.delete_message(idFila, listaMensagens[0])
        else:
            print ("Nao ha mensagens")

    if (opcao == 2):
        try:
            DEST_USER_ID = int(raw_input("Destinatario: "))
        except ValueError:
            print("Digite apenas numeros")
            opcao = 0
            pass
        else:
            DEST_USER_ID = str(DEST_USER_ID)
            msgBody = raw_input("Digite a mensagem: ")
            msgDict = { "from": USER_ID,
                        "timestamp": time.strftime("%Y/%m/%d %H:%M:%S", time.gmtime()),
                        "body": str(msgBody)
                      }
            print("Conectando no topico do destinatario...")
            destTopic = SNSConnection.create_topic(DEST_USER_ID)
            destArn = destTopic["CreateTopicResponse"]["CreateTopicResult"]["TopicArn"]
            destQueue = SQSConnection.create_queue(DEST_USER_ID)
            SNSConnection.subscribe_sqs_queue(destArn, destQueue)
            status = SNSConnection.publish(topic=destArn,
                                           message=msgDict,
                                           message_structure="text")
            print ("Mensagem enviada!")

    if (opcao == 3):
        topicName = str(raw_input("Qual o nome do grupo a ser criado? "))

        # Verificar se o grupo ja existe => tentar conectar no topico
        topicos = SNSConnection.get_all_topics()
        lstTopicos = topicos['ListTopicsResponse']['ListTopicsResult']['Topics']
        nomesDosTopicos = [t['TopicArn'].split(':')[5] for t in lstTopicos]
        if (topicName in nomesDosTopicos):
            print("Esse grupo ja existe")
            break

        # Criar o grupo
        destTopic = SNSConnection.create_topic(topicName)
        destArn = destTopic["CreateTopicResponse"]["CreateTopicResult"]["TopicArn"]

        # Me registrar no topico
        SNSConnection.subscribe_sqs_queue(destArn, idFila)

        NEW_USER = 0
        while (NEW_USER != -1):
            NEW_USER = int(raw_input("Qual o ID do usuario que vai ser incluido no grupo (-1 parar)? "))
            if (NEW_USER != -1):
                destQueue = SQSConnection.create_queue(str(NEW_USER))
                SNSConnection.subscribe_sqs_queue(destArn, destQueue)
                print("Usuario incluido")

    if (opcao == 4):
        topicos = SNSConnection.get_all_topics()
        lstTopicos = topicos['ListTopicsResponse']['ListTopicsResult']['Topics']
        nomesDosTopicos = [t['TopicArn'].split(':')[5] for t in lstTopicos]
        print("Escolha o grupo onde vai publicar")
        for t in nomesDosTopicos:
            print("   " + t)

        topicName = str(raw_input(">> "))
        msgBody = raw_input("Digite a mensagem: ")
        msgDict = {"from": USER_ID,
                   "timestamp": time.strftime("%Y/%m/%d %H:%M:%S", time.gmtime()),
                   "body": str(msgBody)
                   }
        destArn = "arn:aws:sns:us-east-1:[redacted]:"+topicName
        status = SNSConnection.publish(topic=destArn,
                                       message=msgDict,
                                       message_structure="text")

SNSConnection.close()
SQSConnection.close()
