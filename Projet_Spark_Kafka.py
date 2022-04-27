#!/usr/bin/env python3

import sys
import threading
import json
import re

from kafka import KafkaProducer, KafkaConsumer

should_quit = False
## Consumer : permet de lire le message
def read_messages(consumer):
    # Partie ou on va configurer le consumer

    ## A quoi sert le should_quit ???
    while not should_quit:
        # On utilise poll pour ne pas bloquer indéfiniment quand should_quit
        # devient True
        received = consumer.poll(100)

        ## Dans un channel spécifique, on récupère les messages qui sont reçues
        for channel, messages in received.items():
            for msg in messages:
                msg = deserializer(msg.value.decode())
                if msg["message"] == "rtfs":
                    message = msg["name"]+" has join the channel"
                    print(message)
                elif msg["message"] == "rgbd":
                    message = msg["name"]+" has left the channel"
                    print(message)
                elif msg["message"] == "tbgd":
                    message = msg["name"]+" left the channel"
                    print(message)
                else:
                    message = msg["name"] + " : " + msg["message"]
                    print(message)


def cmd_msg(producer, channel, line, nick):
    # Channel null : pas de channel déjà rejoint
    if channel == None :
        print("Pas de channel rejoint")
    else:
        chan = "chat_channel_"+channel

        admin = "chat_admin"

        dict = { "name" : nick,
                 "message" : line}

        producer.send(chan, serializer(dict))
        producer.send(admin, serializer(dict))


def cmd_join(consumer, producer, line, nick):

    reg_line = re.compile(r'^#[a-zA-Z0-9_-]+$')
    if reg_line.match(line):
        channel = "chat_channel_"+line[1:]
        chanExist = consumer.subscription()
        if chanExist == None:
            consumer.subscribe(channel)
        else:
            chanExist.add(channel)
            consumer.subscribe(chanExist)
        dict = {"name": nick,
                "message" : "rtfs"}

        producer.send(channel, serializer(dict))
        line = line[1:]
        print("vous avez rejoint le channel")
        return line
    else:
        print("Nom du channel invalide")




def cmd_part(consumer, producer, line, nick):

    #
    reg_line = re.compile(r'^#[a-zA-Z0-9_-]+$')
    if reg_line.match(line):

        channel = "chat_channel_"+line[1:]
        dict = {"name": nick,
                "message": "rgbd"}
        producer.send(channel, serializer(dict))

        if channel in consumer.subscription():

            sub = consumer.subscription()
            sub.remove(channel)
            consumer.unsubscribe()
            if len(sub) != 0:
                consumer.subscribe(sub)
                # Je choisis au hasard un channel avec pop dans les subscriptions restantes
                # Je le réajoute dans les subscriptions
                # Pas opti : situation ou je quitte un channel alors que je suis dans un autre
                chanRand = consumer.subscription().pop()
                sub = consumer.subscription()
                sub.add(chanRand)
                # Je récupère le channel pour permettre d'avoir le changement
                line = chanRand[13:]

                return line
            else:
                print("Vous n'avez plus de channel auquel vous êtes abonnés")
        else:
            print("Vous n'avez pas ce channel dans vos abonnements")
    else:
        print("Le nom du channel est invalide")



def cmd_quit(producer, consumer, line, nick):
    chan = consumer.subscription()
    for channel in chan:
        dict = { "name" : nick,
                 "message" : "tbgd"}
        producer.send(channel, serializer(dict))
    pass


def cmd_active(consumer, line):
    reg_line = re.compile(r'^#[a-zA-Z0-9_-]+$')  # Relire le TP pour cette partie
    if reg_line.match(line):
        channel = "chat_channel_" + line[1:]
        sub = consumer.subscription()
        i = 0
        for chan in sub:
            if chan == channel:
                chan = chan[13:]
                i = i + 1
                return chan
        if i == 0:
            print("Vous n'avez pas rejoint ce channel")
            x,y = 0, 1
            for chan in sub:
                x = x + 1
                if y == x:
                    chan = chan[13:]
                    return chan

    else:
        print("Le channel spécifié ne respecte pas les règles du serveur")



def serializer(data):
    return json.dumps(data).encode("utf-8")

def deserializer(data):
    return json.loads(data)



def main_loop(nick, consumer, producer):
    curchan = None

    while True:
        try:
            if curchan is None:
                line = input("> ")
            else:
                line = input("[#%s]>" % curchan)
        except EOFError:
            print("/quit")
            line = "/quit"

        if line.startswith("/"):
            cmd, *args = line[1:].split(" ", maxsplit=1)
            cmd = cmd.lower()
            args = None if args == [] else args[0]
        else:
            cmd = "msg"
            args = line

        if cmd == "msg":
            cmd_msg(producer, curchan, args, nick)
        elif cmd == "join":
            curchan = cmd_join(consumer, producer, args, nick)
        elif cmd == "part":
            curchan = cmd_part(consumer, producer, args, nick)
        elif cmd == "active":
            curchan = cmd_active(consumer, args)
        elif cmd == "quit":
            cmd_quit(producer, consumer, args, nick)

            break
        # TODO: rajouter des commandes ici



def main():
    if len(sys.argv) != 2:
        print("usage: %s nick" % sys.argv[0])
        return 1

    nick = sys.argv[1]
    consumer = KafkaConsumer(bootstrap_servers="localhost:9092", group_id=nick)
    producer = KafkaProducer(bootstrap_servers="localhost:9092")
    th = threading.Thread(target=read_messages, args=(consumer,))
    th.start()


    try:
        main_loop(nick, consumer, producer)
    finally:
        global should_quit
        should_quit = True
        th.join()



if __name__ == "__main__":
    sys.exit(main())
