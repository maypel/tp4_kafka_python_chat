#!/usr/bin/env python3

import sys
import threading
import re
import json

from kafka import KafkaProducer, KafkaConsumer

should_quit = False

def serializer(message):
    return json.dumps(message).encode('utf-8')

def deserializer(message):
    return json.loads(message)

def read_messages(consumer, *args):
    # TODO À compléter
    
    while not should_quit:
        
        # On utilise poll pour ne pas bloquer indéfiniment quand should_quit
        # devient True
        received = consumer.poll(100)
        

        for channel, messages in received.items():
            for msg in messages:
                message = deserializer(msg.value.decode()) #dictionnaire obtenu
                print("< #%s: %s" % (channel, message))
            

def cmd_msg(producer, channel, line, nick):
    # TODO À compléter
    
    
    if channel == None:
        print("veuillez joindre un canal")
    else:
        channel = "chat_channel_" + channel[1:]
        message = serializer(line) #+ nick
        producer.send(channel, message)
    
  
def cmd_join(consumer, producer, line):
    
    match_line = re.match(r"^#[a-zA-Z0-9_-]+$", line)
    
    if match_line:
        # attention subscribe n'est pas incrménetale
        # créer une liste des souscriptions
        # y ajouter la dernière
        list_subscription = consumer.subscription()
        chan = "chat_channel_" + line[1:]

        if list_subscription == None:
            list_subscription = set()
        list_subscription.add(chan)
        consumer.subscribe(list_subscription)
        print(f"vous avez rejoint : {chan}")

        return line
    else:
        print("Veuillez rentrer un nom de canal valide (commence par #, pas de caractères spéciaux excepté - et _")
        
def cmd_part(consumer, producer, line):
    # TODO À compléter
    # on récupère le set de nos abonnements
    channel_exist = consumer.subscription()
    chan = "chat_channel_"+line[1:]

    try:
        channel_exist.remove(chan)
        if len(channel_exist) == 0:
            consumer.unsubscribe()

        else:
            consumer.subscribe(channel_exist)

        return channel_exist
    except Exception as e:
        print(e)
        print('Something went wrong try again')
    

def cmd_quit(producer, line):
    # TODO À compléter
    pass

def change_curchan(curchan):
    print(curchan)
    

def main_loop(nick, consumer, producer):
    curchan = None
    

    while True:
        try:
            if curchan is None:
                line = input("> ")
            else:
                line = input("[%s]>" % curchan)
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
            curchan = cmd_join(consumer, producer, args)

        elif cmd == "part":
            cmd_part(consumer, producer, args)
            curchan = change_curchan(curchan)
            print('la', curchan)
           # print(consumer, producer, args)

        elif cmd == "quit":
            cmd_quit(producer, args)
            break
        # TODO: rajouter des commandes ici
        # read_messages(consumer, *args)


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
