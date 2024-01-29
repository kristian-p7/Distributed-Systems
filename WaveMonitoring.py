#Neustart

import socket
import uuid
import threading
import ipaddress
from threading import Thread
import json
import time


# UUID für den lokalen Peer (wird dynamisch generiert)
LOCAL_UUID = str(uuid.uuid4())

#List of Peers Tupel(ip, uuid)
peers = []

SUBNETMASK = "255.255.255.0"
HOST = socket.gethostname()
IP_ADDR = socket.gethostbyname(HOST)
BROADCAST_PORT = 59073      #  Port für den Broadcast
TCP_UNICAST_PORT = 59074    #  Port für den Unicast

def debug( message ):
    if True:
        print(f"{threading.current_thread().name}: {message}")

#Liste einfügen mit UUID Zuweisung

# Hilfsfunktionen und globale Variablen
def encodeMessage(message):
    return json.dumps(message).encode('utf-8')

def decodeMessage(message):
    return json.loads(message.decode('utf-8'))

def getBroadcastIP():
    # Implementieren Sie hier die Logik, um die Broadcast-IP-Adresse zu erhalten
    networkaddress = ipaddress.IPv4Network(IP_ADDR + '/' + SUBNETMASK, False)
    return networkaddress.broadcast_address.exploded



class BroadcastListener(Thread):
    # ... [Unveränderte Initialisierungsmethode]
    def __init__(self):
        debug(" BroadcastListener init ...")
        Thread.__init__(self)
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.listen_socket.bind(('', BROADCAST_PORT))

    def run(self):
        debug(" BroadcastListener run  ...")
        while True:
            data, addr = self.listen_socket.recvfrom(1024)
            if data:
                message = decodeMessage(data)
                # debug(f"[BroadcastListener] Empfangene Nachricht von {addr}: {message}")
                self.handleMessage(message)

    def handleMessage(self, message):
        # debug(f"[BroadcastListener] Verarbeite Nachricht: {message}")
        # Implementieren Sie die Verarbeitungslogik hier
        if message["cmd"] == "NEW_SENSOR":

            #Neuer Peer in Liste
            new_peer_ip = message["ip"]
            new_peer_uuid = message["uuid"]

            # Hinzufügen des neuen Peers zur Liste, falls nicht bereits vorhanden
            if not any(peer["uuid"] == new_peer_uuid for peer in peers):
                peers.append({"uuid": new_peer_uuid, "ip": new_peer_ip})
                debug(f"Neuer Teilnehmer: {new_peer_ip}, UUID: {new_peer_uuid}")
                debug(f"Aktualisierte Peers-Liste: {peers}")

        if message["cmd"] == "LOST_SENSOR":

            # Neuer Peer in Liste
            lost_peer_uuid = message["uuid"]

            peers = [peer for peer in peers if peer["uuid"] != uuid]
            debug(f"Aktualisierte Peers-Liste: {peers}")




class BroadcastSender():
    def __init__(self):
        debug(" BroadcastSender init started")

        self.bcip = getBroadcastIP()
        self.bcport = BROADCAST_PORT
        self.UUID = str(uuid.uuid4())
        debug(" BroadcastSender initialized with " + self.bcip + "  and uuid:" + self.UUID)



    def sendBroadcast(self, command, message):
        broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        self.msg = {
            "cmd": command,
            "uuid": str(self.UUID),
            "ip": IP_ADDR,
            "message": message

        }

        encoded_message = encodeMessage(self.msg)

        broadcast_socket.sendto(encoded_message, (self.bcip, self.bcport))
        # debug(f"[BroadcastSender] Gesendete Nachricht: {self.msg}")


#Hearthbeat Listener

class HeartbeatListener(threading.Thread):
    def __init__(self, listen_port):
        debug(" HearthbeatListener init started...")
        threading.Thread.__init__(self)
        self.listen_port = listen_port

    def run(self):
        debug(" Hearthbeat Listener started...")
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', self.listen_port))
            s.listen()
            while True:
                conn, addr = s.accept()
                with conn:
                    data = conn.recv(1024)
                    if data:
                        self.handle_heartbeat(data, addr)

    def handle_heartbeat(self, data, addr):
        debug(f"Received heartbeat from {addr}: {data.decode()}")

# Hier könnte der Code für das Starten des HeartbeatListener hinzugefügt werden


#HearthbeatSender with TCP Unicast

class HeartbeatSender(threading.Thread):

    def __init__(self):
        debug(" HearthbeatSender init started...")
        threading.Thread.__init__(self)
        self.running = False

    def find_neighbors(self):
        debug(" FindNeighbors started")
        sorted_uuids = sorted(peers.keys())
        my_index = sorted_uuids.index(self.local_uuid)
        left_neighbor = sorted_uuids[(my_index - 1) % len(sorted_uuids)]
        right_neighbor = sorted_uuids[(my_index + 1) % len(sorted_uuids)]
        return (left_neighbor, right_neighbor)

    def run(self):
        debug(" Hearthbeat gesendet...")
        self.running = True
        while self.running and len(peers) >= 3:
            neighbors = self.find_neighbors()
            for uuid in self.neighbors:
                self.send_heartbeat(peers[uuid])
            time.sleep(5)

    def send_heartbeat(self, ip_address):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((ip_address, 5000))  # Port 5000 angenommen
                s.sendall(encodeMessage({"cmd": "heartbeat", "uuid": LOCAL_UUID}))
                debug(f"[HeartbeatSender] Heartbeat gesendet an {ip_address}")

                # If no response from Neighbor, then update peers list & Send UDP Broadcast to all sensors
                bsender = BroadcastSender()
                bsender.sendBroadcast("LOST_SENSOR", "Lost Peer")



        except Exception as e:
            debug(f"[HeartbeatSender] Fehler beim Senden des Heartbeats: {e}")


    def start_heartbeat(self):
        self.running = True
        self.start()

    def stop_heartbeat(self):
        self.running = False

        #Wenn ein Peer im hearthbeat nicht mehr erreichbar ist, muss er aus der Liste geworfen werden


# Main thread to initialize the message listeners, start the dicsovery and the game afterwards
if __name__ == '__main__':
    try:

        debug("Starting main  ...")


        blistener = BroadcastListener()
        blistener.start()

        debug(" sleeping 2 seconds ..")
        time.sleep(2)

        debug("-"*30)

        bsender = BroadcastSender()

        while True:
            bsender.sendBroadcast("NEW_SENSOR", "New Peer joining")
            time.sleep(5)

        # Abrufen der Liste der Peers

        # Starten des HeartbeatListeners
        listener = HeartbeatListener(TCP_UNICAST_PORT)
        listener.start()
        debug("[Main] HeartbeatListener gestartet")

        # Starten des HeartbeatSenders, wenn mindestens 3 Peers vorhanden sind
        if len(peers) >= 3:
            sender = HeartbeatSender()
            sender.start()
            debug("[Main] HeartbeatSender gestartet")
        else:
            debug("[Main] Nicht genügend Peers für Heartbeat vorhanden")

    except KeyboardInterrupt:
        sys.exit(0)