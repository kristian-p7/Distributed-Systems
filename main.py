#Distributed Systems Environmental Monitoring


import socket
import threading
import time
import random
import os

debug = True

BROADCAST_IP = "192.168.2.255"

#UDP Broadcast sender
def broadcast(ip, port, broadcast_message):
    # Create a UDP socket
    broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Send message on broadcast address
    broadcast_socket.sendto(str.encode(broadcast_message), (ip, port))
    broadcast_socket.close()

def debug( message ):
    if debug:
        print(f"{threading.current_thread().name}: {message}")

'''
if __name__ == '__main__':
    # Broadcast address and port, subnet
    BROADCAST_IP = "192.168.0.255"
    BROADCAST_PORT = 5973

    # Local host information
    MY_HOST = socket.gethostname()
    MY_IP = socket.gethostbyname(MY_HOST)

    # Send broadcast message
    message = MY_IP + ' sent a broadcast'
    broadcast(BROADCAST_IP, BROADCAST_PORT, message)

#UDP Broadcast listener

if __name__ == '__main__':
    # Listening port
    BROADCAST_PORT = 5973

    # Local host information
    MY_HOST = socket.gethostname()
    MY_IP = socket.gethostbyname(MY_HOST)

    # Create a UDP socket
    listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Set the socket to broadcast and enable reusing addresses
    listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    # Bind socket to address and port
    listen_socket.bind((MY_IP, BROADCAST_PORT))

    print("Listening to broadcast messages")

    while True:
        data, addr = listen_socket.recvfrom(1024)
        if data:
            print("Received broadcast message:", data.decode())
            print("xxx")

# Dynamische Peerverwaltung
'''

class Peer:
    def __init__(self, broadcast_port, listen_port):
        # Initialisierung der Ports und Peers-Liste
        self.broadcast_port = broadcast_port
        self.listen_port = listen_port
        self.peers = set()  # Verwendet ein Set, um Duplikate zu vermeiden
        self.last_wave_height = 0  # Speichert die letzte Wellenhöhe
        self.wave_heights = {}  # Hinzugefügt: Speichert die Wellenhöhen der anderen Peers
        self.running = True

        # Erstellen des Broadcast-Sockets
        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Erstellen des Listening-Sockets
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listen_socket.bind(('', listen_port))
        self.listen_socket.settimeout(5)  # Timeout, um das Blockieren zu verhindern

    def broadcast(self):
        # Senden von Broadcast-Nachrichten
        debug( "broadcast started")
        while self.running:
            try:
                message = f'peer:{socket.gethostbyname(socket.gethostname())}'
                debug("broadcasting " + message + " to " + BROADCAST_IP + ":"+str (self.broadcast_port) )
                self.broadcast_socket.sendto(message.encode(), (BROADCAST_IP, self.broadcast_port))
                debug("sleeping for 5 seconds")

                time.sleep(5)  # Broadcast alle 5 Sekunden
            except Exception as e:
                print(f"Broadcast error: {e}")

    def listen(self):
        debug( "listen started")

        # Lauschen auf eingehende Nachrichten
        while self.running:
            try:
                data, addr = self.listen_socket.recvfrom(1024)
                if data:
                    message = data.decode()
                    debug("received message" + message)
                    #Erweitern der peer Klasse mit Beitrittsnachricht
                    if message.startswith("peer:"):
                        peer_ip = message.split(':')[1]
                        if peer_ip not in self.peers:
                            debug("Hello Sensor, welcome to the WaveMonitoringSystem!")
                        self.peers.add(peer_ip)
                        self.peers.discard(socket.gethostbyname(socket.gethostname()))  # Eigene IP entfernen
                        debug(f"Current number of sensors: {len(self.peers)}")
            except socket.timeout:
                continue
            except Exception as e:
                print(f"Listen error: {e}")

    def check_peers(self):
        debug( "check_peers started")

        # Überprüft regelmäßig die Verfügbarkeit der Peers
        while self.running:
            for peer in list(self.peers):
                if not self.is_peer_alive(peer):
                    self.peers.discard(peer)
                    # Entfernt die Wellenhöhe des nicht mehr erreichbaren Peers
                    self.wave_heights.pop(peer, None)
            time.sleep(10)  # Prüft alle 10 Sekunden

    def is_peer_alive(self, peer_ip):
        # Überprüft, ob ein Peer erreichbar ist
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(2)
                s.connect((peer_ip, self.listen_port))
                s.sendall(b'check')
                # Warten auf eine Antwort, um sicherzustellen, dass der Peer funktionsfähig ist
                s.recv(1024)
                return True
        except:
            return False

    def send_wave_height(self):
        while self.running:
            for peer in self.peers:
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((peer, self.listen_port))
                        s.sendall(f'wave_height:{self.last_wave_height}'.encode())
                except:
                    pass
            time.sleep(5)

    def elect_leader(self):
            # Ermittelt den Peer mit der niedrigsten IP-Adresse als Leader
            if self.peers:
                return min(self.peers)
            return None

    def generate_wave_height(self):
        debug("generate_wave_height started")
        # Generiert zufällige Wellenhöhen und speichert sie
        while self.running:
            self.last_wave_height = random.uniform(0, 10)  # Zufällige Wellenhöhe zwischen 0 und 10
            debug("generated wave_height: " + str(self.last_wave_height ))
            time.sleep(5)  # Generiert alle 5 Sekunden eine neue Wellenhöhe

    def check_for_high_waves(self, peers_wave_heights):
        # Verändert: Verwendet die gespeicherten Wellenhöhen
        all_wave_heights = list(self.wave_heights.values()) + [self.last_wave_height]
        high_waves = [height for height in all_wave_heights if height >= 8]
        if len(high_waves) >= 2:
            avg_height = sum(all_wave_heights) / len(all_wave_heights)
            print(f"WARNING! High Waves Detected: {avg_height:.2f} meters")

    # Hinzugefügt: Methode zum Empfangen und Speichern der Wellenhöhen von anderen Peers
    def receive_wave_heights(self):
        while self.running:
            try:
                data, addr = self.listen_socket.recvfrom(1024)
                if data:
                    message = data.decode()
                    if message.startswith("peer:"):
                        # ... [Unveränderte Logik für Peer-Erkennung]
                        print("Kein Plan")
                    elif message.startswith("wave_height:"):
                        height = float(message.split(':')[1])
                        self.wave_heights[addr[0]] = height
            except socket.timeout:
                continue
            except Exception as e:
                print(f"Error receiving wave heights: {e}")

    def get_peers(self):
        # Gibt die Liste der Peers zurück
        return self.peers

    def stop(self):
        # Stoppt den Peer und schließt Sockets
        self.running = False
        self.broadcast_socket.close()
        self.listen_socket.close()



#Hearthbeat und bzw. Fault Tolerance
    def heartbeat(self):
        debug("heartbeat started")
        # Sendet regelmäßig Heartbeats an den nächsten Peer
        while self.running:
            try:
                # Bestimmen des nächsten Peers im Ring
                sorted_peers = sorted(list(self.peers))

                debug(sorted_peers)
                my_index = sorted_peers.index(socket.gethostbyname(socket.gethostname()))

                next_peer_index = (my_index + 1) % len(sorted_peers)
                next_peer = sorted_peers[next_peer_index]

                # Aufbau einer TCP-Verbindung zum nächsten Peer
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((next_peer, self.listen_port))
                    s.sendall(b'heartbeat')
                    time.sleep(2)  # Sendet alle 2 Sekunden einen Heartbeat
            except Exception as e:
                debug(f"Heartbeat error: {e}")
                time.sleep(5)  # Warte 5 Sekunden, falls ein Fehler auftritt





# Hauptprogramm
if __name__ == "__main__":
    peer = Peer(broadcast_port=5973, listen_port=5974)

    if debug: print( peer )

    threading.Thread(target=peer.broadcast).start()
    threading.Thread(target=peer.listen).start()
    threading.Thread(target=peer.heartbeat).start()
    threading.Thread(target=peer.generate_wave_height).start()
    threading.Thread(target=peer.send_wave_height).start()
    threading.Thread(target=peer.receive_wave_heights).start()

    leader = None
    system_started = False
    try:
        while True:
            time.sleep(10)
            peers = peer.get_peers()
            if len(peers) < 3:
                system_started = False
                debug("Not enough peers, waiting for more peers...")
                continue

            if not system_started and len(peers) >= 3:
                new_leader = peer.elect_leader()
                if new_leader != leader:
                    leader = new_leader
                    debug(f"New leader elected: {leader}")
                print("Active peers:", peers)

                # Leader fragt, ob das System gestartet werden soll
                if leader == socket.gethostbyname(socket.gethostname()):
                    start_system = input("Do you want to start the WaveMonitoringSystem? (yes/no): ")
                    if start_system.lower() == "yes":
                        system_started = True
                        print("Starting the WaveMonitoringSystem...")

            elif system_started:
                # Sammeln der Wellenhöhen von allen Sensoren und Durchschnittsberechnung
                peers_wave_heights = [peer.last_wave_height] + [random.uniform(0, 10) for _ in peers]
                peer.check_for_high_waves(peers_wave_heights)
    except KeyboardInterrupt:
        peer.stop()