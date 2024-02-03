# Distributed Systems Group 14
# Wave Sensor System

import socket
import uuid
import random

import threading
import ipaddress

from threading import Thread

import json
import time
import argparse


from datetime import datetime

# Global Variables and Functions

# UUID for local peer (dynamically generated)
LOCAL_UUID = str(uuid.uuid4())

#List of Peers
peers = []

SUBNETMASK = "255.255.255.0"

HOST = socket.gethostname()

IP_ADDR = socket.gethostbyname(HOST)

BROADCAST_PORT = 59073      #  Port for Broadcast

TCP_UNICAST_PORT = 59074    #  Port for Unicast

#debug messages
def debug( message ):
    if True:
        # Get the current time
        current_time = datetime.now().time()

        # Format the time as HH:MM:SS
        formatted_time = current_time.strftime('%H:%M:%S')
        print(f"{formatted_time} {threading.current_thread().name}: {message}")

# return my own UUID (unique for each of the members)
def get_my_uuid ():
    return LOCAL_UUID

# return a random wave height between 0 and 10
def get_my_wave_height ():
    random_number = random.randint(0, 10)
    return random_number

# return the global array of peers
def get_peers ():
    return peers

def set_peers ( new_peers ):
    global peers
    peers = new_peers

def encodeMessage(message):

    return json.dumps(message).encode('utf-8')

def decodeMessage(message):

    return json.loads(message.decode('utf-8'))

def getBroadcastIP():

    # Implementation to get Broadcast-IP-Address

    networkaddress = ipaddress.IPv4Network(IP_ADDR + '/' + SUBNETMASK, False)

    return networkaddress.broadcast_address.exploded

# initiate the Voting process
def run_Voting_Process():

    time.sleep(1)
    sender = TCPSender()
    sender.send_Vote( get_my_uuid(), False, 0,  "Start Voting" )

# run the Voting process in the background
def start_Voting_Asynch():

    # Create a Thread object, passing the function to run
    my_thread = threading.Thread(target=run_Voting_Process)

    # Start the thread
    my_thread.start()


class BroadcastListener(Thread):

    # method of initialization
    def __init__(self):

        debug(" BroadcastListener init ...")

        Thread.__init__(self)

        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)        
        self.listen_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)        

        self.listen_socket.bind(('', BROADCAST_PORT))

    def run(self):

        debug(" BroadcastListener run  ...")

        while True:

            data, addr = self.listen_socket.recvfrom(1024)

            if data:

                #debug(" received data: " + str(data))

                message = decodeMessage(data)

                #debug(f"[BroadcastListener] Empfangene Nachricht von {addr}: {message}")

                self.handleMessage(message)



    def handleMessage(self, message):

        #debug(f"[BroadcastListener] Verarbeite Nachricht: {message}")

        # Implementation processing logic

        command = message["cmd"]
        peer_ip = message["ip"]
        peer_port = message["tcp_port"]
        peer_uuid = message["uuid"]

        if command == "NEW_SENSOR":

            # debug(f"Neuer Teilnehmer: {peer_ip}, UUID: {peer_uuid}")

            # Add the new peer to List, if not already there
            peers = get_peers()

            if not any(peer["uuid"] == peer_uuid for peer in peers):

                peers.append({"uuid": peer_uuid, "ip": peer_ip, "tcp_port": peer_port })
                debug(f"Updated Peers-List: {peers}")
                if len (peers) >= 3:
                    start_Voting_Asynch()
        elif command == "LOST_SENSOR":
            debug(f"[BroadcastListener] LOST_SENSOR Nachricht: {message}")

            # Remove Peer from List and update the list
            peers = get_peers()
            new_peers = [peer for peer in peers if peer["uuid"] != peer_uuid]
            set_peers( new_peers )
            debug(f"Updated Peers-List: {get_peers()}")
            number_of_peers = len(peers)
            if number_of_peers >= 3:
                start_Voting_Asynch()
            else:
                debug(f"Need 3 or more Peers to elect the Leader. Only {number_of_peers} peers are active.")
        else:
            debug(f"ERROR: unknown command {message}")

class BroadcastSender(Thread):

    def __init__(self):

        Thread.__init__(self)

        self.bcip = getBroadcastIP()

        self.bcport = BROADCAST_PORT

        self.UUID = get_my_uuid()

        debug(" BroadcastSender initialized with " + self.bcip + "  and uuid:" + self.UUID)

    def run(self):

        debug(" BroadcastSender running ...")

        while True:
            time.sleep(3)
            self.sendBroadcast("NEW_SENSOR", self.UUID, "Peer status update")


    def sendBroadcast(self, cmd, uuid, message ):

        broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)


        self.msg = {

            "cmd": cmd,
            "uuid": uuid,
            "ip": IP_ADDR,
            "tcp_port": TCP_UNICAST_PORT,
            "message": message
        }

        encoded_message = encodeMessage(self.msg)
        broadcast_socket.sendto(encoded_message, (self.bcip, self.bcport))

        #debug(f"[BroadcastSender] Gesendete Nachricht: {self.msg}")

#Generic TCP Listener for Heartbeat and exchange of Wave information


class TCPListener(threading.Thread):

    def __init__(self):

        threading.Thread.__init__(self)

        self.listen_port = TCP_UNICAST_PORT
        self.isLeader = False

        debug(" TCPListener listening on port: " + str(self.listen_port))

    def run(self):

        debug(" TCPListener running ...")

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('', self.listen_port))

            s.listen()

            while True:

                conn, addr = s.accept()
                with conn:
                    # receive data 
                    data = conn.recv(1024)

                    if data:
                        self.handle_message(conn, addr, data)

    # Handling the TCP messages, 
    # - acknowledge HEARTBEAT messages and initiate the Voting process

    def handle_message(self, conn, addr , data):

        msg =  decodeMessage( data )

        command = msg["cmd"] 
        peer_ip = msg["ip"]
        peer_port = msg["tcp_port"]
        peer_uuid = msg["uuid"]

        # debug(f"Received {command} from {addr} ")
        # debug( "msg:" + str(msg))

        if (command == "HEARTBEAT"):
            # Acknowledgement to let the hearbeat sender know, that the peer is still Ok
            msg_OK = {
                "cmd": "OK",
                "uuid": get_my_uuid(),
                "ip": IP_ADDR,
                "tcp_port": TCP_UNICAST_PORT,
                "message": "all fine with me."
            }
            conn.sendall(encodeMessage(msg_OK))

        # React to an incoming voting message        
        # The LCR Voting algorithm
        #
        elif (command == 'VOTE'):
            #debug( "Voting msg:" + str(msg))
            isLeaderElected = msg["leaderElected"]

            wave_height = msg["wave_height"] # get_wave_height( msg["wave_height"] )
            number_of_peers = len(peers)
            if number_of_peers < 3:
                debug(f"Need 3 or more Peers to elect the Leader. Only {number_of_peers} peers are active.")
            else:
                if isLeaderElected:
                    if peer_uuid != get_my_uuid():
                        self.isLeader  = False 
                        debug("Leader is elected but it's not me:" + peer_uuid)       
                        sender = TCPSender()

                        my_wave_height = get_my_wave_height()
                        new_height = wave_height + my_wave_height
                        debug( "wave_height: " + str(wave_height) + " my wave heigt:" + str(my_wave_height))
                        sender.send_Vote( peer_uuid, True, new_height, "Forward incoming Vote" )
                    else:
                        # My leader declararation passed through the ring and calculated the sum of all wave heights           
                        debug("*** I am the leader: " + peer_uuid)
                        avg_wave_height = wave_height / number_of_peers
                        debug(f"All waves height summary:  {wave_height} with {number_of_peers} participants --> AVG: {avg_wave_height}")
                        self.isLeader = True
                else:
                    if peer_uuid == get_my_uuid():
                        my_wave_height = get_my_wave_height()
                        debug("I am leader, wave height: " + str(my_wave_height))
                        sender = TCPSender()
                        sender.send_Vote( get_my_uuid(), True, my_wave_height, "Leader elected" )
                    elif peer_uuid > get_my_uuid():
                        #debug( "Forward incoming Vote:" + str(msg))
                        sender = TCPSender()
                        sender.send_Vote( peer_uuid, False, 0, "Forward incoming Vote" )
                    elif peer_uuid < get_my_uuid():
                        #debug("discard this Vote because its lesser than own UUID")
                        pass
                    else:
                        print("ERROR: Should not get into this")
                        pass


# TCP Sender with TCP Unicast for Heartbeat and Voting
class TCPSender(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.running = False

    def find_neighbors(self):

        # Sort based on the uuid)
        sorted_uuids = sorted(get_peers(), key=lambda x: x["uuid"])

        #debug(" sorted_uuids " + str(sorted_uuids))

        # Find the index based on the own uuid   (index 0)
        my_uuid = get_my_uuid()

        index = None
        for i, element in enumerate(sorted_uuids):
            if element["uuid"] == my_uuid:
                index = i
                break

        left_neighbor = None
        right_neighbor = None

        if index is not None:
            #debug(f"Index of '{my_uuid}': {index}")

            left_index = (index - 1) % len(sorted_uuids)
            right_index = (index + 1) % len(sorted_uuids)

            # Left and right neighbors must be different than self, otherwise no heartbeat for self needed
            if left_index != index:
                left_neighbor = sorted_uuids[left_index]
            if right_index != index:
                right_neighbor = sorted_uuids[right_index]
            #debug(f"left_neighbor: '{left_neighbor}'  right_neighbor: {right_neighbor}")
        else:
            # this should not happen
            print(f"ERROR: '{my_uuid}' not found in the list.")
        return (left_neighbor, right_neighbor)

    # sending heartbeats to all neighbors 
    def run(self):

        debug("Running TCP Sender for Heartbeats ...")

        self.running = True

        while self.running:  

            neighbors = self.find_neighbors()

            for peer in neighbors:
                if peer != None:
                    self.send_heartbeat( peer )
            time.sleep(1)


    # start the Voting  
    def send_Vote( self, uuid, leader_elected, wave_height, message ):

        msg =  {
            "cmd": "VOTE",
            "uuid": uuid,
            "leaderElected": leader_elected,
            "wave_height": wave_height,
            "ip": IP_ADDR,
            "tcp_port": TCP_UNICAST_PORT,
            "msg": message
        }

        #debug("send_Vote: " + str(msg))

        left_neighbor, right_neighbor = self.find_neighbors()
        peer = left_neighbor

        #debug("left_neighbor: " + str(peer))

        if ( left_neighbor != None ):
            try:

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    # setting timeout on the socket to get a faster error response at connect
                    s.settimeout(1)
                    s.connect((left_neighbor["ip"], left_neighbor["tcp_port"]))  # connect to TCP IP and Port of the Peer
                    s.settimeout(None)

                    s.sendall(encodeMessage( msg ))

                    # Close the socket when done
                    s.close()

            except Exception as e:
                debug(f"[send_Vote] ERROR: Fehler beim Senden / Empfangen des Votings: {e}")
        else:
                debug(f"send_Vote: No left neighbor")

    def send_heartbeat(self, peer ):


        self.msg = {
            "cmd": "HEARTBEAT",
            "uuid": get_my_uuid(),
            "ip": IP_ADDR,
            "tcp_port": TCP_UNICAST_PORT,
            "message": "check availability"
        }

        try:

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:

                # setting timeout on the socket to get a faster error response at connect
                s.settimeout(1)

                s.connect((peer["ip"], peer["tcp_port"]))  # connect to TCP IP and Port of the Peer
                s.settimeout(None)

                s.sendall(encodeMessage( self.msg ))

                #debug(f"[send_heartbeat]  an " + str( self.msg) )

                data = s.recv(1024)
                if not data:
                    debug(f"[send_heartbeat] ERROR did not receive and data back !! " + str( self.msg) )
                    bsender = BroadcastSender()
                    bsender.sendBroadcast("LOST_SENSOR", peer["uuid"], "Lost Peer")
                else:
                    msg =  decodeMessage( data )
                    command = msg["cmd"] 
                    if command != "OK":
                        print(f"ERROR: Received wrong  response '{command}', expecting 'OK' for Heartbeat {msg}")
                # Close the socket when done
                s.close()

        except Exception as e:
            debug(f"[send_heartbeat] ERROR: Fehler beim Senden / Empfangen des Heartbeats: {e}")
            # If no response from peer, then update peers list & Send UDP Broadcast to all sensors
            bsender = BroadcastSender()
            bsender.sendBroadcast("LOST_SENSOR", peer["uuid"], "Lost Peer")

    def stop_heartbeat(self):
        self.running = False


def main():
    # Create an argument parser
    parser = argparse.ArgumentParser(description='Command-line argument parser')

    # Add an optional command-line argument for TCP_PORT
    parser.add_argument('--TCP_PORT', type=int, help='TCP port number', required=False)

    # Parse the command-line arguments
    args = parser.parse_args()

    # Access the value of TCP_PORT
    tcp_port = args.TCP_PORT

    # Check if TCP_PORT is provided
    if tcp_port is not None:
        debug(f'TCP_PORT specified: {tcp_port}')
        global TCP_UNICAST_PORT
        TCP_UNICAST_PORT = tcp_port

# Main thread to initialize the message listeners, start the dicsovery and the game afterwards

if __name__ == '__main__':

    try:

        debug("Starting main  ...")

        # parsing commandline
        main()

        blistener = BroadcastListener( )
        blistener.start()

        # Start of TCP Listener and HeartbeatSender
        listener = TCPListener()
        listener.start()

        time.sleep(0.5)

        # Create a TCP Sender, which is used to send regular heartbeats
        sender = TCPSender()

        # broadcast an alle -> new Peer
        bsender = BroadcastSender()
        bsender.sendBroadcast("NEW_SENSOR", get_my_uuid(), "New Peer is joining")

        # broadcasting regularly to all members
        bsender.start() 

        # start the Heartbeat process
        sender.start()

    except KeyboardInterrupt:

        sys.exit(0)



