import logging
import threading

import time
from node_socket import UdpSocket
import math


def thread_exception_handler(args):
    logging.error(f"Uncaught exception", exc_info=(
        args.exc_type, args.exc_value, args.exc_traceback))


FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"
TERM = 1
MSG = 0
VOTE_REQUEST = "Vote Request"
VOTE_RESPONSE = "Vote Response"
LOG_REQUEST = "Log Request"


class Raft:

    def __init__(self, node_id: int, port: int, neighbors_ports: list, lb_fault_duration: int, is_continue: bool,
                 heartbeat_duration: float):
        self.heartbeat_duration = heartbeat_duration
        self.is_continue = is_continue
        self.lb_fault_duration = lb_fault_duration
        self.neighbors_ports = neighbors_ports
        self.port = port
        self.node_id = node_id
        
        # TODO: Will be changed to real DB later
        self.db = []

    def start(self):

        logging.info("Start Raft Algorithm")

        # Initializing node and socket
        self.initialization()
        global my_socket
        my_socket = UdpSocket(self.port)

        # Initializing listening_procedure
        thread = threading.Thread(
            target=self.listening_procedure, args=())
        thread.name = "listening_procedure_thread"
        thread.start()

        # Initializing leader detector
        thread = threading.Thread(
            target=self.detect_leader_alive, args=())
        thread.name = "detect_leader_alive"
        thread.start()

        # Initializing heartbeat sender
        thread = threading.Thread(
            target=self.leader_sending_heartbeat, args=())
        thread.name = "heartbeat"
        thread.start()

    def listening_procedure(self):

        while True:
            message, address = my_socket.listen()
            message_evaluated = eval(message)

            message_type = message_evaluated[0]
            if message_type == VOTE_REQUEST:
                c_id = message_evaluated[1]
                logging.info(f"Get vote request from {c_id}")
                c_term = message_evaluated[2]
                c_log_length = message_evaluated[3]
                c_log_term = message_evaluated[4]
                self.voting_new_leader(c_id, c_term, c_log_length, c_log_term)

            elif message_type == VOTE_RESPONSE:
                voter_id = message_evaluated[1]
                term = message_evaluated[2]
                granted = message_evaluated[3]
                logging.info(
                    f"Receive vote response from {voter_id}, granted = {granted}")
                self.collecting_votes(voter_id, term, granted)

            elif message_type == LOG_REQUEST:

                # TIMER
                self.leader_is_alive = True
                self.message_received_from_leader = self.message_received_from_leader + 1

                leader_id = message_evaluated[1]
                term = message_evaluated[2]
                prefix_len = message_evaluated[3]
                prefix_term = message_evaluated[4]
                leader_commit = message_evaluated[5]
                suffix = message_evaluated[6]
                flag = False
                if self.current_leader == None:
                    flag = True
                logging.info(
                    f"Receive log from leader with id {leader_id}, message: {message}")
                self.follower_receive_message(
                    leader_id, term, prefix_len, prefix_term, leader_commit, suffix)
                if flag:
                    self.ask_db()
                
                
            
            elif message_type == "USER_WRITE":
                # Receive data from user
                data = message_evaluated[1]
                self.send_db(data)
                print(f"DB for {self.node_id}: {self.db}")
            
            elif message_type == "USER_READ":
                # Receive data from user
                index = message_evaluated[1]
                data = self.db[index]
                print(f"Data from {self.node_id} in index {index}: {data}")
                
            elif message_type == "BROADCAST_DB":
                # Broadcasting method
                
                data = message_evaluated[1]
                leader_len_db = message_evaluated[2]
                self.db.append(data)
                print(f"DB for {self.node_id}: {self.db}")
            elif message_type == "FORWARD_TO_LEADER":
                # Leader receive forwarded message
                data = message_evaluated[1]
                self.send_db(data)
                print(f"DB for {self.node_id}: {self.db}")
            elif message_type == "ASK_DB":
                # After crash recovery
                len_db = message_evaluated[1]
                port = message_evaluated[2]
                # print(f"RECEIVE Request DB from {port}")
                self.complete_db(len_db, port)
            elif message_type == "COMPLETE_DB":
                # print("GOT DATA")
                data = message_evaluated[1]
                self.db.append(data)
                print(f"DB for {self.node_id}: {self.db}")
            self.write_file()

    def election_timer_procedure(self):
        time.sleep(self.lb_fault_duration)
        if self.stop_election_timer == False:
            self.suspects_leader_fail_or_election_timeout()

    def detect_leader_alive(self):
        while True:
            previous_total_message = self.message_received_from_leader
            time.sleep(self.lb_fault_duration)
            if (self.message_received_from_leader == previous_total_message) and self.current_role != LEADER:
                self.leader_is_alive = False

            if self.leader_is_alive == False:
                self.suspects_leader_fail_or_election_timeout()

    def leader_sending_heartbeat(self):
        while True:
            if self.current_role == LEADER:
                time.sleep(self.heartbeat_duration)
                self.broadcasting_message_periodically()

    def vote_request(self, node_id, current_term, log_length, last_term):
        message = str(
            [VOTE_REQUEST, node_id, current_term, log_length, last_term])
        return message

    def vote_response(self, node_id, current_term, granted):
        message = str([VOTE_RESPONSE, node_id, current_term, granted])
        return message

    def log_request(self, leader_id, current_term, prefix_len, prefix_term, commit_length, suffix):
        message = str([LOG_REQUEST, leader_id, current_term,
                      prefix_len, prefix_term, commit_length, suffix])
        return message

    def write_file(self):
        f = open(f"persistent/node{self.node_id}_current_term.txt", "w")
        f.write(f"{self.current_term}")
        f.close()
        f = open(f"persistent/node{self.node_id}_voted_for.txt", "w")
        f.write(f"{self.voted_for}")
        f.close()
        f = open(f"persistent/node{self.node_id}_db.txt", "w")
        f.write(f"{self.db}")
        f.close()

    def initialization(self):
        # Step 1/6
        try:
            # On recovery from crash
            logging.info("Recovery from crash")
            f = open(f"persistent/node{self.node_id}_current_term.txt", "r")
            current_term = int(f.read())
            f.close()

            f = open(f"persistent/node{self.node_id}_voted_for.txt", "r")
            voted_for = f.read()
            if voted_for == 'None':
                logging.info("NULL")
                voted_for = None
            f.close()
            
            f = open(f"persistent/node{self.node_id}_db.txt", "r")
            db = f.read()
            self.db = eval(db)
            f.close()
            
            

            self.current_term = current_term
            self.voted_for = voted_for
            self.log = []  # Message and term
            self.commit_length = 0
        except:
            logging.info("Starting new node")
            self.current_term = 0
            self.voted_for = None
            self.log = []  # Message and term
            self.commit_length = 0

        self.write_file()

        self.current_role = FOLLOWER
        self.current_leader = None
        self.votes_received = set()
        self.sent_length = {}
        self.acked_length = {}
        for port in self.neighbors_ports:
            # Using port instead of node_id as key
            self.sent_length[port] = 0
            self.acked_length[port] = 0

        # Extra variables
        # Dict for translating node id into port
        self.node_port_dict = {}
        for i in range(len(self.neighbors_ports)):
            self.node_port_dict[i+1] = self.neighbors_ports[i]

        self.leader_is_alive = False
        self.message_received_from_leader = 0
        self.stop_election_timer = False

    def suspects_leader_fail_or_election_timeout(self):
        # Step 1/6
        logging.info(f"Node{self.node_id} suspects leader fail")
        self.current_term = self.current_term + 1
        self.current_role = CANDIDATE
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        logging.info(f"Node{self.node_id} become candidate")
        self.write_file()

        last_term = 0
        log_length = len(self.log)
        if log_length > 0:
            last_term = self.log[log_length-1][TERM]

        message = self.vote_request(
            self.node_id, self.current_term, log_length, last_term)

        for port in self.neighbors_ports:
            if port != self.port:
                my_socket.send(message, port)

        # Start election timer
        self.stop_election_timer = False
        thread = threading.Thread(
            target=self.election_timer_procedure, args=())
        thread.name = "election_timer"
        thread.start()

    def voting_new_leader(self, c_id, c_term, c_log_length, c_log_term):
        # Step 2/6
        # On receiving vote request
        if c_term > self.current_term:
            self.current_term = c_term
            self.current_role = FOLLOWER
            self.voted_for = None
            self.write_file()

        last_term = 0
        log_length = len(self.log)

        if log_length > 0:
            last_term = self.log[log_length-1][TERM]

        log_ok = (c_log_term > last_term) or (c_log_term ==
                                              last_term and c_log_length >= log_length)

        if c_term == self.current_term and log_ok and ((self.voted_for == None) or (self.voted_for == self.node_id)):
            self.voted_for = c_id
            self.write_file()
            message = self.vote_response(self.node_id, self.current_term, True)
            port = self.node_port_dict[c_id]
            my_socket.send(message, port)
        else:
            message = self.vote_response(
                self.node_id, self.current_term, False)
            port = self.node_port_dict[c_id]
            my_socket.send(message, port)

    def collecting_votes(self, voter_id, term, granted):
        # Step 3/6
        # On receiving vote responses
        if (self.current_role == CANDIDATE) and (term == self.current_term) and granted:
            self.votes_received.add(voter_id)
            votes_received_length = len(self.votes_received)
            nodes = len(self.neighbors_ports)
            logging.info(f"Votes received: {self.votes_received}")
            if votes_received_length >= math.ceil((nodes+1)/2):
                self.current_role = LEADER
                logging.info(
                    f"{self.node_id} is leader! With term = {self.current_term}")
                self.current_leader = self.node_id
                # Cancel election timer
                self.stop_election_timer = True
                self.leader_is_alive = True

                for port in self.neighbors_ports:
                    if port != self.port:
                        self.sent_length[port] = len(self.log)
                        self.acked_length[port] = 0
                        self.replicate_log(self.node_id, port)

        elif term > self.current_term:
            self.current_term = term
            self.current_role = FOLLOWER
            self.voted_for = None
            self.write_file()
            # Cancel election timer
            self.stop_election_timer = True

    def broadcasting_message(self, message):
        # Step 4/6
        # on request to broadcast msg at node nodeId do
        if self.current_role == LEADER:
            self.log.append([message, self.current_term])
            self.acked_length[self.port] = len(self.log)
            for port in self.neighbors_ports:
                if port != self.port:
                    self.replicate_log(self.node_id, port)
        else:
            # No need to be implemented: forward the request to currentLeader via a FIFO link
            pass

    def broadcasting_message_periodically(self):
        # Step 4/6
        # periodically at node node_id do
        if self.current_role == LEADER:
            for port in self.neighbors_ports:
                if port != self.port:
                    self.replicate_log(self.node_id, port)

    def replicate_log(self, node_id, port):
        # Step 5/6
        prefix_len = self.sent_length[port]
        suffix = []
        log_length = len(self.log)
        for i in range(prefix_len, log_length):
            suffix.append(self.log[i])
        prefix_term = 0
        if prefix_len > 0:
            prefix_term = self.log[prefix_len-1][TERM]
        message = self.log_request(
            node_id, self.current_term, prefix_len, prefix_term, self.commit_length, suffix)
        my_socket.send(message, port)

    def follower_receive_message(self, leader_id, term, prefix_len, prefix_term, leader_commit, suffix):
        # Step 6/6
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.write_file()
            # Cancel election timer
            self.stop_election_timer = True

        if term == self.current_term:
            self.current_role = FOLLOWER
            self.current_leader = leader_id
        log_length = len(self.log)
        log_ok = (log_length >= prefix_len) and ((prefix_len == 0)
                                                 or self.log[prefix_len-1][TERM] == prefix_term)

        if term == self.current_term and log_ok:
            # No need to be implemented AppendEntries(prefixLen, leaderCommit, suffix )
            ack = prefix_len + len(suffix)
            # No need to be implemented send (LogResponse, nodeId, currentTerm, ack,true) to leaderId
        else:
            # No need to be implemented send (LogResponse, nodeId, currentTerm, 0, false) to leaderId
            pass
    
    def send_db(self, data):
        if self.current_role == LEADER:
            message = str(["BROADCAST_DB", data, len(self.db)])
            self.db.append(data)
            for port in self.neighbors_ports:
                if port != self.port:
                    my_socket.send(message, port)
        else:
            message = str(["FORWARD_TO_LEADER", data])
            my_socket.send(message, self.node_port_dict[self.current_leader])
    
    def ask_db(self):
        len_db = len(self.db)
        message = str(["ASK_DB", len_db, self.port])
        if self.current_leader!=None:
            my_socket.send(message, self.node_port_dict[self.current_leader])
    
    def complete_db(self, len_db, port):
        my_db_len = len(self.db)
        
        if my_db_len != 0:
            for i in range(len_db, my_db_len):
                message = str(["COMPLETE_DB", self.db[i]])
                my_socket.send(message, port)
            
    

def reload_logging_windows(filename):
    log = logging.getLogger()
    for handler in log.handlers:
        log.removeHandler(handler)
    logging.basicConfig(format='%(asctime)-4s %(levelname)-6s %(threadName)s:%(lineno)-3d %(message)s',
                        datefmt='%H:%M:%S',
                        filename=filename,
                        filemode='w',
                        level=logging.INFO)


def main(heartbeat_duration=1, lb_fault_duration=1, port=1000,
         node_id=1, neighbors_ports=(1000,), is_continue=False):
    reload_logging_windows(f"logs/node{node_id}.txt")
    threading.excepthook = thread_exception_handler
    try:
        logging.info(f"Node with id {node_id} is running...")
        logging.debug(f"heartbeat_duration: {heartbeat_duration}")
        logging.debug(f"lower_bound_fault_duration: {lb_fault_duration}")
        logging.debug(
            f"upper_bound_fault_duration = {lb_fault_duration}s + 4s")
        logging.debug(f"port: {port}")
        logging.debug(f"neighbors_ports: {neighbors_ports}")
        logging.debug(f"is_continue: {is_continue}")

        logging.info("Create raft object...")
        raft = Raft(node_id, port, neighbors_ports,
                    lb_fault_duration, is_continue, heartbeat_duration)

        logging.info("Execute raft.start()...")
        raft.start()
    except Exception:
        logging.exception("Caught Error")
        raise

def send_db(message, port):
    message = str(["USER_WRITE", message])
    my_socket = UdpSocket(10999)
    my_socket.send(message, port)
    
def read_db(message, port):
    message = str(["USER_READ", int(message)])
    my_socket = UdpSocket(10998)
    my_socket.send(message, port)
    