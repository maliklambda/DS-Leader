use std::{
    io::{self, Read},
    net::{Ipv4Addr, SocketAddrV4, TcpListener, UdpSocket},
    time::Duration,
};

use log::{debug, info};

use crate::{
    constants::{
        LOCALHOST, MAX_CLUSTER_SIZE, MAX_MESSAGES, MAX_MESSAGES_PER_TICK, PORT_START, READ_TIMEOUT,
    },
    message::{Message, MessageType},
    snowflake::generate_snowflake_id,
    view::{
        View,
        quorum::{Quorum, VotingStatus},
    },
};

pub type ReplicaId = u32;

#[derive(Debug)]
pub struct Replica {
    pub id: ReplicaId,

    /// Each replica keeps track of the current state of the view.
    /// When a change happens, it must be updated for each view individually.
    pub view: View,

    /// Keep a ring buffer of the last MAX_MESSAGES many messages received.
    pub buffer: MessageBuffer,

    /// Buffer for heartbeats sent by other replicas.
    pub heartbeats: [bool; MAX_CLUSTER_SIZE],

    /// host ip - for testing it's always localhost
    ip: Ipv4Addr,

    /// port that the replica is listening on.
    port: u16,

    /// listen on self.port
    udp_socket: UdpSocket,

    /// status of potential current vote
    voting_status: VotingStatus,
}

impl Replica {
    pub fn init() -> Self {
        let ip = LOCALHOST;
        // check for other ports that are listening
        let mut active_ports = [0u32; MAX_CLUSTER_SIZE];
        let mut count: u32 = 0;
        check_available_ports_in_range(ip, PORT_START, |port| {
            active_ports[count as usize] = port;
            count += 1;
        });
        info!("Active ports: {:?}", active_ports);

        // no active ports
        if count == 0 {
            info!("Found empty cluster. I am becoming the primary!");
            let id: ReplicaId = count;
            let mut replicas = [0u32; MAX_CLUSTER_SIZE];
            let port = PORT_START as u32;
            active_ports[0] = port;
            replicas[0] = id;
            let view = View {
                leader: id,
                replicas,
                occupied_ports: active_ports,
            };
            info!("Got this view: {:?}", view);
            let udp_socket = UdpSocket::bind(SocketAddrV4::new(LOCALHOST, port as u16)).unwrap();
            udp_socket.set_nonblocking(true).unwrap();
            info!("I am listening on {LOCALHOST}:{PORT_START}");

            return Replica {
                id,
                view,
                buffer: MessageBuffer::zero_buffer(),
                ip,
                heartbeats: [false; MAX_CLUSTER_SIZE],
                port: PORT_START,
                udp_socket,
                voting_status: VotingStatus::Idle,
            };
        }

        debug!("Found active ports: {:?}", active_ports);

        // select port
        let (id, port) = {
            let pos = active_ports
                .iter()
                .position(|p| *p == 0)
                .expect("No free port found");
            let port = PORT_START + pos as u16;
            (pos as ReplicaId, port)
        };
        let udp_socket = UdpSocket::bind(SocketAddrV4::new(LOCALHOST, port)).unwrap();
        udp_socket.set_read_timeout(Some(READ_TIMEOUT)).unwrap();
        info!("I am listening on {LOCALHOST}:{port}");

        let msg_id = generate_snowflake_id(id);

        // braodcast WhoIsPrimary Message
        let msg = Message::new(id, MessageType::ViewInfoRequest, msg_id);
        info!("Broadcasting: {:?}", msg);
        for active_port in active_ports.iter().filter(|port| **port != 0) {
            udp_socket
                .send_to(
                    &msg.to_bytes(),
                    SocketAddrV4::new(LOCALHOST, (*active_port).try_into().unwrap()),
                )
                .unwrap();
        }

        // Receive XIsPrimary Message
        let mut buf = [0_u8; Message::BYTE_LENGTH];
        match udp_socket.recv(&mut buf) {
            Ok(_) => (),
            Err(err) => {
                if err.kind() == io::ErrorKind::TimedOut || err.kind() == io::ErrorKind::WouldBlock
                {
                    panic!("Connection timed out");
                } else {
                    panic!("IO Error");
                }
            }
        }

        let view_info = {
            let msg = Message::from_bytes(buf).unwrap();
            if let MessageType::ViewInfoResponse(view_info) = msg.msg_type {
                info!("Primary is #{:?}", view_info.leader_id);
                view_info
            } else {
                panic!("Received wrong message type");
            }
        };

        info!("Requesting view change from primary");
        let primary_addr = SocketAddrV4::new(LOCALHOST, view_info.leader_id as u16 + PORT_START);
        let msg = Message::new(
            id,
            MessageType::RequestViewChange(view_info),
            generate_snowflake_id(id),
        );
        udp_socket.send_to(&msg.to_bytes(), primary_addr).unwrap();
        let mut msg_bytes = [0_u8; Message::BYTE_LENGTH];
        std::thread::sleep(Duration::from_millis(30));
        udp_socket.set_nonblocking(false).unwrap(); // enable blocking
        udp_socket.recv(&mut msg_bytes).unwrap();
        let msg = Message::from_bytes(msg_bytes).unwrap();
        assert!(msg.checksum_matches(), "Checksum missmatch");
        info!("Received this answer to view change request: {:?}", msg);
        info!("Added node #{id} to cluster");
        let mut view = View::from(view_info);
        view.add_replica(id);
        Replica {
            id,
            view,
            buffer: MessageBuffer::zero_buffer(),
            ip,
            heartbeats: [false; MAX_CLUSTER_SIZE],
            port,
            udp_socket,
            voting_status: VotingStatus::Idle,
        }
    }

    /// Runs for some time
    pub fn run_for_ms(&mut self, _millis: usize) -> Result<(), std::io::Error> {
        if self.buffer.msg_count_since_last_tick > 0 {
            info!("We have a new message");
            let last_msg = self.buffer.retrieve_message();
            match last_msg.msg_type {
                MessageType::ViewInfoRequest => {
                    // response to sender
                    let sender = last_msg.header.sender_id;
                    let msg_id = generate_snowflake_id(self.id);
                    let msg = Message::new(
                        self.id,
                        MessageType::ViewInfoResponse(self.view.to_view_info()),
                        msg_id,
                    );
                    self.udp_socket
                        .send_to(
                            &msg.to_bytes(),
                            SocketAddrV4::new(LOCALHOST, PORT_START + sender as u16),
                        )
                        .unwrap();
                    debug!("view: {:?}", self.view);

                    self.try_add_new_replica(sender).unwrap();
                }
                _ => info!("Some other message"),
            }
        }
        // info!("Running for {_millis} milliseconds.");
        // info!("I am this replica: {:?}", self);
        Ok(())
    }

    /// Tick to keep the clocks in sync.
    /// Receive messages during this time.
    /// Send heartbeat to other replicas in cluster.
    /// Receive heartbeats from other replicas.
    pub fn tick(&mut self) -> Result<(), std::io::Error> {
        self.collect_messages();
        self.heartbeats = [false; MAX_CLUSTER_SIZE];

        let msgs = self.buffer.retrieve_messages_since_last_tick();
        let (mut idx_other_msgs, mut other_msgs) = (0, [None; MAX_MESSAGES_PER_TICK]); // index in msgs
        for msg in msgs {
            if msg.is_none() {
                break;
            }
            let msg = msg.unwrap();
            if let MessageType::Heartbeat = msg.msg_type {
                assert!(msg.header.sender_id < MAX_CLUSTER_SIZE as u32);
                self.heartbeats[msg.header.sender_id as usize] = true;
            } else {
                other_msgs[idx_other_msgs] = Some(msg);
                idx_other_msgs += 1;
            }
        }

        self.check_heartbeats();
        self.send_heartbeats();
        Ok(())
    }

    fn listen_on_port(&self, port: u16) {
        let addr = SocketAddrV4::new(self.ip, port);

        // TcpListener::bind accepts SocketAddrV4, avoiding String allocation
        if let Ok(listener) = TcpListener::bind(addr) {
            debug!("Node listening on port {}", port);

            // Accept only the first connection for this example
            if let Ok((mut stream, _)) = listener.accept() {
                // Fixed-size buffer on the stack (no Vec)
                let mut buffer = [0u8; 512]; // TODO: change this to message size
                if let Ok(bytes_read) = stream.read(&mut buffer) {
                    debug!("Received {} bytes", bytes_read);
                }
            }
        }
    }

    /// Collect messages from the network into
    ///     1) the network buffer (byte array)
    ///     2) the message ring buffer
    fn collect_messages(&mut self) {
        if let Ok(num_bytes) = self.udp_socket.recv(&mut self.buffer.network_buffer) {
            assert_eq!(
                num_bytes % Message::BYTE_LENGTH,
                0,
                "Received uneven message bytes (modulo msg_byte_len != 0)"
            );
            let num_messages = num_bytes / Message::BYTE_LENGTH;
            info!("Replica received [{num_messages}] new messages");
            info!(
                "Msg count since last tick: {}",
                self.buffer.msg_count_since_last_tick
            );
            for i in 0..num_messages {
                let idx = i * Message::BYTE_LENGTH;
                let msg = Message::from_bytes(
                    self.buffer.network_buffer[idx..idx + Message::BYTE_LENGTH]
                        .try_into()
                        .unwrap(),
                )
                .unwrap();
                assert!(msg.checksum_matches(), "Checksum missmatch");
                self.buffer.add_message(msg);
            }
        }
    }

    /// Send heartbeat messages to all other replicas in cluster.
    fn send_heartbeats(&self) {
        for port in self
            .view
            .occupied_ports
            .iter()
            .filter(|port| **port != 0 && **port != self.port as u32)
        {
            self.send_heartbeat(SocketAddrV4::new(LOCALHOST, *port as u16));
        }
    }

    /// Send a single heartbeat message.
    fn send_heartbeat(&self, addr: SocketAddrV4) {
        debug!(
            "Sending from {:?} (port = {:?}) heartbeat to {:?}",
            self.udp_socket.local_addr(),
            self.port,
            addr
        );
        self.udp_socket
            .send_to(
                &Message::new(
                    self.id,
                    MessageType::Heartbeat,
                    generate_snowflake_id(self.id),
                )
                .to_bytes(),
                addr,
            )
            .unwrap();
    }

    /// Check if other replicas replied to the heartbeat.
    /// This assumes that the message buffer is filled with the latest heartbeats
    fn check_heartbeats(&self) {
        info!("Checking heartbeats: {:?}", self.heartbeats);
        let received_hbs = self.heartbeats.iter().filter(|v| **v).count();
        assert!(received_hbs < self.view.replicas.len());
        if received_hbs
            != self
                .view
                .replicas
                .iter()
                .filter(|r| **r != 0 && **r != self.id)
                .count()
        {
            debug!("rc_hbs: {:?} - {:?}", received_hbs, self.view.replicas);
            todo!("Missing a heartbeat");
        }
    }

    /// Initializes adding of a new replica.
    /// The broadcast message is sent here.
    /// The replica's voting_status will change based on this.
    fn try_add_new_replica(&mut self, new_id: ReplicaId) -> Result<(), &str> {
        assert_eq!(
            self.view.leader, self.id,
            "Cannot add a new replica if not primary"
        );
        info!("Trying to add {new_id} to current view");
        if let VotingStatus::Voting(_) = &self.voting_status {
            return Err("Replica is already voting on something");
        }

        let num_of_replicas = self
            .view
            .occupied_ports
            .iter()
            .filter(|port| **port != self.port as u32 && **port != 0)
            .count() as u16;

        let num_of_quorum_votes = match num_of_replicas {
            0 => {
                // directly add replica to cluster
                debug!("I am alone in this cluster!");
                self.view.add_replica(new_id);
                // acknowledge to new replica that it can be part of the cluster
                self.send_message(
                    Message::new(
                        self.id,
                        MessageType::RequestViewChangeOk,
                        generate_snowflake_id(self.id),
                    ),
                    SocketAddrV4::new(LOCALHOST, PORT_START + new_id as u16),
                )
                .map_err(|_| "Message send failure")?;
                return Ok(());
            }
            1 => 1,
            n => n / 2 + 1,
        };
        debug!("Waiting for answer of {num_of_quorum_votes} replicas");

        let quorum = Quorum::new(num_of_quorum_votes);
        self.voting_status = VotingStatus::Voting(quorum);

        let mut new_view = self.view;
        new_view.add_replica(new_id);

        // Send ViewChangeRequests to all other nodes in cluster
        self.broadcast_message(Message::new(
            self.id,
            MessageType::ViewChange(new_view.to_view_info()),
            generate_snowflake_id(self.id),
        ));
        Ok(())
    }

    pub fn send_message(&mut self, msg: Message, to: SocketAddrV4) -> io::Result<usize> {
        debug!("Sending message to {:?}: {:?}", to, msg);
        self.udp_socket.send_to(&msg.to_bytes(), to)
    }

    /// Broadcast a message to all active nodes in the current view
    pub fn broadcast_message(&mut self, msg: Message) {
        for port in self.view.occupied_ports {
            let _ = self.send_message(msg, SocketAddrV4::new(LOCALHOST, port as u16));
        }
    }
}

#[derive(Debug)]
pub struct MessageBuffer {
    /// Ring buffer of messages.
    buffer: [Message; MAX_MESSAGES],

    // Count messages received since last tick
    msg_count_since_last_tick: u16,

    /// Represents the index of the last message.
    idx_last_message: usize,

    /// Keep a byte buffer for accepting messages over the network.
    /// This buffer is filled and emptied on every tick.
    network_buffer: [u8; Message::BYTE_LENGTH * MAX_MESSAGES_PER_TICK],
}

impl MessageBuffer {
    pub fn zero_buffer() -> Self {
        Self {
            buffer: [Message::zero_message(); MAX_MESSAGES],
            msg_count_since_last_tick: 0,
            idx_last_message: 0,
            network_buffer: [0_u8; Message::BYTE_LENGTH * MAX_MESSAGES_PER_TICK],
        }
    }

    pub fn add_message(&mut self, msg: Message) {
        self.buffer[self.idx_last_message] = msg;
        let idx = (self.idx_last_message + 1) % MAX_MESSAGES;
        assert!(
            idx < MAX_MESSAGES,
            "MessageBuffer.idx_last_message overflows the buffer"
        );
        self.msg_count_since_last_tick += 1;
    }

    pub fn retrieve_message(&mut self) -> &Message {
        let idx = self.idx_last_message;
        self.msg_count_since_last_tick -= 1;
        self.idx_last_message += 1;
        self.idx_last_message %= MAX_MESSAGES;
        debug!("Retrieving message from {idx}");
        &self.buffer[idx]
    }

    /// Returns all messages since last tick
    /// Messages start @0
    pub fn retrieve_messages_since_last_tick(
        &mut self,
    ) -> [Option<&Message>; MAX_MESSAGES_PER_TICK] {
        let mut msgs: [Option<&Message>; MAX_MESSAGES_PER_TICK] = [None; MAX_MESSAGES_PER_TICK];
        let mut idx_msgs = 0;
        if self.msg_count_since_last_tick == 0 {
            debug!("No message since last tick");
            return msgs;
        }
        panic!("received a message");
        loop {
            let idx = self.idx_last_message;
            self.msg_count_since_last_tick -= 1;
            self.idx_last_message += 1;
            self.idx_last_message %= MAX_MESSAGES;
            debug!("Retrieving message from {idx}");
            msgs[idx_msgs] = Some(&self.buffer[idx]);
            idx_msgs += 1;
            if self.msg_count_since_last_tick == 0 {
                break;
            }
        }
        msgs
    }
}

fn check_available_ports_in_range<F>(ip: Ipv4Addr, start: u16, mut on_found: F)
where
    F: FnMut(u32),
{
    let end: u16 = start + u16::try_from(MAX_CLUSTER_SIZE).unwrap();
    for port in start..end {
        let addr = SocketAddrV4::new(ip, port);

        debug!("Checking port {port} for connection");
        match UdpSocket::bind(addr) {
            Ok(_) => {
                // Port active
            }
            Err(_) => {
                // port cannot be connected to => is occupied
                on_found(port.into())
            }
        }
        // match TcpStream::connect_timeout(&addr.into(), std::time::Duration::from_millis(10)) {
        //     Ok(_) => {
        //         // Port is active and reachable
        //         on_found(port.into());
        //     }
        //     Err(e) if e.kind() == ErrorKind::ConnectionRefused => {
        //         // Port is closed (no one is listening)
        //     }
        //     Err(_) => {
        //         // Other errors (timeout, etc.)
        //     }
        // }
    }
}
