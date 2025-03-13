use std::collections::HashSet;
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use std::net::SocketAddr;
#[derive(Serialize, Deserialize, Debug, Clone)]
enum MessageType {
    Gossip(String),
    JoinRequest,
    JoinResponse(Vec<SocketAddr>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Message {
    id: Uuid,
    sender: SocketAddr,
    msg_type: MessageType,
}

use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
#[derive(Clone)]
struct Node {
    addr: SocketAddr,
    state: Arc<NodeState>,
}
struct NodeState {
    neighbors: Mutex<Vec<SocketAddr>>,
    known_messages: Mutex<HashSet<Uuid>>,
}

impl Node {
    fn new(addr: SocketAddr) -> Self {
        Node {
            addr,
            state: Arc::new(NodeState {
                neighbors: Mutex::new(Vec::new()),
                known_messages: Mutex::new(HashSet::new()),
            }),
        }
    }
    async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(self.addr).await?;
        println!("in nodestart {} started", self.addr);

        loop {
            let (stream, _) = listener.accept().await?;
            let state = Arc::clone(&self.state);
            let addr = self.addr;
            // tokio::spawn(async move {
            handle_connection(stream, addr, state).await;
            // });
        }
    }

    async fn join_network(&self, known_node: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = TcpStream::connect(known_node).await?;
        let join_message = Message {
            id: Uuid::new_v4(),
            sender: self.addr,
            msg_type: MessageType::JoinRequest,
        };
        println!("Sending join {:?}", join_message);
        stream.write_all(serde_json::to_string(&join_message)?.as_bytes()).await?;

        let mut buf = [0; 1024];
        let n = stream.read(&mut buf).await?;
        let response: Message = serde_json::from_slice(&buf[..n])?;

        if let MessageType::JoinResponse(neighbors) = response.msg_type {
            let mut self_neighbors = self.state.neighbors.lock().await;
            for neighbor in neighbors {
                if !self_neighbors.contains(&neighbor) && neighbor != self.addr {
                    self_neighbors.push(neighbor);
                }
            }
        }
        Ok(())
    }

    async fn gossip(&self, content: &str) {
        let message = Message {
            id: Uuid::new_v4(),
            sender: self.addr,
            msg_type: MessageType::Gossip(content.to_string()),
        };
        println!("Sending gossip {:?}", message);
        let neighbors = self.state.neighbors.lock().await.clone();
        for neighbor in neighbors {
            let msg_clone = message.clone();
            tokio::spawn(async move {
                if let Ok(mut stream) = TcpStream::connect(neighbor).await {
                    let _ = stream.write_all(serde_json::to_string(&msg_clone).unwrap().as_bytes()).await;
                }
            });
        }
    }
    async fn get_status(&self) -> NodeState {
        let start_time = SystemTime::now();
        println!("{:?}", start_time);
        NodeState {
            neighbors: Mutex::from(self.state.neighbors.lock().await.clone()),
            known_messages: Mutex::from(self.state.known_messages.lock().await.clone())
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    self_addr: SocketAddr,
    state: Arc<NodeState>,
) {
    let sender_addr = match stream.peer_addr() {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("Failed to get peer address: {}", e);
            return;
        }
    };

    let mut buf = [0; 1024];
    let n = match stream.read(&mut buf).await {
        Ok(n) => n,
        Err(e) => {
            eprintln!("Failed to read from stream: {}", e);
            return;
        }
    };

    let received_data = &buf[..n];
    println!("Received data: {:?}", received_data);
    let message: Message = match serde_json::from_slice(received_data) {
        Ok(msg) => msg,
        Err(e) => {
            eprintln!("Failed to deserialize message: {}", e);
            return;
        }
    };

    let mut known_messages = state.known_messages.lock().await;
    println!("Received message: {:?}", message);
    if known_messages.insert(message.id) {
        match message.msg_type {
            MessageType::Gossip(ref content) => {
                println!("Node {} received gossip: {}", self_addr, content);
                let neighbors = state.neighbors.lock().await.clone();
                for neighbor in neighbors.iter().filter(|&n| *n != sender_addr) {
                    let msg_clone = message.clone();
                    let neighbor = *neighbor;
                    tokio::spawn(async move {
                        if let Ok(mut stream) = TcpStream::connect(neighbor).await {
                            let _ = stream.write_all(serde_json::to_string(&msg_clone).unwrap().as_bytes()).await;
                        }
                    });
                }
            }
            MessageType::JoinRequest => {
                let mut neighbors = state.neighbors.lock().await;
                if !neighbors.contains(&sender_addr) {
                    neighbors.push(sender_addr);
                }
                let response = Message {
                    id: Uuid::new_v4(),
                    sender: self_addr,
                    msg_type: MessageType::JoinResponse(neighbors.clone()),
                };
                let _ = stream.write_all(serde_json::to_string(&response).unwrap().as_bytes()).await;
            }
            MessageType::JoinResponse(neighbors) => {
                let mut self_neighbors = state.neighbors.lock().await;
                for neighbor in neighbors {
                    if !self_neighbors.contains(&neighbor) && neighbor != self_addr {
                        self_neighbors.push(neighbor);
                    }
                }
            }
        }
    }
}

use clap::Parser;

#[derive(Parser, Debug)]
#[clap(version = "1.0", about = "Gossip协议节点")]
struct Args {
    /// 绑定地址（格式：IP:PORT）
    #[clap(short, long, default_value = "0.0.0.0:8000")]
    bind: String,

    /// 要连接的已知节点地址
    #[clap(short, long)]
    connect: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use tokio::io::AsyncBufReadExt;
    let args = Args::parse();

    // 解析绑定地址
    let bind_addr: SocketAddr = args.bind.parse()
        .map_err(|_| format!("无效的绑定地址: {}", args.bind))?;

    // 创建节点实例
    let node = Node::new(bind_addr);
    println!("🔄 节点启动于 {}", bind_addr);

    // 启动节点服务
    let node_clone = node.clone();
    let handle = tokio::spawn(async move {
        node_clone.start().await.expect("节点服务启动失败");
    });

    // 连接已知节点（如果指定）
    if let Some(connect_addr) = args.connect {
        let target_addr: SocketAddr = connect_addr.parse()
            .map_err(|_| format!("无效的连接地址: {}", connect_addr))?;

        // 带重试的连接机制
        let mut retries = 3;
        while retries > 0 {
            match node.join_network(target_addr).await {
                Ok(_) => {
                    println!("✅ 成功连接到 {}", target_addr);
                    break;
                }
                Err(e) => {
                    eprintln!("⚠️ 连接失败: {} (剩余重试次数: {})", e, retries);
                    retries -= 1;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
    // 主循环控制
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    let mut message_counter = 0;
    loop {
        tokio::select! {
            // 定时发送消息
            _ = interval.tick() => {
                message_counter += 1;
                let msg = format!("心跳消息 #{}", message_counter);
                node.gossip(&msg).await;
                println!("🔄 主循环发送消息: {}", msg);
            }
            // 处理控制台输入
            input = async {
                 println!("请输入一些文本：");
                 let stdin = tokio::io::stdin();
                 let mut buffered_stdin = BufReader::new(stdin);
                 let mut buffer = String::new();
                 buffered_stdin.read_line(&mut buffer).await.expect("读取输入失败");
                 buffer.trim().to_string()
            } => {
                match input.as_str() {
                    "exit" => break,
                    "list" => {
                        println!("🌐 当前节点列表:");
                        println!("节点1: {}", node.addr);
                        println!("节点1: {}", node.state.known_messages.lock().await.len());
                        // println!("节点2: {}", node2.addr);
                        // println!("节点3: {}", node3.addr);
                    }
                    cmd => {
                        node.gossip(cmd).await;
                        println!("📨 已发送自定义消息: {}", cmd);
                    }
                }
            }
            // 处理终止信号
            _ = tokio::signal::ctrl_c() => {
                println!("🚦 收到终止信号，开始优雅关闭...");
                break;
            }
        }
    }
    Ok(())
}
