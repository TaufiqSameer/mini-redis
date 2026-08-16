use mini_redis::client;
use bytes::Bytes;
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;
#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    },
}
use tokio::sync::mpsc;
use tokio::sync::oneshot;



#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(32);
    let mut tx1 = tx.clone();
    let mut tx2 = tx.clone();
    
    // tokio::spawn(async move {
    //     tx.send("From first transmitter").await.unwrap();
    // });
    // tokio::spawn(async move {
    //     tx2.send("From second transmitter").await.unwrap();
    // });
    // let manager = tokio::spawn(async move {
    //     let mut client = client::connect("127.0.0.1:6379").await.unwrap();

    //     while let Some(cmd) = rx.recv().await {
    //         use Command::*;
            
    //         match cmd {
    //             Get { key,resp } => {
    //                 client.get(&key).await;
    //             }
    //             Set { key, val,resp } => {
    //                 client.set(&key, val.clone()).await;
    //             }
    //         }
    //     }
    // });
    let tx2 = tx.clone();
    
    let t1 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Get {
            key: "foo".to_string(),
            resp : resp_tx
        };
        
        tx1.send(cmd).await.unwrap();
    });
    let mut val = tx2.clone();
    let t2 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Set {
            key: "foo".to_string(),
            val: "bar".into(),
            resp : resp_tx
        };
        
        val.send(cmd).await.unwrap();
    });
    t1.await.unwrap();
    t2.await.unwrap();
    
    let t1 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Get {
            key: "foo".to_string(),
            resp: resp_tx,
        };
    
        tx.send(cmd).await.unwrap();
    
        let res = resp_rx.await;
        println!("GOT = {:?}", res);
    });
    
    let t2 = tokio::spawn(async move {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Set {
            key: "foo".to_string(),
            val: "bar".into(),
            resp: resp_tx,
        };
    
        tx2.send(cmd).await.unwrap();
    
        let res = resp_rx.await;
        println!("GOT = {:?}", res);
    });
    let mut client = client::connect("127.0.0.1:6379")
    .await
    .unwrap();

while let Some(cmd) = rx.recv().await {
    match cmd {
        Command::Get { key, resp } => {
            let res = client.get(&key).await;
            let _ = resp.send(res);
        }

        Command::Set { key, val, resp } => {
            let res = client.set(&key, val).await;
            let _ = resp.send(res);
        }
    }
}
}