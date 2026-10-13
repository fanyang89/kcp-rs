use futures_lite::{
    future,
    io::{AsyncReadExt, AsyncWriteExt},
};
use kcp_rs::asynchronous::Endpoint;
use std::{io, net::SocketAddr};

pub async fn exchange(
    mut client: Endpoint,
    mut server: Endpoint,
    server_addr: SocketAddr,
) -> io::Result<()> {
    let (client_stream, server_stream) =
        future::zip(client.connect(server_addr), server.accept()).await;
    let mut client_stream = client_stream?;
    let mut server_stream = server_stream?;
    let payload = vec![0x5a; 256 * 1024];
    let client_task = async {
        client_stream.write_all(&payload).await?;
        client_stream.close().await?;
        let mut reply = Vec::new();
        client_stream.read_to_end(&mut reply).await?;
        assert_eq!(reply, b"received");
        Ok::<_, io::Error>(())
    };
    let server_task = async {
        let mut received = Vec::new();
        server_stream.read_to_end(&mut received).await?;
        assert_eq!(received, payload);
        server_stream.write_all(b"received").await?;
        server_stream.close().await?;
        Ok::<_, io::Error>(())
    };
    let (a, b) = future::zip(client_task, server_task).await;
    a?;
    b?;
    println!("256 KiB delivered over UDP; bidirectional half-close succeeded");
    Ok(())
}
