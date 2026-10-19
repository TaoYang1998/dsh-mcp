use dsh_mcp::mcp_transport::{BoundedMcpTransport, FRAME_MAX, REQUESTS_MAX};
use rmcp::transport::Transport;
use tokio::io::AsyncWriteExt;
/// 未完成请求达到上限时，关闭单连接而不继续堆积 SDK 任务。
#[tokio::test]
async fn unfinished_request_count_is_bounded() {
    let (mut input, read) = tokio::io::duplex(65536);
    let (output, _consumer) = tokio::io::duplex(65536);
    let mut transport = BoundedMcpTransport::new(read, output);
    for n in 0..=REQUESTS_MAX {
        input
            .write_all(
                format!("{{\"jsonrpc\":\"2.0\",\"id\":{n},\"method\":\"ping\",\"params\":{{}}}}\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
    }
    for _ in 0..REQUESTS_MAX {
        assert!(transport.receive().await.is_some());
    }
    assert!(transport.receive().await.is_none());
}
/// 无换行的超大输入也必须在 JSON 解析前终止。
#[tokio::test]
async fn oversized_unterminated_frame_is_rejected() {
    let (mut input, read) = tokio::io::duplex(FRAME_MAX + 8192);
    let (output, _consumer) = tokio::io::duplex(64);
    input.write_all(&vec![b'x'; FRAME_MAX + 1]).await.unwrap();
    let mut transport = BoundedMcpTransport::new(read, output);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), transport.receive())
            .await
            .unwrap()
            .is_none()
    );
}
/// 不读取响应的客户端在写期限后失去连接，内存不会持续积压。
#[tokio::test(start_paused = true)]
async fn slow_reader_hits_a_bounded_write_deadline() {
    let (_input, read) = tokio::io::duplex(64);
    let (output, _consumer) = tokio::io::duplex(1);
    let mut transport = BoundedMcpTransport::new(read, output);
    let response =
        serde_json::from_value(serde_json::json!({"jsonrpc":"2.0","id":1,"result":{}})).unwrap();
    assert!(transport.send(response).await.is_err());
    assert!(transport.receive().await.is_none());
}
/// 取消通知必须释放请求计数，正常客户端不能累计到无故断线。
#[tokio::test]
async fn cancelled_requests_release_their_transport_slots() {
    let (mut input, read) = tokio::io::duplex(65536);
    let (output, _consumer) = tokio::io::duplex(65536);
    let mut transport = BoundedMcpTransport::new(read, output);
    for n in 0..REQUESTS_MAX * 2 {
        input
            .write_all(
                format!("{{\"jsonrpc\":\"2.0\",\"id\":{n},\"method\":\"ping\",\"params\":{{}}}}\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        assert!(transport.receive().await.is_some(), "request {n}");
        input.write_all(format!("{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{{\"requestId\":{n}}}}}\n").as_bytes()).await.unwrap();
        assert!(transport.receive().await.is_some());
    }
}
