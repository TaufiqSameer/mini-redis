use mini_redis::{Result, client};
#[tokio::main]
async fn main() -> Result<()> {
    let mut cilent = client::connect("127.0.0.1:6379").await?;
    cilent.set("hello", "world".into()).await?;
    let result = cilent.get("hello").await?;
    println!("The value from the result is {:?}", result);
    Ok(())
}
