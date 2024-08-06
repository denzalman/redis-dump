# Redis Dump Tool
## Live redis cluster dump of all keys with TTL with live sync support

Full cluster dump with key's TTL and pub/sub support for real-time sync. Dump process is running in parallel threads for faster sync.
As well as Live sync using pub/sub for real-time sync while dump is running to keep the target redis in sync with source.

Useful for live migrations from one redis cluster to another without downtime. 

### Installation
1. Clone the repository and have Rust installed (1.80 or later)
2. Add source and target redis URLs in `rd.toml`
```toml
# Required configurations
source = "redis://localhost:6379"
target = "redis://localhost:6380"

# Optional configurations
batch = 120 # Number of keys to move in a single batch, default is 100
threads = 20 # Number of threads to run in parallel for copy, default is 10
sync_db = "*" # Database to sync, default is "*"
sync_key = "*" # Key pattern to sync, default is "*"
```
3. Run `cargo run` to start the sync process

### Build and Run
if you need to build and run/distribute the tool as binary, you can use the following commands:
```shell
cargo build --release
./target/release/rd
```
### Pre-requisites for source cluster live sync
For make sync functional you have to activate Redis keyspace notifications feature, which is supported by Redis 2.8.6 and later.

Source cluster configuration process example for AWS Elasticache Redis:

1. Open the ElastiCache console.
2. To see a list of the available parameter groups, choose Parameter Groups in the navigation pane.
3. Select the parameter group that you want to modify.

>Note: You can't modify a default parameter group. If all the parameter groups listed are default, then create a new parameter group.
4. (Optional) To create a new parameter group, choose **Create Parameter Group**:
   1. For the **Name** field, enter a unique name. 
   2. For the **Description** field, enter a description. 
   3. Select a **Family**. 
   4. Choose **Create**. 
5. Choose Edit parameters values. 
6. Navigate to notify-keyspace-events, and then enter AKE in the Value field. For more information about allowed values, see Redis 2.8.6 added parameters. 
7. Choose Save changes. The changes take effect immediately and the cluster doesn't need to be restarted.
> Note: Be sure to assign the new modified parameter group to your Redis cluster.
docs: https://repost.aws/knowledge-center/elasticache-redis-keyspace-notifications