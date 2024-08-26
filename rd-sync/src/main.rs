
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use redis::{Client, RedisResult, Connection};
use std::error::Error;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::mpsc::Sender;
use std::thread;
use std::time::Duration;

const BATCH_SIZE: usize = 100;
const NUM_THREADS: usize = 10;
const SYNC_NUM_THREADS: usize = 20;

fn main() -> Result<(), Box<dyn Error>> {
    //read source and target from config file rd.toml
    let config = std::fs::read_to_string("rd.toml").unwrap_or_else(|_| {
        panic!("Could not read config file rd.toml");
    });
    let config: toml::Value = toml::from_str(&config).unwrap_or_else(|_| {
        panic!("Could not parse config file rd.toml");
    });
    let source_url = config["source"].as_str().unwrap_or_else(|| {
        panic!("Source URL not found in config file");
    });
    let target_url = config["target"].as_str().unwrap_or_else(|| {
        panic!("Target URL not found in config file");
    });

    let batch_size = config["batch"].as_integer().unwrap_or(BATCH_SIZE as i64) as usize;
    let num_threads = config["threads"].as_integer().unwrap_or(NUM_THREADS as i64) as usize;
    let sync_num_threads = config["sync_threads"].as_integer().unwrap_or(SYNC_NUM_THREADS as i64) as usize;

    let sync_db = config["sync_db"].as_str().unwrap_or("*").to_string();
    let sync_key = config["sync_key"].as_str().unwrap_or("*").to_string();

    let source_client = Arc::new(Client::open(source_url)?);
    let target_client = Arc::new(Client::open(target_url)?);






    // Create shared variables for threads
    let cursor = Arc::new(Mutex::new(0));
    let total_processed = Arc::new(Mutex::new(0));


    // Get total number of keys
    let mut conn = source_client.get_connection()?;
    let total_keys: usize = redis::cmd("DBSIZE").query(&mut conn)?;

    

    // Set up progress bars
    let multi_progress = Arc::new(MultiProgress::new());
    let main_progress = multi_progress.add(ProgressBar::new(total_keys as u64));
    main_progress.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} ({percent}%) | {per_sec} | ETA: {eta_precise} \n{spinner:.blue} Copied: {pos:>7} | {msg} ")
            .unwrap()
            .progress_chars("##-"));
    main_progress.tick();

    let copy_errors = multi_progress.add(ProgressBar::new_spinner());
    copy_errors.set_style(
        ProgressStyle::with_template("{spinner:.red} Copy Err: {pos:>5} | {msg}").unwrap());

    let sync_errors = multi_progress.add(ProgressBar::new_spinner());
    sync_errors.set_style(
        ProgressStyle::with_template("{spinner:.red} Sync Err: {pos:>5} | {msg}").unwrap());

    let mut handles = vec![];

        let source_client = Arc::clone(&source_client);
        let target_client = Arc::clone(&target_client);
        let multi_progress = Arc::clone(&multi_progress);
        let sync_progress = multi_progress.add(ProgressBar::new_spinner());
        sync_progress.set_message("Live sync progress");
        sync_progress.set_style(
            ProgressStyle::with_template("{spinner:.green} Synced: {pos:>7} | {msg}")
                .unwrap()
                .progress_chars("##-"));


        let (tx, rx) = mpsc::channel();
        let tx: Sender<(String, String)> = tx.clone();
        let rx = std::sync::Arc::new(std::sync::Mutex::new(rx));

        for _ in 0..sync_num_threads {
            let source_client = source_client.clone();
            let target_client = target_client.clone();
            let sync_progress = sync_progress.clone();
            let sync_errors = sync_errors.clone();
            let thread_rx = rx.clone();

            let handle = thread::spawn(move || {
                let mut source_conn = source_client.get_connection().unwrap();
                let mut target_conn = target_client.get_connection().unwrap();

                loop {
                    let (key, event) = {
                        let rx = thread_rx.lock().unwrap();
                        rx.recv().unwrap()
                    };

                    let _ = dump_key(
                        &mut source_conn,
                        &mut target_conn,
                        &key,
                        sync_progress.clone(),
                        sync_errors.clone(),
                        &event
                    );
                }
            });
            handles.push(handle);
        }

        // Main loop to receive messages and send to worker threads
        let handle = thread::spawn({
            let source_client = source_client.clone();
            let sync_db = Arc::new(sync_db.clone());
            let sync_key = Arc::new(sync_key.clone());
            move || {
                let mut pubsub_conn = source_client.get_connection().unwrap();
                let pchannel = format!("__keyspace@{sync_db}__:{sync_key}");
                let mut pubsub = pubsub_conn.as_pubsub();
                pubsub.psubscribe(pchannel).unwrap();
                loop {
                    let msg = pubsub.get_message().unwrap();
                    let channel: String = msg.get_channel().unwrap();
                    let event: String = msg.get_payload().unwrap();
                    // Extract key from channel
                    let key = channel.split("__:").last().unwrap_or("").to_string();

                    // Send the key and event to a worker thread
                    tx.send((key, event)).unwrap();
                }
                drop(tx);
            }
        });

        handles.push(handle);




    // let sync_updates_handle = thread::spawn({
    //     let source_client = Arc::clone(&source_client);
    //     let target_client = Arc::clone(&target_client);
    //     let multi_progress = Arc::clone(&multi_progress);
    //     let sync_progress = multi_progress.add(ProgressBar::new_spinner());
    //     sync_progress.set_message("Live sync progress");
    //     sync_progress.set_style(
    //         ProgressStyle::with_template("{spinner:.green} Synced: {pos:>7} | {msg}")
    //             .unwrap()
    //             .progress_chars("##-"));
    //     move || {
    //         let result = sync_updates_handle(
    //             source_client,
    //             target_client,
    //             sync_progress,
    //             sync_errors.clone(),
    //             &sync_db,
    //             &sync_key
    //         );
    //         if let Err(e) = result {
    //             sync_errors.set_message(format!("Sync process error: {}", e));
    //         }
    //     }
    // });
    //
    // handles.push(sync_updates_handle);

    for i in 0..num_threads {
        let source_client = Arc::clone(&source_client);
        let target_client = Arc::clone(&target_client);
        let cursor = Arc::clone(&cursor);
        let total_processed = Arc::clone(&total_processed);
        let multi_progress = Arc::clone(&multi_progress);
        let main_progress = main_progress.clone();
        let copy_errors = copy_errors.clone();
        let handle = thread::spawn(move || {
            let thread_progress = multi_progress.add(ProgressBar::new_spinner());
            let result = process_batch(
                source_client,
                target_client,
                cursor,
                total_processed,
                main_progress,
                copy_errors,
                batch_size
            );
            if let Err(e) = result {
                thread_progress.set_message(format!("Copy thread {} error: {}", i + 1, e));
            }
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
        main_progress.finish_with_message("Copy completed successfully, Ctrl-C for stop sync...");
    }

    Ok(())
}

fn sync_updates_handle(
    source_client: Arc<Client>,
    target_client: Arc<Client>,
    sync_progress: ProgressBar,
    sync_errors: ProgressBar,
    sync_db: &str,
    sync_key: &str,
) -> Result<(), Box<dyn Error>> {
    let mut pubsub_conn = source_client.get_connection()?;
    let mut source_conn = source_client.get_connection()?;
    let mut target_conn = target_client.get_connection()?;

    let pchannel = format!("__keyspace@{sync_db}__:{sync_key}");
    // Subscribe to keyspace notifications
    let mut pubsub = pubsub_conn.as_pubsub();
    pubsub.psubscribe(pchannel)?;

    loop {
        let msg = pubsub.get_message()?;
        let channel: String = msg.get_channel()?;
        let event: String = msg.get_payload()?;
        // Extract key from channel
        let key = channel.split("__:").last().unwrap_or("");

        let _ = dump_key(
            &mut source_conn,
            &mut target_conn,
            key,
            sync_progress.clone(),
            sync_errors.clone(),
            &event
        );
    }
}

fn process_batch(
    source_client: Arc<Client>,
    target_client: Arc<Client>,
    cursor: Arc<Mutex<i64>>,
    total_processed: Arc<Mutex<usize>>,
    main_progress: ProgressBar,
    copy_errors_progress: ProgressBar,
    batch_size: usize,
) -> Result<(), Box<dyn Error>> {
    let mut source_conn = source_client.get_connection()?;
    let mut target_conn = target_client.get_connection()?;

    loop {
        let mut cursor_guard = cursor.lock().unwrap();
        if *cursor_guard == -1 {
            break;
        }
        let (new_cursor, keys): (i64, Vec<String>) = redis::cmd("SCAN")
            .arg(&*cursor_guard)
            .arg("COUNT")
            .arg(batch_size)
            .query(&mut source_conn)?;

        *cursor_guard = new_cursor;
        drop(cursor_guard);

        for key in &keys {
            let _ = dump_key(
                &mut source_conn,
                &mut target_conn,
                key,
                main_progress.clone(),
                copy_errors_progress.clone(),
                "copy"
            );
        }

        let mut total = total_processed.lock().unwrap();
        *total += keys.len();

        if new_cursor == 0 {
            let mut cursor_guard = cursor.lock().unwrap();
            *cursor_guard = -1;
            break;
        }
        thread::sleep(Duration::from_millis(1000));
    }
    Ok(())
}

fn dump_key(
    source_conn: &mut Connection,
    target_conn: &mut Connection,
    key: &str,
    progress_bar: ProgressBar,
    errors_bar: ProgressBar,
    event: &str,
) -> Result<(), Box<dyn Error>>{
    // Check if the key exists in the source
    progress_bar.inc(1);
    progress_bar.set_message(format!("{} > {}", &event, &key));

    let ignore_events = vec!["del", "hdel", "expired", "expire", "hexpire"];
    if ignore_events.contains(&event) {
        return Ok(());
    }

    let exists: bool = redis::cmd("EXISTS").arg(key).query(source_conn)?;

    if exists {
        // If the key exists, DUMP it from the source
        let dump: Vec<u8> = redis::cmd("DUMP").arg(key).query(source_conn)?;
        // Get the TTL
        let ttl: i64 = redis::cmd("PTTL").arg(key).query(source_conn).unwrap_or(0);
        // RESTORE in the target
        let restore_result: RedisResult<String> = redis::cmd("RESTORE")
            .arg(key)
            .arg(ttl.max(0))  // Use 0 if TTL is negative (no expiration)
            .arg(&dump)
            .arg("REPLACE")  // Replace if key already exists in target
            .query(target_conn);
        if let Err(e) = restore_result {
            progress_bar.inc(1);
            progress_bar.set_message(format!("{} > {}", key, e));
        }
    } else {
        // If the key doesn't exist in source, delete it from target if it exists
        let del_res: RedisResult<String> = redis::cmd("DEL").arg(key).query(target_conn);
        if let Err(e) = del_res {
            errors_bar.inc(1);
            errors_bar.set_message(format!("{} > Failed to delete non-existent key: {}", key, e));
        }
    }
    Ok(())
}