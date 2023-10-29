use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topic {
    topic_name: String,
    partition_number: u32,
}

pub struct TopicMgr {
    db_connection: Arc<Mutex<Connection>>,
    topic_cache: Arc<RwLock<HashMap<String, Topic>>>,
}

impl TopicMgr {
    pub fn new(db_path: &str) -> Self {
        let base_dir = PathBuf::from(db_path);
        let db_file_path = base_dir.join("topic.db");
        let conn = Connection::open(db_file_path).unwrap();

        // create table for topic meta
        conn.execute(
            "CREATE TABLE IF NOT EXISTS topic (\
            id INTEGER PRIMARY KEY, \
            topic_name TEXT, \
            partition_number INTEGER)",
            [],
        ).unwrap();

        TopicMgr {
            db_connection: Arc::new(Mutex::new(conn)),
            topic_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn create_topic(&self, topic: Topic) -> Result<()> {
        let conn = self.db_connection.lock().unwrap();
        conn.execute(
            "INSERT INTO topic (topic_name, partition_number) VALUES (?1, ?2)",
            params![topic.topic_name, topic.partition_number],
        )?;

        let mut topics = self.topic_cache.write().unwrap();

        let topic_name = topic.topic_name.clone();
        let topic_clone = topic.clone();

        topics.insert(topic_name, topic_clone);

        Ok(())
    }

    pub fn delete_topic(&self, topic_name: &str) -> Result<()> {
        let conn = self.db_connection.lock().unwrap();
        conn.execute("DELETE FROM topic WHERE topic_name=?1",
                     params![topic_name],
        )?;

        let mut topics = self.topic_cache.write().unwrap();
        topics.remove(topic_name);

        Ok(())
    }

    pub fn list_topics(&self) -> Result<Vec<Topic>> {
        let conn = self.db_connection.lock().unwrap();
        let mut stmt = conn.prepare("SELECT * FROM topic")?;
        let topic_iter = stmt.query_map([], |row| {
            Ok(Topic {
                topic_name: row.get(1)?,
                partition_number: row.get(2)?,
            })
        });

        match topic_iter {
            Ok(_) => {
                let mut topic_list = Vec::new();
                for topic_result in topic_iter.unwrap() {
                    topic_list.push(topic_result.unwrap());
                };

                Ok(topic_list)
            }
            Err(error) => {
                Err(error)
            }
        }
    }

    pub fn get_topic_info(&self, topic_name: &str) -> Result<Topic> {
        let topics = self.topic_cache.read().unwrap();
        if let Some(topic) = topics.get(topic_name) {
            println!("Name: {}", topic.topic_name);

            return Ok(topic.clone());
        }

        let conn = self.db_connection.lock().unwrap();
        conn.query_row(
            "SELECT * FROM topic WHERE topic_name=?1",
            [topic_name],
            |row| {
                Ok(Topic {
                    topic_name: row.get(1)?,
                    partition_number: row.get(2)?,
                })
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use tempfile::{TempDir};
    use std::io::{Result};
    use crate::topic_mgr::{Topic, TopicMgr};

    pub fn create_temp_dir(prefix: &str) -> TempDir {
        tempfile::Builder::new().prefix(prefix).tempdir().unwrap()
    }

    #[tokio::test]
    pub async fn test_write_read() -> Result<()> {
        let dir_path = create_temp_dir("topic_mgr_test");
        // Create or open the memory-mapped file.
        let mut topic_mgr = TopicMgr::new(dir_path.path().to_str().unwrap());

        topic_mgr.create_topic(Topic {
            topic_name: "test_topic_name".to_string(),
            partition_number: 4,
        }).unwrap();

        topic_mgr.get_topic_info("test_topic_name").expect("topic should exist");
        topic_mgr.get_topic_info("test_topic_name_xxx").expect_err("topic should not exist");

        topic_mgr.list_topics().expect("topic list should exist");

        topic_mgr.delete_topic("test_topic_name").unwrap();

        Ok(())
    }
}