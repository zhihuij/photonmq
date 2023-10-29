use std::net::SocketAddr;
use std::sync::Arc;
use async_trait::async_trait;

use axum::{routing::{get, post}, http::{Response}, body::{Body}, Router, debug_handler, Json};
use axum::extract::State;
use config::Config;
use serde_json::Value;
use crate::commit_log::CommitLog;

use crate::server::Server;
use crate::message::{ConsumeMessage, Message};
use crate::topic_mgr::{Topic, TopicMgr};

pub struct HttpServer;

#[debug_handler]
async fn produce_message(State(commit_log_state): State<Arc<CommitLog>>, body: String) -> Response<Body> {
    let body_str = body.as_str();
    let message = Message::decode_json(body_str).unwrap();

    println!("produce message: {:?}", &message);

    let write_result = commit_log_state.write_records(message.encode().unwrap()).await;
    match write_result {
        Ok(_) => {
            Response::new(Body::from("Hello, Produce Message"))
        }
        Err(error) => {
            let err_msg = format!("Write message error: {}", error);
            Response::new(Body::from(err_msg))
        }
    }
}

#[debug_handler]
async fn consume_message(State(commit_log_state): State<Arc<CommitLog>>, body: String) -> Response<Body> {
    let body_str = body.as_str();
    let consume_msg = ConsumeMessage::decode_json(body_str).unwrap();

    println!("consume message: {:?}", &consume_msg);

    let read_result = commit_log_state.read_records(consume_msg.offset as usize).await;
    match read_result {
        Ok(msg_bytes) => {
            let msg_result = Message::decode(msg_bytes.as_slice()).unwrap();
            let msg_json = msg_result.encode_json().unwrap();

            let consumed_msg = format!("{:?}", msg_json);
            Response::new(Body::from(consumed_msg))
        }
        Err(error) => {
            let err_msg = format!("consume message error: {}", error);
            Response::new(Body::from(err_msg))
        }
    }
}

#[debug_handler]
async fn create_topic(State(topic_mgr_state): State<Arc<TopicMgr>>,
                      Json(new_topic): Json<Topic>) -> Response<Body> {
    let create_result = topic_mgr_state.create_topic(new_topic);
    Response::new(Body::from("create ok"))
}

#[debug_handler]
async fn delete_topic(State(topic_mgr_state): State<Arc<TopicMgr>>,
                      Json(topic_info): Json<Value>) -> Response<Body> {
    let delete_result = topic_mgr_state.delete_topic(topic_info["topic_name"].as_str().unwrap());
    Response::new(Body::from("delete ok"))
}

#[debug_handler]
async fn list_topics(State(topic_mgr_state): State<Arc<TopicMgr>>) -> Response<Body> {
    let topic_list = topic_mgr_state.list_topics().unwrap();
    let result_json_str = serde_json::to_string(&topic_list).unwrap();
    Response::new(Body::from(result_json_str))
}


#[async_trait]
impl Server for HttpServer {
    async fn start(&self, listening: SocketAddr, config: &Config) {
        let commit_log = CommitLog::new(
            config.get_string("msg_store_path").unwrap().as_str(), 1024).unwrap();
        let commit_log_state = Arc::new(commit_log);

        let topic_mgr = TopicMgr::new(
            config.get_string("topic_store_path").unwrap().as_str());
        let topic_mgr_state = Arc::new(topic_mgr);

        let message_routes = Router::new()
            .route("/produce_message", post(produce_message))
            .route("/consume_message", get(consume_message))
            .with_state(commit_log_state);

        let topic_routes = Router::new()
            .route("/create_topic", post(create_topic))
            .route("/delete_topic", post(delete_topic))
            .route("/list_topics", get(list_topics))
            .with_state(topic_mgr_state);

        let app = Router::new()
            .merge(message_routes)
            .merge(topic_routes);

        // Start the Axum server on the specified address.
        let server = axum::Server::bind(&listening)
            .serve(app.into_make_service())
            .with_graceful_shutdown(async {
                tokio::signal::ctrl_c()
                    .await
                    .expect("failed to install CTRL+C signal handler");
            });

        println!("Axum HTTP server listening on http://{}", &listening);

        if let Err(e) = server.await {
            eprintln!("Axum HTTP server error: {}", e);
        }
    }
}