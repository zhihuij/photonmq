use std::net::SocketAddr;
use std::sync::Arc;
use async_trait::async_trait;

use axum::{routing::{get, post}, http::{Response}, body::{Body}, Router, debug_handler, Json};
use axum::extract::State;
use serde_json::Value;
use crate::config::ConfigOptions;

use crate::server::Server;
use crate::message::{ConsumeMessageRequest, Message};
use crate::storage::msg_store::MessageStore;
use crate::topic_mgr::{Topic, TopicMgr};

pub struct HttpServer;

#[debug_handler]
async fn produce_message(State(msg_store_state): State<Arc<MessageStore>>,
                         Json(produce_msg): Json<Message>) -> Response<Body> {
    println!("produce message: {:?}", &produce_msg);

    let write_result = msg_store_state.write_msg(produce_msg).await;
    match write_result {
        Ok(_) => {
            Response::new(Body::from("Hello, Produce Message"))
        }
        Err(error) => {
            let err_msg = format!("Write message error: {:?}", error);
            Response::new(Body::from(err_msg))
        }
    }
}

#[debug_handler]
async fn consume_message(State(msg_store_state): State<Arc<MessageStore>>,
                         Json(consume_msg): Json<ConsumeMessageRequest>) -> Response<Body> {
    println!("consume message: {:?}", &consume_msg);

    let read_result = msg_store_state.read_msg(consume_msg).await;
    match read_result {
        Ok(msg_list) => {
            let msg_json = serde_json::to_string(&msg_list).unwrap();
            Response::new(Body::from(msg_json))
        }
        Err(error) => {
            let err_msg = format!("consume message error: {:?}", error);
            Response::new(Body::from(err_msg))
        }
    }
}

#[debug_handler]
async fn create_topic(State(topic_mgr_state): State<Arc<TopicMgr>>,
                      Json(new_topic): Json<Topic>) -> Response<Body> {
    let _ = topic_mgr_state.create_topic(new_topic);
    Response::new(Body::from("create ok"))
}

#[debug_handler]
async fn delete_topic(State(topic_mgr_state): State<Arc<TopicMgr>>,
                      Json(topic_info): Json<Value>) -> Response<Body> {
    let _ = topic_mgr_state.delete_topic(topic_info["topic_name"].as_str().unwrap());
    Response::new(Body::from("delete ok"))
}

#[debug_handler]
async fn get_topic(State(topic_mgr_state): State<Arc<TopicMgr>>,
                   Json(topic_info): Json<Value>) -> Response<Body> {
    let topic_info_result = topic_mgr_state.get_topic_info(topic_info["topic_name"].as_str().unwrap());
    match topic_info_result {
        Ok(topic_info) => {
            let result_json_str = serde_json::to_string(&topic_info).unwrap();
            Response::new(Body::from(result_json_str))
        }
        Err(error) => {
            let err_msg = format!("Write message error: {:?}", error);
            Response::new(Body::from(err_msg))
        }
    }
}

#[debug_handler]
async fn list_topics(State(topic_mgr_state): State<Arc<TopicMgr>>) -> Response<Body> {
    let topic_list = topic_mgr_state.list_topics().unwrap();
    let result_json_str = serde_json::to_string(&topic_list).unwrap();
    Response::new(Body::from(result_json_str))
}


#[async_trait]
impl Server for HttpServer {
    async fn start(&self, listening: SocketAddr, config: ConfigOptions) {
        let msg_store = MessageStore::new(&config).unwrap();
        let msg_store_state = Arc::new(msg_store);

        let topic_mgr = TopicMgr::new(config.topic_store_path.as_str()).unwrap();
        let topic_mgr_state = Arc::new(topic_mgr);

        let message_routes = Router::new()
            .route("/produce_message", post(produce_message))
            .route("/consume_message", get(consume_message))
            .with_state(msg_store_state);

        let topic_routes = Router::new()
            .route("/create_topic", post(create_topic))
            .route("/delete_topic", post(delete_topic))
            .route("/get_topic", get(get_topic))
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