//! Regression test for tracing requests originated by the last proxy.

use agent_client_protocol::schema::v1::{NewSessionRequest, PromptRequest};
use agent_client_protocol::{Agent, Client, Conductor, ConnectTo, Proxy};
use agent_client_protocol_conductor::trace::TraceEvent;
use agent_client_protocol_conductor::{ConductorImpl, ProxiesAndAgent};
use agent_client_protocol_test::testy::{Testy, TestyCommand};
use futures::StreamExt;
use futures::channel::mpsc;
use tokio::io::duplex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

#[derive(Debug)]
struct PromptingProxy;

impl ConnectTo<Conductor> for PromptingProxy {
    async fn connect_to(
        self,
        conductor: impl ConnectTo<Proxy>,
    ) -> Result<(), agent_client_protocol::Error> {
        Proxy
            .builder()
            .name("prompting-proxy")
            .on_receive_request_from(
                Client,
                async |request: NewSessionRequest, responder, connection| {
                    let prompt_connection = connection.clone();
                    connection
                        .build_session_from(request)
                        .on_proxy_session_start(responder, move |session_id| async move {
                            prompt_connection
                                .send_request_to(
                                    Agent,
                                    PromptRequest::new(
                                        session_id,
                                        vec![TestyCommand::Greet.to_prompt().into()],
                                    ),
                                )
                                .block_task()
                                .await?;
                            Ok(())
                        })
                },
                agent_client_protocol::on_receive_request!(),
            )
            .connect_to(conductor)
            .await
    }
}

#[tokio::test]
async fn traces_request_originated_by_last_proxy() -> Result<(), agent_client_protocol::Error> {
    let (trace_tx, trace_rx) = mpsc::unbounded();
    let (editor_write, conductor_read) = duplex(8192);
    let (conductor_write, editor_read) = duplex(8192);

    let conductor = tokio::spawn(async move {
        ConductorImpl::new_agent(
            "conductor",
            ProxiesAndAgent::new(Testy::new()).proxy(PromptingProxy),
        )
        .trace_to(trace_tx)
        .run(agent_client_protocol::ByteStreams::new(
            conductor_write.compat_write(),
            conductor_read.compat(),
        ))
        .await
    });

    tokio::time::timeout(std::time::Duration::from_secs(30), async move {
        yopo::prompt(
            agent_client_protocol::ByteStreams::new(
                editor_write.compat_write(),
                editor_read.compat(),
            ),
            TestyCommand::Echo {
                message: "client prompt".into(),
            }
            .to_prompt(),
        )
        .await
    })
    .await
    .expect("test timed out")?;

    let events = tokio::time::timeout(std::time::Duration::from_secs(1), async move {
        let mut trace_rx = trace_rx;
        let mut events = Vec::new();
        let mut request_id = None;
        while let Some(event) = trace_rx.next().await {
            if let TraceEvent::Request(request) = &event
                && request.from == "Proxy(0)"
                && request.to == "Agent"
                && request.method == "session/prompt"
                && request
                    .params
                    .pointer("/prompt/0/text")
                    .and_then(serde_json::Value::as_str)
                    == Some(TestyCommand::Greet.to_prompt().as_str())
            {
                request_id = Some(request.id.clone());
            }
            let finished = matches!(
                &event,
                TraceEvent::Response(response)
                    if response.from == "Agent"
                        && response.to == "Proxy(0)"
                        && request_id.as_ref() == Some(&response.id)
            );
            events.push(event);
            if finished {
                return events;
            }
        }
        events
    })
    .await
    .expect("timed out waiting for the proxy-originated prompt trace");
    conductor.abort();

    let request_id = events.iter().find_map(|event| match event {
        TraceEvent::Request(request)
            if request.from == "Proxy(0)"
                && request.to == "Agent"
                && request.method == "session/prompt"
                && request
                    .params
                    .pointer("/prompt/0/text")
                    .and_then(serde_json::Value::as_str)
                    == Some(TestyCommand::Greet.to_prompt().as_str()) =>
        {
            Some(request.id.clone())
        }
        _ => None,
    });
    let request_id = request_id.unwrap_or_else(|| {
        panic!("missing Proxy(0) -> Agent session/prompt request in {events:#?}")
    });

    assert!(
        events.iter().any(|event| matches!(
            event,
            TraceEvent::Response(response)
                if response.from == "Agent"
                    && response.to == "Proxy(0)"
                    && response.id == request_id
        )),
        "missing correlated Agent -> Proxy(0) response"
    );

    Ok(())
}
