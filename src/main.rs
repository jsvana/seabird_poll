pub mod proto {
    pub mod common {
        tonic::include_proto!("common");
    }
    pub mod seabird {
        tonic::include_proto!("seabird");
    }

    pub use self::common::*;
    pub use self::seabird::*;
}

use std::sync::Arc;

use anyhow::{format_err, bail, Context, Result};
use async_trait::async_trait;
use env_logger::Env;
use http::Uri;
use log::{error, info};
use maplit::hashmap;
use tokio::sync::Mutex;
use tokio_postgres::{Config as PostgresConfig, NoTls};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;

use proto::seabird_client::SeabirdClient;

#[derive(Debug)]
struct DbConfig {
    username: String,
    password: String,
    hostname: String,
    port: u16,
    database: String,
}

impl DbConfig {
    fn from_env() -> Result<Self> {
        let username = dotenv::var("DATABASE_USERNAME")
            .ok()
            .with_context(|| "Missing $DATABASE_USERNAME value")?;

        let password = dotenv::var("DATABASE_PASSWORD")
            .ok()
            .with_context(|| "Missing $DATABASE_PASSWORD value")?;

        let hostname = dotenv::var("DATABASE_HOSTNAME").unwrap_or("localhost".to_string());

        let port = dotenv::var("DATABASE_PORT")
            .unwrap_or("5432".to_string())
            .parse()
            .with_context(|| "$DATABASE_PORT is not a valid u16")?;

        let database = dotenv::var("DATABASE_NAME")
            .ok()
            .with_context(|| "Missing $DATABASE_NAME value")?;

        Ok(Self {
            username,
            password,
            hostname,
            port,
            database,
        })
    }
}

#[derive(Debug)]
struct Config {
    token: String,
    protocol: String,
    hostname: String,
    port: u16,

    db_config: DbConfig,
}

impl Config {
    fn from_env() -> Result<Self> {
        let token = dotenv::var("CORE_TOKEN")
            .ok()
            .with_context(|| "Missing $CORE_TOKEN value")?;

        let protocol = dotenv::var("CORE_PROTOCOL").unwrap_or("http".to_string());

        let hostname = dotenv::var("CORE_HOSTNAME").unwrap_or("localhost".to_string());

        let port = dotenv::var("CORE_PORT")
            .unwrap_or("11235".to_string())
            .parse()
            .with_context(|| "$CORE_PORT is not a valid u16")?;

        let db_config = DbConfig::from_env()?;

        Ok(Self {
            token,
            protocol,
            hostname,
            port,
            db_config,
        })
    }
}

#[async_trait]
trait ReplyTo {
    async fn reply_to(&self, event: &proto::CommandEvent, message: &str) -> Result<()>;
}

#[async_trait]
impl ReplyTo for Arc<Mutex<SeabirdClient<Channel>>> {
    async fn reply_to(&self, event: &proto::CommandEvent, message: &str) -> Result<()> {
        let request = tonic::Request::new(proto::SendMessageRequest {
            channel_id: event
                .source
                .as_ref()
                .map(|s| s.channel_id.to_string())
                .ok_or_else(|| format_err!("message missing channel_id"))?,
            text: format!(
                "{}: {}", source_nick(event)?, message),
        });
        self.lock().await.send_message(request).await?;

        Ok(())
    }
}

fn source_nick(event: &proto::CommandEvent) -> Result<String> {
    event
        .source
        .as_ref()
        .map(|s| s.user.clone())
        .ok_or_else(|| format_err!("message missing user"))?
        .map(|u| u.display_name.to_string())
        .ok_or_else(|| format_err!("message's user missing display_name"))
}

async fn handle_command(
    client: Arc<Mutex<SeabirdClient<Channel>>>,
    event: &proto::CommandEvent,
    pg_client: &tokio_postgres::Client,
) -> Result<()> {
    match event.command.as_str() {
        "poll" => {
            if event.arg.len() == 0 {
                bail!("usage: poll <question>");
            }

            let row = pg_client
                .query_one(
                    r#"
                        INSERT INTO
                            polls
                            (
                                creator,
                                question
                            )
                        VALUES
                            (
                                $1,
                                $2
                            )
                        RETURNING id
                    "#,
                    &[&source_nick(event)?, &event.arg],
                )
                .await?;

            let id = row.try_get::<_, i32>(0)?;

            client.reply_to(event, &format!("ID {}", id)).await?;
        }
        "poll_options" => {
            if event.arg.len() == 0 {
                bail!("usage: poll_options <poll_id>");
            }

            let poll_id: i32 = event.arg.parse()?;

            let row = pg_client
                .query_one(
                    r#"
                        SELECT id, question
                        FROM polls
                        WHERE id = $1
                    "#,
                    &[&poll_id],
                )
                .await?;
            client.reply_to(event, &format!("Poll {}: {}", row.try_get::<_, i32>(0)?, row.try_get::<_, String>(1)?)).await?;

            let rows = pg_client
                .query(
                    r#"
                        SELECT id, text
                        FROM poll_options
                        WHERE poll_id = $1
                    "#,
                    &[&poll_id],
                )
                .await?;

            for row in rows.into_iter() {
                client.reply_to(event, &format!("Option [ID {}]: {}", row.try_get::<_, i32>(0)?, row.try_get::<_, String>(1)?)).await?;
            }
        }
        "add_poll_option" => {
            let args: Vec<_> = event.arg.splitn(2, " ").into_iter().collect();

            let poll_id: i32 = args.get(0).ok_or_else(|| format_err!("usage: add_poll_option <poll_id> <option>"))?.parse()?;
            let text = args.get(1).ok_or_else(|| format_err!("usage: add_poll_option <poll_id> <text>"))?;

            let row = pg_client
                .query_one(
                    r#"
                        INSERT INTO
                            poll_options
                            (
                                poll_id,
                                text,
                                creator
                            )
                        VALUES
                            (
                                $1,
                                $2,
                                $3
                            )
                        RETURNING id
                    "#,
                    &[&poll_id, text, &source_nick(event)?],
                )
                .await?;

            let id = row.try_get::<_, i32>(0)?;

            client.reply_to(event, &format!("added option {}", id)).await?;
        }
        "vote" => {
            // TODO(jsvana): handle multiple votes in the same poll
            if event.arg.len() == 0 {
                bail!("usage: vote <option_id>");
            }

            let option_id: i32 = event.arg.parse()?;

            pg_client
                .execute(
                    r#"
                        INSERT INTO poll_votes
                        (option_id, voter)
                        VALUES ($1, $2)
                    "#,
                    &[&option_id, &source_nick(event)?],
                )
                .await?;

            client.reply_to(event, "your vote has been noted").await?;
        }
        "poll_results" => {
            if event.arg.len() == 0 {
                bail!("usage: poll_results <poll_id>");
            }

            let poll_id: i32 = event.arg.parse()?;

            let row = pg_client
                .query_one(
                    r#"
                        SELECT id, question
                        FROM polls
                        WHERE id = $1
                    "#,
                    &[&poll_id],
                )
                .await?;
            client.reply_to(event, &format!("Poll {}: {}", row.try_get::<_, i32>(0)?, row.try_get::<_, String>(1)?)).await?;

            let rows = pg_client
                .query(
                    r#"
                        SELECT poll_options.id, poll_options.text, COUNT(poll_votes.id)
                        FROM poll_options
                        LEFT JOIN poll_votes ON poll_options.id = poll_votes.option_id
                        WHERE poll_options.poll_id = $1
                        GROUP BY poll_options.id, poll_options.text
                        ORDER BY COUNT(poll_votes.id) DESC
                    "#,
                    &[&poll_id],
                )
                .await?;

            for row in rows.into_iter() {
                let votes = row.try_get::<_, i64>(2)?;
                let suffix = if votes == 1 {
                    ""
                } else {
                    "s"
                };
                client.reply_to(event, &format!("Option [ID {}]: {}. {} vote{}",
                                                row.try_get::<_, i32>(0)?,
                                                row.try_get::<_, String>(1)?,
                                                votes,
                                                suffix
                                                )).await?;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn command_loop(
    client: Arc<Mutex<SeabirdClient<Channel>>>,
    pg_client: tokio_postgres::Client,
) -> Result<()> {
    let request = tonic::Request::new(proto::StreamEventsRequest {
        commands: hashmap! {
            "poll".into() => proto::CommandMetadata {
                name: "poll".into(),
                short_help: "create a new poll".into(),
                full_help: "create a new poll".into(),
            },
            "poll_options".into() => proto::CommandMetadata {
                name: "poll_options".into(),
                short_help: "shows all poll options".into(),
                full_help: "shows all poll options".into(),
            },
            "add_poll_option".into() => proto::CommandMetadata {
                name: "add_poll_option".into(),
                short_help: "adds an option to a poll".into(),
                full_help: "adds an option to a poll".into(),
            },
            "vote".into() => proto::CommandMetadata {
                name: "vote".into(),
                short_help: "vote in a poll".into(),
                full_help: "vote in a poll".into(),
            },
            "poll_results".into() => proto::CommandMetadata {
                name: "poll_results".into(),
                short_help: "get the results of a poll".into(),
                full_help: "get the results of a poll".into(),
            },
        },
    });

    let mut stream = client
        .lock()
        .await
        .stream_events(request)
        .await?
        .into_inner();

    while let Some(event) = stream.message().await? {
        if let Some(proto::event::Inner::Command(event)) = event.inner {
            if let Err(e) = handle_command(client.clone(), &event, &pg_client).await {
                client.reply_to(&event, &format!("error handling command: {}", e)).await?
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::from_env(Env::default().default_filter_or("info")).init();

    let config = Config::from_env()?;

    let (pg_client, connection) = PostgresConfig::new()
        .user(&config.db_config.username)
        .password(&config.db_config.password)
        .host(&config.db_config.hostname)
        .port(config.db_config.port)
        .dbname(&config.db_config.database)
        .connect(NoTls)
        .await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("postgres connection error: {}", e);
        }
    });

    pg_client
        .batch_execute(
            r#"
    CREATE TABLE IF NOT EXISTS polls (
        id SERIAL PRIMARY KEY,
        creator VARCHAR(512) NOT NULL,
        question VARCHAR(512) NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        state VARCHAR(32) DEFAULT 'active'
    );

    CREATE TABLE IF NOT EXISTS poll_options (
        id SERIAL PRIMARY KEY,
        creator VARCHAR(512) NOT NULL,
        text VARCHAR(512) NOT NULL,
        poll_id INT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_poll_id FOREIGN KEY(poll_id) REFERENCES polls(id),
        CONSTRAINT unique_text UNIQUE (poll_id, text)
    );

    CREATE TABLE IF NOT EXISTS poll_votes (
        id SERIAL PRIMARY KEY,
        voter VARCHAR(512) NOT NULL,
        option_id INT NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_option_id FOREIGN KEY(option_id) REFERENCES poll_options(id)
    );
    "#,
        )
        .await?;

    let uri_string = format!("{}://{}:{}", config.protocol, config.hostname, config.port);
    let uri: Uri = uri_string.parse().context("failed to build URI")?;
    let mut channel_builder = Channel::builder(uri);

    if config.protocol == "https" {
        channel_builder = channel_builder.tls_config(ClientTlsConfig::new());
    }

    let channel = channel_builder
        .connect()
        .await
        .context("Failed to connect")?;

    info!("connected to {}", uri_string);

    let token: MetadataValue<_> =
        format!("Bearer {}", config.token).parse()?;
    let client = Arc::new(Mutex::new(SeabirdClient::with_interceptor(
        channel,
        move |mut request: Request<()>| {
            request
                .metadata_mut()
                .insert("authorization", token.clone());
            Ok(request)
        },
    )));

    command_loop(client.clone(), pg_client).await
}
