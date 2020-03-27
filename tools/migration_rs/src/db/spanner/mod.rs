use std::sync::Arc;
use std::str::FromStr;

use googleapis_raw::spanner::v1::{
    result_set::ResultSet,
    spanner::{CreateSessionRequest, ExecuteSqlRequest, BeginTransactionRequest, Session},
    transaction::{TransactionOptions, TransactionOptions_ReadWrite, TransactionSelector},
    spanner_grpc::SpannerClient,
};
use grpcio::{
    CallOption, ChannelBuilder, ChannelCredentials, EnvBuilder, MetadataBuilder,
};


use crate::error::{ApiErrorKind, ApiResult};
use crate::settings::Settings;
use crate::db::{User, Bso};
use crate::db::collections::{Collection, Collections};

const MAX_MESSAGE_LEN: i32 = 104_857_600;

#[derive(Clone)]
pub struct Spanner {
    pub client: SpannerClient,
    database_name: String,
}

fn get_path(raw: &str) -> ApiResult<String> {
    let url = match url::Url::parse(raw){
        Ok(v) => v,
        Err(e) => {
            return Err(ApiErrorKind::Internal(format!("Invalid Spanner DSN {}", e)).into())
        }
    };
    Ok(format!("{}{}", url.host_str().unwrap(), url.path()))
}

fn create_session(client: &SpannerClient, database_name: &str) -> Result<Session, grpcio::Error> {
    let mut req = CreateSessionRequest::new();
    req.database = database_name.to_owned();
    let mut meta = MetadataBuilder::new();
    meta.add_str("google-cloud-resource-prefix", database_name)?;
    meta.add_str("x-goog-api-client", "gcp-grpc-rs")?;
    let opt = CallOption::default().headers(meta.build());
    client.create_session_opt(&req, opt)
}

const SPANNER_ADDRESS: &str = "spanner.googleapis.com:443";

impl Spanner {
    pub fn new(settings: &Settings) -> ApiResult<Self> {
        if settings.dsns.spanner.is_none() ||
            settings.dsns.mysql.is_none() {
                return Err(ApiErrorKind::Internal("No DSNs set".to_owned()).into())
            }
        let creds = ChannelCredentials::google_default_credentials()?;
        let env = Arc::new(EnvBuilder::new().build());
        let spanner_path = &settings.dsns.spanner.clone().unwrap();
        dbg!(&spanner_path);
        let database_name = get_path(&spanner_path).unwrap();
        dbg!(&database_name);
        let chan = ChannelBuilder::new(env.clone())
            .max_send_message_len(MAX_MESSAGE_LEN)
            .max_receive_message_len(MAX_MESSAGE_LEN)
            .secure_connect(SPANNER_ADDRESS, creds);

        let client = SpannerClient::new(chan);

        Ok(Self {client, database_name})
    }

    pub async fn transaction(&self, sql: &str) -> ApiResult<ResultSet> {
        let session = create_session(&self.client, &self.database_name)?;

        let mut meta = MetadataBuilder::new();
        match meta.add_str("google-cloud-resource-prefix", &self.database_name) {
            Ok(_) => {},
            Err(e) => {
                return Err(ApiErrorKind::Internal(format!("Could not add prefix meta {:?}", e)).into())
            }
        };
        match meta.add_str("x-goog-api-client", "gcp-grpc-rs"){
            Ok(_) => {},
            Err(e) => {
                return Err(ApiErrorKind::Internal(format!("Could not add client meta {:?}", e)).into())
            }
        };

        /*
        let opts = TransactionOptions::new();
        opts.set_read_write(TransactionOperations_ReadWrite::new());
        session.in_write_transaction = true;
        let mut treq = BeginTransactionRequest::new();
        treq.set_session(session.name.clone());
        treq.set_options(opts);

        let mut txn = self.client.begin_transaction(&treq)?;
        let mut txns = TransactionSelector::new();
        txns.set_id(txn.take_id());
        */
        let mut sreq = ExecuteSqlRequest::new();
        sreq.set_session(session.name.clone());
        // sreq.set_transaction(txns);

        sreq.set_sql(sql.to_owned());
        match self.client.execute_sql(&sreq) {
            Ok(v) => Ok(v),
            Err(e) => {
                Err(ApiErrorKind::Internal(format!("spanner transaction failed: {}", e)).into())
            }
        }
    }

    pub async fn get_collections(&self) -> ApiResult<Collections> {
        let result = self.clone().transaction(
            "SELECT
                DISTINCT uc.collection_id, cc.name,
            FROM
                user_collections as uc,
                collections as cc
            WHERE
                uc.collection_id = cc.collection_id
            ORDER BY
                uc.collection_id"
        ).await?;
        // get the default base of collections (in case the original is missing them)
        let mut collections = Collections::default();
        // back fill with the values from the collection db table, which is our source
        // of truth.
        for row in result.get_rows() {
            let id: u16 = u16::from_str(row.values[0].get_string_value())?;
            let name:&str = row.values[1].get_string_value();
            if collections.get(name).is_none(){
                collections.set(name,
                Collection{
                    name: name.to_owned(),
                    collection: id,
                    last_modified: 0,
                });
            }
        }
        Ok(collections)
    }

    pub async fn add_new_collections(&self, new_collections: Collections) -> ApiResult<ResultSet> {
        // TODO: is there a more ORM way to do these rather than build the sql?
        let header = "INSERT INTO collections (collection_id, name)";
        let mut values = Vec::<String>::new();
        for collection in new_collections.items() {
            values.push(format!("(\"{}\", {})", collection.name, collection.collection));
        };
        self.transaction(&format!("{} VALUES {}", header, values.join(", "))).await
    }

    pub async fn load_user_collections(&mut self, user: &User, collections: Vec<Collection>) -> ApiResult<ResultSet> {
        let mut values: Vec<String> = Vec::new();
        let header = "
            INSERT INTO
                user_collections
                (collection_id,
                 fxa_kid,
                 fxa_uid,
                 modified)";
        for collection in collections {
            values.push(format!("({}, \"{}\", \"{}\", {})",
            collection.collection,
            user.fxa_data.fxa_kid,
            user.fxa_data.fxa_uid,
            collection.last_modified,
            ));
        }
        self.transaction(&format!("{} VALUES {}", header, values.join(", "))).await
    }

    pub async fn add_user_bsos(&mut self, user: &User, bsos: &[Bso], collections: &Collections) -> ApiResult<ResultSet> {
        let header = "
        INSERT INTO
            bso (
                collection_id,
                fxa_kid,
                fxa_uid,
                bso_id,
                expiry,
                modified,
                payload,
                sortindex
            )
            ";
        let mut values:Vec<String> = Vec::new();
        for bso in bsos{
            let collection = collections
                .get(&bso.col_name)
                .unwrap_or(
                    &Collection{
                        collection: bso.col_id,
                        name: bso.col_name.clone(),
                        last_modified:0}).clone();
            // blech
            values.push(format!("({}, \"{}\", \"{}\", {}, {}, {}, \"{}\", {})",
                collection.collection,
                user.fxa_data.fxa_kid,
                user.fxa_data.fxa_uid,
                bso.bso_id,
                bso.expiry,
                bso.modify,
                bso.payload,
                bso.sort_index.unwrap_or(0)
            ));
        };
        self.transaction(&format!("{} VALUES {}", header, values.join(", "))).await
    }
}
