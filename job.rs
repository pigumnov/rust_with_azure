use tiberius::{Client, Row, Config};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use tokio::net::TcpStream;
use std::convert::TryFrom;
use std::env;
use uuid::Uuid;

//pigumnov helps stricture
struct PrRecord {
    id: Uuid,
    owner_id: Uuid,
}

impl TryFrom<Row> for PrRecord {
    type Error = tiberius::error::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(PrRecord {
            id: row.get::<Uuid, _>(0).ok_or_else(|| tiberius::error::Error::Conversion("Missing id".into()))?,
            owner_id: row.get::<Uuid, _>(1).ok_or_else(|| tiberius::error::Error::Conversion("Missing owner_id".into()))?,
        })
    }
}

//pigumnov helps stricture with uuid
struct ItemRecord {
    raw_price: f64,
    raw_qty: f64,
    currency_id: Uuid,  
    // category_id: Option<Uuid>,  // todo for test
}

impl TryFrom<Row> for ItemRecord {
    type Error = tiberius::error::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(ItemRecord {
            raw_price: row.get::<f64, _>(0).ok_or_else(|| tiberius::error::Error::Conversion("Missing raw_price".into()))?,
            raw_qty: row.get::<f64, _>(1).ok_or_else(|| tiberius::error::Error::Conversion("Missing raw_qty".into()))?,
            currency_id: row.get::<Uuid, _>(2).ok_or_else(|| tiberius::error::Error::Conversion("Missing currency_id".into()))?,
            // category_id: row.get::<Uuid, _>(3),  //todo for test CategoryId
        })
    }
}

//pigumnov currency structure
// struct CurrencyRecord {
//     id: Uuid,
//     name: String,
// }

//New client function
async fn create_client() -> Result<Client<Compat<TcpStream>>, Box<dyn std::error::Error + Send + Sync>> {
    let database_url = env::var("DATABASE_URL")?;
    let config = Config::from_ado_string(&database_url)?;
    
    let addr = config.get_addr();
    let tcp = TcpStream::connect(addr).await?;
    tcp.set_nodelay(true)?;
    
    let client = Client::connect(config, tcp.compat_write()).await?;
    
    Ok(client)
}

pub async fn run(client: &mut Client<Compat<TcpStream>>) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Get batch tasks with row locking (analog SKIP LOCKED)
    let prs = client
        .simple_query(
            r#"
            SELECT TOP (50) Id, OwnerId
            FROM PurchaseRequests WITH (ROWLOCK, READPAST, UPDLOCK)
            WHERE Processed = 0
            ORDER BY Id
            "#
        )
        .await?
        .into_first_result()
        .await?;

    if prs.is_empty() {
        return Ok(());
    }

    //Decerialize rows to stucture
    let mut pr_records = Vec::new();
    for row in prs {
        pr_records.push(PrRecord::try_from(row)?);
    }

    //pigumnov handle any record in it task
    let mut handles = vec![];
    
    for pr in pr_records {
        let handle = tokio::spawn(async move {
            //pigumnov create client in task
            match create_client().await {
                Ok(mut client_clone) => {
                    if let Err(e) = process_pr(&mut client_clone, pr.id, pr.owner_id).await {
                        tracing::error!("Failed PR {}: {:?}", pr.id, e);
                    }
                },
                Err(e) => {
                    tracing::error!("Failed to create client for PR {}: {:?}", pr.id, e);
                }
            }
        });
        handles.push(handle);
    }

    //pigumnov waiting for all tasks finish
    for handle in handles {
        handle.await?;
    }

    Ok(())
}

async fn process_pr(
    client: &mut Client<Compat<TcpStream>>,
    pr_id: Uuid,
    company_id: Uuid,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    // --- ITEMS ---
    let items = client
        .query(
            r#"
            SELECT RawPrice, RawQty, CurrencyId
            FROM PurchaseRequestItems
            WHERE PurchaseRequestId = @P1
            "#,
            &[&pr_id]
        )
        .await?
        .into_first_result()
        .await?;

    if items.is_empty() {
        mark_done(client, pr_id, None).await?;
        return Ok(());
    }

    //pigumnov rows to structure
    let mut items_data = Vec::new();
    for row in items {
        items_data.push(ItemRecord::try_from(row)?);
    }

    // --- SUM ---
    let sum_price: f64 = items_data.iter()
        .map(|i| i.raw_price * i.raw_qty)
        .sum();

    let currency_id = items_data[0].currency_id;

    // --- CURRENCY ---
    let currency = client
        .query(
            "SELECT Id, name FROM Currencies WHERE Id = @P1",
            &[&currency_id]
        )
        .await?
        .into_row()
        .await?;

    let currency_name = match currency {
        Some(row) => {
            let _currency_id: Uuid = row.get::<Uuid, _>(0)
                .ok_or_else(|| tiberius::error::Error::Conversion("Missing currency id".into()))?;
            row.get::<&str, _>(1).unwrap_or("").to_string()
        },
        None => String::new(),
    };

    // --- AUTOMATION TASK ---
    let task = client
        .query(
            r#"
            SELECT TOP 1 at.Id
            FROM AutomationTasks at
            WHERE at.CompanyId = @P1
              AND at.IsEnabled = 1
              AND (
                    (
                        at.UseMaxPerRequisition = 1
                        AND at.MaxPerRequisition >= @P2
                        AND at.Currency = @P3
                    )
                    OR
                    (
                        at.UseMaxPerRequisition = 0
                        AND at.Currency = @P3
                    )
              )
              AND (
                    NOT EXISTS (
                        SELECT 1 FROM AutomationCategories ac
                        WHERE ac.AutomationTaskId = at.Id
                    )
                    OR
                    EXISTS (
                        SELECT 1
                        FROM AutomationCategories ac
                        JOIN PurchaseRequestItems pri
                          ON pri.CategoryId = ac.Id
                        WHERE ac.AutomationTaskId = at.Id
                          AND pri.PurchaseRequestId = @P4
                    )
              )
            "#,
            &[&company_id, &sum_price, &currency_name, &pr_id]
        )
        .await?
        .into_row()
        .await?;

    let task_id = task.and_then(|row| row.get::<Uuid, _>(0));

    mark_done(client, pr_id, task_id).await?;

    Ok(())
}

async fn mark_done(
    client: &mut Client<Compat<TcpStream>>,
    pr_id: Uuid,
    task_id: Option<Uuid>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {

    client
        .execute(
            r#"
            UPDATE PurchaseRequests
            SET Processed = 1,
                RobotTask = 1,
                AutomationTaskId = @P2
            WHERE Id = @P1
            "#,
            &[&pr_id, &task_id]
        )
        .await?;

    Ok(())
}