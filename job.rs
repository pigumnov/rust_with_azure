use tiberius::{Client, Row};
//use tiberius::{Client, Row, Config};
//use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};
use std::convert::TryFrom;
use tokio::net::TcpStream;
use tokio_util::compat::Compat;
//use std::env;
use uuid::Uuid;
//use rust_decimal::Decimal;
//use rust_decimal::prelude::*;
use tiberius::numeric::Numeric;

//pigumnov helps stricture
struct PrRecord {
    id: Uuid,
    owner_id: Uuid,
}

impl TryFrom<Row> for PrRecord {
    type Error = tiberius::error::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(PrRecord {
            id: row
                .get::<Uuid, _>(0)
                .ok_or_else(|| tiberius::error::Error::Conversion("Missing id".into()))?,
            owner_id: row
                .get::<Uuid, _>(1)
                .ok_or_else(|| tiberius::error::Error::Conversion("Missing owner_id".into()))?,
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
            raw_price: {

                let numeric_opt: Option<Numeric> = row.get(0);

                match numeric_opt {
                    Some(numeric) => {
              
                        let value = numeric.value(); // i128
                        let scale = numeric.scale(); // u8
                        (value as f64) / 10_f64.powi(scale as i32)
                    }
                    None => {
                
                        0.0
                    }
                }

     
            },

            raw_qty: {
                let numeric: Numeric = row
                    .get(1)
                    .ok_or_else(|| tiberius::error::Error::Conversion("Missing raw_qty".into()))?;
                let value = numeric.value();
                let scale = numeric.scale();
                (value as f64) / 10_f64.powi(scale as i32)
            },

            currency_id: {
                row.get(2).ok_or_else(|| {
                    tiberius::error::Error::Conversion("Missing currency_id".into())
                })?
            },
        })
    }

}

//pigumnov currency structure


pub async fn run(
    client: &mut Client<Compat<TcpStream>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let prs = client
        .simple_query(
            r#"
            SELECT TOP (50) Id, OwnerId
            FROM PurchaseRequests WITH (ROWLOCK, READPAST, UPDLOCK)
            WHERE Processed = 0
            ORDER BY Id
            "#,
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
    // let mut handles = vec![];

    //     Ok(mut client_cl;
    for pr in pr_records {
        //start
        // --- ITEMS ---
        let items = client
            .query(
                r#"
            SELECT RawPrice, RawQty, CurrencyId
            FROM PurchaseRequestItems
            WHERE PurchaseRequestId = @P1
            "#,
                &[&pr.id],
            )
            .await?
            .into_first_result()
            .await?;

        if items.is_empty() {
            client
                .execute(
                    r#"
            UPDATE PurchaseRequests
            SET Processed = 1
            WHERE Id = @P1
            "#,
                    &[&pr.id],
                )
                .await?;
            // mark_done(client, pr_id, None).await?;
            return Ok(());
        }

        //pigumnov rows to structure
        let mut items_data = Vec::new();
        for row in items {
            items_data.push(ItemRecord::try_from(row)?);
        }

        // --- SUM ---
        let sum_price: f64 = items_data.iter().map(|i| i.raw_price * i.raw_qty).sum();

        let currency_id = items_data[0].currency_id;

        // --- CURRENCY ---
        let currency = client
            .query(
                "SELECT Id, Name FROM Currencies WHERE Id = @P1",
                &[&currency_id],
            )
            .await?
            .into_row()
            .await?;

        let currency_name = match currency {
            Some(row) => {
                let _currency_id: Uuid = row.get::<Uuid, _>(0).ok_or_else(|| {
                    tiberius::error::Error::Conversion("Missing currency id".into())
                })?;
                row.get::<&str, _>(1).unwrap_or("").to_string()
            }
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
                        JOIN Nomenclatures nom
                          ON nom.CategoryId = ac.NomenclatureCategoryId
                        JOIN PurchaseRequestItems pri
                          ON pri.NomenclatureId = nom.Id
                        WHERE ac.AutomationTaskId = at.Id
                          AND pri.PurchaseRequestId = @P4
                    )
              ) 

            "#,
                &[&pr.owner_id, &sum_price, &currency_name, &pr.id],
            )
            .await?
            .into_row()
            .await?;


        let task_id = task.and_then(|row| row.get::<Uuid, _>(0));

        if let Some(_id) = task_id {
            //final
            client
                .execute(
                    r#"
                UPDATE PurchaseRequests
                SET Processed = 1,
                    RobotTask = 1,
                    AutomationTaskId = @P2
                WHERE Id = @P1
                "#,
                    &[&pr.id, &task_id],
                )
                .await?;
        } else {

            client
                .execute(
                    r#"
                UPDATE PurchaseRequests
                SET Processed = 1
                WHERE Id = @P1
                "#,
                    &[&pr.id],
                )
                .await?;
        }

    }
    Ok(())
}

/////Customers GetData

pub struct Customers {
    pub id: Uuid,
    pub name: String,
}

impl TryFrom<Row> for Customers {
    type Error = tiberius::error::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(Customers {
            id: row
                .get::<Uuid, _>(0)
                .ok_or_else(|| tiberius::error::Error::Conversion("Missing id".into()))?,
            name: row
                .get::<&str, _>(1)
                .map(|s| s.to_string())
                .unwrap_or_default(),
        })
    }
}

pub async fn get_data_customers(
    client: &mut Client<Compat<TcpStream>>,
    owner_id: Uuid,
    page: i32,
    per_page: i32,
    sort_field: String,
    sort_asc: bool,
    search: String,
) -> Result<(i64, Vec<Customers>), Box<dyn std::error::Error + Send + Sync>> {
    let sort_col = match sort_field.as_str() {
        "PublicId" => "PublicId",
        _ => "Name",
    };
    let order = if sort_asc { "ASC" } else { "DESC" };

    let offset: i64 = ((page - 1) as i64) * (per_page as i64);

    let total: i64 = if search.trim().is_empty() {
        let row = client
            .query(
                "SELECT COUNT_BIG(*) FROM Customers WHERE OwnerId = @P1 AND Name IS NOT NULL AND Name <> ''",
                &[&owner_id],
            )
            .await?
            .into_row()
            .await?;

        match row {
            Some(r) => r.get::<i64, _>(0).unwrap_or(0),
            None => 0,
        }
    } else {
        let pattern = format!("%{}%", search);
        let row = client
            .query(
                "SELECT COUNT_BIG(*) FROM Customers WHERE OwnerId = @P1 AND Name IS NOT NULL AND Name <> '' AND Name LIKE @P2",
                &[&owner_id, &pattern],
            )
            .await?
            .into_row()
            .await?;

        match row {
            Some(r) => r.get::<i64, _>(0).unwrap_or(0),
            None => 0,
        }
    };

    let customers_rows = if search.trim().is_empty() {
        let sql = format!(
            "SELECT Id, ISNULL(Name, '') AS Name FROM Customers WHERE OwnerId = @P3 AND Name IS NOT NULL AND Name <> '' ORDER BY {} {} OFFSET @P1 ROWS FETCH NEXT @P2 ROWS ONLY",
            sort_col, order
        );

        client
            .query(&sql, &[&offset, &per_page, &owner_id])
            .await?
            .into_first_result()
            .await?
    } else {
        let pattern = format!("%{}%", search);
        let sql = format!(
            "SELECT Id, ISNULL(Name, '') AS Name FROM Customers WHERE OwnerId = @P3 AND Name IS NOT NULL AND Name <> '' AND Name LIKE @P4 ORDER BY {} {} OFFSET @P1 ROWS FETCH NEXT @P2 ROWS ONLY",
            sort_col, order
        );

        client
            .query(&sql, &[&offset, &per_page, &owner_id, &pattern])
            .await?
            .into_first_result()
            .await?
    };

    let mut customer_records = Vec::new();
    for row in customers_rows {
        customer_records.push(Customers::try_from(row)?);
    }

    Ok((total, customer_records))
}

pub struct SupplierIndexDataItem {
    pub id: Uuid,
    pub name: String,
    pub public_id: i32,
    pub status_name: String,
    pub supplier_type: String,
    pub emails: String,
}

impl TryFrom<Row> for SupplierIndexDataItem {
    type Error = tiberius::error::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        Ok(SupplierIndexDataItem {
            id: row
                .get::<Uuid, _>(0)
                .ok_or_else(|| tiberius::error::Error::Conversion("Missing id".into()))?,
            name: row
                .get::<&str, _>(1)
                .map(|s| s.to_string())
                .unwrap_or_default(),
            public_id: row.get::<i32, _>(2).unwrap_or(0),
            status_name: String::new(),
            supplier_type: String::new(),
            emails: String::new(),
        })
    }
}

pub async fn get_data_suppliers(
    client: &mut Client<Compat<TcpStream>>,
    owner_id: Uuid,
    page: i32,
    per_page: i32,
    sort_field: String,
    sort_asc: bool,
    search: String,
    get_potential_sups: bool,
    get_blacklist_sups: bool,
) -> Result<(i64, Vec<SupplierIndexDataItem>), Box<dyn std::error::Error + Send + Sync>> {

    let sort_col = match sort_field.as_str() {
        "PublicId" => "PublicId",
        _ => "Name",
    };
    let order = if sort_asc { "ASC" } else { "DESC" };

    let offset: i64 = ((page - 1) as i64) * (per_page as i64);

    let mut base_sql = String::from("SELECT Id, ISNULL(Name, '') AS Name, ISNULL(PublicId, 0) AS PublicId FROM Suppliers WHERE OwnerId = @P1 AND IsDeleted = 0");


    if get_potential_sups && !get_blacklist_sups {
        base_sql.push_str(" AND (StatusId IN (SELECT Id FROM SupplierStatusNames WHERE Name IN ('Потенциальный','Potential'))) ");
    } else if get_blacklist_sups && !get_potential_sups {
        base_sql.push_str(" AND (StatusId IN (SELECT Id FROM SupplierStatusNames WHERE Name IN ('Чёрный список','Black list'))) ");
    } else if get_blacklist_sups && get_potential_sups {
        base_sql.push_str(" AND (StatusId IN (SELECT Id FROM SupplierStatusNames WHERE Name IN ('Потенциальный','Potential','Чёрный список','Black list'))) ");
    }

   
    let count_sql = format!("SELECT COUNT_BIG(*) FROM ({}) AS t", base_sql);
    let row = client.query(&count_sql, &[&owner_id]).await?.into_row().await?;
    let total: i64 = match row { Some(r) => r.get::<i64, _>(0).unwrap_or(0), None => 0 };

  
    let suppliers_rows = if search.trim().is_empty() {
        let sql = format!("{} ORDER BY {} {} OFFSET @P2 ROWS FETCH NEXT @P3 ROWS ONLY", base_sql, sort_col, order);
        client.query(&sql, &[&owner_id, &offset, &per_page]).await?.into_first_result().await?
    } else {
        let pattern = format!("%{}%", search.trim());
        let mut sql = base_sql.clone();
        sql.push_str(" AND (Name LIKE @P2)");
        sql = format!("{} ORDER BY {} {} OFFSET @P3 ROWS FETCH NEXT @P4 ROWS ONLY", sql, sort_col, order);
        client.query(&sql, &[&owner_id, &pattern, &offset, &per_page]).await?.into_first_result().await?
    };

    let mut supplier_items = Vec::new();
    for row in suppliers_rows {
        supplier_items.push(SupplierIndexDataItem::try_from(row)?);
    }

    //Enrich items with status, type and emails
    for item in supplier_items.iter_mut() {
        //status name
        let status_row = client.query("SELECT s.Name FROM SupplierStatusNames s JOIN Suppliers sup ON sup.StatusId = s.Id WHERE sup.Id = @P1", &[&item.id]).await?.into_row().await?;
        if let Some(r) = status_row {
            item.status_name = r.get::<&str, _>(0).unwrap_or("").to_string();
        }

     
        let type_row = client.query("SELECT t.Name FROM SupplierTypeNames t JOIN Suppliers sup ON sup.TypeId = t.Id WHERE sup.Id = @P1", &[&item.id]).await?.into_row().await?;
        if let Some(r) = type_row {
            item.supplier_type = r.get::<&str, _>(0).unwrap_or("").to_string();
        }

        //emails
        let conts = client.query("SELECT ISNULL(Email, '') FROM SupplierContactPersons WHERE SupplierId = @P1", &[&item.id]).await?.into_first_result().await?;
        let mut emails = Vec::new();
        for r in conts {
            if let Some(e) = r.get::<&str, _>(0) {
                if !e.is_empty() {
                    emails.push(e.to_string());
                }
            }
        }
        item.emails = emails.join(", ");
    }

    Ok((total, supplier_items))
}

