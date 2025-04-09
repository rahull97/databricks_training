-- Databricks notebook source
transaction_id,customer_id,amount,currency,transaction_type,timestamp
TXN1001,CUST001,250.75,USD,DEPOSIT,2025-04-08T09:15:00Z
TXN1002,CUST002,100.00,USD,WITHDRAWAL,2025-04-08T09:16:00Z
TXN1003,CUST003,450.50,EUR,TRANSFER,2025-04-08T09:17:00Z
TXN1004,CUST004,300.00,USD,PAYMENT,2025-04-08T09:18:00Z
TXN1005,CUST005,150.00,GBP,DEPOSIT,2025-04-08T09:19:00Z
TXN1006,CUST001,75.25,USD,WITHDRAWAL,2025-04-08T09:20:00Z
TXN1007,CUST006,500.00,USD,DEPOSIT,2025-04-08T09:21:00Z
TXN1008,CUST007,325.50,USD,PAYMENT,2025-04-08T09:22:00Z
TXN1009,CUST008,200.00,EUR,TRANSFER,2025-04-08T09:23:00Z
TXN1010,CUST009,99.99,USD,WITHDRAWAL,2025-04-08T09:24:00Z
TXN1011,CUST010,800.00,USD,DEPOSIT,2025-04-08T09:25:00Z
TXN1012,CUST011,60.00,INR,PAYMENT,2025-04-08T09:26:00Z
TXN1013,CUST012,130.00,USD,TRANSFER,2025-04-08T09:27:00Z
TXN1014,CUST013,510.00,USD,DEPOSIT,2025-04-08T09:28:00Z
TXN1015,CUST014,200.00,GBP,WITHDRAWAL,2025-04-08T09:29:00Z
TXN1016,CUST015,345.00,USD,PAYMENT,2025-04-08T09:30:00Z
TXN1017,CUST016,270.00,USD,DEPOSIT,2025-04-08T09:31:00Z
TXN1018,CUST017,150.00,EUR,TRANSFER,2025-04-08T09:32:00Z
TXN1019,CUST018,320.00,USD,DEPOSIT,2025-04-08T09:33:00Z
TXN1020,CUST019,400.00,USD,PAYMENT,2025-04-08T09:34:00Z
TXN1021,CUST020,100.00,USD,WITHDRAWAL,2025-04-08T09:35:00Z


-- COMMAND ----------

-- DBTITLE 1,customer_profile
{"customer_id": "CUST001", "name": "Alice Smith", "email": "alice@example.com", "phone": "1234567890", "address": "NY, USA", "last_updated": "2025-04-08T08:00:00Z"}
{"customer_id": "CUST002", "name": "Bob Johnson", "email": "bob@example.com", "phone": "2345678901", "address": "LA, USA", "last_updated": "2025-04-08T08:01:00Z"}
{"customer_id": "CUST003", "name": "Charlie Lee", "email": "charlie@example.com", "phone": "3456789012", "address": "London, UK", "last_updated": "2025-04-08T08:02:00Z"}
{"customer_id": "CUST004", "name": "David Kim", "email": "david@example.com", "phone": "4567890123", "address": "Berlin, Germany", "last_updated": "2025-04-08T08:03:00Z"}
{"customer_id": "CUST005", "name": "Ella Brown", "email": "ella@example.com", "phone": "5678901234", "address": "Toronto, Canada", "last_updated": "2025-04-08T08:04:00Z"}
{"customer_id": "CUST006", "name": "Fiona Scott", "email": "fiona@example.com", "phone": "6789012345", "address": "Paris, France", "last_updated": "2025-04-08T08:05:00Z"}
{"customer_id": "CUST007", "name": "George White", "email": "george@example.com", "phone": "7890123456", "address": "Rome, Italy", "last_updated": "2025-04-08T08:06:00Z"}
{"customer_id": "CUST008", "name": "Hannah Green", "email": "hannah@example.com", "phone": "8901234567", "address": "Madrid, Spain", "last_updated": "2025-04-08T08:07:00Z"}
{"customer_id": "CUST009", "name": "Ian Black", "email": "ian@example.com", "phone": "9012345678", "address": "Mumbai, India", "last_updated": "2025-04-08T08:08:00Z"}
{"customer_id": "CUST010", "name": "Julia Grey", "email": "julia@example.com", "phone": "0123456789", "address": "Dubai, UAE", "last_updated": "2025-04-08T08:09:00Z"}
{"customer_id": "CUST011", "name": "Kevin Adams", "email": "kevin@example.com", "phone": "2223334444", "address": "Chicago, USA", "last_updated": "2025-04-08T08:10:00Z"}
{"customer_id": "CUST012", "name": "Linda Perez", "email": "linda@example.com", "phone": "3334445555", "address": "Delhi, India", "last_updated": "2025-04-08T08:11:00Z"}
{"customer_id": "CUST013", "name": "Mike Taylor", "email": "mike@example.com", "phone": "4445556666", "address": "Melbourne, Australia", "last_updated": "2025-04-08T08:12:00Z"}
{"customer_id": "CUST014", "name": "Nina Patel", "email": "nina@example.com", "phone": "5556667777", "address": "Singapore", "last_updated": "2025-04-08T08:13:00Z"}
{"customer_id": "CUST015", "name": "Oscar Wang", "email": "oscar@example.com", "phone": "6667778888", "address": "Tokyo, Japan", "last_updated": "2025-04-08T08:14:00Z"}
{"customer_id": "CUST016", "name": "Priya Sharma", "email": "priya@example.com", "phone": "7778889999", "address": "Bangalore, India", "last_updated": "2025-04-08T08:15:00Z"}
{"customer_id": "CUST017", "name": "Quinn Lin", "email": "quinn@example.com", "phone": "8889990000", "address": "Beijing, China", "last_updated": "2025-04-08T08:16:00Z"}
{"customer_id": "CUST018", "name": "Rachel Singh", "email": "rachel@example.com", "phone": "9990001111", "address": "Hyderabad, India", "last_updated": "2025-04-08T08:17:00Z"}
{"customer_id": "CUST019", "name": "Steve Rogers", "email": "steve@example.com", "phone": "1112223333", "address": "Boston, USA", "last_updated": "2025-04-08T08:18:00Z"}
{"customer_id": "CUST020", "name": "Tina Kapoor", "email": "tina@example.com", "phone": "2223334445", "address": "Pune, India", "last_updated": "2025-04-08T08:19:00Z"}


-- COMMAND ----------

-- DBTITLE 1,account_info
{"account_id": "ACC1001", "customer_id": "CUST001", "account_type": "Savings", "account_balance": 1200.75, "status": "Active", "last_updated": "2025-04-08T07:00:00Z"}
{"account_id": "ACC1002", "customer_id": "CUST002", "account_type": "Checking", "account_balance": 800.00, "status": "Active", "last_updated": "2025-04-08T07:01:00Z"}
{"account_id": "ACC1003", "customer_id": "CUST003", "account_type": "Savings", "account_balance": 5000.25, "status": "Inactive", "last_updated": "2025-04-08T07:02:00Z"}
{"account_id": "ACC1004", "customer_id": "CUST004", "account_type": "Business", "account_balance": 3500.00, "status": "Active", "last_updated": "2025-04-08T07:03:00Z"}
{"account_id": "ACC1005", "customer_id": "CUST005", "account_type": "Savings", "account_balance": 250.50, "status": "Active", "last_updated": "2025-04-08T07:04:00Z"}
{"account_id": "ACC1006", "customer_id": "CUST006", "account_type": "Business", "account_balance": 10000.00, "status": "Closed", "last_updated": "2025-04-08T07:05:00Z"}
{"account_id": "ACC1007", "customer_id": "CUST007", "account_type": "Checking", "account_balance": 450.25, "status": "Active", "last_updated": "2025-04-08T07:06:00Z"}
{"account_id": "ACC1008", "customer_id": "CUST008", "account_type": "Savings", "account_balance": 300.00, "status": "Active", "last_updated": "2025-04-08T07:07:00Z"}
{"account_id": "ACC1009", "customer_id": "CUST009", "account_type": "Savings", "account_balance": 100.00, "status": "Active", "last_updated": "2025-04-08T07:08:00Z"}
{"account_id": "ACC1010", "customer_id": "CUST010", "account_type": "Business", "account_balance": 8500.00, "status": "Active", "last_updated": "2025-04-08T07:09:00Z"}
{"account_id": "ACC1011", "customer_id": "CUST011", "account_type": "Checking", "account_balance": 75.00, "status": "Active", "last_updated": "2025-04-08T07:10:00Z"}
{"account_id": "ACC1012", "customer_id": "CUST012", "account_type": "Savings", "account_balance": 999.99, "status": "Closed", "last_updated": "2025-04-08T07:11:00Z"}
{"account_id": "ACC1013", "customer_id": "CUST013", "account_type": "Checking", "account_balance": 2100.00, "status": "Active", "last_updated": "2025-04-08T07:12:00Z"}
{"account_id": "ACC1014", "customer_id": "CUST014", "account_type": "Savings", "account_balance": 500.00, "status": "Inactive", "last_updated": "2025-04-08T07:13:00Z"}
{"account_id": "ACC1015", "customer_id": "CUST015", "account_type": "Business", "account_balance": 4200.00, "status": "Active", "last_updated": "2025-04-08T07:14:00Z"}
{"account_id": "ACC1016", "customer_id": "CUST016", "account_type": "Savings", "account_balance": 600.00, "status": "Active", "last_updated": "2025-04-08T07:15:00Z"}
{"account_id": "ACC1017", "customer_id": "CUST017", "account_type": "Checking", "account_balance": 155.00, "status": "Active", "last_updated": "2025-04-08T07:16:00Z"}
{"account_id": "ACC1018", "customer_id": "CUST018", "account_type": "Savings", "account_balance": 999.00, "status": "Active", "last_updated": "2025-04-08T07:17:00Z"}
{"account_id": "ACC1019", "customer_id": "CUST019", "account_type": "Checking", "account_balance": 350.00, "status": "Active", "last_updated": "2025-04-08T07:18:00Z"}
{"account_id": "ACC1020", "customer_id": "CUST020", "account_type": "Savings", "account_balance": 888.00, "status": "Active", "last_updated": "2025-04-08T07:19:00Z"}
