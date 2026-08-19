%sql
CREATE CATALOG IF NOT EXISTS fmcg;


%sql
USE catalog fmcg;

%sql
CREATE SCHEMA IF NOT EXISTS fmcg.gold;

%sql
SHOW DATABASES FROM fmcg;

# %sql
# DROP CATALOG IF EXISTS ecommerce CASCADE;

# **Create Bronze and Silver schemas for child company**

%sql
CREATE SCHEMA IF NOT EXISTS fmcg.bronze;
CREATE SCHEMA IF NOT EXISTS fmcg.silver;
