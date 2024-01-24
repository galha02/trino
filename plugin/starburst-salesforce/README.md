# Starburst Enterprise Salesforce Connector

Rather than using Salesforce's native APIs and having a lot of code to write and maintain,
we instead leverage CData's Salesforce JDBC Driver to abstract away the complexity of communicating with Salesforce.
This leads to easier integration, however there are some quirks with the driver that developers should be aware of which is detailed below. 

## About the CData JDBC Driver

### Overview

The full documentation for the CData JDBC Driver for Salesforce can be found at [CData's website here](http://cdn.cdata.com/help/RFF/jdbc/default.htm).

### Licensing

Starburst has an OEM contract with CData to use their drivers.
Part of this process was providing CData with a class or wildcarded package that will always be on the JVM stack when interacting with the CData driver.
CData built and provided us with a JAR file that includes this wildcarded package (`com.starburstdata.*`).
All drivers moving forward will have this built in, so we shouldn't need to worry about this.
If at any time the package name changes, we'd need to email `oemsupport@cdata.com` with the new package name so they can send us a new driver.
It typically takes 5 business days for them to provide an updated driver.

In addition to the wildcarded package, CData also gave us an OEM key which we add as a JDBC connection property.
This, combined with the driver, allows us to use any CData JDBC driver on any machine without any expiration date.
The OEM key should be held strictly private to Starburst and should not be given out to users.
The OEM key is hardcoded in `SalesforceConnectionFactory` in case it ever needs updated.

### Support

CData provides support for their drivers.
Email oemsupport@cdata.com to open a ticket with them for any support issues or general questions about the drivers.

### Deploying JDBC Driver to Starburst Maven

CData delivers the OEM drivers to their customers via their Subscription Manager portal or will provide download links via email.
Developers should create an account using their email address and the below Subscription ID at https://www.cdata.com/subscriptions/manage/login.aspx.

Subscription ID for Starburst: `XSURB-VAENT-P3ZNR-2151A-TS8H6`

You can then click on the "Included Products" tab which has all of their drivers available for download.
They provide an executable`setup.jar` file to install the JAR file on your machine.
CData doesn't seem to have any versioning to their drivers besides the year, e.g. 2020.
As we get new JARs for a particular year, we can increment the suffixed version for the Maven artifact version.
View the `pom.xml` file for what version that is currently in use, then deploy the next version using the below command as an example.

```bash
mvn deploy:deploy-file \
-Dfile=/path/to/cdata-salesforce-jdbc.jar \
-DgroupId=com.cdata \
-DartifactId=salesforce-jdbc \
-Dversion=2022-3 \
-Dpackaging=jar \
-DrepositoryId=starburstdata.releases \
-Durl=https://starburstdata-sep-cicd-843985043183.d.codeartifact.us-east-2.amazonaws.com/maven/releases/
```

## Custom Tables and Columns in Salesforce

Salesforce identifies any custom objects a user creates by adding `__c` to the object name.
This means when we run a `CREATE TABLE` command, the table and column names will all end in `__c`.
This complicates testing since it is expected that you're able to insert and select data using the same names when you made the table.
Because of this, the `TestSalesforceConnectorTest` contains a copy of all tests from the `BaseJdbcConnectorTest` and parent
classes with the queries changed to use table and column names ending in `__c`.
Additionally, each table has some Salesforce system columns that were not declared in the DDL but will show up when the table is described.
Further, to share same salesforce instance across different repositories' tests and to
uniquely identify repository specific tables, tables are suffixed with repository identifier.

You'll also notice some classes that were copied from `trino-testing` to support the `TestSalesforceTypeMapping`.
They were copied because 1) the table and column names are different and 2) CData caches metadata which is documented below.

## CData Metadata Cache

CData drivers automatically cache metadata about tables and columns from Salesforce.
This metadata is not refreshed whenever a new table is created, so inserting data after creating a table may fail.
You can reset the metadata cache by executing `RESET SCHEMA CACHE` on the connection.
CData keeps a cache per distinct set of JDBC connection properties, so if you are using a separate connection
you will need to make sure you provide the same set of properties.
The SalesforceJdbcClient will invalidate the cache after a table is created.

In practice, I am not sure how much users will hit this metadata caching issue.
The connector is read-only, so they won't be able to create tables and insert data using Trino.
They'd have to open a Trino connection, run a query against Salesforce to cache metadata, create the table in Salesforce and upload data to it, and then go and query it again.


## Data Type Mappings

[CData documentation](http://cdn.cdata.com/help/RFF/jdbc/pg_datatypemapping.htm) for type mappings.

`TestSalesforceTypeMapping` uses only static table names rather than a table with a random suffix.
Tables created by the driver are "custom objects", and Salesforce has a limit on the number of custom objects you can have.
When a table is dropped, the custom object is soft-deleted and goes into a bin where it is eventually
hard deleted after a couple weeks. Because of this, we can quickly reach the limit on the number of custom
objects due to CI builds. There is no way to programatically hard delete a table, so instead
we create static table names and truncate them rather than deleting them. The Salesforce CI job is configured
to only run one job at a time across all builds.

Additionally, the TPC-H tables are also only created and the data is only loaded if they do not exist.
This is to cut down on the custom object limit as well as to cut down on CI build times as
loading the TPC-H data into Salesforce can take several minutes.

Note that these custom objects can be deleted by hand in Salesforce using the Object Manager.

### Booleans

Salesforce has a "Checkbox" data type which mato a CData `bool`.
In practice, if you create a table with a `boolean` type the driver wants a max length, and then it turns into a `varchar` on read.
You then pass strings to the drivers for the values, e.g. `0` or `1` for `boolean(1)` and `true` or `false` for `boolean(5)`.

For example, you can create a table like below, however describing the table results in the columns `a` and `b` to be `varchar(1)` and `varchar(5)` respectively.

```sql
CREATE TABLE test_booleans (a boolean(1), b boolean(5))
```

For this reason Booleans are untested but supported in the column mappings.

### Numeric types

All numeric types from the CData driver come back as a `double` type.
We have column mappings for integer, bigint, and decimal however they are not tested at this time as the data type metadata come back as `double` on read. 

### Timestamp/Time Precision

Salesforce documents state millisecond precision, however the CData driver truncates all precision timestamp and time types with no rounding.
This is for both reads and writes to Salesforce.

### Varchar

Salesforce has a max length of 255 characters for `varchar` types and requires the length to be specified


## CI Tests - Password Expiration

**Note:** The tests use basic user/password auth. User's password periodically expires every three months. 
Tests using expired password will fail with an `INVALID_OPERATION_WITH_EXPIRED_PASSWORD` error.
A new password will need to be created upon expiration, which generates a new security token as well.

1. Login to https://starburstdata--partial.sandbox.my.salesforce.com/ as `sep.salesforcedl.test2@starburstdata.com` and the current (expired) password
2. You will be prompted to create a new password. Once created, an email is sent to `sep.salesforcedl.test2@starburstdata.com` containing the new security token
   * Following associates are part of the DL.  
   Wojciech Biela, Piotr Findeisen, Mateusz Gajewski, Ashhar Hasan, Grzegorz Kokosinski, Anu Sudarsan, Mayank Vadariya
3. Update the secret value for `SALESFORCE_USER2_PASSWORD` and `SALESFORCE_USER2_SECURITY_TOKEN` to the new values through script located 
   in [sep-ci-infra](https://github.com/starburstdata/sep-ci-infra/tree/main/secrets) repo.
4. Let the CI build run; the tests should no longer fail with an expired password error