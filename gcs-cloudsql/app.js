/**
 * Background Cloud Function to be triggered by Cloud Storage.
 *
 * @param {object} event The Cloud Functions event.
 */
// Get a reference to the Cloud Storage component
const storage = require('@google-cloud/storage')();
// Get a reference to the Cloud SQL Component
const google = require('googleapis');
const sqlAdmin = google.sqladmin('v1beta4');
const mysql = require('mysql');

// Set the variables for the SQL instance
const database = 'fintech';
const connectionName = 'project:region:database';
const keyfile = require('./sqlcredentials.json');

exports.sqlGCSLoad = function(event) {
    const file = event.data;

    if (file.resourceState === 'not_exists') {
        console.log(`File ${file.name} deleted.`);

        return 'Ok';
    } else if (file.metageneration === '1' && file.name.indexOf(".tmp")===-1) {
        // metageneration attribute is updated on metadata changes.
        // on create value is 1
        console.log(`File ${file.name} added to bucket.`);
        let tableName = file.name.replace('.CSV', '');
        tableName = tableName.replace('.csv', '');
        tableName = tableName.replace(/[^A-Z0-9]+/ig, '');

        return Promise.resolve()
            .then(() => {
                console.log(`Creating the table ${tableName}`);
                var sqlConn = mysql.createConnection({
                    socketPath: `/cloudsql/${connectionName}`,
                    user: 'reportbot',
                    password: 'abab14&lemon',
                    database: database
                });
                sqlConn.connect();
                const sql = `CREATE TABLE IF NOT EXISTS ${tableName} (hour int(4), num_ticks int(8))`;
                return sqlConn.query(sql);
            })
            .then(() => {
                console.log(`Table created now to authenticate with the admin api`);

                let jwtClient = new google.auth.JWT(
                    keyfile.client_email,
                    null,
                    keyfile.private_key, 
                    ['https://www.googleapis.com/auth/cloud-platform'],
                    null
                );

                jwtClient.authorize((err, tokens) => {
                    if (err) {
                        console.log(err);
                        return;
                    }

                    const options = {
                        "importContext": {
                            "csvImportOptions": {
                                "table": tableName
                            },
                            "database": database,
                            "fileType": "CSV",
                            "kind": "sql#importContext",
                            "uri": "gs://mybucket/" + file.name
                        }
                    };

                    const request = {
                        project: 'cf-pipeline',
                        instance: 'dashboard-metrics',
                        resource: options,
                        auth: jwtClient
                    };

                    sqlAdmin.instances.import(request, (err, response) => {
                        if (err) {
                            return Promise.reject(err);
                        }

                        console.log(`SQL import completed for ${file.name}. Results: ` + JSON.stringify(response, null, 2));
                    });
                });
            })
            .catch((err) => {
                console.log(`Job failed for ${file.name}`);
                return Promise.reject(err);
            })
    } else {
        console.log(`File ${file.name} temporary from BigTable.`);

        return 'Ok';
    }
};