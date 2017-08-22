/**
 * Background Cloud Function to be triggered by Cloud Storage.
 *
 * @param {object} event The Cloud Functions event.
 */
// Get a reference to the Cloud Storage component
const storage = require('@google-cloud/storage')();
// Get a reference to the BigQuery component
const bigquery = require('@google-cloud/bigquery')();
const project = 'project';
const datasetName = 'dataset';

/**
 * Helper method to get a handle on a BigQuery table. Automatically creates the
 * dataset and table if necessary.
 */
function getTable(datasetName, tableName, params) {
    const dataset = bigquery.dataset(datasetName);

    return dataset.get({
            autoCreate: true
        })
        .then(([dataset]) => dataset.createTable(tableName, params));
}

exports.bqGCSLoad = function(event) {
    const file = event.data;
    const tableName = file.name.replace('.csv', '');

    if (file.resourceState === 'not_exists') {
        console.log(`File ${file.name} deleted.`);

        return 'Ok';
    } else if (file.metageneration === '1') {
        // metageneration attribute is updated on metadata changes.
        // on create value is 1
        console.log(`File ${file.name} uploaded.`);

        return Promise.resolve()
            .then(() => {
                const options = {
                    schema: 'venue:STRING,currencies:STRING,time:TIMESTAMP,bid:FLOAT,ask:FLOAT'
                };

                return getTable(datasetName, tableName, options);
            })
            .then(([table]) => {
                console.log(`Table created. Now to load data`);
                const fileObj = storage.bucket(file.bucket).file(file.name);
                const metadata = {
                    sourceFormat: 'CSV'
                }
                return table.import(fileObj, metadata);
            })
            .then(([job]) => job.promise())
            .then(() => {
                console.log(`Insert job complete for ${file.name}`);
                const sourceTable = '[' + project + ':' + datasetName + '.' + tableName + ']';
                const destTable = bigquery.dataset(datasetName).table(tableName + '_CLEANSED');

                const query = `SELECT HOUR(time) AS hour, COUNT(time) AS num_ticks
                          FROM ${sourceTable}
                          WHERE time BETWEEN TIMESTAMP("2014-01-16 00:00:00.000") AND TIMESTAMP("2014-01-16 23:59:59.999")
                          GROUP BY hour ORDER BY hour ASC;`;
                var options = {
                    destination: destTable,
                    query: query
                }

                return bigquery.startQuery(options);
            })
            .then(([job]) => job.promise())
            .then(() => {
                console.log(`Query finished for ${tableName}. Now to start the export to GCS.`);
                const exportFile = storage.bucket('cleansed-data').file(tableName + '_CLEANSED.CSV');

                return bigquery.dataset(datasetName).table(tableName + '_CLEANSED').export(exportFile);
            })
            .then(([job]) => job.promise())
            .then(() => console.log(`BigQuery actions and export completed for ${file.name}.`))
            .catch((err) => {
                console.log(`Job failed for ${file.name}`);
                return Promise.reject(err);
            })
    } else {
        console.log(`File ${file.name} metadata updated.`);

        return 'Ok';
    }
};