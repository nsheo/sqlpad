const tdv = require('odbc');
const appLog = require('../../lib/app-log');
const sqlLimiter = require('sql-limiter');
const { formatSchemaQueryResults } = require('../utils');
const { resolvePositiveNumber } = require('../../lib/resolve-number');

const id = 'tdv';
const name = 'TIBCO Data Virtualization';
const odbc_driver_path = getDriverPath();

function getDriverPath(){
  const path = require('path')
  let driver_base_path = __dirname + path.sep + 'drivers' + path.sep;
  const window_odbc_filename = 'composite86{archtype}.dll'
  const other_odbc_filename = 'libcomposite86{archtype}.so'
  const platform = process.platform
  const archtype = process.arch

  if (platform == "win32") {
    if (archtype == 'x64') {
        return  driver_base_path + 'win' + path.sep + window_odbc_filename.replace('{archtype}', '_x64');
     }
     else {
       return  driver_base_path + 'win' + path.sep + window_odbc_filename.replace('{archtype}', '');
     }
  }
  else {
    let buffer_platform = '';
    if (platform != 'linux' &&  platform != 'alx') {
        buffer_platform = 'linux/';
    } else {
        buffer_platform = platform + path.sep;
    }

    if (archtype == 'x64') {
        return  driver_base_path + buffer_platform + other_odbc_filename.replace('{archtype}', '_x64');
    }
    else {
        return  driver_base_path + buffer_platform + other_odbc_filename.replace('{archtype}', '');
     }
  }
}

function getTDVSchemaSql(database) {
  return `
    SELECT 
       schema_name AS table_schema, 
       table_name AS table_name, 
       column_name AS column_name, 
       data_type AS data_type
    FROM /services/databases/system/ALL_COLUMNS
    WHERE
       DATASOURCE_NAME = '${database}'
    ORDER BY 
      table_schema, 
      table_name, 
      ordinal_position
  `;
}

/**
 * Clean and validate strategies to use for sql-limiter
 * @param {String} limitStrategies - comma delimited list of limit strategies
 */
 function cleanAndValidateLimitStrategies(limitStrategies) {
  const allowed = ['fetch'];
  const strategies = (limitStrategies || '')
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter((s) => s !== '');

  strategies.forEach((strategy) => {
    if (!allowed.includes(strategy)) {
      const allowedStr = allowed.map((s) => `"${s}"`).join(', ');
      throw new Error(
        `Limit strategy "${strategy}" not allowed. Must be one of ${allowedStr}`
      );
    }
  });

  return strategies;
}

/**
 * Run query for connection
 * Should return { rows, incomplete }
 * @param {string} query
 * @param {object} connection
 */

async function runQuery(query, connection) {
  const client = new Client(connection);
  await client.connect();
  try {
    const result = await client.runQuery(query);
    await client.disconnect();
    return result;
  } catch (error) {
    // try disconnecting just to be sure
    await client.disconnect();
    throw error;
  }
}

/**
 * Test connectivity of connection
 * @param {*} connection
 */
function testConnection(connection) {
  const query = "SELECT 'success' AS TestQuery FROM /services/databases/system/dual;";
  return runQuery(query, connection);
}

/**
 * Get schema for connection
 * @param {*} connection
 */
function getSchema(connection) {
  return runQuery(getTDVSchemaSql(connection.database), connection).then((queryResult) =>
    formatSchemaQueryResults(queryResult)
  );
}

function getConnection_string(database, host, port, domain) {
  return 'Driver=' + odbc_driver_path + ";host=" + host + ";port=" + port + ";datasource=" + database + ";domain=" + domain;
}

class Client {
  constructor(connection) {
    this.connection = connection;
    this.client = null;
  }

  async connect() {
    if (this.client) {
      throw new Error('Client already connected');
    }

    const { username, password } = this.connection;
    let domain =  this.connection.domain ?  this.connection.domain : 'composite';
    let host = this.connection.host ? this.connection.host: 'localhost';
    let port = this.connection.port ? this.connection.port: 9401;
    const connection_string = getConnection_string(this.connection.database, host, port, domain);
    let cn = connection_string;

    // Not all drivers require auth
    if (username) {
      cn = cn + ';Uid=' + username;
    }
    if (password) {
      cn = cn + ';Pwd=' + password;
    }

    try {
      this.client = await tdv.connect(cn);
    } catch (error) {
      // unixodb error has additional info about why the error occurred
      // It has an array of objects with messages.
      // If that exists send an error with the first message.
      if (Array.isArray(error.odbcErrors)) {
        const e = error.odbcErrors[0];
        throw new Error(e.message);
      }
      throw error;
    }
  }

  /**
   * Disconnect the connected client
   * Does not propagate error up
   */
  async disconnect() {
    try {
      if (this.client && this.client.close) {
        await this.client.close();
      }
    } catch (error) {
      appLog.error(error, 'error closing connection after error');
    }
    this.client = null;
  }

  async runQuery(query) {
    const limit_strategies= 'fetch';

    // Check to see if a custom maxrows is set, otherwise use default
    const maxRows = resolvePositiveNumber(
      this.connection.maxrows_override,
      this.connection.maxRows
    );

    appLog.info(maxRows, "Check maxRows")

    let cleanedQuery = query;
    const strategies = cleanAndValidateLimitStrategies(limit_strategies);
    let checkMaxRows = true;
    if(cleanedQuery.includes("/services/databases/system/ALL_COLUMNS")) {
      checkMaxRows = false;
    }
    
    if (checkMaxRows && strategies.length) {
      cleanedQuery = sqlLimiter.limit(query, strategies, maxRows + 1);
    }

    appLog.info(cleanedQuery, "Check SQL")

    try {
      let incomplete = false;
      const queryResult = await this.client.query(cleanedQuery);

      // The result of the query seems to be dependent on the odbc driver impmlementation used
      // Try to determine if the result is what we expect. If not, return an empty rows array
      if (
        !queryResult ||
        !queryResult.columns ||
        queryResult.columns.length === 0
      ) {
        return { rows: [] };
      }

      // Format data correctly
      // node-odbc gives a mix of results depending on query type
      // If columns oject returned with results the query returned rows
      const { columns } = queryResult;
      const rows = [];

      if (columns && columns.length > 0) {
        // iterate over queryResult, which is also an array of rows
        for (const row of queryResult) {
          if (checkMaxRows && maxRows) {
            if (rows.length < maxRows) {
              rows.push(row);
            } else {
              incomplete = true;
            }
          } else {
            // Just in case maxRows is not defined push the row
            rows.push(row);
          }
        }
      }

      return { rows, incomplete };
    } catch (error) {
      appLog.error(error);
      // unixodb error has additional info about why the error occurred
      // It has an array of objects with messages.
      // If that exists try to create a message of everything together and throw that
      // Otherwise throw what we got
      if (Array.isArray(error.odbcErrors)) {
        const message = error.odbcErrors.map((e) => e.message).join('; ');
        throw new Error(message);
      }
      throw error;
    }
  }
}


const fields = [
  {
    key: 'host',
    formType: 'TEXT',
    label: 'Host/Server/IP Address',
  },
  {
    key: 'port',
    formType: 'TEXT',
    label: 'Port (optional)',
  },
  {
    key: 'domain',
    formType: 'TEXT',
    label: 'Domain',
  },
  {
    key: 'database',
    formType: 'TEXT',
    label: 'Database',
  },
  {
    key: 'username',
    formType: 'TEXT',
    label: 'Database Username',
  },
  {
    key: 'password',
    formType: 'PASSWORD',
    label: 'Database Password',
  },
  {
    key: 'maxrows_override',
    formType: 'TEXT',
    label: 'Maximum rows to return',
    description: 'Optional',
  },
];

module.exports = {
  Client,
  fields,
  getSchema,
  id,
  name,
  runQuery,
  testConnection,
};
