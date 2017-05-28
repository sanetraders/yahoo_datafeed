const mysql = require('mysql')

const pool = mysql.createPool({
  host:            process.env.DATAFEED_DB_HOST,
  user:            process.env.DATAFEED_DB_USER,
  password:        process.env.DATAFEED_DB_PASSWORD,
  database:        process.env.DATAFEED_DB_DATABASE,
  connectionLimit: process.env.DATAFEED_DB_POOL
})

const queryPromise = (queryString, isCollection=true) => {
  return new Promise((resolve, reject) => {
    pool.query(queryString, (error, results, fields) => {
      if (error) { 
        reject(error)
      } else if (isCollection) {
        resolve(results)
      } else {
        resolve(results[0] || null)
      }
    })
  })
}

exports.search = (searchString, type, exchange, maxRecords=50) => {
  searchString = pool.escape(`%${searchString}%`)
  maxRecords   = parseInt(maxRecords) || 50
  return queryPromise(`SELECT name AS symbol,
                              name AS full_name,
                              description,
                              exchange,
                              symbol_type AS type
                       FROM symbol_infos
                       WHERE symbol_type = ${pool.escape(type)} AND
                       			 exchange = ${pool.escape(exchange)} AND
                       			 (name LIKE ${searchString} OR
                             description LIKE ${searchString})
                       LIMIT ${maxRecords}`)
}

exports.symbolInfo = (symbolName) => {
  return queryPromise(`SELECT name,
                              description,
                              exchange,
                              symbol_type as type
                       FROM symbol_infos
                       WHERE name = ${pool.escape(symbolName)}
                       LIMIT 1`, false)
}