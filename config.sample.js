module.exports = {
  nodeAddress: '',
  mysql: {
    client: 'mysql',
    connection: {
      host: 'localhost',
      user: 'root',
      password: 'xxxxx',
      database: 'nanodb_development'
    },
    pool: {
      min: 2,
      max: 10
    }
  }
}
