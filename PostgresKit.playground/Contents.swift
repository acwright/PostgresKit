import Cocoa
import PostgresKit

let connection = PGConnection()
let status = connection.connectdb("postgresql://127.0.0.1/postgres")
let result = connection.exec(statement: "SELECT version();")

result.getFieldString(tupleIndex: 0, fieldIndex: 0)

connection.close()
