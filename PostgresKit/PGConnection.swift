//
//  PostgreSQL.swift
//  PostgresKit
//
//  Created by Aaron Wright on 3/20/19.
//  Copyright © 2019 Infinite Token. All rights reserved.
//

import Foundation
import libpq

public final class PGConnection {
    
    public enum StatusType {
        case ok
        case bad
    }
    
    var conn = OpaquePointer(bitPattern: 0)
    var connectInfo: String = ""
    
    public init() {}
    
    deinit {
        close()
    }
    
    public func connectdb(_ info: String) -> StatusType {
        conn = PQconnectdb(info)
        connectInfo = info
        return status()
    }
    
    public func close() {
        finish()
    }
    
    public func finish() {
        if conn != nil {
            PQfinish(conn)
            conn = OpaquePointer(bitPattern: 0)
        }
    }
    
    public func status() -> StatusType {
        let status = PQstatus(conn)
        return status == CONNECTION_OK ? .ok : .bad
    }
    
    public func errorMessage() -> String {
        return String(validatingUTF8: PQerrorMessage(conn)) ?? ""
    }
    
    public func exec(statement: String) -> PGResult {
        return PGResult(PQexec(conn, statement))
    }
    
}
