//
//  PGResult.swift
//  PostgresKit
//
//  Created by Aaron Wright on 3/20/19.
//  Copyright © 2019 Infinite Token. All rights reserved.
//

import Foundation
import libpq

public final class PGResult {
    
    public enum StatusType {
        case emptyQuery
        case commandOK
        case tuplesOK
        case badResponse
        case nonFatalError
        case fatalError
        case singleTuple
        case unknown
    }
    
    private var res: OpaquePointer? = OpaquePointer(bitPattern: 0)
    private var borrowed = false
    
    init(_ res: OpaquePointer?, isBorrowed: Bool = false) {
        self.res = res
        self.borrowed = isBorrowed
    }
    
    deinit {
        close()
    }
    
    func isValid() -> Bool {
        return res != nil
    }
    
    public func close() {
        clear()
    }
    
    public func clear() {
        if let res = self.res {
            if !self.borrowed {
                PQclear(res)
            }
            self.res = OpaquePointer(bitPattern: 0)
        }
    }
    
    public func statusInt() -> Int {
        guard let res = self.res else {
            return 0
        }
        let s = PQresultStatus(res)
        return Int(s.rawValue)
    }
    
    public func status() -> StatusType {
        guard let res = self.res else {
            return .unknown
        }
        let s = PQresultStatus(res)
        switch(s.rawValue) {
        case PGRES_EMPTY_QUERY.rawValue:
            return .emptyQuery
        case PGRES_COMMAND_OK.rawValue:
            return .commandOK
        case PGRES_TUPLES_OK.rawValue:
            return .tuplesOK
        case PGRES_BAD_RESPONSE.rawValue:
            return .badResponse
        case PGRES_NONFATAL_ERROR.rawValue:
            return .nonFatalError
        case PGRES_FATAL_ERROR.rawValue:
            return .fatalError
        case PGRES_SINGLE_TUPLE.rawValue:
            return .singleTuple
        default:
            print("Unhandled PQresult status type \(s.rawValue)")
        }
        return .unknown
    }
    
    public func errorMessage() -> String {
        guard let res = self.res else {
            return ""
        }
        return String(validatingUTF8: PQresultErrorMessage(res)) ?? ""
    }
    
    public func numFields() -> Int {
        guard let res = self.res else {
            return 0
        }
        return Int(PQnfields(res))
    }
    
    public func fieldName(index: Int) -> String? {
        guard let res = self.res,
            let fn = PQfname(res, Int32(index)),
            let ret = String(validatingUTF8: fn) else {
                return nil
        }
        return ret
    }
    
    public func fieldType(index: Int) -> Oid? {
        guard let res = self.res else {
            return nil
        }
        let fn = PQftype(res, Int32(index))
        return fn
    }
    
    public func numTuples() -> Int {
        guard let res = self.res else {
            return 0
        }
        return Int(PQntuples(res))
    }
    
    public func fieldIsNull(tupleIndex: Int, fieldIndex: Int) -> Bool {
        return 1 == PQgetisnull(res, Int32(tupleIndex), Int32(fieldIndex))
    }
    
    public func getFieldString(tupleIndex: Int, fieldIndex: Int) -> String? {
        guard !fieldIsNull(tupleIndex: tupleIndex, fieldIndex: fieldIndex),
            let v = PQgetvalue(res, Int32(tupleIndex), Int32(fieldIndex)) else {
                return nil
        }
        return String(validatingUTF8: v)
    }
    
    public func getFieldBool(tupleIndex: Int, fieldIndex: Int) -> Bool? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return s == "t"
    }
    
    public func getFieldInt(tupleIndex: Int, fieldIndex: Int) -> Int? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return Int(s)
    }
    
    public func getFieldInt8(tupleIndex: Int, fieldIndex: Int) -> Int8? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return Int8(s)
    }
    
    public func getFieldInt16(tupleIndex: Int, fieldIndex: Int) -> Int16? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return Int16(s)
    }
    
   public func getFieldInt32(tupleIndex: Int, fieldIndex: Int) -> Int32? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return Int32(s)
    }
    
    public func getFieldInt64(tupleIndex: Int, fieldIndex: Int) -> Int64? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return Int64(s)
    }
    
    public func getFieldUInt(tupleIndex: Int, fieldIndex: Int) -> UInt? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return UInt(s)
    }
    
    public func getFieldUInt8(tupleIndex: Int, fieldIndex: Int) -> UInt8? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return UInt8(s)
    }
    
    public func getFieldUInt16(tupleIndex: Int, fieldIndex: Int) -> UInt16? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return UInt16(s)
    }
    
    public func getFieldUInt32(tupleIndex: Int, fieldIndex: Int) -> UInt32? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return UInt32(s)
    }
    
    public func getFieldUInt64(tupleIndex: Int, fieldIndex: Int) -> UInt64? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return UInt64(s)
    }
    
    public func getFieldDouble(tupleIndex: Int, fieldIndex: Int) -> Double? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return Double(s)
    }
    
    public func getFieldFloat(tupleIndex: Int, fieldIndex: Int) -> Float? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        return Float(s)
    }
    
    public func getFieldBlob(tupleIndex: Int, fieldIndex: Int) -> [Int8]? {
        guard let s = getFieldString(tupleIndex: tupleIndex, fieldIndex: fieldIndex) else {
            return nil
        }
        let sc = s.utf8
        guard sc.count % 2 == 0, sc.count >= 2, s[s.startIndex] == "\\", s[s.index(after: s.startIndex)] == "x" else {
            return nil
        }
        var ret = [Int8]()
        var index = sc.index(sc.startIndex, offsetBy: 2)
        while index != sc.endIndex {
            let c1 = Int8(sc[index])
            index = sc.index(after: index)
            let c2 = Int8(sc[index])
            guard let byte = byteFromHexDigits(one: c1, two: c2) else {
                return nil
            }
            ret.append(byte)
            index = sc.index(after: index)
        }
        return ret
    }
    
    private func byteFromHexDigits(one c1v: Int8, two c2v: Int8) -> Int8? {
        
        let capA: Int8 = 65
        let capF: Int8 = 70
        let lowA: Int8 = 97
        let lowF: Int8 = 102
        let zero: Int8 = 48
        let nine: Int8 = 57
        
        var newChar = Int8(0)
        
        if c1v >= capA && c1v <= capF {
            newChar = c1v - capA + 10
        } else if c1v >= lowA && c1v <= lowF {
            newChar = c1v - lowA + 10
        } else if c1v >= zero && c1v <= nine {
            newChar = c1v - zero
        } else {
            return nil
        }
        
        newChar = newChar &* 16
        
        if c2v >= capA && c2v <= capF {
            newChar += c2v - capA + 10
        } else if c2v >= lowA && c2v <= lowF {
            newChar += c2v - lowA + 10
        } else if c2v >= zero && c2v <= nine {
            newChar += c2v - zero
        } else {
            return nil
        }
        return newChar
    }
    
}
