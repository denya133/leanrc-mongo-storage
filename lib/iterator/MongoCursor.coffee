_  = require 'lodash'
RC = require 'RC'
LeanRC = require 'LeanRC'


module.exports = (MongoStorage)->
  class MongoStorage::MongoCursor extends RC::CoreObject
    @inheritProtected()
    @implements LeanRC::CursorInterface

    @Module: MongoStorage

    ipoCursor = @private _cursor: RC::Constants.ANY
    ipcRecord = @private Record: RC::Class

    @public setCursor: Function,
      args: [RC::Constants.ANY]
      return: LeanRC::CursorInterface
      default: (aoCursor)->
        @[ipoCursor] = aoCursor
        return @

    @public setRecord: Function,
      default: (acRecord)->
        @[ipcRecord] = acRecord
        return @

    @public @async toArray: Function,
      default: (acRecord = null)->
        while yield @hasNext()
          yield @next acRecord

    @public @async next: Function,
      default: (acRecord = null)->
        acRecord ?= @[ipcRecord]
        data = yield @[ipoCursor].next()
        if acRecord?
          if data?
            acRecord.new data
          else
            data
        else
          data

    @public @async hasNext: Function,
      default: -> yield @[ipoCursor].hasNext()

    @public @async close: Function,
      default: -> yield @[ipoCursor].close()

    @public @async count: Function,
      default: -> yield @[ipoCursor].count()

    @public @async forEach: Function,
      default: (lambda, acRecord = null)->
        index = 0
        try
          while yield @hasNext()
            yield lambda (yield @next acRecord), index++
          return
        catch err
          yield @close()
          throw err

    @public @async map: Function,
      default: (lambda, acRecord = null)->
        index = 0
        try
          while yield @hasNext()
            yield lambda (yield @next acRecord), index++
        catch err
          yield @close()
          throw err

    @public @async filter: Function,
      default: (lambda, acRecord = null)->
        index = 0
        records = []
        try
          while yield @hasNext()
            record = yield @next acRecord
            if yield lambda record, index++
              records.push record
          records
        catch err
          yield @close()
          throw err

    @public @async find: Function,
      default: (lambda, acRecord = null)->
        index = 0
        _record = null
        try
          while yield @hasNext()
            record = yield @next acRecord
            if yield lambda record, index++
              _record = record
              break
          _record
        catch err
          yield @close()
          throw err

    @public @async compact: Function,
      default: (acRecord = null)->
        acRecord ?= @[ipcRecord]
        index = 0
        records = []
        try
          while yield @hasNext()
            rawRecord = yield @[ipoCursor].next()
            unless _.isEmpty rawRecord
              record = acRecord.new rawRecord
              records.push record
          records
        catch err
          yield @close()
          throw err

    @public @async reduce: Function,
      default: (lambda, initialValue, acRecord = null)->
        try
          index = 0
          _initialValue = initialValue
          while yield @hasNext()
            _initialValue = yield lambda _initialValue, (yield @next acRecord), index++
          _initialValue
        catch err
          yield @close()
          throw err

    @public @async first: Function,
      default: (acRecord = null)->
        try
          if yield @hasNext()
            yield @next acRecord
          else
            null
        catch err
          yield @close()
          throw err

    constructor: (acRecord, aoCursor = null)->
      super arguments...
      @[ipcRecord] = acRecord
      @[ipoCursor] = aoCursor


  return MongoStorage::MongoCursor.initialize()
