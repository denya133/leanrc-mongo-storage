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
      return: ArangoCursorInterface
      default: (aoCursor)->
        @[ipoCursor] = aoCursor
        return @

    @public setRecord: Function,
      default: (acRecord)->
        @[ipcRecord] = acRecord
        return @

    @public toArray: Function,
      default: (acRecord = null)->
        while @hasNext()
          @next(acRecord)

    @public next: Function,
      default: (acRecord = null)->
        acRecord ?= @[ipcRecord]
        data = @[ipoCursor].next()
        if acRecord?
          if data?
            acRecord.new data
          else
            data
        else
          data

    @public hasNext: Function,
      default: -> @[ipoCursor].hasNext()

    @public close: Function,
      default: -> @[ipoCursor].close()

    @public count: Function,
      default: -> @[ipoCursor].count arguments...

    @public forEach: Function,
      default: (lambda, acRecord = null)->
        index = 0
        try
          while @hasNext()
            lambda @next(acRecord), index++
          return
        catch err
          @dispose()
          throw err

    @public map: Function,
      default: (lambda, acRecord = null)->
        index = 0
        try
          while @hasNext()
            lambda @next(acRecord), index++
        catch err
          @dispose()
          throw err

    @public filter: Function,
      default: (lambda, acRecord = null)->
        index = 0
        records = []
        try
          while @hasNext()
            record = @next(acRecord)
            if lambda record, index++
              records.push record
          records
        catch err
          @dispose()
          throw err

    @public find: Function,
      default: (lambda, acRecord = null)->
        index = 0
        _record = null
        try
          while @hasNext()
            record = @next(acRecord)
            if lambda record, index++
              _record = record
              break
          _record
        catch err
          @dispose()
          throw err

    @public compact: Function,
      default: (acRecord = null)->
        acRecord ?= @[ipcRecord]
        index = 0
        records = []
        try
          while @hasNext()
            rawRecord = @[ipoCursor].next()
            unless _.isEmpty rawRecord
              record = acRecord.new rawRecord
              records.push record
          records
        catch err
          @dispose()
          throw err

    @public reduce: Function,
      default: (lambda, initialValue, acRecord = null)->
        try
          index = 0
          _initialValue = initialValue
          while @hasNext()
            _initialValue = lambda _initialValue, @next(acRecord), index++
          _initialValue
        catch err
          @dispose()
          throw err

    @public first: Function,
      default: (acRecord = null)->
        try
          if @hasNext()
            @next(acRecord)
          else
            null
        catch err
          @dispose()
          throw err

    constructor: (acRecord, aoCursor = null)->
      super arguments...
      @[ipcRecord] = acRecord
      @[ipoCursor] = aoCursor


  return MongoStorage::MongoCursor.initialize()
