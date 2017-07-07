_               = require 'lodash'
{MongoClient}   = require 'mongodb'
{GridFSBucket}  = require 'mongodb'
Parser          = require 'mongo-parse' #mongo-parse@2.0.2
moment          = require 'moment'


###
```coffee
# in application when its need

module.exports = (Module)->
  class MongoCollection extends Module::Collection
    @inheritProtected()
    @module Module

    @include Module::MongoCollectionMixin

  MongoCollection.initialize()
```
###


module.exports = (Module)->
  {
    ANY

    Collection
    QueryableCollectionMixinInterface
    PromiseInterface
    Utils: { co }
    Query
    MongoCursor
  } = Module::

  _connection = null
  _consumers = null

  Module.defineMixin Collection, (BaseClass) ->
    class MongoCollectionMixin extends BaseClass
      @inheritProtected()

      @implements QueryableCollectionMixinInterface

      ipoCollection       = @private collection: PromiseInterface
      ipoBucket           = @private bucket: PromiseInterface

      @public connection: PromiseInterface,
        get: ->
          _connection ?= co =>
            credentials = ''
            { username, password, host, port, dbName } = @getData().mongodb
            if username and password
              credentials =  "#{username}:#{password}@"
            db_url = "mongodb://#{credentials}#{host}:#{port}/#{dbName}?authSource=admin"
            connection = yield MongoClient.connect db_url
            yield return connection
          _connection

      @public collection: PromiseInterface,
        get: ->
          @[ipoCollection] ?= co =>
            # {db, collection: collectionName} = @getData()
            connection = yield @connection
            # voDB = connection.db db
            # yield return voDB.collection collectionName
            yield return connection.collection @collectionFullName()
          @[ipoCollection]

      @public bucket: PromiseInterface,
        get: ->
          @[ipoBucket] ?= co =>
            { dbName } = @configs.mongodb
            # {db, collection: collectionName} = @getData()
            connection = yield @connection
            # voDB = connection.db db
            voDB = connection.db "#{dbName}_fs"
            yield return new GridFSBucket voDB,
              chunkSizeBytes: 64512
              bucketName: 'binary-store'
          @[ipoBucket]

      @public onRegister: Function,
        default: ->
          @super()
          do => @connection
          _consumers ?= 0
          _consumers++
          return

      @public @async onRemove: Function,
        default: ->
          @super()
          _consumers--
          if _consumers is 0
            connection = yield _connection
            yield connection.close(true)
            _connection = undefined
          yield return

      @public @async push: Function,
        default: (aoRecord)->
          voQuery = Query.new()
            .insert aoRecord
            .into @collectionFullName()
          yield @query voQuery
          return yield Module::Promise.resolve yes

      @public @async remove: Function,
        default: (id)->
          voQuery = Query.new()
            .forIn '@doc': @collectionFullName()
            .filter '@doc.id': $eq: id
            .remove()
          yield @query voQuery
          return yield Module::Promise.resolve yes

      @public @async take: Function,
        default: (id)->
          voQuery = Query.new()
            .forIn '@doc': @collectionFullName()
            .filter '@doc.id': $eq: id
            .return '@doc'
          cursor = yield @query voQuery
          return yield cursor.first()

      @public @async takeMany: Function,
        default: (ids)->
          voQuery = Query.new()
            .forIn '@doc': @collectionFullName()
            .filter '@doc.id': $in: ids
            .return '@doc'
          return yield @query voQuery

      @public @async takeAll: Function,
        default: ->
          voQuery = Query.new()
            .forIn '@doc': @collectionFullName()
            .return '@doc'
          return yield @query voQuery

      @public @async override: Function,
        default: (id, aoRecord)->
          voQuery = Query.new()
            .forIn '@doc': @collectionFullName()
            .filter '@doc.id': $eq: id
            .replace aoRecord
            .into @collectionFullName()
          return yield @query voQuery

      @public @async patch: Function,
        default: (id, aoRecord)->
          voQuery = Query.new()
            .forIn '@doc': @collectionFullName()
            .filter '@doc.id': $eq: id
            .update aoRecord
            .into @collectionFullName()
          return yield @query voQuery

      @public @async includes: Function,
        default: (id)->
          voQuery = Query.new()
            .forIn '@doc': @collectionFullName()
            .filter '@doc.id': $eq: id
            .limit 1
            .return '@doc'
          cursor = yield @query voQuery
          return yield cursor.hasNext()

      @public @async length: Function,
        default: ->
          voQuery = Query.new()
            .forIn '@doc': @collectionFullName()
            .count()
          cursor = yield @query voQuery
          obj = yield cursor.first()
          result = (obj).result
          return yield Module::Promise.resolve result

      wrapReference = (value)->
        if _.isString(value)
          if /^\@doc\./.test value
            value.replace '@doc.', ''
          else
            value.replace '@', ''
        else
          value

      buildIntervalQuery = (aoKey, aoInterval, aoIntervalSize, aoDirect)->
        voIntervalStart = aoInterval.startOf(aoIntervalSize).toISOString()
        voIntervalEnd = aoInterval.endOf(aoIntervalSize).toISOString()
        if aoDirect
          $and: [
            "#{aoKey}": $gte: voIntervalStart
            "#{aoKey}": $lt: voIntervalEnd
          ]
        else
          $not: $and: [
            "#{aoKey}": $gte: voIntervalStart
            "#{aoKey}": $lt: voIntervalEnd
          ]

      # @TODO Нужно добавить описание входных параметров опреторам и соответственно их проверку
      @public operatorsMap: Object,
        default:
          # Logical Query Operators
          $and: (def)-> $and: def
          $or: (def)-> $or: def
          $not: (def)-> $not: def
          $nor: (def)-> $nor: def # not or # !(a||b) === !a && !b

          # Comparison Query Operators (aoSecond is NOT sub-query)
          $eq: (aoFirst, aoSecond)->
            "#{wrapReference(aoFirst)}": $eq: wrapReference(aoSecond) # ==
          $ne: (aoFirst, aoSecond)->
            "#{wrapReference(aoFirst)}": $ne: wrapReference(aoSecond) # !=
          $lt: (aoFirst, aoSecond)->
            "#{wrapReference(aoFirst)}": $lt: wrapReference(aoSecond) # <
          $lte: (aoFirst, aoSecond)->
            "#{wrapReference(aoFirst)}": $lte: wrapReference(aoSecond) # <=
          $gt: (aoFirst, aoSecond)->
            "#{wrapReference(aoFirst)}": $gt: wrapReference(aoSecond) # >
          $gte: (aoFirst, aoSecond)->
            "#{wrapReference(aoFirst)}": $gte: wrapReference(aoSecond) # >=
          $in: (aoFirst, alItems)-> # check value present in array
            "#{wrapReference(aoFirst)}": $in: alItems
          $nin: (aoFirst, alItems)-> # ... not present in array
            "#{wrapReference(aoFirst)}": $nin: alItems

          # Array Query Operators
          $all: (aoFirst, alItems)-> # contains some values
            "#{wrapReference(aoFirst)}": $all: alItems
          $elemMatch: (aoFirst, aoSecond)-> # conditions for complex item
            "#{wrapReference(aoFirst)}": $elemMatch: aoSecond
          $size: (aoFirst, aoSecond)->
            "#{wrapReference(aoFirst)}": $size: aoSecond

          # Element Query Operators
          $exists: (aoFirst, aoSecond)-> # condition for check present some value in field
            "#{wrapReference(aoFirst)}": $exists: aoSecond
          $type: (aoFirst, aoSecond)->
            "#{wrapReference(aoFirst)}": $type: aoSecond

          # Evaluation Query Operators
          $mod: (aoFirst, aoSecond)->
            "#{wrapReference(aoFirst)}": $mod: aoSecond
          $regex: (aoFirst, aoSecond, aoThird)-> # value must be string. ckeck it by RegExp.
            [full, regexp, params] = /^\/([\s\S]*)\/(i?m?)$/i.exec aoSecond
            value = $regex: new RegExp regexp, params
            if aoThird?
              value["$options"] = aoThird
            "#{wrapReference(aoFirst)}": value
          $text: ()-> throw new Error 'Not supported'
          $where: ()-> throw new Error 'Not supported'

          # Datetime Query Operators
          $td: (aoFirst, aoSecond)-> # this day (today)
            buildIntervalQuery wrapReference(aoFirst), moment(), 'day', aoSecond
          $ld: (aoFirst, aoSecond)-> # last day (yesterday)
            buildIntervalQuery wrapReference(aoFirst), moment().subtract(1, 'days'), 'day', aoSecond
          $tw: (aoFirst, aoSecond)-> # this week
            buildIntervalQuery wrapReference(aoFirst), moment(), 'week', aoSecond
          $lw: (aoFirst, aoSecond)-> # last week
            buildIntervalQuery wrapReference(aoFirst), moment().subtract(1, 'weeks'), 'week', aoSecond
          $tm: (aoFirst, aoSecond)-> # this month
            buildIntervalQuery wrapReference(aoFirst), moment(), 'month', aoSecond
          $lm: (aoFirst, aoSecond)-> # last month
            buildIntervalQuery wrapReference(aoFirst), moment().subtract(1, 'months'), 'month', aoSecond
          $ty: (aoFirst, aoSecond)-> # this year
            buildIntervalQuery wrapReference(aoFirst), moment(), 'year', aoSecond
          $ly: (aoFirst, aoSecond)-> # last year
            buildIntervalQuery wrapReference(aoFirst), moment().subtract(1, 'years'), 'year', aoSecond

      @public parseFilter: Function,
        args: [Object]
        return: ANY
        default: ({field, parts = [], operator, operand, implicitField})->
          if field? and operator isnt '$elemMatch' and parts.length is 0
            customFilter = @delegate.customFilters[field]
            if (customFilterFunc = customFilter?[operator])?
              customFilterFunc.call @, operand
            else
              @operatorsMap[operator] field, operand
          else if field? and operator is '$elemMatch'
            @operatorsMap[operator] field, parts.reduce (result, part)=>
              if implicitField and not part.field? and (not part.parts? or part.parts.length is 0)
                subquery = @operatorsMap[part.operator] 'temporaryField', part.operand
                Object.assign result, subquery.temporaryField
              else
                Object.assign result, @parseFilter part
            , {}
          else
            @operatorsMap[operator ? '$and'] parts.map @parseFilter.bind @

      @public parseQuery: Function,
        default: (aoQuery)->
          if aoQuery.$join?
            throw new Error '`$join` not available for Mongo queries'
          if aoQuery.$let?
            throw new Error '`$let` not available for Mongo queries'
          if aoQuery.$aggregate?
            throw new Error '`$aggregate` not available for Mongo queries'

          voQuery = {}
          aggUsed = aggPartial = intoUsed = intoPartial = finAggUsed = finAggPartial = null
          isCustomReturn = no

          if aoQuery.$remove?
            do =>
              if aoQuery.$forIn?
                # работа будет только с одной коллекцией, поэтому игнорируем
                voQuery.queryType = 'remove'
                if (voFilter = aoQuery.$filter)?
                  voQuery.filter = @parseFilter Parser.parse voFilter
                voQuery
          else if (voRecord = aoQuery.$insert)?
            do =>
              if aoQuery.$into?
                voQuery.queryType = 'insert'
                # if aoQuery.$forIn?
                  # работа будет только с одной коллекцией, поэтому игнорируем
                voQuery.snapshot = @serializer.serialize voRecord
                voQuery
          else if (voRecord = aoQuery.$update)?
            do =>
              if aoQuery.$into?
                voQuery.queryType = 'update'
                if aoQuery.$forIn?
                  # работа будет только с одной коллекцией, поэтому игнорируем $forIn
                  if (voFilter = aoQuery.$filter)?
                    voQuery.filter = @parseFilter Parser.parse voFilter
                voQuery.snapshot = @serializer.serialize voRecord
                voQuery
          else if (voRecord = aoQuery.$replace)?
            do =>
              if aoQuery.$into?
                voQuery.queryType = 'replace'
                if aoQuery.$forIn?
                  # работа будет только с одной коллекцией, поэтому игнорируем $forIn
                  if (voFilter = aoQuery.$filter)?
                    voQuery.filter = @parseFilter Parser.parse voFilter
                voQuery.snapshot = @serializer.serialize voRecord
                voQuery
          else if aoQuery.$forIn?
            do =>
              voQuery.queryType = 'find'
              voQuery.pipeline = []

              if (voFilter = aoQuery.$filter)?
                voQuery.pipeline.push $match: @parseFilter Parser.parse voFilter

              if (voSort = aoQuery.$sort)?
                voQuery.pipeline.push $sort: voSort.reduce (result, item)->
                  for own asRef, asSortDirect of item
                    result[wrapReference asRef] = if asSortDirect is 'ASC'
                      1
                    else
                      -1
                  result
                , {}

              if (vnOffset = aoQuery.$offset)?
                voQuery.pipeline.push $skip: vnOffset

              if (vnLimit = aoQuery.$limit)?
                voQuery.pipeline.push $limit: vnLimit

              if (voCollect = aoQuery.$collect)?
                isCustomReturn = yes
                collect = {}
                for own asRef, aoValue of voCollect
                  do (asRef, aoValue)=>
                    collect[wrapReference asRef] = wrapReference aoValue

                into = if (vsInto = aoQuery.$into)?
                  wrapReference vsInto
                else
                  'GROUP'
                voQuery.pipeline.push $group:
                  _id: collect
                  "#{into}":
                    $push: Object.keys(@delegate.attributes).reduce (p, c)->
                      p[c] = "$#{c}"
                      p
                    , {}

              if (voHaving = aoQuery.$having)?
                voQuery.pipeline.push $match: @parseFilter Parser.parse voHaving

              if (aoQuery.$count)?
                isCustomReturn = yes
                voQuery.pipeline.push $count: 'result'

              else if (vsSum = aoQuery.$sum)?
                isCustomReturn = yes
                voQuery.pipeline.push $group:
                  _id : null
                  result: $sum: "$#{wrapReference vsSum}"
                voQuery.pipeline.push $project: _id: 0

              else if (vsMin = aoQuery.$min)?
                isCustomReturn = yes
                voQuery.pipeline.push $sort: "#{wrapReference vsMin}": 1
                voQuery.pipeline.push $limit: 1
                voQuery.pipeline.push $project:
                  _id: 0
                  result: "$#{wrapReference vsMin}"

              else if (vsMax = aoQuery.$max)?
                isCustomReturn = yes
                voQuery.pipeline.push $sort: "#{wrapReference vsMax}": -1
                voQuery.pipeline.push $limit: 1
                voQuery.pipeline.push $project:
                  _id: 0
                  result: "$#{wrapReference vsMax}"

              else if (vsAvg = aoQuery.$avg)?
                isCustomReturn = yes
                voQuery.pipeline.push $group:
                  _id : null
                  result: $avg: "$#{wrapReference vsAvg}"
                voQuery.pipeline.push $project: _id: 0

              else
                if (voReturn = aoQuery.$return)?
                  if voReturn isnt '@doc'
                    isCustomReturn = yes
                  if _.isString voReturn
                    if voReturn isnt '@doc'
                      voQuery.pipeline.push
                        $project:
                          _id: 0
                          "#{wrapReference voReturn}": 1
                  else if _.isObject voReturn
                    vhObj = {}
                    projectObj = {}
                    for own key, value of voReturn
                      do (key, value)->
                        vhObj[key] = "$#{wrapReference value}"
                        projectObj[key] = 1
                    voQuery.pipeline.push $addFields: vhObj
                    voQuery.pipeline.push $project: projectObj

                  if aoQuery.$distinct
                    voQuery.pipeline.push $group:
                      _id : '$$CURRENT'

          voQuery.isCustomReturn = isCustomReturn
          return voQuery

      @public @async executeQuery: Function,
        default: (aoQuery, options)->
          collection = yield @collection
          voNativeCursor = switch aoQuery.queryType
            when 'find'
              yield collection.aggregate aoQuery.pipeline, cursor: batchSize: 1
            when 'insert'
              yield collection.insertOne aoQuery.snapshot,
                w: "majority"
                j: yes
                wtimeout: 500
              yield collection.find _id: $eq: aoQuery.snapshot._id
            when 'update'
              yield collection.updateMany aoQuery.filter, $set: aoQuery.snapshot,
                multi: yes
                w: "majority"
                j: yes
                wtimeout: 500
              yield collection.find aoQuery.filter
            when 'replace'
              yield collection.updateMany aoQuery.filter, $set: aoQuery.snapshot,
                multi: yes
                w: "majority"
                j: yes
                wtimeout: 500
              yield collection.find aoQuery.filter
            when 'remove'
              yield collection.deleteMany aoQuery.filter,
                w: "majority"
                j: yes
                wtimeout: 500
              # Строка ниже всегда будет возращать пустой массив.
              # Нужна для того, чтобы была общая логика после свича (создание курсора)
              yield collection.find aoQuery.filter

          voCursor = if aoQuery.isCustomReturn
            MongoCursor.new null, voNativeCursor
          else
            MongoCursor.new @, voNativeCursor
          return voCursor

      @public @async createFileWriteStream: Function,
        args: [Object]
        return: Object
        default: (opts) ->
          # console.log '@@@@@@@!!!!!!! Storage.createFileWriteStream', opts
          bucket = yield @bucket
          yield return bucket.openUploadStream opts._id, {}

      @public @async createFileReadStream: Function,
        args: [Object]
        return: Object
        default: (opts) ->
          # console.log '@@@@@@@!!!!!!! Storage.createFileReadStream', opts
          bucket = yield @bucket
          yield return bucket.openDownloadStreamByName opts._id, {}

      @public @async fileExists: Function,
        args: [Object]
        return: Boolean
        default: (opts, callback) ->
          # console.log '@@@@@@@!!!!!!! Storage.fileExists', opts
          bucket = yield @bucket
          yield return (yield bucket.find filename: opts._id).hasNext()


    MongoCollectionMixin.initializeMixin()
