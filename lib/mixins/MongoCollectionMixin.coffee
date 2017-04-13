_               = require 'lodash'
{MongoClient}   = require 'mongodb'
{GridFSBucket}  = require 'mongodb'
Parser          = require 'mongo-parse' #mongo-parse@2.0.2
moment          = require 'moment'
RC              = require 'RC'
LeanRC          = require 'LeanRC'


###
```coffee
# in application when its need

LeanRC = require 'LeanRC'
MongoStorage = require 'leanrc-mongo-storage'

module.exports = (App)->
  class App::MongoCollection extends LeanRC::Collection
    @include MongoStorage::MongoCollectionMixin

  return App::MongoCollection.initialize()
```
###


module.exports = (MongoStorage)->
  class MongoStorage::MongoCollectionMixin extends RC::Mixin
    @inheritProtected()
    @implements LeanRC::QueryableMixinInterface

    @Module: MongoStorage

    @public onRegister: Function,
      default: ->
        @super()
        do => @connection
        return

    cpoConnection = @private @static _connection: RC::PromiseInterface
    ipoCollection = @private _collection: RC::PromiseInterface
    ipoBucket     = @private _bucket: RC::PromiseInterface

    @public connection: RC::PromiseInterface,
      get: ->
        @constructor[cpoConnection] ?= RC::Utils.co =>
          credentials = ''
          {username, password, host, port, default_db} = @getData()
          if username and password
            credentials =  "#{username}:#{password}@"
          db_url = "mongodb://#{credentials}#{host}:#{port}/#{default_db}?authSource=admin"
          conn = yield MongoClient.connect db_url
          return conn
        @constructor[cpoConnection]

    @public collection: RC::PromiseInterface,
      get: ->
        @[ipoCollection] ?= RC::Utils.co =>
          {db, collection} = @getData()
          conn = yield @connection
          voDB = conn.db db
          voDB.collection collection
        @[ipoCollection]

    @public bucket: RC::PromiseInterface,
      get: ->
        @[ipoBucket] ?= RC::Utils.co =>
          {db, collection} = @getData()
          conn = yield @connection
          voDB = conn.db db
          new GridFSBucket voDB,
            chunkSizeBytes: 64512
            bucketName: collection
        @[ipoBucket]

    @public @async push: Function,
      default: (aoRecord)->
        voQuery = LeanRC::Query.new()
          .insert aoRecord
          .into @collectionFullName()
        yield @query voQuery
        return yes

    @public @async remove: Function,
      default: (id)->
        voQuery = LeanRC::Query.new()
          .forIn '@doc': @collectionFullName()
          .filter '@doc._key': {$eq: id}
          .remove()
        yield @query voQuery
        return yes

    @public @async take: Function,
      default: (id)->
        voQuery = LeanRC::Query.new()
          .forIn '@doc': @collectionFullName()
          .filter '@doc._key': {$eq: id}
          .return '@doc'
        cursor = yield @query voQuery
        cursor.first()

    @public @async takeMany: Function,
      default: (ids)->
        voQuery = LeanRC::Query.new()
          .forIn '@doc': @collectionFullName()
          .filter '@doc._key': {$in: ids}
          .return '@doc'
        yield @query voQuery

    @public @async takeAll: Function,
      default: ->
        voQuery = LeanRC::Query.new()
          .forIn '@doc': @collectionFullName()
          .return '@doc'
        yield @query voQuery

    @public @async override: Function,
      default: (id, aoRecord)->
        voQuery = LeanRC::Query.new()
          .forIn '@doc': @collectionFullName()
          .filter '@doc._key': {$eq: id}
          .replace aoRecord
        yield @query voQuery

    @public @async patch: Function,
      default: (id, aoRecord)->
        voQuery = LeanRC::Query.new()
          .forIn '@doc': @collectionFullName()
          .filter '@doc._key': {$eq: id}
          .update aoRecord
        yield @query voQuery

    @public @async includes: Function,
      default: (id)->
        voQuery = LeanRC::Query.new()
          .forIn '@doc': @collectionFullName()
          .filter '@doc._key': {$eq: id}
          .limit 1
          .return '@doc'
        cursor = yield @query voQuery
        cursor.hasNext()

    @public @async length: Function,
      default: ->
        voQuery = LeanRC::Query.new()
          .forIn '@doc': @collectionFullName()
          .count()
        cursor = yield @query voQuery
        cursor.first()

    wrapReference = (value)->
      if _.isString(value) and /^\@doc\./.test value
        value.replace '@doc.', ''
      else
        value.replace '@', ''

    @public operatorsMap: Object,
      default:
        # Logical Query Operators
        $and: (def)-> $and: def
        $or: (def)-> $or: def
        $not: (def)-> $not: def
        $nor: (def)-> $nor: def # not or # !(a||b) === !a && !b

        # Comparison Query Operators (aoSecond is NOT sub-query)
        $eq: (aoFirst, aoSecond)->
          $eq: "#{wrapReference(aoFirst)}": wrapReference(aoSecond) # ==
        $ne: (aoFirst, aoSecond)->
          $neq: "#{wrapReference(aoFirst)}": wrapReference(aoSecond) # !=
        $lt: (aoFirst, aoSecond)->
          $lt: "#{wrapReference(aoFirst)}": wrapReference(aoSecond) # <
        $lte: (aoFirst, aoSecond)->
          $lte: "#{wrapReference(aoFirst)}": wrapReference(aoSecond) # <=
        $gt: (aoFirst, aoSecond)->
          $gt: "#{wrapReference(aoFirst)}": wrapReference(aoSecond) # >
        $gte: (aoFirst, aoSecond)->
          $gte: "#{wrapReference(aoFirst)}": wrapReference(aoSecond) # >=
        $in: (aoFirst, alItems)-> # check value present in array
          $in: "#{wrapReference(aoFirst)}": alItems
        $nin: (aoFirst, alItems)-> # ... not present in array
          $nin: "#{wrapReference(aoFirst)}": alItems

        # Array Query Operators
        $all: (aoFirst, alItems)-> # contains some values
          $and: alItems.map (aoItem)->
            $in: "#{wrapReference(aoItem)}": wrapReference(aoFirst)
        $elemMatch: (aoFirst, aoSecond)-> # conditions for complex item
          $elemMatch: "#{wrapReference(aoFirst)}": $and: aoSecond
        $size: (aoFirst, aoSecond)->
          $size: "#{wrapReference(aoFirst)}": aoSecond

        # Element Query Operators
        $exists: (aoFirst, aoSecond)-> # condition for check present some value in field
          $exists: "#{wrapReference(aoFirst)}": aoSecond
        $type: (aoFirst, aoSecond)->
          $exists: "#{wrapReference(aoFirst)}": aoSecond

        # Evaluation Query Operators
        $mod: (aoFirst, aoSecond)->
          $mod: "#{wrapReference(aoFirst)}": aoSecond
        $regex: (aoFirst, aoSecond)-> # value must be string. ckeck it by RegExp.
          $regex: "#{wrapReference(aoFirst)}": new RefExp aoSecond

        # Datetime Query Operators
        $td: (aoFirst, aoSecond)-> # this day (today)
          todayStart = moment().startOf 'day'
          todayEnd = moment().endOf 'day'
          if aoSecond
            $and: [
              $gte: "#{wrapReference(aoFirst)}": todayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": todayEnd.toISOString()
            ]
          else
            $not: $and: [
              $gte: "#{wrapReference(aoFirst)}": todayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": todayEnd.toISOString()
            ]
        $ld: (aoFirst, aoSecond)-> # last day (yesterday)
          yesterdayStart = moment().subtract(1, 'days').startOf 'day'
          yesterdayEnd = moment().subtract(1, 'days').endOf 'day'
          if aoSecond
            $and: [
              $gte: "#{wrapReference(aoFirst)}": yesterdayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": yesterdayEnd.toISOString()
            ]
          else
            $not: $and: [
              $gte: "#{wrapReference(aoFirst)}": yesterdayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": yesterdayEnd.toISOString()
            ]
        $tw: (aoFirst, aoSecond)-> # this week
          weekStart = moment().startOf 'week'
          weekEnd = moment().endOf 'week'
          if aoSecond
            $and: [
              $gte: "#{wrapReference(aoFirst)}": weekStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": weekEnd.toISOString()
            ]
          else
            $not: $and: [
              $gte: "#{wrapReference(aoFirst)}": weekStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": weekEnd.toISOString()
            ]
        $lw: (aoFirst, aoSecond)-> # last week
          weekStart = moment().subtract(1, 'weeks').startOf 'week'
          weekEnd = weekStart.clone().endOf 'week'
          if aoSecond
            $and: [
              $gte: "#{wrapReference(aoFirst)}": weekStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": weekEnd.toISOString()
            ]
          else
            $not: $and: [
              $gte: "#{wrapReference(aoFirst)}": weekStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": weekEnd.toISOString()
            ]
        $tm: (aoFirst, aoSecond)-> # this month
          firstDayStart = moment().startOf 'month'
          lastDayEnd = moment().endOf 'month'
          if aoSecond
            $and: [
              $gte: "#{wrapReference(aoFirst)}": firstDayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": lastDayEnd.toISOString()
            ]
          else
            $not: $and: [
              $gte: "#{wrapReference(aoFirst)}": firstDayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": lastDayEnd.toISOString()
            ]
        $lm: (aoFirst, aoSecond)-> # last month
          firstDayStart = moment().subtract(1, 'months').startOf 'month'
          lastDayEnd = firstDayStart.clone().endOf 'month'
          if aoSecond
            $and: [
              $gte: "#{wrapReference(aoFirst)}": firstDayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": lastDayEnd.toISOString()
            ]
          else
            $not: $and: [
              $gte: "#{wrapReference(aoFirst)}": firstDayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": lastDayEnd.toISOString()
            ]
        $ty: (aoFirst, aoSecond)-> # this year
          firstDayStart = moment().startOf 'year'
          lastDayEnd = firstDayStart.clone().endOf 'year'
          if aoSecond
            $and: [
              $gte: "#{wrapReference(aoFirst)}": firstDayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": lastDayEnd.toISOString()
            ]
          else
            $not: $and: [
              $gte: "#{wrapReference(aoFirst)}": firstDayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": lastDayEnd.toISOString()
            ]
        $ly: (aoFirst, aoSecond)-> # last year
          firstDayStart = moment().subtract(1, 'years').startOf 'year'
          lastDayEnd = firstDayStart.clone().endOf 'year'
          if aoSecond
            $and: [
              $gte: "#{wrapReference(aoFirst)}": firstDayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": lastDayEnd.toISOString()
            ]
          else
            $not: $and: [
              $gte: "#{wrapReference(aoFirst)}": firstDayStart.toISOString()
              $lt: "#{wrapReference(aoFirst)}": lastDayEnd.toISOString()
            ]

    @public parseFilter: Function,
      args: [Object]
      return: RC::Constants.ANY
      default: ({field, parts, operator, operand, implicitField})->
        if field? and operator isnt '$elemMatch' and parts.length is 0
          @operatorsMap[operator] field, operand
        else if field? and operator is '$elemMatch'
          if implicitField is yes
            @operatorsMap[operator] field, parts.map (part)=>
              @parseFilter part
          else
            @operatorsMap[operator] field, parts.map (part)=>
              @parseFilter part
        else
          @operatorsMap[operator ? '$and'] parts.map @parseFilter.bind @

    @public parseQuery: Function,
      default: (aoQuery)->
        voQuery = null
        aggUsed = aggPartial = intoUsed = intoPartial = finAggUsed = finAggPartial = null
        if aoQuery.$remove?
          do =>
            if aoQuery.$forIn?
              # работа будет только с одной коллекцией, поэтому игнорируем
              if aoQuery.$join?
                throw new Error '`$join` not available for Mongo queries'
              if aoQuery.$let?
                throw new Error '`$let` not available for Mongo queries'
              voQuery.queryType = 'remove'
              if (voFilter = aoQuery.$filter)?
                voQuery.filter = @parseFilter Parser.parse voFilter
              voQuery
        else if (voRecord = aoQuery.$insert)?
          do =>
            if aoQuery.$into?
              if aoQuery.$forIn?
                # работа будет только с одной коллекцией, поэтому игнорируем
                if aoQuery.$join?
                  throw new Error '`$join` not available for Mongo queries'
                if aoQuery.$let?
                  throw new Error '`$let` not available for Mongo queries'
              voQuery.queryType = 'insert'
              voQuery.snapshot = @serializer.serialize voRecord
              voQuery
        else if (voRecord = aoQuery.$update)?
          do =>
            if aoQuery.$into?
              voQuery.queryType = 'update'
              if aoQuery.$forIn?
                # работа будет только с одной коллекцией, поэтому игнорируем $forIn
                if aoQuery.$join?
                  throw new Error '`$join` not available for Mongo queries'
                if aoQuery.$let?
                  throw new Error '`$let` not available for Mongo queries'

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
                if aoQuery.$join?
                  throw new Error '`$join` not available for Mongo queries'
                if aoQuery.$let?
                  throw new Error '`$let` not available for Mongo queries'
                if (voFilter = aoQuery.$filter)?
                  voQuery.filter = @parseFilter Parser.parse voFilter

              voQuery.snapshot = @serializer.serialize voRecord

              voQuery
        else if aoQuery.$forIn?
          do =>
            voQuery.queryType = 'find'
            voQuery.pipeline = []

            if aoQuery.$join?
              throw new Error '`$join` not available for Mongo queries'
            if aoQuery.$let?
              throw new Error '`$let` not available for Mongo queries'
            if aoQuery.$aggregate?
              throw new Error '`$aggregate` not available for Mongo queries'

            if (voFilter = aoQuery.$filter)?
              voQuery.pipeline.push $match: @parseFilter Parser.parse voFilter

            if (voSort = aoQuery.$sort)?
              voQuery.pipeline.push $sort: aoQuery.$sort.reduce (item, prev)->
                prev[wrapReference asRef] = if asSortDirect is 'ASC'
                  1
                else
                  -1
                prev
              , {}

            if (vnOffset = aoQuery.$offset)?
              voQuery.pipeline.push $skip: vnOffset

            if (vnLimit = aoQuery.$limit)?
              voQuery.pipeline.push $limit: vnLimit


            if (voCollect = aoQuery.$collect)?
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
                  $push: Object.keys(@delegate.attributes).reduce (c, p)->
                    p[c] = "$#{c}"
                  , {}

            if (voHaving = aoQuery.$having)?
              voQuery.pipeline.push $match: @parseFilter Parser.parse voHaving

            if (aoQuery.$count)?
              voQuery.pipeline.push $count: 'result'
              voQuery.pipeline.push $replaceRoot: "$result"

            else if (vsSum = aoQuery.$sum)?
              voQuery.pipeline.push $group:
                _id : null
                result: $sum: "${wrapReference vsSum}"
              voQuery.pipeline.push $replaceRoot: "$result"

            else if (vsMin = aoQuery.$min)?
              voQuery.pipeline.push $sort: "#{wrapReference vsMin}": 1
              voQuery.pipeline.push $limit: 1
              voQuery.pipeline.push $replaceRoot: "$#{wrapReference vsMin}"

            else if (vsMax = aoQuery.$max)?
              voQuery.pipeline.push $sort: "#{wrapReference vsMax}": -1
              voQuery.pipeline.push $limit: 1
              voQuery.pipeline.push $replaceRoot: "$#{wrapReference vsMax}"

            else if (vsAvg = aoQuery.$avg)?
              voQuery.pipeline.push $group:
                _id : null
                result: $avg: "${wrapReference vsAvg}"
              voQuery.pipeline.push $replaceRoot: "$result"

            else
              if (voReturn = aoQuery.$return)?
                if _.isString aoQuery.$return
                  unless voReturn isnt '@doc'
                    voQuery.pipeline.push
                      $replaceRoot: "$#{wrapReference voReturn}"
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
                  voQuery.pipeline.push $project: _id : 0
                  voQuery.pipeline.push $group:
                    _id : '$$CURRENT'
                  voQuery.pipeline.push $replaceRoot: "$_id"


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
            yield collection.find _key: $eq: aoQuery.snapshot._key
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
              w: "majority", j: yes, wtimeout: 500
            yield collection.find aoQuery.filter

        voCursor = MongoStorage::MongoCursor.new @delegate, voNativeCursor
        return voCursor

    @public @async createFileWriteStream: Function,
      args: [Object]
      return: Object
      default: (opts) ->
        console.log '@@@@@@@!!!!!!! Storage.createFileWriteStream', opts
        bucket = yield @bucket
        bucket.openUploadStream opts._id, {}

    @public @async createFileReadStream: Function,
      args: [Object]
      return: Object
      default: (opts) ->
        console.log '@@@@@@@!!!!!!! Storage.createFileReadStream', opts
        bucket = yield @bucket
        bucket.openDownloadStreamByName opts._id, {}

    @public @async fileExists: Function,
      args: [Object]
      return: Boolean
      default: (opts, callback) ->
        console.log '@@@@@@@!!!!!!! Storage.fileExists', opts
        bucket = yield @bucket
        yield bucket.find(filename: opts._id).hasNext()


  return MongoStorage::MongoCollectionMixin.initialize()
