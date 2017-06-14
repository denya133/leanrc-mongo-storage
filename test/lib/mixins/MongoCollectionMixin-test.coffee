{ expect, assert }  = require 'chai'
sinon               = require 'sinon'
_                   = require 'lodash'
MongoStorage        = require.main.require 'lib'
LeanRC              = require 'LeanRC'
{ co }              = LeanRC::Utils

describe 'MongoCollectionMixin', ->
  describe '.new', ->
    it 'should create HTTP collection instance', ->
      co ->
        class Test extends LeanRC
          @inheritProtected()
          @include MongoStorage
          @root __dirname
        Test.initialize()
        class Test::MongoCollection extends LeanRC::Collection
          @inheritProtected()
          @include Test::MongoCollectionMixin
          @module Test
        Test::MongoCollection.initialize()
        collection = Test::MongoCollection.new()
        assert.instanceOf collection, Test::MongoCollection
        yield return
