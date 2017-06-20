LeanRC = require.main.require 'lib'

module.exports = (Module) ->
  class Migration1 extends Module
    @inheritProtected()
    @module Module
  Migration1.initialize()
