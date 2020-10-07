# This file is part of leanrc-mongo-storage.
#
# leanrc-mongo-storage is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# leanrc-mongo-storage is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with leanrc-mongo-storage.  If not, see <https://www.gnu.org/licenses/>.

# _         = require 'lodash'
# fs        = require 'fs'

###
Example of use

```coffee
LeanRC = require '@leansdk/leanrc/lib'
MongoStorage = require '@leansdk/leanrc-mongo-storage/lib'

class TestApp extends LeanRC
  @inheritProtected()
  @include MongoStorage

  @const ANIMATE_ROBOT: Symbol 'animateRobot'
  @const ROBOT_SPEAKING: Symbol 'robotSpeaking'

  require('./controller/command/StartupCommand') TestApp
  require('./controller/command/PrepareControllerCommand') TestApp
  require('./controller/command/PrepareViewCommand') TestApp
  require('./controller/command/PrepareModelCommand') TestApp
  require('./controller/command/AnimateRobotCommand') TestApp

  require('./view/component/ConsoleComponent') TestApp
  require('./view/mediator/ConsoleComponentMediator') TestApp

  require('./model/proxy/RobotDataProxy') TestApp

  require('./AppFacade') TestApp


module.exports = TestApp.initialize().freeze()
```
###

Extension = (BaseClass) ->
  class extends BaseClass
    @inheritProtected()

    require('./iterator/MongoCursor') @Module

    require('./mixins/MongoCollectionMixin') @Module
    require('./mixins/MongoSerializerMixin') @Module
    require('./mixins/MongoMigrationMixin') @Module
    @initializeMixin()

Reflect.defineProperty Extension, 'name',
  value: 'MongoStorage'


module.exports = Extension
