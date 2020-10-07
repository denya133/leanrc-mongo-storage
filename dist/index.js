(function() {
  // This file is part of leanrc-mongo-storage.

  // leanrc-mongo-storage is free software: you can redistribute it and/or modify
  // it under the terms of the GNU Lesser General Public License as published by
  // the Free Software Foundation, either version 3 of the License, or
  // (at your option) any later version.

  // leanrc-mongo-storage is distributed in the hope that it will be useful,
  // but WITHOUT ANY WARRANTY; without even the implied warranty of
  // MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  // GNU Lesser General Public License for more details.

  // You should have received a copy of the GNU Lesser General Public License
  // along with leanrc-mongo-storage.  If not, see <https://www.gnu.org/licenses/>.

  // _         = require 'lodash'
  // fs        = require 'fs'
  /*
  Example of use

  ```coffee
  LeanRC = require '@leansdk/leanrc'
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
  */
  var Extension;

  Extension = function(BaseClass) {
    return (function() {
      var _Class;

      _Class = class extends BaseClass {};

      _Class.inheritProtected();

      require('./iterator/MongoCursor')(_Class.Module);

      require('./mixins/MongoCollectionMixin')(_Class.Module);

      require('./mixins/MongoSerializerMixin')(_Class.Module);

      require('./mixins/MongoMigrationMixin')(_Class.Module);

      _Class.initializeMixin();

      return _Class;

    }).call(this);
  };

  Reflect.defineProperty(Extension, 'name', {
    value: 'MongoStorage'
  });

  module.exports = Extension;

}).call(this);
