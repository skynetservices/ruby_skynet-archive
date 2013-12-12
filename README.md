ruby_skynet
===========

Ruby Client for calling [Skynet](https://github.com/skynetservices/skynet) services, and
the server side so that [Skynet](https://github.com/skynetservices/skynet) services can be hosted in Ruby

* http://github.com/skynetservices/ruby_skynet

### Client Example

```ruby
require 'rubygems'
require 'ruby_skynet'

client = RubySkynet::Client.new('TutorialService')
p client.call('AddOne', :value => 5)
```

For details on installing and running the GoLang Tutorial Service: https://github.com/skynetservices/skynet/wiki/Service-Tutorial

### Server Example

```ruby
require 'rubygems'
require 'ruby_skynet'

# Just echo back any parameters received when the echo method is called
class EchoService
  include RubySkynet::Service

  # Methods implemented by this service
  # Must take a Hash as input
  # Must Return a Hash response or nil for no response
  def echo(params)
    params
  end
end

# Start the server
server = RubySkynet::Server.new
```

Client to call the above Service

```ruby
require 'rubygems'
require 'ruby_skynet'

class Echo < RubySkynet::Client
  self.skynet_name = "EchoService"
end

client = Echo.new
p client.echo(:hello => 'world')
```

### Logging

Since ruby_skynet uses SemanticLogger, trace level logging of all TCP/IP
calls can be enabled as follows:

```ruby
require 'rubygems'
require 'ruby_skynet'

SemanticLogger.default_level = :info
SemanticLogger.add_appender('skynet.log')

class Echo < RubySkynet::Client
  self.skynet_name = "EchoService"
end

client = Echo.new
p client.echo(:hello => 'world')
```

### Dependencies

- Ruby 1.8.7, Ruby 1.9.3, Ruby 2.0.0, or JRuby 1.6.3 (or higher)
- [SemanticLogger](http://github.com/ClarityServices/semantic_logger)
- [ResilientSocket](https://github.com/ClarityServices/resilient_socket)
- [multi_json](https://github.com/intridea/multi_json)

One of the following Service Registry Implementations
- ZooKeeper Ruby Client [zk](https://github.com/slyphon/zk)
- [ruby_doozer](http://github.com/skynetservices/ruby_doozer)

### Install

Installing for a ZooKeeper centralized service registry - Recommended

    gem install zk
    gem install ruby_skynet

OR, Installing for a Doozer centralized service registry

    gem install ruby_doozer
    gem install ruby_skynet

Development
-----------

Want to contribute to Ruby Skynet?

First clone the repo and run the tests:

    git clone git://github.com/skynetservices/ruby_skynet.git
    cd ruby_skynet
    ruby -S rake test

Feel free to submit an issue and we'll try to resolve it.

Contributing
------------

Once you've made your great commits:

1. [Fork](http://help.github.com/forking/) ruby_skynet
2. Create a topic branch - `git checkout -b my_branch`
3. Push to your branch - `git push origin my_branch`
4. Create an [Issue](http://github.com/skynetservices/ruby_skynet/issues) with a link to your branch
5. That's it!

Meta
----

* Code: `git clone git://github.com/skynetservices/ruby_skynet.git`
* Home: <https://github.com/skynetservices/ruby_skynet>
* Bugs: <http://github.com/skynetservices/ruby_skynet/issues>
* Gems: <http://rubygems.org/gems/ruby_skynet>

This project uses [Semantic Versioning](http://semver.org/).

Authors
-------

Reid Morrison :: reidmo@gmail.com :: @reidmorrison

License
-------

Copyright 2012,2013 Clarity Services, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
