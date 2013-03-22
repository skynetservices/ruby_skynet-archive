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
RubySkynet::Server.start
```

Client to call the above Service
```ruby
require 'rubygems'
require 'ruby_skynet'

client = RubySkynet::Client.new('EchoService')
p client.call('echo', :hello => 'world')
```

### Logging

Since ruby_skynet uses SemanticLogger, trace level logging of all TCP/IP
calls can be enabled as follows:

```ruby
require 'rubygems'
require 'ruby_skynet'

SemanticLogger::Logger.default_level = :trace
SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new('skynet.log')

client = RubySkynet::Client.new('EchoService')
p client.call('echo', :hello => 'world')
```

### Architecture

ruby_skynet implements its own doozer client which has been tested against
the doozer fork: https://github.com/4ad/doozerd.
The doozer client uses the active [ruby_protobuf](https://github.com/macks/ruby-protobuf)
project for marshaling data for communicating with doozer

### Dependencies

- Ruby MRI 1.8.7 (or above), Ruby 1.9.3,  Or JRuby 1.6.3 (or above)
- [SemanticLogger](http://github.com/ClarityServices/semantic_logger)
- [ResilientSocket](https://github.com/ClarityServices/resilient_socket)
- [ruby_protobuf](https://github.com/macks/ruby-protobuf)
- [multi_json](https://github.com/intridea/multi_json)

### Install

    gem install ruby_skynet

### Future

* Immediately drop connections to a service on a host when that instance
  shuts down or stops. ( Doozer::Wait )
* More intelligent selection of available Skynet services. For example
  nearest, or looking at load etc.

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

Copyright 2012 Clarity Services, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
