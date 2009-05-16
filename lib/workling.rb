#
#  I can haz am in your Workling are belong to us! 
#
require 'mattr_accessor' unless Module.respond_to?(:mattr_accessor)
require 'cattr_accessor' unless Class.respond_to?(:cattr_accessor)

gem 'activesupport'
require 'active_support/inflector'
require 'active_support/core_ext/hash/keys'

class Hash #:nodoc:
  include ActiveSupport::CoreExtensions::Hash::Keys
end

require 'yaml'

module Workling
  class WorklingError < StandardError; end
  class WorklingNotFoundError < WorklingError; end
  class WorklingConnectionError < WorklingError; end
  class QueueserverNotFoundError < WorklingError
    def initialize
      super "config/workling.yml configured to connect to queue server on #{ Workling.config[:listens_on] } for this environment. could not connect to queue server on this host:port. for starling users: pass starling the port with -p flag when starting it. If you don't want to use Starling, then explicitly set Workling::Remote.dispatcher (see README for an example)"
    end
  end

  class ConfigurationError < WorklingError
    def initialize
      super File.exist?(Workling.path('config', 'starling.yml')) ? 
        "config/starling.yml has been depracated. rename your config file to config/workling.yml then try again!" :
        "config/workling.yml could not be loaded. check out README.markdown to see what this file should contain. "
    end
  end
  
  def self.path(*args)
    if defined?(RAILS_ROOT)
      File.join(RAILS_ROOT, *args)
    else
      File.join(Dir.pwd, *args)
    end
  end

  def self.env
    @env ||= if defined?(RAILS_ENV)
               RAILS_ENV.to_s
             elsif defined?(RACK_ENV)
               RACK_ENV.to_s
             end
  end

  mattr_accessor :load_path
  @@load_path = [ File.expand_path(path('app', 'workers')) ]

  VERSION = "0.4.9"

  #
  # determine the runner to use if nothing is specifically set. workling will try to detect
  # starling, spawn, or bj, in that order. if none of these are found, notremoterunner will
  # be used. 
  #
  # this can be overridden by setting Workling::Remote.dispatcher, eg:
  #   Workling::Remote.dispatcher = Workling::Remote::Runners::StarlingRunner.new
  #
  def self.default_runner
    if env == "test"
      Workling::Remote::Runners::NotRemoteRunner.new
    elsif Workling::Remote::Runners::StarlingRunner.installed?
      Workling::Remote::Runners::StarlingRunner.new
    elsif Workling::Remote::Runners::SpawnRunner.installed?
      Workling::Remote::Runners::SpawnRunner.new
    elsif Workling::Remote::Runners::BackgroundjobRunner.installed?
      Workling::Remote::Runners::BackgroundjobRunner.new
    else
      Workling::Remote::Runners::NotRemoteRunner.new
    end
  end

  #
  # gets the worker instance, given a class. the optional method argument will cause an 
  # exception to be raised if the worker instance does not respoind to said method. 
  #
  def self.find(clazz, method = nil)
    begin
      inst = clazz.to_s.camelize.constantize.new 
    rescue NameError
      raise_not_found(clazz, method)
    end
    raise_not_found(clazz, method) if method && !inst.respond_to?(method)
    inst
  end

  # returns Workling::Return::Store.instance. 
  def self.return
    Workling::Return::Store.instance
  end

  #
  #  returns a config hash. reads ./config/workling.yml
  #
  mattr_writer :config
  def self.config
    return @@config if defined?(@@config) && @@config

    return nil unless File.exists?(config_path)

    @@config ||= YAML.load_file(config_path)[env || 'development'].symbolize_keys
    @@config[:memcache_options].symbolize_keys! if @@config[:memcache_options]
    @@config
  end

  mattr_writer :config_path
  def self.config_path
    return @@config_path if defined?(@@config_path) && @@config_path
    @@config_path = File.join(RAILS_ROOT, 'config', 'workling.yml')
  end

  #
  #  Raises exceptions thrown inside of the worker. normally, these are logged to 
  #  logger.error. it's easy to miss these log calls while developing, though. 
  #
  mattr_writer :raise_exceptions
  def raise_exceptions
    return @@raise_exceptions if defined?(@@raise_exceptions)
    @@raise_exceptions = (RAILS_ENV == "test" || RAILS_ENV == "development")
  end

  def self.raise_exceptions?
    @@raise_exceptions
  end

  private
    def self.raise_not_found(clazz, method)
      raise Workling::WorklingNotFoundError.new("could not find #{ clazz }:#{ method } workling. ") 
    end

end

def require_in_tree(name)
  require File.join(File.dirname(__FILE__), name)
end

require_in_tree "workling/discovery"
require_in_tree "workling/base"
require_in_tree "workling/remote"

# load all possible extension classes
["clients", "remote/invokers", "remote/runners", "return/store", "routing"].each do |e_dirs|
  # first the base
  require_in_tree "workling/#{e_dirs}/base"

  # now the implemenations
  Dir.glob(File.join(File.dirname(__FILE__), "workling", e_dirs, "*.rb")).each do |rb_file|
    require File.join(File.dirname(rb_file), File.basename(rb_file, ".rb"))
  end
end
