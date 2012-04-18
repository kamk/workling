require 'workling/clients/base'

#
#  This client can be used for all Queue Servers that speak Memcached, such as Starling. 
#
#  Wrapper for the memcache connection. The connection is made using fiveruns-memcache-client, 
#  or memcache-client, if this is not available. See the README for a discussion of the memcache 
#  clients. 
#
#  method_missing delegates all messages through to the underlying memcache connection. 
#
module Workling
  module Clients
    class MemcacheQueueClient < Workling::Clients::Base

      CLIENT_CONNECTION_POOL_SIZE = 7

      # the class with which the connection is instantiated
      cattr_accessor :memcache_client_class
      @@memcache_client_class ||= ::MemCache

      # the url with which the memcache client expects to reach starling
      attr_accessor :queueserver_urls

      # the memcache connection object
      attr_accessor :connection_pool

      attr_accessor :to_signup_server

      #
      #  the client attempts to connect to queueserver using the configuration options found in 
      #
      #      Workling.config. this can be configured in config/workling.yml. 
      #
      #  the initialization code will raise an exception if memcache-client cannot connect 
      #  to queueserver.
      #
      def connect(to_signup_server = false)

        self.to_signup_server = to_signup_server

        self.connection_pool ||= []
        if to_signup_server
          @queueserver_urls = Workling.config[:for_signup_listens_on]
        else
          @queueserver_urls = Workling.config[:listens_on]
        end
        if self.connection_pool.blank?
          CLIENT_CONNECTION_POOL_SIZE.times{self.connection_pool << MemcacheQueueClient.memcache_client_class.new(@queueserver_urls)}
        else
          self.connection_pool << MemcacheQueueClient.memcache_client_class.new(@queueserver_urls)
        end

        raise_unless_connected!
      end

      # closes the memcache connection
      def close
        self.connection_pool.each do |connection|
          begin
            connection.flush_all
          rescue MemCache::MemCacheError => err
            STDERR.puts "Memcache client doesn't respond to flush all"
          ensure
            connection.reset
          end
        end
      end

      # implements the client job request and retrieval 
      def request(key, value)
        set(key, value)
      end

      def retrieve(key)
        begin
          get(key)
        rescue MemCache::MemCacheError => e
          # failed to enqueue, raise a workling error so that it propagates upwards
          raise Workling::WorklingError.new("#{e.class.to_s} - #{e.message}")        
        end
      end

      private
        # make sure we can actually connect to queueserver on the given port
        def raise_unless_connected!
            self.connection_pool.each {|connection|
              begin
                connection.stats
              rescue
                puts 'Not connected. Will reconnect and retry subsequently.'
                puts 'Need to worry only if this message occurs too frequently or this is the last useful message printed'
              end
            }
        end

        # delegates directly through to the memcache connection. 
        def method_missing(method, *args)
          retry_count = 0
          reconnected = false
          begin
            conn = self.connection_pool[rand(self.connection_pool.size)]
            if conn
              conn.send(method, *args)
            else
              connect(self.to_signup_server)
              conn = self.connection_pool[rand(self.connection_pool.size)]
              conn.send(method, *args)
            end
          rescue MemCache::MemCacheError => e
            if (retry_count += 1) < 2
              self.connection_pool -= [conn]
              retry
            elsif !reconnected
              reconnected = true
              self.connect(self.to_signup_server)
              retry # one final time
            else
              # Can fail when invoked from another worker or when invoked from a resq or a rake task
              # So the rescue nil
              ExceptionNotifier::Notifier.exception_notification(request.env, e).deliver rescue nil
            end
          end
        end
    end
  end
end
