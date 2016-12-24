# -*- coding: utf-8 -*-
require 'workling/remote/invokers/base'

#
#  A threaded polling Invoker.
#
#  TODO: refactor this to make use of the base class.
#
module Workling
  module Remote
    module Invokers
      class ThreadedPoller < Workling::Remote::Invokers::Base

        cattr_accessor :sleep_time, :reset_time

        # If Worker classes is nil, means all classes are to be loaded
        # worker_classes is a ',' separated list of CamelCased class names of workers
        def initialize(routing, client_class, high_priority, worker_classes = nil)
          super(routing, client_class)

          ThreadedPoller.sleep_time = Workling.config[:sleep_time] || 2
          ThreadedPoller.reset_time = Workling.config[:reset_time] || 30

          if worker_classes
            @worker_classes = worker_classes.split(',')
            @worker_classes.map!{|classname| eval classname}
          end
          @workers = ThreadGroup.new
          @high_priority = high_priority
          @mutex = Mutex.new
          @em_thread = nil
        end

        def listen
          # Allow concurrency for our tasks
          ActiveRecord::Base.allow_concurrency = true

          # Start the EM loop. Some indexers will need it

          EM.stop if EM.reactor_running?
          @em_thread = Thread.new{
            begin
              EM.run
            rescue Exception => xcp
              puts 'EM thread exception'
              puts xcp.message
              puts xcp.class
              puts xcp.backtrace
              retry
            end
          }
          until EM.reactor_running?
            puts "Waiting for eventloop to start..."
            sleep 0.25
          end
          puts "eventloop started ..."

          # Create a thread for each worker.
          idx = 0
          Workling::Discovery.discovered.each do |clazz|
            # Run the worker classes listeners one by one and cycle
            if @worker_classes
              clazz =  @worker_classes[idx]
              idx += 1
              idx = 0 if idx >= @worker_classes.size
            end
            logger.debug("Discovered listener #{clazz}")
            t = Thread.new(clazz) { |c|
              Thread.current.priority = 10 if c.superclass == HighPriorityWorker
              clazz_listen(c) if ((@high_priority && c.superclass == HighPriorityWorker) || (!@high_priority && c.superclass != HighPriorityWorker))
            }
            @workers.add(t)
          end

          # Wait for all workers to complete
          @workers.list.each { |t| t.join }

          logger.debug("Reaped listener threads. ")

          # Clean up all the connections.
          ActiveRecord::Base.verify_active_connections!
          logger.debug("Cleaned up connection: out!")
        end

        # Check if all Worker threads have been started.
        def started?
          logger.debug("checking if started... list size is #{ worker_threads }")
          Workling::Discovery.discovered.size == worker_threads
        end

        # number of worker threads running
        def worker_threads
          @workers.list.size
        end

        # Gracefully stop processing
        def stop
          EM.stop if EM.reactor_running?
          @em_thread.exit if @em_thread
          logger.info("stopping threaded poller...")
          #sleep 1 until started? # give it a chance to start up before shutting down.
          logger.info("Giving Listener Threads a chance to shut down. This may take a while... ")
          @workers.list.each { |w| w[:shutdown] = true }
          logger.info("Listener threads were shut down.  ")
        end

        # Listen for one worker class
        def clazz_listen(clazz)
          logger.debug("Listener thread #{clazz.name} started")

          # Read thread configuration if available
          if Workling.config.has_key?(:listeners)
            if Workling.config[:listeners].has_key?(clazz.to_s)
              config = Workling.config[:listeners][clazz.to_s].symbolize_keys
              thread_sleep_time = config[:sleep_time] if config.has_key?(:sleep_time)
            end
          end

          thread_sleep_time ||= self.class.sleep_time

          # Setup connection to client (one per thread)
          connection = @client_class.new
          if clazz.superclass == HighPriorityWorker
            connection.connect(true)
          else
            connection.connect(false)
          end

          puts("** Starting client #{ connection.class } for #{clazz.name} queue")

          # Start dispatching those messages
          while (!Thread.current[:shutdown]) do
            begin

              # Thanks for this Brent!
              #
              #     ...Just a heads up, due to how rails’ MySQL adapter handles this
              #     call ‘ActiveRecord::Base.connection.active?’, you’ll need
              #     to wrap the code that checks for a connection in in a mutex.
              #
              #     ....I noticed this while working with a multi-core machine that
              #     was spawning multiple workling threads. Some of my workling
              #     threads would hit serious issues at this block of code without
              #     the mutex.
              #
              @mutex.synchronize do
                ActiveRecord::Base.connection.verify!  # Keep MySQL connection alive
                unless ActiveRecord::Base.connection.active?
                  logger.fatal("Failed - Database not available!")
                end
              end

              # Dispatch and process the messages
              n = dispatch!(connection, clazz)
              logger.debug("Listener thread #{clazz.name} processed #{n.to_s} queue items") if n > 0
              sleep(self.class.sleep_time) unless n > 0

              # If there is a memcache error, hang for a bit to give it a chance to fire up again
              # and reset the connection.
              rescue Workling::WorklingConnectionError
                logger.warn("Listener thread #{clazz.name} failed to connect. Resetting connection.")
                sleep(self.class.reset_time)
                connection.reset
            end
          end

          logger.debug("Listener thread #{clazz.name} ended")
        end

        # Dispatcher for one worker class. Will throw MemCacheError if unable to connect.
        # Returns the number of worker methods called
        def dispatch!(connection, clazz)
          n = 0
          for queue in @routing.queue_names_routing_class(clazz)
            begin
              result = connection.retrieve(queue)
              if result
                n += 1
                handler = @routing[queue]
                method_name = @routing.method_name(queue)
                handler.dispatch_to_worker_method(method_name, result)
              end
            rescue MemCache::MemCacheError => e
              logger.error("FAILED to connect with queue #{ queue }: #{ e } }")
              raise e
            end
          end

          return n
        end
      end
    end
  end
end
