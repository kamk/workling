require 'workling/return/store/base'

#
#  Stores directly into memory. This is for tests only - not for production use. aight?
#
module Workling
  module Return
    module Store
      class MemcacheReturnStore < Base
        MEMCACHE_DEFAULT_OPTIONS = { :namespace => "workling",
                                     :timeout => 5
                                   }
        attr_accessor :cache
        
        def initialize(servers, options = {})
          self.cache = MemCache.new(servers,
                                    MEMCACHE_DEFAULT_OPTIONS.merge(options))
        end
        
        def set(key, value)
          self.cache[key] = value
        end
        
        def get(key)
          returning self.cache[key] do
            self.cache.delete(key)
          end
        end

        def peek(key)
          self.cache[key]
        end
      end
    end
  end
end