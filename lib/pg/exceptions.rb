#!/usr/bin/env ruby

require 'pg' unless defined?( PG )


module PG

	class Error < StandardError
    attr_reader :result

    def initialize msg = "", result = nil
      super msg
      @result = result
    end
  end

end # module PG
