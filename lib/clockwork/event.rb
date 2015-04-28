module Clockwork
  class Event
    attr_accessor :job, :last

    def initialize(manager, period, job, block, options={})
      validate_if_option(options[:if])
      @manager = manager
      @period = period
      @job = job
      @at = At.parse(options[:at])
      @last = nil
      @paused = nil
      @paused_mutex = Mutex.new
      @block = block
      @if = options[:if]
      @thread = options.fetch(:thread, @manager.config[:thread])
      @timezone = options.fetch(:tz, @manager.config[:tz])
    end

    def init_zk(zk, root_node)
      @zk = zk
      @root_node = "#{root_node}/#{@job}"
      @paused_node = "#{@root_node}/paused"

      if !@zk.exists?(@root_node)
        @zk.mkdir_p(@root_node)
      end

      # ZK watches for pause
      @zk.register(@paused_node) do |event|
        if event.node_created? || event.node_deleted?
          self.paused = @zk.exists?(@paused_node, :watch => true)
          if paused?
            @manager.log "Pausing '#{self}'"
          else
            @manager.log "Unpausing '#{self}'"
          end
        end
      end

      # arm and get initial state
      self.paused = @zk.exists?(@paused_node, :watch => true)
      if paused?
        @manager.log "Initial state of '#{self}' is paused."
      end

      @last = convert_timezone(Time.at(@zk.get(@root_node)[0].to_i)) rescue nil
    end

    def convert_timezone(t)
      @timezone ? t.in_time_zone(@timezone) : t
    end

    def paused=(v)
      @paused_mutex.synchronize { @paused = !!v }
    end

    def paused?
      @paused_mutex.synchronize { @paused }
    end

    def run_now?(t)
      t = convert_timezone(t)
      elapsed_ready(t) and !paused? and (@at.nil? or @at.ready?(t)) and (@if.nil? or @if.call(t))
    end

    def thread?
      @thread
    end

    def run(t)
      @manager.log "Triggering '#{self}'"
      if @zk && @root_node
        @zk.set(@root_node, t.to_i.to_s)
      end
      @last = convert_timezone(t)
      if thread?
        if @manager.thread_available?
          t = Thread.new do
            execute
          end
          t['creator'] = @manager
        else
          @manager.log_error "Threads exhausted; skipping #{self}"
        end
      else
        execute
      end
    end

    def to_s
      job.to_s
    end

    private
    def execute
      @block.call(@job, @last)
    rescue => e
      @manager.log_error e
      @manager.handle_error e
    end

    def elapsed_ready(t)
      @last.nil? || (t - @last.to_i).to_i >= @period
    end

    def validate_if_option(if_option)
      if if_option && !if_option.respond_to?(:call)
        raise ArgumentError.new(':if expects a callable object, but #{if_option} does not respond to call')
      end
    end
  end
end
