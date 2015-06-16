module Lhm
  module Throttler
    class SlaveLag
      include Command

      INITIAL_TIMEOUT = 0.1
      DEFAULT_STRIDE = 40_000
      DEFAULT_MAX_ALLOWED_LAG = 10

      MAX_TIMEOUT = INITIAL_TIMEOUT * 1024

      attr_accessor :timeout_seconds, :allowed_lag, :stride, :connection

      def initialize(options = {})
        @timeout_seconds = INITIAL_TIMEOUT
        @stride = options[:stride] || DEFAULT_STRIDE
        @allowed_lag = options[:allowed_lag] || DEFAULT_MAX_ALLOWED_LAG
        @get_current_shard = options[:get_current_shard]
      end

      def execute
        sleep(throttle_seconds)
      end

      private

      SQL_SELECT_SLAVE_HOSTS = "SELECT host FROM information_schema.processlist WHERE command='Binlog Dump'"
      SQL_SELECT_MAX_SLAVE_LAG = 'SHOW SLAVE STATUS'

      private_constant :SQL_SELECT_SLAVE_HOSTS, :SQL_SELECT_MAX_SLAVE_LAG

      def throttle_seconds
        lag = max_current_slave_lag

        if lag > @allowed_lag && @timeout_seconds < MAX_TIMEOUT
          Lhm.logger.info("Increasing timeout between strides from #{@timeout_seconds} to #{@timeout_seconds * 2} because #{lag} seconds of slave lag detected is greater than the maximum of #{@allowed_lag} seconds allowed.")
          @timeout_seconds = @timeout_seconds * 2
        elsif lag <= @allowed_lag && @timeout_seconds > INITIAL_TIMEOUT
          Lhm.logger.info("Decreasing timeout between strides from #{@timeout_seconds} to #{@timeout_seconds / 2} because #{lag} seconds of slave lag detected is less than or equal to the #{@allowed_lag} seconds allowed.")
          @timeout_seconds = @timeout_seconds / 2
        else
          @timeout_seconds
        end
      end

      def slave_hosts
        @slave_hosts ||= get_slave_hosts
      end

      def get_slave_hosts
        slave_hosts = []
        slaves = slaves_for_connection(@connection)
        while slaves.any? do
          slave = slaves.pop
          if slave_hosts.exclude?(slave) && conn = slave_connection(slave)
            slave_hosts << slave
            slaves << slaves_for_connection(conn)
            slaves.flatten!
          end
        end
        slave_hosts
      end

      def slaves_for_connection(connection)
        select_slave_hosts(connection).map { |slave_host| slave_host.partition(':')[0] }
          .delete_if { |slave| slave == 'localhost' || slave == '127.0.0.1' }
      end

      def select_slave_hosts(connection)
        connection.select_values(SQL_SELECT_SLAVE_HOSTS)
      end

      def max_current_slave_lag
        max = slave_hosts.map { |slave| slave_lag(slave) }.flatten.push(0).max
        Lhm.logger.info "Max current slave lag: #{max}"
        max
      end

      def slave_lag(slave)
        conn = slave_connection(slave)
        if conn.respond_to?(:exec_query)
          result = conn.exec_query(SQL_SELECT_MAX_SLAVE_LAG)
          result.map { |row| row['Seconds_Behind_Master'].to_i }
        else
          result = conn.execute(SQL_SELECT_MAX_SLAVE_LAG)
          fetch_slave_seconds(result)
        end
      rescue Error => e
        raise Lhm::Error, "Unable to connect and/or query slave to determine slave lag. Migration aborting because of: #{e}"
      end

      def slave_connection(slave)
        adapter_method = defined?(Mysql2) ? 'mysql2_connection' : 'mysql_connection'
        begin
          ActiveRecord::Base.send(adapter_method, slave_config(slave))
        rescue Mysql2::Error => e
          Lhm.logger.info "Error conecting to #{slave}: #{e}"
          nil
        end
      end

      def slave_config(slave)
        config = ActiveRecord::Base.connection_pool.spec.config.dup
        config = config[@get_current_shard.call.to_sym] if @get_current_shard && @get_current_shard.call
        config[:host] = slave
        config
      end

      # This method fetch the Seconds_Behind_Master, when exec_query is no available, on AR 2.3.
      def fetch_slave_seconds(result)
        unless result.is_a? Mysql::Result
          Lhm.logger.info "Not a Mysql::Result from the slave assuming 0 lag"
          return 0
        end

        keys = []
        result.each_hash { |h| keys << h['Seconds_Behind_Master'].to_i }
        keys
      end

    end
  end
end
