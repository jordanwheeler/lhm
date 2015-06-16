require File.expand_path(File.dirname(__FILE__)) + '/../unit_helper'

require 'lhm/throttler/slave_lag'

describe Lhm::Throttler::SlaveLag do
  include UnitHelper

  before :each do
    @throttler = Lhm::Throttler::SlaveLag.new
  end

  describe '#throttle_seconds' do
    describe 'with no slave lag' do
      before do
        def @throttler.max_current_slave_lag
          0
        end
      end

      it 'does not alter the currently set timeout' do
        timeout = @throttler.timeout_seconds
        assert_equal(timeout, @throttler.send(:throttle_seconds))
      end
    end

    describe 'with a large slave lag' do
      before do
        def @throttler.max_current_slave_lag
          100
        end
      end

      it 'doubles the currently set timeout' do
        timeout = @throttler.timeout_seconds
        assert_equal(timeout * 2, @throttler.send(:throttle_seconds))
      end

      it 'does not increase the timeout past the maximum' do
        @throttler.timeout_seconds = Lhm::Throttler::SlaveLag::MAX_TIMEOUT
        assert_equal(Lhm::Throttler::SlaveLag::MAX_TIMEOUT, @throttler.send(:throttle_seconds))
      end
    end

    describe 'with no slave lag after it has previously been increased' do
      before do
        def @throttler.max_current_slave_lag
          0
        end
      end

      it 'halves the currently set timeout' do
        @throttler.timeout_seconds *= 2 * 2
        timeout = @throttler.timeout_seconds
        assert_equal(timeout / 2, @throttler.send(:throttle_seconds))
      end

      it 'does not decrease the timeout past the minimum on repeated runs' do
        @throttler.timeout_seconds = Lhm::Throttler::SlaveLag::INITIAL_TIMEOUT * 2
        assert_equal(Lhm::Throttler::SlaveLag::INITIAL_TIMEOUT, @throttler.send(:throttle_seconds))
        assert_equal(Lhm::Throttler::SlaveLag::INITIAL_TIMEOUT, @throttler.send(:throttle_seconds))
      end
    end
  end

  describe '#slaves_for_connection' do
    describe 'with no slaves' do
      before do
        def @throttler.select_slave_hosts(conn)
          []
        end
      end

      it 'returns no slave hosts' do
        assert_equal([], @throttler.send(:slaves_for_connection, 'conn'))
      end
    end

    describe 'with only localhost slaves' do
      before do
        def @throttler.select_slave_hosts(conn)
          ['localhost:1234', '127.0.0.1:5678']
        end
      end

      it 'returns no slave hosts' do
        assert_equal([], @throttler.send(:slaves_for_connection, 'conn'))
      end
    end

    describe 'with only remote slaves' do
      before do
        def @throttler.select_slave_hosts(conn)
          ['server.example.com:1234', 'anotherserver.example.com']
        end
      end

      it 'returns remote slave hosts' do
        assert_equal(['server.example.com', 'anotherserver.example.com'], @throttler.send(:slaves_for_connection, 'conn'))
      end
    end
  end

  describe '#get_slave_hosts' do
    describe 'with no slaves' do
      before do
        def @throttler.slaves_for_connection(conn)
          []
        end
      end

      it 'returns no slave hosts' do
        assert_equal([], @throttler.send(:get_slave_hosts))
      end
    end

    describe 'with a master slave' do
      before do
        def @throttler.slaves_for_connection(conn)
          ['1.1.1.1']
        end
        def @throttler.slave_connection(slave1)
          'conn'
        end
      end

      it 'returns the slave host' do
        assert_equal(['1.1.1.1'], @throttler.send(:get_slave_hosts))
      end
    end

    describe 'with multiple slaves' do
      before do
        @throttler.instance_variable_set(:@connection, 'conn')
        def @throttler.slaves_for_connection(conn)
          if conn == 'conn'
            ['1.1.1.1']
          elsif conn == 'conn1'
            ['1.1.1.3', '1.1.1.2']
          end
        end
        def @throttler.slave_connection(slave)
          if slave == '1.1.1.1'
            'conn1'
          elsif slave == '1.1.1.2' || slave == '1.1.1.3'
            'conn2'
          else
            nil
          end
        end
      end

      it 'returns the slaves' do
        assert_equal(['1.1.1.1', '1.1.1.2', '1.1.1.3'], @throttler.send(:get_slave_hosts))
      end
    end
  end

  describe "#slave_connection" do
    before do
      class ActiveRecord::Base
        def self.mysql2_connection(config)
          raise Mysql2::Error.new("connection error")
        end

        def self.mysql_connection(config)
          raise Mysql2::Error.new("connection error")
        end
      end

      def @throttler.slave_config(slave)
        nil
      end
    end

    it 'logs and returns nil on Mysql2::Error' do
      assert_send([Lhm.logger, :info, "Error connecting to slave: connection error"])
      assert_nil(@throttler.send(:slave_connection, 'slave'))
    end
  end

  describe "#slave_config" do
    before do
      class Spec
        def spec
          self
        end

        def config
          {:host => 'master', :shard_1 => {:host => 'master'}}
        end
      end

      class ActiveRecord::Base
        def self.connection_pool
          Spec.new
        end
      end
    end

    it "sets the proper host" do
      assert_equal({:host => 'slave', :shard_1 => {:host => 'master'}}, @throttler.send(:slave_config, 'slave'))
    end

    it "returns shard specific config when the shard is set" do
      @throttler.instance_variable_set(:@get_current_shard, lambda { 'shard_1' })
      assert_equal({:host => 'slave'}, @throttler.send(:slave_config, 'slave'))
    end
  end
end
