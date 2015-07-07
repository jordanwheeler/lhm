# Copyright (c) 2011 - 2013, SoundCloud Ltd., Rany Keddo, Tobias Bielohlawek, Tobias
# Schmidt

require 'lhm/table'
require 'lhm/invoker'
require 'lhm/throttler'
require 'lhm/version'
require 'logger'

# Large hadron migrator - online schema change tool
#
# @example
#
#   Lhm.change_table(:users) do |m|
#     m.add_column(:arbitrary, "INT(12)")
#     m.add_index([:arbitrary, :created_at])
#     m.ddl("alter table %s add column flag tinyint(1)" % m.name)
#   end
#
module Lhm
  extend Throttler
  extend self

  DEFAULT_LOGGER_OPTIONS =  { level: Logger::INFO, file: STDOUT }

  # Alters a table with the changes described in the block
  #
  # @param [String, Symbol] table_name Name of the table
  # @param [Hash] options Optional options to alter the chunk / switch behavior
  # @option options [Fixnum] :stride
  #   Size of a chunk (defaults to: 40,000)
  # @option options [Fixnum] :throttle
  #   Time to wait between chunks in milliseconds (defaults to: 100)
  # @option options [Fixnum] :start
  #   Primary Key position at which to start copying chunks
  # @option options [Fixnum] :limit
  #   Primary Key position at which to stop copying chunks
  # @option options [Boolean] :atomic_switch
  #   Use atomic switch to rename tables (defaults to: true)
  #   If using a version of mysql affected by atomic switch bug, LHM forces user
  #   to set this option (see SqlHelper#supports_atomic_switch?)
  # @yield [Migrator] Yielded Migrator object records the changes
  # @return [Boolean] Returns true if the migration finishes
  # @raise [Error] Raises Lhm::Error in case of a error and aborts the migration
  def change_table(table_name, options = {}, &block)
    origin = Table.parse(table_name, connection)
    invoker = Invoker.new(origin, connection)
    block.call(invoker.migrator)
    invoker.run(options)
    true
  end

  # Cleanup tables and triggers
  #
  # @param [Boolean] run execute now or just display information
  # @param [Hash] options Optional options to alter cleanup behaviour
  # @option options [Time] :until
  #   Filter to only remove tables up to specified time (defaults to: nil)
  def cleanup(run = false, options = {})
    drop_triggers_and_tables('lhma_', run, options)
  end

  def cleanup_aborted(run = false, options = {})
    drop_triggers_and_tables('lhmn_', run, options)
  end

  def drop_triggers_and_tables(pattern, run, options)
    lhm_tables = connection.select_values('show tables').select { |name| name =~ /^#{pattern}/ }
    if options[:until]
      lhm_tables.select! do |table|
        table_date_time = Time.strptime(table, 'lhma_%Y_%m_%d_%H_%M_%S')
        table_date_time <= options[:until]
      end
    end

    lhm_triggers = connection.select_values('show triggers').collect do |trigger|
      trigger.respond_to?(:trigger) ? trigger.trigger : trigger
    end

    lhm_triggers = lhm_triggers.select do |trigger|
      lhm_tables.each do |table_name|
        name = table_name.split('_').last
        trigger.name =~ /^lhmt_(\w{3})_#{name}/
      end
    end

    if run
      lhm_triggers.each do |trigger|
        connection.execute("drop trigger if exists #{trigger}")
      end
      lhm_tables.each do |table|
        connection.execute("drop table if exists #{table}")
      end
      true
    elsif lhm_tables.empty? && lhm_triggers.empty?
      puts 'Everything is clean. Nothing to do.'
      true
    else
      puts "Existing LHM tables: #{lhm_tables.join(', ')}."
      puts "Existing LHM triggers: #{lhm_triggers.join(', ')}."
      method = pattern == 'lhma_' ? 'cleanup' : 'cleanup_aborted'
      puts "Run Lhm.#{method}(true) to drop them all."
      false
    end
  end

  def setup(connection)
    @@connection = connection
  end

  def self.setup(adapter)
    @@adapter = adapter
  end

  def self.adapter
    @@adapter ||=
      begin
        raise 'Please call Lhm.setup' unless defined?(ActiveRecord)
        ActiveRecord::Base.connection
      end
  end

  def connection
    @@connection ||=
      begin
        raise 'Please call Lhm.setup' unless defined?(ActiveRecord)
        ActiveRecord::Base.connection
      end
  end

  def self.logger_params=(params)
    @@logger_params = params
  end

  def self.logger
    @@logger ||=
      begin
        params = (defined?(@@logger_params) && @@logger_params) ? @@logger_params : DEFAULT_LOGGER_OPTIONS
        logger = Logger.new(params[:file])
        logger.level = params[:level]
        logger.formatter = nil
        logger
      end
  end
end
