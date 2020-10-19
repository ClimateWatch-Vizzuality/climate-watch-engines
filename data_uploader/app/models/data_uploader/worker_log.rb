module DataUploader
  class WorkerLog < ApplicationRecord
    self.table_name = 'worker_logs'
    belongs_to :section, class_name: 'DataUploader::Section'

    enum state: {
      started: 1,
      finished: 2,
      failed: 3,
      dead: 4,
      aborted: 5
    }

    def details_errors
      details&.dig('errors') || []
    end

    def details_errors_messages
      details_errors.map { |e| e['msg'] }.join('<br>').html_safe
    end
  end
end
