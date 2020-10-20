module DataUploader
  class BaseImportWorker
    include Sidekiq::Worker
    include DataUploader::WithJobLogging

    sidekiq_options queue: :database, retry: false

    def perform(section_id, importer_class_name, admin)
      section = DataUploader::Section.find(section_id)
      importer = importer_class_name.constantize.new

      return if job_in_progress?(section)

      log_job(jid, section_id, admin) { import_data(importer) }
    end

    private

    def import_data(importer)
      importer.call
      importer
    end

    def job_in_progress?(section)
      section.worker_logs.started.any?
    end
  end
end
