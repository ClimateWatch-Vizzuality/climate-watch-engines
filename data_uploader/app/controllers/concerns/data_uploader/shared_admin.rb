module DataUploader
  module SharedAdmin
    extend ActiveSupport::Concern

    # rubocop:disable Metrics/MethodLength
    # rubocop:disable Metrics/AbcSize
    def self.included(base)
      base.send(:page_action, :upload_datafile, method: :post) do
        datafile_not_chosen = proc {
          redirect_to path, alert: 'Upload failed: file missing'
          return
        }
        datafile_wrong_content_type = proc {
          redirect_to path, alert: "Upload failed due to wrong file
            extension. File's extension should be .csv"
          return
        }
        success = proc { redirect_to path, notice: 'Uploaded successfully' }

        callbacks = {
          datafile_not_chosen: datafile_not_chosen,
          datafile_wrong_content_type: datafile_wrong_content_type,
          success: success
        }

        DataUploader::UseCase::UploadDatafile.
          new(dataset_repository, s3_folder_path).
          call(params, callbacks)
      end

      base.send(:page_action, :download_datafile, method: :post) do
        no_datafile = proc { return; }
        send_data_to_client = proc { |file, datafile|
          send_data file.read.force_encoding('BINARY'),
                    filename: datafile.filename.to_s,
                    disposition: 'attachment'
        }

        callbacks = {
          no_datafile: no_datafile,
          send_data_to_client: send_data_to_client
        }

        DataUploader::UseCase::DownloadDatafile.
          new(dataset_repository, s3_folder_path).
          call(params, callbacks)
      end

      base.send(:page_action, :download_datafiles, method: :post) do
        datafiles_empty = proc { return; }
        send_data_to_client = proc { |file, zip_filename|
          send_data file.read.force_encoding('BINARY'),
                    filename: "#{zip_filename}.zip",
                    type: 'application/zip',
                    disposition: 'attachment'
        }

        callbacks = {
          datafiles_empty: datafiles_empty,
          send_data_to_client: send_data_to_client
        }

        attrs = {
          section_name: section_name,
          platform_name: platform_name
        }

        DataUploader::UseCase::DownloadZippedDatafiles.
          new(dataset_repository, s3_folder_path).
          call(attrs, callbacks)
      end

      base.send(:page_action, :run_importer, method: :post) do
        import_worker
        notice = "Files import scheduled. This might take to few minutes,
          please refresh the page to see the status in logs table below."

        redirect_to path, notice: notice
      end

      base.send(:page_action, :abort_importer, method: :patch) do
        worker_log = DataUploader::WorkerLog.find_by_id(params[:id])
        if worker_log
          # if the job has not been started yet, we might still be able to cancel it
          DataUploader::BaseImportWorker.cancel!(worker_log.jid)
          worker_log.update(state: 'aborted')
        end
        notice = 'Task aborted.'

        redirect_to path, notice: notice
      end
    end
    # rubocop:enable Metrics/MethodLength
    # rubocop:enable Metrics/AbcSize
  end
end
