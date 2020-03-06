namespace :db do
  namespace :datasets do
    desc 'Creates datasets for sections for admin panel'
    task create: :clear_obsolete do
      config = YAML.load_file(FILE_TO_IMPORT)
      cnt = 0

      config['platforms'].each do |platform_props|
        platform = DataUploader::Platform.find_by(name: platform_props['name'])
        next unless platform

        platform_props['sections'].each do |section_props|
          section = platform.sections.find_by(name: section_props['name'])
          next unless section && section_props['datasets']

          dataset_names = section_props['datasets']
          dataset_names.each do |dataset_name|
            next if section.datasets.find_by(name: dataset_name)

            DataUploader::Dataset.create(name: dataset_name, section: section)
            cnt += 1
          end
        end
      end
      puts "#{cnt} datasets created"
    end

    task clear_obsolete: :environment do
      old_names = Hash[
        DataUploader::Platform.includes(sections: :datasets).map do |p|
          [
            p.name,
            Hash[p.sections.map { |s| [s.name, s.datasets.pluck(:name)] }]
          ]
        end
      ]
      puts old_names.inspect
      config = YAML.load_file(FILE_TO_IMPORT)
      config['platforms'].each do |platform_props|
        platform_name = platform_props['name']
        next unless old_names[platform_name]

        platform_props['sections'].each do |section_props|
          section_name = section_props['name']
          next unless old_names[platform_name][section_name]

          if section_props['datasets']
            section_props['datasets'].each do |dataset_name|
              next unless old_names[platform_name][section_name].find(dataset_name)

              old_names[platform_name][section_name].delete(dataset_name)
            end
          end

          datasets = DataUploader::Dataset.
            joins(section: :platform).
            where(
              'platforms.name' => platform_name,
              'sections.name' => section_name,
              name: old_names[platform_name][section_name]
            )
          next unless datasets.any?

          puts "[Platform: #{platform_name}, section: #{section_name}] Removing obsolete datasets:"
          datasets.each { |d| puts d.name; d.destroy }
        end
      end
    end

    task clear_all: :environment do
      DataUploader::WorkerLog.destroy_all
      DataUploader::Dataset.destroy_all
    end
  end
end
