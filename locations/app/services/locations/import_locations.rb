module Locations
  class ImportLocations
    def call
      import_locations(S3CSVReader.read(Locations.locations_filepath))
      import_topojson
    end

    private

    # rubocop:disable Lint/RedundantWithIndex
    def import_locations(content)
      content.each.with_index(2) do |row|
        attributes = {
          iso_code3: iso_code3(row),
          iso_code2: iso_code2(row),
          wri_standard_name: row[:wri_standard_name],
          pik_name: row[:pik_name],
          cait_name: row[:cait_name],
          ndcp_navigators_name: row[:ndcp_navigators_name],
          unfccc_group: row[:unfccc_group],
          location_type: row[:location_type] || 'COUNTRY',
          show_in_cw: show_in_cw(row)
        }

        create_or_update(attributes)
      end
    end
    # rubocop:enable Lint/RedundantWithIndex

    def import_topojson
      uri = URI(Locations.cartodb_url)
      response = Net::HTTP.get(uri)
      parsed_response = JSON.parse(response, symbolize_names: true)
      parsed_response[:rows].each do |row|
        centroid = row[:centroid].nil? ? {} : JSON.parse(row[:centroid])
        begin
          Location.
            where(iso_code3: row[:iso]).
            update(topojson: JSON.parse(row[:topojson]), centroid: centroid)
        rescue JSON::ParserError => e
          STDERR.puts "Error importing data for #{row[:iso]}: #{e}"
        end
      end
    end

    def iso_code3(row)
      row[:iso_code3] && row[:iso_code3].upcase
    end

    def iso_code2(row)
      if row[:iso_code2].blank?
        ''
      else
        row[:iso_code2] && row[:iso_code2].upcase
      end
    end

    def show_in_cw(row)
      if row[:show_in_cw].blank?
        true
      else
        row[:show_in_cw].match?(/no/i) ? false : true
      end
    end

    def create_or_update(attributes)
      iso_code3 = attributes[:iso_code3]
      location = Location.find_by_iso_code3(iso_code3) ||
        Location.new(iso_code3: iso_code3)
      location.assign_attributes(attributes)

      op = location.new_record? ? 'CREATE' : 'UPDATE'

      if location.save
        Rails.logger.debug "#{op} OK #{iso_code3}"
      else
        Rails.logger.error "#{op} FAILED #{iso_code3}"
      end
    end
  end
end