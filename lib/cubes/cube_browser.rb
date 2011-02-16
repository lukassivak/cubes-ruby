require 'data_objects'
require 'cubes/cube_query'

module Cubes
    
# Result object from {Slice#aggregate}
class AggregationResult

    # Aggregated measure
    attr_accessor :measure

    # Summary of aggregation. Hash with keys: :sum, :record_count
    attr_accessor :summary

    # If aggregation was by breaking down a dimension, rows contains aggregations
    # per each dimension level value specified in aggregation options. For example,
    # if you aggregate by dimension Date at the level Month, then each row will
    # represent aggregation for given month. For more information see {Slice#aggregate}
    attr_accessor :rows

    # Remainder of aggregation if limit was used. Similar hash as summary hash.
    attr_accessor :remainder

    # Options used for aggregation
    attr_accessor :aggregation_options
end # class AggregationResult

class SQLAggregationBrowser
    attr_reader :cube

    def initialize(cube, connection, view_name)
        if !cube
            raise "No cube given for browser"
        end
        @cube = cube
        @connection = connection
        @view_name = view_name.to_sym
        @summaries = Hash.new
    end

    def full_cube
        return Slice.new(self)
    end

    # Aggregate measure.
    #
    # @param [Symbol] measure Measure to be aggregated, for example: amount, price, ...
    # @param [Hash] options Options for more refined aggregation
    # @option options [Symbol, Dimension] :row_dimension Dimension used for row grouping
    # @option options [Array] :row_levels Group by dimension levels
    # @option options [Symbol] :limit Possible values: ':value', `:percent`, `:rank`
    # @option options [Symbol] :limit_aggregation Which aggregation is used for determining limit
    # @option options [Number] :limit_value Limit value based on limit_type 
    # @option options [Symbol] :limit_sort Possible values: `:ascending`, `:descending`
    # @option options [Symbol] :order_by Order field
    # @option options [Symbol] :order_direction Order direction
    # @option options [Symbol] :page Used for pagination of results.
    # @option options [Symbol] :page_size Size of page for paginated results
    # == Examples:
    # * aggregate(:amount, { :row_dimension => [:date], :row_levels => [:year, :month]} )
    # @return [AggregationResult] object with aggregation summary and rows where each
    #   row represents a point at row_dimension if specified.
    # @todo Rewrite this to use StarSchema - reuse code

    def aggregate(slice, measure, options = {})

        query = create_query(slice, options)
        query.measure = measure
        query.create_aggregation_statements(options)
        query.computed_fields = @computed_fields
    
        ################################################
    	# 7. Compute summary

        # Brewery::logger.debug "slice SQL: #{statement}"

        if !@summaries[measure]
            summary_data = query.aggregation_summary

            summary = Hash.new

            if options[:operators]
                aggregations = options[:operators]
            else
                aggregations = [:sum]
            end

            aggregations.each { |agg|
                field = query.aggregated_field_name(measure, agg).to_sym
                value = summary_data[field]
    
                # FIXME: use appropriate type (Sequel SQLite returns String)
                if value.class == String
                    value = value.to_f
                end
                summary[agg] = value
        	}
    	
            value = summary_data[:record_count]
            if value.class == String
                value = value.to_f
            end
            summary[:record_count] = value

        	@summaries[measure] = summary
        else
            summary = @summaries[measure]
        end

        ################################################
    	# 8. Execute main selection

        if query.is_drill_down
            result = query.aggregate_drill_down_rows
            query_record_count = result[:record_count]
            rows = result[:rows]
            r_sum = result[:sum]
        else
            # Only summary
            rows = Array.new
            query_record_count = 0
            r_sum = 0
        end
    
        # Compute remainder
    
        if query.has_limit
            remainder = Hash.new
            sumsum = summary[:sum] ? summary[:sum] : 0
            remainder[:sum] = sumsum - r_sum
            remainder[:record_count] = summary[:record_count] - query_record_count
        else
            remainder = nil
        end

        result = AggregationResult.new
        result.rows = rows
        result.aggregation_options = options
        result.measure = measure
        result.remainder = remainder
        result.summary = @summaries[measure]
    
        return result
    end

    def create_query(slice, options = {})
    	query = Cubes::CubeQuery.new(@cube, @connection, @view_name)

        ################################################
    	# 1. Apply cuts
	
    	slice.cuts.each { |cut|
    		if !cut.dimension
    		    raise RuntimeError, "No dimension in cut (#{cut.class}), slicing cube '#{@cube.name}'"
    		end

    		dimension = @cube.dimension(cut.dimension)
    		if !dimension
    		    raise RuntimeError, "No cut dimension '#{cut.dimension.name}' in cube '#{@cube.name}'"
    		end

    		# puts "==> WHERE COND CUT: #{cut.dimension} DIM: #{dimension} ALIAS: #{dim_alias}"
    		query.add_cut(cut)
    	}
    
        query.order_by = options[:order_by]
        query.order_direction = options[:order_direction]
        query.page = options[:page]
        query.page_size = options[:page_size]
    
        return query
    end

    def facts(slice, options = {})
    	query = create_query(slice, options)

        return query.records
    end

    def dimension_values_at_path(slice, dimension_ref, path, options = {})
        dimension = @cube.dimension(dimension_ref)
        query = create_query(slice, options)

        return query.dimension_values(dimension, path)
    end

end


class Slice
    include DataObjects::Quoting

    # List of cuts which define the slice - portion of a cube.
    attr_reader :cuts

    # @deprecated
    attr_reader :cut_values
    attr_reader :summaries

    # Initialize slice instance as part of a cube
    def initialize(browser)
        @browser = browser
        @cube = @browser.cube
        @cut_values = Hash.new
    
        @cuts = Array.new
    end

    # Copying contructor, called for Slice#dup
    def initialize_copy(*)
        @cut_values = @cut_values.dup
        @cuts = @cuts.dup
        @summaries = Hash.new
        # FIXME: is this the right behaviour?
        @computed_fields = nil
    end

    # Cut slice by provided cut
    # @see Cut#initialize
    # @param [Cut] cut to cut the slice by
    # @return [Slice] new slice with added cut
    def cut_by(cut)
    	slice = self.dup
    	slice.add_cut(cut)
    	return slice
    end

    # Cut slice by dimension point specified by path
    # @param [Array] path Dimension point specified by array of values. See {Dimension}
    # @see Cut#initialize
    # @return [Slice] new slice with added cut
    def cut_by_point(dimension, path)
    	return self.cut_by(Cut.point_cut(@cube.dimension(dimension), path))
    end

    # Cut slice by ordered dimension from point specified by dimension
    # keys in from_key to to_key
    # @return [Slice] new slice with added cut
    # @param [Array] path Dimension point specified by array of values. See {Dimension}
    def cut_by_range(dimension, from_key, to_key)
    	return self.cut_by(Cut.range_cut(@cube.dimension(dimension), from_key, to_key))
    end


    # Add another cut to the receiver.
    # @param [Cut] cut Cut to be added
    def add_cut(cut)
    	@cuts << cut
    	@summaries.clear
    end

    # Remove all cuts by dimension from the receiver.
    def remove_cuts_by_dimension(dimension)
    	@cuts.delete_if { |cut|
    		@cube.dimension(cut.dimension) == @cube.dimension(dimension)
    	}
    	@summaries.clear
    end

    # Find all cuts with dimension
    def cuts_for_dimension(dimension)
      @cuts.select { |cut|
    		@cube.dimension(cut.dimension) == @cube.dimension(dimension)
    	}
    end
    
    def dimension_values_at_path(dimension_ref, path, options = {})
        return @browser.dimension_values_at_path(self, dimension_ref, path, options = {})
    end

    def dimension_detail_at_path(dimension_ref, path)
        dimension = @cube.dimension(dimension_ref)
        query = create_query
        return query.dimension_detail_at_path(dimension, path)
    end
    def aggregate(measure, options = {})
        @browser.aggregate(self, measure, options)
    end
    
    def facts(options = {})
        return @browser.facts(self, options)
    end

    def add_computed_field(field_name, &block) 
        if !@computed_fields
            @computed_fields = Hash.new
        end
    
        @computed_fields[field_name] = block
    end

end # class Slice

class Cut
    attr_accessor :dimension
    attr_accessor :hierarchy

    def initialize(dimension = nil)
        @dimension = dimension
    end

    # Create a cut by a point within dimension.
    def self.point_cut(dimension, path)
        cut = PointCut.new(dimension)
        cut.path = path
        return cut
    end

    # Create a cut within a range defined by keys. Can be used for ordered dimensions,
    # such as date.
    def self.range_cut(dimension, from_key, to_key)
        cut = RangeCut.new(dimension)
        cut.from_key = from_key
        cut.to_key = to_key
        return cut
    end

    # Cut by a set of values
    def self.set_cut(dimension, path_set)
        cut = SetCut.new(dimension)
        cut.path_set = path_set
        return cut
    end

    # Return SQL condition for a cut
    # @private
    # @api private
    def sql_condition(dimension_alias)
        raise RuntimeError, "depreciated"
    end

    def dimension_name
        case dimension
        when String, Symbol
            return dimension
        else
            return dimension.name
        end
    end

end # class Cut

class PointCut < Cut
    include DataObjects::Quoting

    attr_accessor :path

    # @private
    def filter_dataset(dataset)
    	conditions = Array.new
    	level_index = 0

        #FIXME: use more
        hier = dimension.default_hierarchy

        if !hier
            raise RuntimeError, "Dimension has no hierarchy"
        end

    	path.each { |level_value|
    		if level_value != :all
    			level = hier.levels[level_index]
    			level_column = level.key
    			# quoted_value = quote_value(level_value)
    			dataset = dataset.filter([[level_column.to_sym,level_value]])
    		end
    		level_index = level_index + 1
    	}
	
    	return dataset
    end

end # class PointCut

class RangeCut < Cut
    attr_accessor :from_key
    attr_accessor :to_key
    # @api private
    def sql_condition(dimension, dimension_alias)
        dimension_key = dimension.key_field
        if !dimension_key
            dimension_key = :id
        end
        condition = "#{dimension_alias}.#{dimension_key} BETWEEN #{from_key} AND #{to_key}"	
    	return condition
    end
end # class RangeCut

class SetCut < Cut
    attr_accessor :path_set
end # class SetCut


end # module Cubes