require 'json'

class ModelError < Exception
    # """Model related exception."""
end

def load_model(resource)
    # """Load logical model from object reference. `ref` can be an URL, local file path or file-like
    # object."""
    # URL is not supported in ruby

    handle = nil
    
    case resource
    when String
        handle = File.new(resource, "r")
        should_close = true
    else
        handle = resource
    end

    data = handle.read()
    model_desc = JSON.parse(data)

    if should_close
        handle.close()
    end

    return model_from_dict(model_desc)
end

def model_from_dict(desc)
    """Create a model from description dictionary

    Arguments
        desc: model dictionary
    """

    model = Model.new(desc["name"], desc)
    return model
end

class Model
    attr_reader :name
    attr_reader :label
    attr_reader :description
    
    def initialize(name, desc = {})
    	@name = name
    	@label = desc['label']
    	@description = desc['description']

    	@_dimensions = {}

    	dimensions = desc['dimensions']
    	if dimensions
    	    dimensions.each { |dim_name, dim_desc|
    	        dim = Dimension.new(dim_name, dim_desc)
                add_dimension(dim)
            }
        end

    	@cubes = {}

    	cubes = desc['cubes']
    	if cubes
    	    cubes.each { |cube_name, cube_desc|
                create_cube(cube_name, cube_desc)
            }
    	end
    end

    def create_cube(cube_name, info ={})

        cube = Cube.new(cube_name, info)
        cube.model = self
        @cubes[cube_name] = cube

    	dims = info['dimensions']
    	
    	if dims
    	    dims.each {|dim_name|
                begin
    	            dim = dimension(dim_name)
    	        rescue
    	            raise "There is no dimension '#{dim_name}' for cube '#{cube_name}' in model '#{@name}'"
    	        end
                cube.add_dimension(dim)
            }
        end

        return cube
    end
    
    def cube(name)
        """Get a cube with name `name`."""
        return @cubes[name]
    end
    
    def add_dimension(dimension)
        """Add dimension to cube. Replace dimension with same name"""

        # FIXME: Do not allow to add dimension if one already exists
        @_dimensions[dimension.name] = dimension
    end
    def remove_dimension(dimension)
        """Remove a dimension from receiver"""
         @_dimensions.delete(dimension.name)
        # FIXME: check whether the dimension is not used in cubes
    end
    
    def dimensions()
        return @_dimensions.values()
    end

    def dimension(name)
        """Get dimension by name"""
        return @_dimensions[name]
    end
end
 
class Cube
    attr_reader :name
    attr_reader :label
    attr_reader :description
    attr_reader :measures
    attr_reader :attributes
    attr_accessor :model
    attr_reader :mappings
    attr_reader :fact
    attr_reader :joins
    
    def initialize(name, info = {})
        @name = name

        @label = info["label"]
        @description = info["description"]
        @measures = info["measures"]
        @attributes = info["attributes"]
        @model = nil
        @_dimensions = {}
        @mappings = info["mappings"]
        @fact = info["fact"]
        @joins = info["joins"]
    end
    
    def add_dimension(dimension)

        # FIXME: Do not allow to add dimension if one already exists
        @_dimensions[dimension.name] = dimension
        if @model
            @model.add_dimension(dimension)
        end
    end

    def remove_dimension(dimension)
        del @_dimensions[dimension.name]
    end
    
    def dimensions()
        return @_dimensions.values()
    end

    def dimension(name)
        case name
        when String
            dim = @_dimensions[name]
            if not dim
                raise "Invalid dimension reference '#{name}' for cube '#{@name}'"
            end
            return dim
        else
            return name
        end
        # else
        #     raise ModelError("Invalid dimension or dimension reference '%s' for cube '%s'" %
        #                             (name, @name))
    end
end

class Dimension
    attr_reader :name
    attr_reader :label
    attr_reader :level_names
    attr_reader :default_hierarchy_name
    
    def initialize(name, desc = {})

        @name = name

        @label = desc["label"]
        @description = desc["description"]

        @_levels = []
        @level_names = []

        __init_levels(desc["levels"])
        __init_hierarchies(desc["hierarchies"])
        @_flat_hierarchy = nil

        @default_hierarchy_name = desc["default_hierarchy"]
        @key_field = desc["key_field"]
    end
    # def ==(other)
    #     if not other or type(other) != type(self)
    #         return False
    #     if @name != other.name or @label != other.label \
    #         or @description != other.description
    #         return False
    #     elif @default_hierarchy != other.default_hierarchy
    #         return False
    # 
    #     levels = @levels
    #     for level in other.levels
    #         if level not in levels
    #             return False
    # 
    #     hierarchies = @hierarchies
    #     for hier in other.hierarchies
    #         if hier not in hierarchies
    #             return False
    # 
    #     return True
    # end
    # 
    # def __ne__(self, other)
    #     return not @__eq__(other)
    # 

    def __init_levels(desc)
        @_levels = {}

        if not desc
            return
        end

        desc.each { |level_name, level_info|
            level = Level.new(level_name, level_info)
            level.dimension = self
            @_levels[level_name] = level
            @level_names.push(level_name)
        }
    end

    def __init_hierarchies(desc)
        """booo bar"""
        @hierarchies = {}

        if not desc
            return
        end

        desc.each { |hier_name, hier_info|
            hier = Hierarchy.new(hier_name, hier_info)
            hier.dimension = self
            @hierarchies[hier_name] = hier
        }
    end

    def _initialize_default_flat_hierarchy()
        if not @_flat_hierarchy
            @_flat_hierarchy = flat_hierarchy(@levels[0])
        end
    end

    def levels
        """Get list of hierarchy levels (unordered)"""
        return @_levels.values()
    end
    
    def level(name)
        """Get level by name."""
        if not @_levels[name]
            raise "No level #{name} in dimension #{@name}"
        end
        return @_levels[name]
    end
    
    def default_hierarchy
        if @default_hierarchy_name
            hierarchy_name = @default_hierarchy_name
        else
            hierarchy_name = "default"
        end

        hierarchy = @hierarchies[hierarchy_name]

        if not hierarchy
            if @hierarchies.size == 1
                hierarchy = @hierarchies.values()[0]
            else
                if @hierarchies.size == 0
                    if levels.size == 1
                        _initialize_default_flat_hierarchy()
                        return @_flat_hierarchy
                    elsif levels.size > 1
                        raise "There are no hierarchies in dimenson #{@name} "
                                       "and there are more than one level"
                    else
                        raise "There are no hierarchies in dimenson #{@name} "
                                       "and there are no levels to make hierarchy from"
                    end
                else
                    raise "No default hierarchy specified in dimension '#{@name}' " \
                                   "and there is more (#{@hierarchies.size}) than one hierarchy defined"
                end
            end
        end
        return hierarchy
    end
    
    def flat_hierarchy(level)
        hier = Hierarchy.new(level.name)
        hier.level_names = [level.name]
        hier.dimension = self
        return hier
    end
    
    def is_flat()
        """Return true if dimension has only one level"""
        return @levels.size == 1
    end

    def all_attributes(hierarchy = nil)
        if not hierarchy
            hier = @default_hierarchy
        elsif type(hierarchy) == str
            hier = @hierarchies[hierarchy]
        else
            hier = hierarchy
        end

        attributes = []
        hier.levels.each {|level|
            attributes.extend(level.attributes)
        }
        return attributes
    end
end

class Hierarchy
    attr_reader :name
    attr_reader :label
    attr_reader :levels
    attr_accessor :level_names
    attr_accessor :dimension
    
    def initialize(name, info = {}, dimension = nil)
        @name = name
        @_dimension = nil
        @label = info["label"]
        @level_names = info["levels"]
        @dimension = dimension
        @levels = []
    end

    # def ==(other)
    #     if not other or type(other) != type(self)
    #         return False
    #     elif @name != other.name or @label != other.label
    #         return False
    #     elif @levels != other.levels
    #         return False
    #     # elif @_dimension != other._dimension
    #     #     return False
    #     return True
    # 
    # def __ne__(self, other)
    #     return not @__eq__(other)

    def dimension()
        return @_dimension
    end

    def dimension=(a_dimension)
        @_dimension = a_dimension
        @levels = []

        if a_dimension
            @level_names.each { |level_name|
                level = @_dimension.level(level_name)
                @levels.push(level)
            }
        end
    end

    def levels_for_path(path, drill_down = False)
        """Returns levels for given path. If path is longer than hierarchy levels, exception is raised"""
        if not path
            if drill_down
                return @levels[0..1]
            else
                return []
            end
        end
        
        if drill_down
            ext = 1
        else
            ext = 0
        end
        
        if path.size + ext > @levels.size
            raise "Path #{path} is longer than hierarchy levels #{@level_names}"
        end
        
        return @levels[0..(path.size+ext)]
    end

    def path_is_base(path)
        """Returns True if path is base path for the hierarchy. Base path is a path where there are
        no more levels to be added - no drill down possible."""
        
        return path.size == @levels.size
    end
end

class Level
    attr_reader :name
    attr_reader :label
    attr_reader :attributes
    attr_reader :level_attribute
    attr_accessor :dimension
    
    def initialize(name, desc, dimension = nil)
        @name = name
        @label = desc["label"]
        @_key = desc["key"]
        @attributes = desc["attributes"]
        @label_attribute = desc["label_attribute"]
        @dimension = dimension
    end

    # def ==(other)
    #     if not other or type(other) != type(self)
    #         return False
    #     elif @name != other.name or @label != other.label or @_key != other._key
    #         return False
    #     elif @label_attribute != other.label_attribute
    #         return False
    #     # elif @dimension != other.dimension
    #     #     return False
    # 
    #     if @attributes != other.attributes
    #         return False
    #         
    #     # for attr in other.attributes
    #     #     if attr not in @attributes
    #     #         return False
    # 
    #     return True
    # end
    # def __ne__(self, other)
    #     return not @__eq__(other)

    def key()
        if @_key
            return @_key
        else
            return @attributes[0]
        end
    end
end