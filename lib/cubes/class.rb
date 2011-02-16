# Class and Object additions
#
# Copyright:: (C) 2010 Stefan Urbanek
# 
# Author:: Stefan Urbanek
# Date:: May 2010
#

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


class Class
def self.class_exists?(name)
    name.split(/::/).inject(Object) do |left, right|
        begin
            left.const_get(right)
        rescue NameError
            break nil
        end
    end
end
def self.class_with_name(name)
    if class_exists?(name)
        return Kernel.const_get(name)
    else
        return nil
    end
end
def is_kind_of_class(a_class)
    current = self
    while current do
        if current == a_class
            return true
        end
        current = current.superclass
    end
    return false
end

end

class Object
def is_kind_of_class(a_class)
	return self.class.is_kind_of_class(a_class)
end
end