spec = Gem::Specification.new do |s| 
  s.name = "cubes"
  s.version = "0.1.0"
  s.author = "Stefan Urbanek"
  s.email = "stefan.urbanek@gmail.com"
  s.homepage = "http://github.com/Stiivi/cubes-ruby/"
  s.platform = Gem::Platform::RUBY
  s.summary = "Cubes-ruby - Light-weight ruby port of python OLAP framework Cubes"
  s.files = Dir['lib/**/*.rb']
  s.require_path = "lib"
  s.has_rdoc = true
  s.extra_rdoc_files = ["README"]
end
