# frozen_string_literal: true

require_relative "lib/jr/version"

Gem::Specification.new do |spec|
  spec.name = "jrf"
  spec.version = Jr::VERSION
  spec.authors = ["kazuho"]
  spec.email = ["n/a@example.com"]

  spec.summary = "Small NDJSON transformer with Ruby expressions"
  spec.description = "A small, lightweight NDJSON transformer with Ruby-like expressions."
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.0"

  spec.bindir = "exe"
  spec.executables = ["jrf"]

  spec.files = Dir.glob("{exe,lib,test}/*") + Dir.glob("lib/**/*") + %w[DESIGN.txt jrf.gemspec Gemfile Rakefile]
end
