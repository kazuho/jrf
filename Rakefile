# frozen_string_literal: true

require "rake/testtask"

Rake::TestTask.new do |t|
  t.libs << "test"
  t.test_files = FileList["test/**/*_test.rb"]
end

task default: :test

desc "Build man/jrf.1 from README.md"
task :man do
  ruby "script/build_man_from_readme.rb"
end
