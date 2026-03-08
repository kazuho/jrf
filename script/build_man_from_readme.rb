#!/usr/bin/env ruby
# frozen_string_literal: true

require "date"

ROOT = File.expand_path("..", __dir__)
README = File.join(ROOT, "README.md")
MAN_DIR = File.join(ROOT, "man")
MANPAGE = File.join(MAN_DIR, "jrf.1")
VERSION_FILE = File.join(ROOT, "lib/jrf/version.rb")

def load_version(path)
  src = File.read(path)
  src[/VERSION\s*=\s*"([^"]+)"/, 1] || "unknown"
end

def markdown_to_man(lines)
  out = []
  in_code = false

  lines.each do |line|
    stripped = line.rstrip

    if stripped.start_with?("```")
      if in_code
        out << ".fi"
        out << ""
      else
        out << ".nf"
      end
      in_code = !in_code
      next
    end

    if in_code
      out << stripped.gsub("\\", "\\\\")
      next
    end

    if stripped.start_with?("### ")
      out << ".SS \"#{stripped.sub("### ", "")}\""
      next
    end

    if stripped.start_with?("- ")
      out << ".IP \\[bu] 2"
      out << stripped.sub("- ", "").gsub("`", "")
      next
    end

    next if stripped.start_with?("# ")
    next if stripped.start_with?("## ")

    out << stripped.gsub("`", "")
  end

  out
end

def read_sections(path)
  sections = {}
  current = nil

  File.readlines(path, chomp: true).each do |line|
    if (m = line.match(/^##\s+(.+)$/))
      current = m[1].strip
      sections[current] = []
      next
    end

    sections[current] << line if current
  end

  sections
end

sections = read_sections(README)
version = load_version(VERSION_FILE)
date = Date.today.strftime("%Y-%m-%d")

ordered_sections = [
  "SYNOPSIS",
  "WHY RUBY?",
  "INPUT AND OUTPUT",
  "BUILT-IN FUNCTIONS",
  "LICENSE"
]

man = []
man << ".TH JRF 1 \"#{date}\" \"jrf #{version}\" \"User Commands\""
man << ".SH NAME"
man << "jrf \\- JSON filter with the power and speed of Ruby"

ordered_sections.each do |title|
  next unless sections.key?(title)

  man << ".SH #{title.gsub("?", "")}"
  man.concat(markdown_to_man(sections[title]))
end

Dir.mkdir(MAN_DIR) unless Dir.exist?(MAN_DIR)
File.write(MANPAGE, man.join("\n") + "\n")
puts "wrote #{MANPAGE}"
