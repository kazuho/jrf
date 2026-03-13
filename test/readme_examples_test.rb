# frozen_string_literal: true

require_relative "test_helper"

class ReadmeExamplesTest < Minitest::Test
  def test_built_in_function_examples
    readme_examples = extract_readme_examples("./README.md", section: "BUILT-IN FUNCTIONS")
    refute_empty(readme_examples, "expected README built-in examples")

    readme_examples.each do |example|
      stdout, stderr, status = run_jrf(example[:expr], example[:input])
      assert_success(status, stderr, "README example #{example[:expr]}")
      assert_equal(example[:output], lines(stdout), "README example output #{example[:expr]}")
    end
  end
end
