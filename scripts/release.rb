#!/usr/bin/env ruby
# frozen_string_literal: true
#
# Cuts a GitLab release for a tag pipeline. Reads the matching
# CHANGELOG block, attaches every artifact under target/dist/ as a
# release asset link, and shells out to release-cli. Invoked from
# .gitlab-ci.yml's `release` job.
#
# Local dry-run (without actually pushing the release):
#   CI_COMMIT_TAG=v0.13.0 \
#   CI_PROJECT_URL=https://gitlab.com/rustutils/btrfsutils \
#   ruby scripts/release.rb --print

require 'json'
require_relative 'lib/release_common'

DIST_DIR = 'target/dist'

print_only = ARGV.include?('--print')

tag, version, prerelease = ReleaseCommon.read_tag
project_url = ENV.fetch('CI_PROJECT_URL')

ReleaseCommon.check_cargo_version!(version)
notes = ReleaseCommon.release_notes(version, prerelease)

# Asset URLs use the per-ref artifact endpoint, which resolves to the
# latest job artifact for that tag — stable as long as the package
# job artifacts do not expire.
asset_args = Dir["#{DIST_DIR}/*"].sort.flat_map do |path|
  name = File.basename(path)
  url  = "#{project_url}/-/jobs/artifacts/#{tag}/raw/#{path}?job=package"
  ['--assets-link', JSON.generate({ name: name, url: url })]
end

cmd = [
  'release-cli', 'create',
  '--name', "Release #{tag}",
  '--tag-name', tag,
  '--description', notes,
  *asset_args,
]

if print_only
  puts cmd.inspect
else
  exec(*cmd)
end
